"""
Mega.nz → Telegram Bot
======================
High-performance Pyrogram v2 bot that downloads Mega.nz files/folders
and uploads them to Telegram with real-time progress, queue management,
and storage-optimized sequential processing.

Uses mega.py for single-file downloads and custom Mega API handler
for folder downloads with AES decryption.
"""

import os
import re
import math
import time
import shutil
import random
import asyncio
import logging
from pathlib import Path
from typing import Optional
import subprocess

from dotenv import load_dotenv
load_dotenv()
from flask import Flask
from threading import Thread

# ── Python 3.12+ compatibility shim for mega.py ──
if not hasattr(asyncio, "coroutine"):
    asyncio.coroutine = lambda func: func

import cv2
import requests
import aiofiles
import psutil
from mega import Mega
from mega.crypto import (
    a32_to_str, str_to_a32, base64_url_decode,
    decrypt_attr, decrypt_key,
)
from Crypto.Cipher import AES
from Crypto.Util import Counter
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait, BadRequest, PeerIdInvalid, ChannelInvalid

# ──────────────────────────────────────────────────────────────
#  CONFIGURATION
# ──────────────────────────────────────────────────────────────

APP_ID = int(os.environ["APP_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]

DOWNLOAD_DIR = Path("downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)

CHUNK_SIZE = 2 * 1024 * 1024 * 1024 - 10 * 1024 * 1024   # ~1.99 GB
SPLIT_READ_BUF = 8 * 1024 * 1024          # 8 MB I/O buffer
PROGRESS_EDIT_INTERVAL = 3                 # seconds between edits
MAX_CONCURRENT_USERS = 5

# ── Proxy Configuration ──
# Using the GitHub openproxylist repository to guarantee a large pool of SOCKS5 IPs
PROXY_API_URL = "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5_RAW.txt"
CACHED_PROXIES = []

MEGA_LINK_RE = re.compile(
    r"https?://mega\.nz/(?:file|folder|#|#!|#F!)[^\s]+"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
log = logging.getLogger("mega-bot")

app = Client("mega_bot", api_id=APP_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# ──────────────────────────────────────────────────────────────
#  GLOBAL STATE
# ──────────────────────────────────────────────────────────────

user_queues:  dict[int, asyncio.Queue] = {}
user_workers: dict[int, asyncio.Task]  = {}
user_cancel:  dict[int, asyncio.Event] = {}
user_skip:    dict[int, asyncio.Event] = {}   # skip current file
user_status:  dict[int, str]           = {}
global_semaphore = asyncio.Semaphore(MAX_CONCURRENT_USERS)

# Conversation state: user_id → (msg, link, chat_id, folder_id, file_infos)
folder_pending: dict[int, tuple] = {}

# Destination state pending: user_id → JobDict (used to hold job details until a destination is selected)
destination_pending: dict[int, dict] = {}

# Channel ID state pending: user_id → JobDict
channel_id_pending: dict[int, dict] = {}


class BandwidthLimitError(Exception):
    """Raised when Mega returns 509 Bandwidth Limit Exceeded."""
    pass

def load_proxies():
    """Fetches the latest SOCKS5 proxies from GitHub."""
    global CACHED_PROXIES
    try:
        log.info("Fetching latest SOCKS5 proxies from GitHub...")
        resp = requests.get(PROXY_API_URL, timeout=10)
        resp.raise_for_status()
        
        # Split by \n to handle GitHub's text formatting safely
        raw_ips = resp.text.strip().split('\n') 
        
        # Format them using socks5h:// (Forces DNS resolution on the proxy side)
        CACHED_PROXIES = [
            {"http": f"socks5h://{ip.strip()}", "https": f"socks5h://{ip.strip()}"} 
            for ip in raw_ips if ip.strip()
        ]
        log.info(f"✅ Successfully loaded {len(CACHED_PROXIES)} SOCKS5 proxies.")
    except Exception as e:
        log.warning(f"Failed to fetch proxies: {e}")

# Load the proxies immediately when the bot starts
load_proxies()

def _get_session(use_proxy=False) -> requests.Session:
    """Return a standard requests Session, or a proxied one if explicitly requested."""
    s = requests.Session()
    
    if use_proxy:
        if not CACHED_PROXIES:
            load_proxies()
        if CACHED_PROXIES:
            current_proxy = random.choice(CACHED_PROXIES)
            s.proxies = current_proxy
            log.info(f"🛡️ 509 Fallback: Using proxy {current_proxy['http']}")
            
    return s


# ──────────────────────────────────────────────────────────────
#  UTILITY HELPERS
# ──────────────────────────────────────────────────────────────

def human_size(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def progress_bar(pct: float, length: int = 14) -> str:
    filled = int(length * pct / 100)
    return "▓" * filled + "░" * (length - filled)


async def safe_edit(msg: Message, text: str) -> None:
    try:
        await msg.edit_text(text, disable_web_page_preview=True)
    except FloodWait as e:
        await asyncio.sleep(e.value)
    except BadRequest:
        pass


async def cleanup_path(path: Path) -> None:
    try:
        if path.is_dir():
            await asyncio.to_thread(shutil.rmtree, path, ignore_errors=True)
        elif path.is_file():
            path.unlink(missing_ok=True)
    except Exception as exc:
        log.warning("Cleanup failed for %s: %s", path, exc)


# ──────────────────────────────────────────────────────────────
#  URL HELPERS
# ──────────────────────────────────────────────────────────────

def is_mega_folder(url: str) -> bool:
    """Check if a Mega URL points to a folder."""
    return "/folder/" in url or "/#F!" in url


def parse_mega_folder_url(url: str) -> tuple[str, str]:
    """Extract (folder_id, folder_key_b64) from a folder URL."""
    # New format: mega.nz/folder/ID#KEY
    m = re.match(r"https?://mega\.nz/folder/([^#]+)#(.+)", url)
    if m:
        return m.group(1), m.group(2)
    # Old format: mega.nz/#F!ID!KEY
    m = re.match(r"https?://mega\.nz/#F!([^!]+)!(.+)", url)
    if m:
        return m.group(1), m.group(2)
    raise ValueError(f"Cannot parse Mega folder URL: {url}")


def convert_mega_file_url(url: str) -> str:
    """Convert new-format file URL to old format for mega.py compatibility."""
    # mega.nz/file/ID#KEY → mega.nz/#!ID!KEY
    m = re.match(r"https?://mega\.nz/file/([^#]+)#(.+)", url)
    if m:
        return f"https://mega.nz/#!{m.group(1)}!{m.group(2)}"
    return url  # already old format or something else


# ──────────────────────────────────────────────────────────────
#  MEGA SINGLE-FILE DOWNLOADER  (via mega.py)
# ──────────────────────────────────────────────────────────────

def _mega_download_file_sync(url: str, dest_dir: str) -> str:
    """Download a single Mega file. Returns the local file path."""
    converted_url = convert_mega_file_url(url)
    m = Mega()
    api = m.login()
    downloaded = api.download_url(converted_url, dest_path=dest_dir)
    return str(downloaded)


# ──────────────────────────────────────────────────────────────
#  MEGA FOLDER — METADATA ONLY (no downloads)
# ──────────────────────────────────────────────────────────────

def _mega_get_folder_info_sync(url: str) -> tuple[str, list[dict]]:
    """
    Fetch folder metadata from Mega API.
    Returns (folder_id, file_info_list) where each file_info dict contains:
      - handle, filename, file_size, aes_key (a32), iv (a32)
    NO files are downloaded — only metadata.
    """
    folder_id, folder_key_b64 = parse_mega_folder_url(url)
    folder_key_bytes = base64_url_decode(folder_key_b64)
    folder_key = str_to_a32(folder_key_bytes)

    # Get folder listing with retry
    # Get folder listing (STRICTLY DIRECT, NO PROXY)
    data = None
    MAX_RETRIES = 3
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            sess = _get_session(use_proxy=False) # <--- ALWAYS DIRECT
            resp = sess.post(
                "https://g.api.mega.co.nz/cs",
                params={"id": 0, "n": folder_id},
                json=[{"a": "f", "c": 1, "r": 1}],
                timeout=(10, 30),
            )
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, int) and data < 0:
                raise RuntimeError(f"Mega API error code: {data}")
            data = data[0]
            break
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            log.warning("Folder listing attempt %d/3 failed: %s", attempt, e)
            if attempt == 3:
                raise RuntimeError(f"Failed to fetch folder after 3 attempts: {e}") from e
            import time as _time
            _time.sleep(3 * attempt)

    nodes = data.get("f", [])
    file_nodes = [n for n in nodes if n.get("t") == 0]

    if not file_nodes:
        raise RuntimeError("No files found in folder")

    file_infos: list[dict] = []
    for node in file_nodes:
        try:
            key_parts = node["k"].split(":")
            encrypted_key_b64 = None
            for part in key_parts:
                if "/" in part:
                    encrypted_key_b64 = part.split("/")[-1]
                else:
                    encrypted_key_b64 = part
            if not encrypted_key_b64:
                continue

            encrypted_key_a32 = str_to_a32(base64_url_decode(encrypted_key_b64))
            decrypted_key_a32 = decrypt_key(encrypted_key_a32, folder_key)

            k = (
                decrypted_key_a32[0] ^ decrypted_key_a32[4],
                decrypted_key_a32[1] ^ decrypted_key_a32[5],
                decrypted_key_a32[2] ^ decrypted_key_a32[6],
                decrypted_key_a32[3] ^ decrypted_key_a32[7],
            )
            iv = decrypted_key_a32[4:6] + (0, 0)

            attr = decrypt_attr(base64_url_decode(node["a"]), k)
            if not attr:
                continue

            file_infos.append({
                "handle":    node["h"],
                "filename":  attr.get("n", f"file_{node['h']}"),
                "file_size": node.get("s", 0),
                "aes_key":   k,
                "iv":        iv,
            })
        except Exception as exc:
            log.warning("Skipping node %s: %s", node.get("h"), exc)

    return folder_id, file_infos


# ──────────────────────────────────────────────────────────────
#  MEGA FOLDER — DOWNLOAD ONE FILE (by node info)
# ──────────────────────────────────────────────────────────────

def _mega_download_one_file_sync(
    folder_id: str, file_info: dict, dest_dir: str, use_proxy: bool = False
) -> str:
    """
    Download a SINGLE file from a Mega folder using its pre-decrypted keys.
    Returns local file path.
    """
    handle   = file_info["handle"]
    filename = file_info["filename"]
    k        = file_info["aes_key"]
    iv       = file_info["iv"]

    # Get download URL (through WARP proxy if enabled)
    sess = _get_session(use_proxy=use_proxy)
    dl_resp = sess.post(
        "https://g.api.mega.co.nz/cs",
        params={"id": 1, "n": folder_id},
        json=[{"a": "g", "g": 1, "n": handle}],
        timeout=(10, 120),
    )
    if dl_resp.status_code == 509:
        raise BandwidthLimitError(f"Mega bandwidth limit exceeded for {filename}")
    dl_resp.raise_for_status()
    dl_data = dl_resp.json()[0]

    if isinstance(dl_data, int) and dl_data < 0:
        if dl_data == -509:
            raise BandwidthLimitError(f"Mega bandwidth limit exceeded for {filename}")
        raise RuntimeError(f"Mega API error {dl_data} for {filename}")

    dl_url    = dl_data["g"]
    file_size = dl_data["s"]

    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)
    file_path = dest / filename

    # AES-CTR decrypt
    initial_value = ((iv[0] << 32) + iv[1]) << 64
    counter = Counter.new(128, initial_value=initial_value)
    aes = AES.new(a32_to_str(k), AES.MODE_CTR, counter=counter)

    with sess.get(dl_url, stream=True, timeout=(10, None)) as r:
        if r.status_code == 509:
            raise BandwidthLimitError(
                f"Mega bandwidth limit exceeded for {filename}"
            )
        r.raise_for_status()
        with open(file_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(aes.decrypt(chunk))

    # Trim AES padding
    if file_path.stat().st_size > file_size:
        with open(file_path, "r+b") as f:
            f.truncate(file_size)

    log.info("Downloaded %s (%s)", filename, human_size(file_size))
    return str(file_path)


# ──────────────────────────────────────────────────────────────
#  FILE SPLITTER (2 GB chunks)
# ──────────────────────────────────────────────────────────────

async def split_file(filepath: Path, cancel_event: asyncio.Event) -> list[Path]:
    file_size = filepath.stat().st_size
    if file_size <= CHUNK_SIZE:
        return [filepath]

    parts: list[Path] = []
    num_parts = math.ceil(file_size / CHUNK_SIZE)
    digits = len(str(num_parts))

    async with aiofiles.open(filepath, "rb") as src:
        for idx in range(1, num_parts + 1):
            if cancel_event.is_set():
                for p in parts:
                    await cleanup_path(p)
                return []

            part_path = filepath.parent / f"{filepath.name}.part{str(idx).zfill(digits)}"
            written = 0
            async with aiofiles.open(part_path, "wb") as dst:
                while written < CHUNK_SIZE:
                    to_read = min(SPLIT_READ_BUF, CHUNK_SIZE - written)
                    chunk = await src.read(to_read)
                    if not chunk:
                        break
                    await dst.write(chunk)
                    written += len(chunk)
            parts.append(part_path)
            log.info("Split part %d/%d: %s (%s)", idx, num_parts, part_path.name, human_size(written))

    await cleanup_path(filepath)
    return parts


# ──────────────────────────────────────────────────────────────
#  PROGRESS TRACKER  (for Telegram uploads)
# ──────────────────────────────────────────────────────────────

class ProgressTracker:
    def __init__(self, status_msg: Message, label: str):
        self.msg = status_msg
        self.label = label
        self._last_edit = 0.0
        self._start = time.time()

    async def update(self, current: int, total: int) -> None:
        now = time.time()
        if now - self._last_edit < PROGRESS_EDIT_INTERVAL:
            return
        self._last_edit = now

        elapsed = max(now - self._start, 0.1)
        pct = current / total * 100 if total else 0
        speed = current / elapsed
        eta = (total - current) / speed if speed > 0 else 0

        text = (
            f"**{self.label}**\n\n"
            f"`[{progress_bar(pct)}]` {pct:.1f}%\n"
            f"📦 {human_size(current)} / {human_size(total)}\n"
            f"⚡ {human_size(speed)}/s  •  ⏱ ETA {int(eta)}s"
        )
        await safe_edit(self.msg, text)

    def pyrogram_progress(self):
        async def _cb(current: int, total: int):
            await self.update(current, total)
        return _cb


# ──────────────────────────────────────────────────────────────
#  UPLOAD HELPER
# ──────────────────────────────────────────────────────────────

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".bmp", ".webp"}
VIDEO_EXTENSIONS = {".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".wmv", ".3gp"}


def _detect_file_type(filename: str) -> str:
    """Return 'photo', 'video', or 'document' based on file extension."""
    ext = Path(filename).suffix.lower()
    if ext in IMAGE_EXTENSIONS:
        return "photo"
    if ext in VIDEO_EXTENSIONS:
        return "video"
    return "document"


def _generate_video_thumb(video_path: str) -> tuple[Optional[str], int, int, int]:
    """
    Extract a thumbnail and metadata from a video using OpenCV.
    Returns (thumb_path or None, duration_seconds, width, height).
    """
    thumb_path = None
    duration = 0
    width = 0
    height = 0
    try:
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            return None, 0, 0, 0

        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        fps = cap.get(cv2.CAP_PROP_FPS) or 30
        frame_count = cap.get(cv2.CAP_PROP_FRAME_COUNT)
        duration = int(frame_count / fps) if fps > 0 else 0

        # Seek to 1 second or 10% of the video, whichever is smaller
        target_frame = min(int(fps), int(frame_count * 0.1)) if frame_count > 0 else 0
        if target_frame > 0:
            cap.set(cv2.CAP_PROP_POS_FRAMES, target_frame)

        ret, frame = cap.read()
        cap.release()

        if ret and frame is not None:
            # Resize thumbnail to max 320px on longest side
            h, w = frame.shape[:2]
            scale = min(320 / max(w, h), 1.0)
            if scale < 1.0:
                frame = cv2.resize(frame, (int(w * scale), int(h * scale)))

            thumb_path = video_path + "_thumb.jpg"
            cv2.imwrite(thumb_path, frame, [cv2.IMWRITE_JPEG_QUALITY, 85])
    except Exception as exc:
        log.warning("Thumbnail generation failed for %s: %s", video_path, exc)

    return thumb_path, duration, width, height



async def _upload_file(
    user_id: int,
    target_chat_id: int,  # Added target_chat_id
    filepath: Path,
    original_name: str,
    file_idx: int,
    total_files: int,
    status_msg: Message,
    cancel_event: asyncio.Event,
) -> bool:
    """Split if needed, upload parts, delete after each. Returns True on success."""
    parts = await split_file(filepath, cancel_event)
    if cancel_event.is_set() or not parts:
        return False

    total_parts = len(parts)
    for part_idx, part in enumerate(parts, 1):
        if cancel_event.is_set():
            return False

        if total_files > 1 and total_parts > 1:
            label = f"⬆️ File {file_idx}/{total_files} — Part {part_idx}/{total_parts}"
        elif total_files > 1:
            label = f"⬆️ Uploading [{file_idx}/{total_files}]: `{original_name}`"
        elif total_parts > 1:
            label = f"⬆️ Uploading part {part_idx}/{total_parts}"
        else:
            label = f"⬆️ Uploading `{original_name}`"

        tracker = ProgressTracker(status_msg, label)
        send_name = part.name if total_parts > 1 else original_name
        caption = (
            f"📁 `{original_name}` — Part {part_idx}/{total_parts}"
            if total_parts > 1
            else f"📁 `{original_name}`"
        )
        if total_files > 1:
            caption = f"[{file_idx}/{total_files}] {caption}"

        # Determine how to send: only use photo/video for non-split files
        file_type = _detect_file_type(original_name) if total_parts == 1 else "document"

        try:
            if file_type == "photo":
                await app.send_photo(
                    chat_id=target_chat_id,
                    photo=str(part),
                    caption=caption,
                    progress=tracker.pyrogram_progress(),
                )
            elif file_type == "video":
                thumb_path, vid_duration, vid_w, vid_h = await asyncio.to_thread(
                    _generate_video_thumb, str(part)
                )
                try:
                    await app.send_video(
                        chat_id=target_chat_id,
                        video=str(part),
                        thumb=thumb_path,
                        duration=vid_duration,
                        width=vid_w,
                        height=vid_h,
                        caption=caption,
                        supports_streaming=True,
                        progress=tracker.pyrogram_progress(),
                    )
                finally:
                    if thumb_path and Path(thumb_path).exists():
                        Path(thumb_path).unlink(missing_ok=True)
            else:
                await app.send_document(
                    chat_id=target_chat_id,
                    document=str(part),
                    file_name=send_name,
                    caption=caption,
                    progress=tracker.pyrogram_progress(),
                )
        except Exception as upload_err:
            await safe_edit(status_msg, f"❌ Upload failed for `{send_name}`: `{upload_err}`")
            return False
        finally:
            await cleanup_path(part)

    return True


# ──────────────────────────────────────────────────────────────
#  CORE PROCESSING PIPELINE
# ──────────────────────────────────────────────────────────────

async def process_folder(
    user_id: int,
    target_chat_id: int,
    folder_id: str,
    file_infos: list[dict],
    start_from: int,
    status_msg: Message,
    cancel_event: asyncio.Event,
) -> None:
    """
    Process a Mega folder: download → upload → delete, one file at a time.
    Starts from the given 1-based index for resumability.
    """
    user_dir = DOWNLOAD_DIR / str(user_id)
    user_dir.mkdir(parents=True, exist_ok=True)
    total_files = len(file_infos)
    succeeded = 0
    failed = 0

    try:
        for file_idx in range(start_from, total_files + 1):
            if cancel_event.is_set():
                return

            finfo = file_infos[file_idx - 1]  # 0-based list
            fname = finfo["filename"]
            fsize = finfo["file_size"]

            # ── Periodic progress every 100 files ──
            if (file_idx - start_from) > 0 and (file_idx - start_from) % 100 == 0:
                disk_path = "/" if os.name != "nt" else "C:\\"
                disk = psutil.disk_usage(disk_path)
                await safe_edit(
                    status_msg,
                    f"📊 **Progress checkpoint**\n\n"
                    f"✅ Processed: {succeeded} files\n"
                    f"⚠️ Failed: {failed} files\n"
                    f"📍 Current: {file_idx}/{total_files}\n"
                    f"💾 Disk free: {human_size(disk.free)}"
                )
                await asyncio.sleep(2)

            await safe_edit(
                status_msg,
                f"⬇️ **[{file_idx}/{total_files}]** Downloading `{fname}` ({human_size(fsize)})…"
            )

            # ── Download with retry (3 attempts + bandwidth wait) ──
            # ── Download with Smart Proxy Fallback ──
            filepath = None
            MAX_DL_RETRIES = 15
            force_proxy = False  # Start direct!
            
            for attempt in range(1, MAX_DL_RETRIES + 1):
                if cancel_event.is_set():
                    return

                # Check if user wants to skip this file
                skip_event = user_skip.get(user_id)
                if skip_event and skip_event.is_set():
                    skip_event.clear()
                    await safe_edit(status_msg, f"⏭️ Skipped `{fname}`. Continuing…")
                    await asyncio.sleep(1)
                    break

                try:
                    # Pass the force_proxy flag to the download function
                    result = await asyncio.to_thread(
                        _mega_download_one_file_sync, folder_id, finfo, str(user_dir), force_proxy
                    )
                    filepath = Path(result)
                    break  # success!

                except BandwidthLimitError:
                    # ── Mega 509: Enable Proxy Mode ──
                    force_proxy = True  # All future attempts for this file will use proxies
                    
                    if CACHED_PROXIES:
                        log.warning("Bandwidth limit on %s. Enabling Proxy Fallback…", fname)
                        await safe_edit(
                            status_msg,
                            f"🔄 **Mega Quota Hit!** Activating SOCKS5 Proxy evasion…\n"
                            f"📍 File: `{fname}`"
                        )
                        await asyncio.sleep(1)
                        continue
                        # Because of how the loop works, 'continue' will automatically call _get_proxied_session() again, which grabs a random new proxy!

                    # ── Fallback: wait 10 minutes ──
                    wait_minutes = 10
                    wait_seconds = wait_minutes * 60
                    log.warning(
                        "Bandwidth limit hit at file %d/%d (%s). "
                        "Waiting %d minutes…",
                        file_idx, total_files, fname, wait_minutes,
                    )
                    await safe_edit(
                        status_msg,
                        f"⏸️ **Mega bandwidth limit reached!**\n\n"
                        f"📍 Paused at file **{file_idx}/{total_files}**: `{fname}`\n"
                        f"⏱️ Waiting **{wait_minutes} minutes** for quota reset…\n"
                        f"✅ {succeeded} done | ❌ {failed} failed\n\n"
                        f"💡 Send `/cancel` to stop, or just wait."
                    )
                    for remaining in range(wait_seconds, 0, -60):
                        if cancel_event.is_set():
                            return
                        mins_left = remaining // 60
                        await safe_edit(
                            status_msg,
                            f"⏸️ **Mega bandwidth limit — waiting…**\n\n"
                            f"⏱️ Resuming in **{mins_left} min**\n"
                            f"📍 Will retry file **{file_idx}/{total_files}**: `{fname}`\n\n"
                            f"💡 Send `/cancel` to stop."
                        )
                        await asyncio.sleep(60)
                    continue

                except (requests.exceptions.Timeout,
                        requests.exceptions.ConnectionError,
                        ConnectionResetError) as dl_err:
                    log.warning("Download attempt %d/3 failed for %s: %s", attempt, fname, dl_err)
                    if attempt < 3:
                        wait_time = 5 * attempt
                        await safe_edit(
                            status_msg,
                            f"⚠️ **[{file_idx}/{total_files}]** Retry {attempt}/3 for `{fname}` in {wait_time}s…"
                        )
                        await asyncio.sleep(wait_time)
                    else:
                        failed += 1
                        await safe_edit(
                            status_msg,
                            f"❌ Failed `{fname}` after 3 attempts: `{dl_err}`\nSkipping…"
                        )
                        await asyncio.sleep(2)

                except requests.exceptions.HTTPError as dl_err:
                    # Catch other HTTP errors (not 509) as non-retryable
                    log.warning("HTTP error for %s: %s", fname, dl_err)
                    failed += 1
                    await safe_edit(
                        status_msg,
                        f"⚠️ Skipped `{fname}`: `{dl_err}`\nContinuing…"
                    )
                    await asyncio.sleep(2)
                    break

                except Exception as dl_err:
                    log.warning("Skipping %s: %s", fname, dl_err)
                    failed += 1
                    await safe_edit(
                        status_msg,
                        f"⚠️ Skipped `{fname}`: `{dl_err}`\nContinuing…"
                    )
                    await asyncio.sleep(2)
                    break  # non-retryable error

            if filepath is None or not filepath.exists():
                continue  # skip to next file

            if cancel_event.is_set():
                await cleanup_path(filepath)
                return

            # Upload → delete
            ok = await _upload_file(
                user_id, target_chat_id, filepath, fname, file_idx, total_files,
                status_msg, cancel_event,
            )
            await cleanup_path(filepath)
            if ok:
                succeeded += 1
            else:
                failed += 1
                if cancel_event.is_set():
                    return

            # Small delay to avoid Mega API rate-limiting
            await asyncio.sleep(0.5)

        if not cancel_event.is_set():
            await safe_edit(
                status_msg,
                f"✅ **Done!** All files processed.\n\n"
                f"📊 Succeeded: {succeeded} | Failed: {failed} | Total: {total_files}"
            )

    except Exception as exc:
        log.exception("Error processing folder for user %d", user_id)
        await safe_edit(
            status_msg,
            f"❌ **Error at file {file_idx}/{total_files}:** `{exc}`\n\n"
            f"💡 Resume with `/start` and send the folder link again,"
            f" then enter **{file_idx}** to continue from where it stopped."
        )
    finally:
        await cleanup_path(user_dir)


async def process_single_file(
    user_id: int,
    target_chat_id: int,
    link: str,
    status_msg: Message,
    cancel_event: asyncio.Event,
) -> None:
    """Download and upload a single Mega file."""
    user_dir = DOWNLOAD_DIR / str(user_id)
    user_dir.mkdir(parents=True, exist_ok=True)

    try:
        await safe_edit(status_msg, "⬇️ **Downloading file from Mega…**")
        result = await asyncio.to_thread(
            _mega_download_file_sync, link, str(user_dir),
        )
        filepath = Path(result)

        if cancel_event.is_set():
            return

        original_name = filepath.name
        file_size = filepath.stat().st_size
        log.info("Downloaded: %s (%s)", original_name, human_size(file_size))

        ok = await _upload_file(
            user_id, target_chat_id, filepath, original_name, 1, 1,
            status_msg, cancel_event,
        )
        if ok and not cancel_event.is_set():
            await safe_edit(status_msg, "✅ **Done!** File sent to your DM.")

    except Exception as exc:
        log.exception("Error processing link %s for user %d", link, user_id)
        await safe_edit(status_msg, f"❌ **Error:** `{exc}`")
    finally:
        await cleanup_path(user_dir)


# ──────────────────────────────────────────────────────────────
#  USER QUEUE SYSTEM
# ──────────────────────────────────────────────────────────────

async def user_worker(user_id: int) -> None:
    q = user_queues[user_id]
    while True:
        job = await q.get()
        cancel_event = asyncio.Event()
        skip_event = asyncio.Event()
        user_cancel[user_id] = cancel_event
        user_skip[user_id] = skip_event

        try:
            if job["type"] == "folder":
                folder_id  = job["folder_id"]
                file_infos = job["file_infos"]
                start_from = job["start_from"]
                link       = job["link"]
                total      = len(file_infos)
                user_status[user_id] = f"Folder ({start_from}-{total}): {link[:50]}…"
                status_msg = await app.send_message(
                    user_id,
                    f"⏳ **Starting folder download…**\n"
                    f"📂 {total} files, starting from #{start_from}"
                )
                target_chat_id = job.get("target_chat_id", user_id)
                async with global_semaphore:
                    await process_folder(
                        user_id, target_chat_id, folder_id, file_infos,
                        start_from, status_msg, cancel_event,
                    )
            else:
                link = job["link"]
                user_status[user_id] = f"File: {link[:60]}…"
                status_msg = await app.send_message(user_id, "⏳ **Starting…**")
                target_chat_id = job.get("target_chat_id", user_id)
                async with global_semaphore:
                    await process_single_file(
                        user_id, target_chat_id, link, status_msg, cancel_event,
                    )
        except Exception as exc:
            log.exception("Worker error for user %d: %s", user_id, exc)
        finally:
            user_status.pop(user_id, None)
            user_cancel.pop(user_id, None)
            user_skip.pop(user_id, None)
            q.task_done()

        if q.empty():
            user_workers.pop(user_id, None)
            user_queues.pop(user_id, None)
            break


def ensure_worker(user_id: int) -> asyncio.Queue:
    if user_id not in user_queues:
        user_queues[user_id] = asyncio.Queue()
    if user_id not in user_workers or user_workers[user_id].done():
        user_workers[user_id] = asyncio.create_task(user_worker(user_id))
    return user_queues[user_id]


# ──────────────────────────────────────────────────────────────
#  COMMAND HANDLERS
# ──────────────────────────────────────────────────────────────

@app.on_message(filters.command("start") & filters.private)
async def cmd_start(_, msg: Message):
    await msg.reply(
        "👋 **Welcome to Mega Downloader Bot!**\n\n"
        "Send me any **Mega.nz** file or folder link and I'll download it "
        "and send the files right here.\n\n"
        "📂 **For folders:** I'll ask which file number to start from, "
        "so you can resume interrupted downloads!\n\n"
        "**Commands:**\n"
        "• `/cancel` — Stop current download/upload\n"
        "• `/skip` — Skip the current file and move to next\n"
        "• `/status` — Server & queue info\n\n"
        "You can also use me in **groups** — I'll send files to your DM!",
        disable_web_page_preview=True,
    )


@app.on_message(filters.command("cancel"))
async def cmd_cancel(_, msg: Message):
    uid = msg.from_user.id
    # Also clear any pending folder prompt
    folder_pending.pop(uid, None)
    event = user_cancel.get(uid)
    if event:
        event.set()
        q = user_queues.get(uid)
        if q:
            while not q.empty():
                try:
                    q.get_nowait()
                    q.task_done()
                except asyncio.QueueEmpty:
                    break
        await msg.reply("🛑 **Cancelled.** Cleaning up temporary files…")
        await cleanup_path(DOWNLOAD_DIR / str(uid))
    else:
        await msg.reply("ℹ️ Nothing is running right now.")


@app.on_message(filters.command("skip"))
async def cmd_skip(_, msg: Message):
    uid = msg.from_user.id
    event = user_skip.get(uid)
    if event:
        event.set()
        await msg.reply("⏭️ **Skipping current file…**")
    else:
        await msg.reply("ℹ️ No file is being processed right now.")


@app.on_message(filters.command("status"))
async def cmd_status(_, msg: Message):
    disk_path = "/" if os.name != "nt" else "C:\\"
    disk = psutil.disk_usage(disk_path)
    active = len(user_status)
    queued_total = sum(q.qsize() for q in user_queues.values())

    lines = [
        "📊 **Server Status**\n",
        f"💾 **Disk:** {human_size(disk.free)} free / {human_size(disk.total)} ({disk.percent}% used)",
        f"👥 **Active tasks:** {active} / {MAX_CONCURRENT_USERS}",
        f"📋 **Queued links:** {queued_total}",
    ]
    if user_status:
        lines.append("\n**Active users:**")
        for uid, st in user_status.items():
            lines.append(f"  • `{uid}` — {st}")

    await msg.reply("\n".join(lines))


# ──────────────────────────────────────────────────────────────
#  MEGA LINK DETECTOR & DESTINATION HANDLER
# ──────────────────────────────────────────────────────────────

def ask_for_destination(chat_id: int, text: str, job_dict: dict):
    # Ask the user where they want to send the files using an inline keyboard
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Send to DM 👤", callback_data="dest_dm"),
            InlineKeyboardButton("Send to Channel 📢", callback_data="dest_channel")
        ]
    ])
    return app.send_message(
        chat_id=chat_id,
        text=f"{text}\n\n**Where do you want to send the file(s)?**",
        reply_markup=keyboard
    )

@app.on_callback_query(filters.regex(r"^dest_"))
async def dest_callback(_, query: CallbackQuery):
    user_id = query.from_user.id
    job = destination_pending.get(user_id)
    
    if not job:
        await query.answer("This request has expired or was already processed.", show_alert=True)
        return

    dest_type = query.data.split("_")[1]
    
    if dest_type == "dm":
        # Send to DM: Immediately queue the job
        destination_pending.pop(user_id, None)
        job["target_chat_id"] = user_id
        q = ensure_worker(user_id)
        
        await query.message.edit_text("✅ **Sending to DM.** Queued successfully!")
        
        position = q.qsize() + 1
        await q.put(job)
        
        if job["type"] == "file" and position > 1:
            try:
                await app.send_message(
                    user_id,
                    f"📋 **Queued** (position #{position}): `{job['link'][:80]}`",
                    disable_web_page_preview=True,
                )
            except Exception:
                pass

    elif dest_type == "channel":
        # Ask for Channel ID
        destination_pending.pop(user_id, None)
        channel_id_pending[user_id] = job
        
        await query.message.edit_text(
            "📢 **Send to Channel selected.**\n\n"
            "Please send the **Channel ID** (e.g. `-100123456789`) or forward a message from the channel.\n\n"
            "_(Make sure the bot is an Admin in the channel first!)_"
        )

@app.on_message(filters.text & ~filters.command(["start", "cancel", "status", "skip"]))
async def handle_text_messages(_, msg: Message):
    if not msg.text:
        return

    user_id = msg.from_user.id

    # ── Check if user is replying with a Channel ID ──
    if user_id in channel_id_pending:
        text = msg.text.strip()
        
        # Try to parse as integer or username
        target_chat = None
        try:
            if text.startswith("-100") and text[4:].isdigit():
                target_chat = int(text)
            elif text.startswith("@"):
                target_chat = text
            else:
                target_chat = text # might be a username without @
                
            # Quick check if bot can access the channel
            await app.get_chat(target_chat)
            
            job = channel_id_pending.pop(user_id)
            job["target_chat_id"] = target_chat
            
            await msg.reply(f"✅ **Channel confirmed!** Queuing download...")
            
            q = ensure_worker(user_id)
            position = q.qsize() + 1
            await q.put(job)
            
            if job["type"] == "file" and position > 1:
                try:
                    await app.send_message(
                        user_id,
                        f"📋 **Queued** (position #{position}): `{job['link'][:80]}`",
                        disable_web_page_preview=True,
                    )
                except Exception:
                    pass
            return
                
        except (PeerIdInvalid, ChannelInvalid, BadRequest, ValueError) as e:
            await msg.reply(
                f"❌ **Invalid Channel ID or the bot is not an admin there.**\n"
                f"Error: `{e}`\n\n"
                f"Please try again or send `/cancel`."
            )
            return

    # ── Check if user is replying with a start-from number ──
    if user_id in folder_pending:
        text = msg.text.strip()
        if text.isdigit():
            start_from = int(text)
            pending = folder_pending.pop(user_id)
            orig_msg, link, chat_id, folder_id, file_infos = pending
            total = len(file_infos)

            if start_from < 1 or start_from > total:
                await msg.reply(
                    f"❌ Invalid number. Must be between **1** and **{total}**.\n"
                    f"Please send a valid number:"
                )
                folder_pending[user_id] = pending  # put it back
                return

            await msg.reply(
                f"✅ Starting download from file **#{start_from}** out of **{total}**."
            )

            job = {
                "type": "folder",
                "link": link,
                "folder_id": folder_id,
                "file_infos": file_infos,
                "start_from": start_from,
            }
            destination_pending[user_id] = job
            await ask_for_destination(msg.chat.id, f"✅ Starting download from file **#{start_from}** out of **{total}**.", job)
            return
        else:
            # Not a number — clear pending and fall through to link detection
            folder_pending.pop(user_id, None)

    # ── Normal link detection ──
    links = MEGA_LINK_RE.findall(msg.text)
    if not links:
        return

    is_group = msg.chat.type in ("group", "supergroup")

    if is_group:
        await msg.reply(
            "📥 **Processing…** I will send this to your DM.",
            disable_web_page_preview=True,
        )

    for link in links:
        if is_mega_folder(link):
            # ── FOLDER: fetch metadata, then ask for start number ──
            fetching_msg = await msg.reply(
                "📂 **Fetching folder info from Mega…** Please wait.",
                disable_web_page_preview=True,
            )
            try:
                folder_id, file_infos = await asyncio.to_thread(
                    _mega_get_folder_info_sync, link,
                )
            except Exception as exc:
                await safe_edit(
                    fetching_msg,
                    f"❌ Failed to fetch folder info: `{exc}`",
                )
                continue

            total = len(file_infos)
            await safe_edit(
                fetching_msg,
                f"📂 **Found {total} file(s)** in this folder.\n\n"
                f"📝 **Send a number to start downloading from:**\n"
                f"• Send `1` to download all files from the beginning\n"
                f"• Send any number (e.g. `500`) to resume from that file\n\n"
                f"_(Valid range: 1 to {total})_",
            )

            # Store pending state
            folder_pending[user_id] = (msg, link, msg.chat.id, folder_id, file_infos)

        else:
            # ── SINGLE FILE: Ask for destination ──
            job = {
                "type": "file",
                "link": link,
            }
            destination_pending[user_id] = job
            await ask_for_destination(msg.chat.id, "✅ **File Link Detected!**", job)


# ──────────────────────────────────────────────────────────────
#  FLASK KEEP-ALIVE (For Render Web Service)
# ──────────────────────────────────────────────────────────────
web_app = Flask(__name__)

@web_app.route('/')
def home():
    return "Mega Downloader Bot is running successfully on Render!", 200

def run_server():
    # Render automatically provides a PORT environment variable
    port = int(os.environ.get("PORT", 8080))
    web_app.run(host='0.0.0.0', port=port)

def keep_alive():
    # Run Flask in a daemon thread so it doesn't block Pyrogram
    server_thread = Thread(target=run_server, daemon=True)
    server_thread.start()

# ──────────────────────────────────────────────────────────────
#  ENTRY POINT
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("Starting Flask Keep-Alive Server…")
    keep_alive()
    
    log.info("Starting Mega Downloader Bot…")
    app.run()

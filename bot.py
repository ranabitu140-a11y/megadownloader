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
import asyncio
import logging
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

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
from pyrogram.types import Message
from pyrogram.errors import FloodWait, BadRequest

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
user_status:  dict[int, str]           = {}
global_semaphore = asyncio.Semaphore(MAX_CONCURRENT_USERS)


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
    data = None
    for attempt in range(1, 4):
        try:
            resp = requests.post(
                "https://g.api.mega.co.nz/cs",
                params={"id": 0, "n": folder_id},
                json=[{"a": "f", "c": 1, "r": 1}],
                timeout=(10, 120),
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
    folder_id: str, file_info: dict, dest_dir: str,
) -> str:
    """
    Download a SINGLE file from a Mega folder using its pre-decrypted keys.
    Returns local file path.
    """
    handle   = file_info["handle"]
    filename = file_info["filename"]
    k        = file_info["aes_key"]
    iv       = file_info["iv"]

    # Get download URL
    dl_resp = requests.post(
        "https://g.api.mega.co.nz/cs",
        params={"id": 1, "n": folder_id},
        json=[{"a": "g", "g": 1, "n": handle}],
        timeout=(10, 120),
    )
    dl_resp.raise_for_status()
    dl_data = dl_resp.json()[0]

    if isinstance(dl_data, int) and dl_data < 0:
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

    with requests.get(dl_url, stream=True, timeout=(10, None)) as r:
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
                    chat_id=user_id,
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
                        chat_id=user_id,
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
                    chat_id=user_id,
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

async def process_link(
    user_id: int,
    chat_id: int,
    link: str,
    status_msg: Message,
    cancel_event: asyncio.Event,
) -> None:
    user_dir = DOWNLOAD_DIR / str(user_id)
    user_dir.mkdir(parents=True, exist_ok=True)

    try:
        if is_mega_folder(link):
            # ── FOLDER: sequential download-one → upload → delete → next ──
            await safe_edit(status_msg, "📂 **Fetching folder info from Mega…**")
            folder_id, file_infos = await asyncio.to_thread(
                _mega_get_folder_info_sync, link,
            )
            total_files = len(file_infos)
            await safe_edit(
                status_msg,
                f"📂 Found **{total_files}** file(s). Processing one by one…"
            )

            for file_idx, finfo in enumerate(file_infos, 1):
                if cancel_event.is_set():
                    return

                fname = finfo["filename"]
                fsize = finfo["file_size"]
                await safe_edit(
                    status_msg,
                    f"⬇️ **[{file_idx}/{total_files}]** Downloading `{fname}` ({human_size(fsize)})…"
                )

                try:
                    result = await asyncio.to_thread(
                        _mega_download_one_file_sync, folder_id, finfo, str(user_dir),
                    )
                    filepath = Path(result)
                except Exception as dl_err:
                    log.warning("Skipping %s: %s", fname, dl_err)
                    await safe_edit(
                        status_msg,
                        f"⚠️ Skipped `{fname}`: `{dl_err}`\nContinuing…"
                    )
                    await asyncio.sleep(2)
                    continue

                if cancel_event.is_set():
                    return

                # Upload → delete
                ok = await _upload_file(
                    user_id, filepath, fname, file_idx, total_files,
                    status_msg, cancel_event,
                )
                # Cleanup this file immediately
                await cleanup_path(filepath)
                if not ok:
                    return

            if not cancel_event.is_set():
                await safe_edit(status_msg, "✅ **Done!** All files sent to your DM.")

        else:
            # ── SINGLE FILE ──
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
                user_id, filepath, original_name, 1, 1,
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
        msg, link, chat_id = await q.get()
        cancel_event = asyncio.Event()
        user_cancel[user_id] = cancel_event
        user_status[user_id] = f"Processing: {link[:60]}…"

        try:
            status_msg = await app.send_message(user_id, "⏳ **Starting…**")
            async with global_semaphore:
                await process_link(user_id, chat_id, link, status_msg, cancel_event)
        except Exception as exc:
            log.exception("Worker error for user %d: %s", user_id, exc)
        finally:
            user_status.pop(user_id, None)
            user_cancel.pop(user_id, None)
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
        "**Commands:**\n"
        "• `/cancel` — Stop current download/upload\n"
        "• `/status` — Server & queue info\n\n"
        "You can also use me in **groups** — I'll send files to your DM!",
        disable_web_page_preview=True,
    )


@app.on_message(filters.command("cancel"))
async def cmd_cancel(_, msg: Message):
    uid = msg.from_user.id
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
#  MEGA LINK DETECTOR
# ──────────────────────────────────────────────────────────────

@app.on_message(filters.text & ~filters.command(["start", "cancel", "status"]))
async def detect_mega_link(_, msg: Message):
    if not msg.text:
        return

    links = MEGA_LINK_RE.findall(msg.text)
    if not links:
        return

    user_id = msg.from_user.id
    is_group = msg.chat.type in ("group", "supergroup")

    if is_group:
        await msg.reply(
            "📥 **Processing…** I will send this to your DM.",
            disable_web_page_preview=True,
        )

    q = ensure_worker(user_id)
    for link in links:
        position = q.qsize() + 1
        await q.put((msg, link, msg.chat.id))
        if position > 1:
            try:
                await app.send_message(
                    user_id,
                    f"📋 **Queued** (position #{position}): `{link[:80]}`",
                    disable_web_page_preview=True,
                )
            except Exception:
                pass


# ──────────────────────────────────────────────────────────────
#  ENTRY POINT
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("Starting Mega Downloader Bot…")
    app.run()

# 🚀 Mega.nz → Telegram Bot

High-performance Pyrogram v2 bot that downloads **Mega.nz** files/folders and uploads them directly to Telegram with real-time progress tracking.

## ✨ Features

- 📁 **Single file & folder** downloads from Mega.nz
- 📊 **Real-time progress** bar with speed & ETA
- 🎬 **Smart uploads** — Images as photos, videos with thumbnails & streaming
- 📦 **Auto-split** files larger than 2 GB
- 👥 **Multi-user queue** system with concurrency control
- 🛑 **Cancel** running downloads/uploads anytime
- 💬 Works in **private chats & groups**

## 📋 Requirements

- Python 3.10+
- Telegram Bot Token (from [@BotFather](https://t.me/BotFather))
- Telegram API credentials (from [my.telegram.org](https://my.telegram.org))

## ⚙️ Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/YOUR_USERNAME/mega-telegram-bot.git
   cd mega-telegram-bot
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment variables**
   
   Create a `.env` file:
   ```env
   APP_ID=your_telegram_app_id
   API_HASH=your_telegram_api_hash
   BOT_TOKEN=your_bot_token
   ```

4. **Run the bot**
   ```bash
   python bot.py
   ```

## 🤖 Bot Commands

| Command   | Description                    |
|-----------|--------------------------------|
| `/start`  | Welcome message & usage info   |
| `/cancel` | Stop current download/upload   |
| `/status` | Server & queue status          |

Simply send any **Mega.nz link** and the bot will download and send the file to you.

## 📄 License

MIT License

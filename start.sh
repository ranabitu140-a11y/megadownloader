#!/bin/bash

echo "Starting Cloudflare WARP..."
# Start the WARP background service manually
warp-svc &
# Give it 5 seconds to boot up
sleep 5

# Register, set proxy mode, define the port, and connect (Updated Syntax)
warp-cli registration new
warp-cli mode proxy
warp-cli proxy port 40000
warp-cli connect

# Wait 2 seconds for the connection to establish
sleep 2

export WARP_PROXY="${WARP_PROXY:-socks5h://127.0.0.1:40000}"

echo "WARP Connected! Starting Telegram Bot..."
python bot.py

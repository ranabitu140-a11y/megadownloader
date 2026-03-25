#!/bin/bash

echo "Starting Cloudflare WARP..."
# Start the WARP background service manually
warp-svc &
# Give it 5 seconds to boot up
sleep 5

# Register, set proxy mode, and connect
warp-cli --accept-tos registration new
warp-cli --accept-tos mode proxy
warp-cli --accept-tos connect

# Wait 2 seconds for the connection to establish
sleep 2

export WARP_PROXY="${WARP_PROXY:-socks5h://127.0.0.1:40000}"

echo "WARP Connected! Starting Telegram Bot..."
python bot.py

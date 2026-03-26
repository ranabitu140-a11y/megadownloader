FROM python:3.11-slim

WORKDIR /app

# 1. Install necessary system tools
RUN apt-get update && apt-get install -y \
    gcc python3-dev libglib2.0-0 \
    curl gnupg ca-certificates dbus \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 2. Install Cloudflare WARP (Specifically for Debian Bookworm)
RUN curl -fsSL https://pkg.cloudflareclient.com/pubkey.gpg | \
    gpg --yes --dearmor -o /usr/share/keyrings/cloudflare-warp-archive-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/cloudflare-warp-archive-keyring.gpg] https://pkg.cloudflareclient.com/ bookworm main" | \
    tee /etc/apt/sources.list.d/cloudflare-client.list && \
    apt-get update && apt-get install -y cloudflare-warp && \
    rm -rf /var/lib/apt/lists/*

COPY . .

# 3. Install Python requirements
RUN pip install --no-cache-dir -U pip
RUN pip install --no-cache-dir -r requirements.txt

# 4. Make the startup script executable
RUN chmod +x start.sh

# 5. Run the startup script!
CMD ["./start.sh"]

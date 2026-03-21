FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies needed for your specific bot:
# 1. gcc & python3-dev: Required to compile tgcrypto for lightning-fast uploads
# 2. libglib2.0-0: Required by OpenCV to process video thumbnails
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Copy all your bot files (bot.py, requirements.txt, etc.) into the container
COPY . .

# Upgrade pip to the latest version
RUN pip install --no-cache-dir -U pip

# Install all the plugins from your requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Command to start your high-performance bot
CMD ["python", "bot.py"]

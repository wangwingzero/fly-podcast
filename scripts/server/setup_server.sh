#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="${FLYING_PODCAST_ROOT:-/www/wwwroot/flying-podcast}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

cd "$ROOT_DIR"

if ! command -v ffmpeg >/dev/null 2>&1; then
  if command -v apt-get >/dev/null 2>&1; then
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y ffmpeg
  else
    echo "ffmpeg is required but apt-get is not available; install ffmpeg manually"
    exit 2
  fi
fi

if [[ ! -d ".venv" ]]; then
  "$PYTHON_BIN" -m venv .venv
fi

.venv/bin/python -m pip install --upgrade pip
.venv/bin/pip install -r requirements.txt

export PLAYWRIGHT_BROWSERS_PATH="${PLAYWRIGHT_BROWSERS_PATH:-$ROOT_DIR/.playwright}"
.venv/bin/python -m playwright install --with-deps chromium

mkdir -p data/raw data/processed data/output data/history
mkdir -p /www/wwwlogs/flying-podcast
chmod 700 /www/wwwlogs/flying-podcast

chmod +x scripts/server/run_daily_digest.sh
chmod +x scripts/server/setup_server.sh
chmod +x scripts/server/install_daily_cron.py
chmod +x scripts/server/publish_static_outputs.py
chmod +x scripts/server/run_podcast_console.sh 2>/dev/null || true
chmod +x scripts/server/install_podcast_console_service.sh 2>/dev/null || true

echo "setup complete: $ROOT_DIR"

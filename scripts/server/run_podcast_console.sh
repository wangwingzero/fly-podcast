#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="${FLYING_PODCAST_ROOT:-/www/wwwroot/flying-podcast}"
HOST="${PODCAST_WEB_HOST:-0.0.0.0}"
PORT="${PODCAST_WEB_PORT:-8091}"

cd "$ROOT_DIR"

if [[ ! -x ".venv/bin/python" ]]; then
  bash scripts/server/setup_server.sh
fi

if ! command -v ffmpeg >/dev/null 2>&1; then
  if command -v apt-get >/dev/null 2>&1; then
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y ffmpeg
  else
    echo "ffmpeg is required but apt-get is not available; install ffmpeg manually"
    exit 2
  fi
fi

if ! .venv/bin/python - <<'PY' >/dev/null 2>&1
import flask
import gunicorn
PY
then
  .venv/bin/python -m pip install -r requirements.txt
fi

export TZ=Asia/Shanghai
export PYTHONUNBUFFERED=1
export PYTHONPATH="$ROOT_DIR/src"
export PLAYWRIGHT_BROWSERS_PATH="${PLAYWRIGHT_BROWSERS_PATH:-$ROOT_DIR/.playwright}"

exec .venv/bin/gunicorn \
  --workers 1 \
  --threads "${PODCAST_WEB_THREADS:-4}" \
  --timeout 0 \
  --bind "$HOST:$PORT" \
  flying_podcast.web.podcast_console:app

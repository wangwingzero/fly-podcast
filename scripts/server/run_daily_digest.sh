#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="${FLYING_PODCAST_ROOT:-/www/wwwroot/flying-podcast}"
LOG_DIR="${FLYING_PODCAST_LOG_DIR:-/www/wwwlogs/flying-podcast}"
LOCK_FILE="${FLYING_PODCAST_LOCK_FILE:-/tmp/flying-podcast-daily.lock}"
TARGET_DATE="${1:-$(TZ=Asia/Shanghai date +%F)}"

mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/daily_${TARGET_DATE}.log"
find "$LOG_DIR" -type f -name "daily_*.log" -mtime +30 -delete 2>/dev/null || true

exec > >(tee -a "$LOG_FILE") 2>&1

echo "== flying-podcast daily digest =="
echo "date=$TARGET_DATE"
echo "root=$ROOT_DIR"
echo "started_at=$(TZ=Asia/Shanghai date '+%F %T %Z')"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "another daily digest run is already active; exiting"
  exit 0
fi

cd "$ROOT_DIR"

if [[ ! -x ".venv/bin/python" ]]; then
  echo "missing .venv/bin/python; run scripts/server/setup_server.sh first"
  exit 2
fi

export TZ=Asia/Shanghai
export PYTHONUNBUFFERED=1
export PLAYWRIGHT_BROWSERS_PATH="${PLAYWRIGHT_BROWSERS_PATH:-$ROOT_DIR/.playwright}"

PYTHON="$ROOT_DIR/.venv/bin/python"

for stage in ingest rank compose verify publish; do
  echo "-- stage: $stage"
  "$PYTHON" run.py "$stage" --date "$TARGET_DATE"
done

echo "-- stage: publish-static"
"$PYTHON" scripts/server/publish_static_outputs.py --date "$TARGET_DATE"

echo "-- stage: notify"
"$PYTHON" run.py notify --date "$TARGET_DATE"

echo "finished_at=$(TZ=Asia/Shanghai date '+%F %T %Z')"

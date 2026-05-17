#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="${FLYING_PODCAST_ROOT:-/www/wwwroot/flying-podcast}"
HOST="${PODCAST_WEB_HOST:-0.0.0.0}"
PORT="${PODCAST_WEB_PORT:-8091}"
SERVICE_NAME="${PODCAST_WEB_SERVICE_NAME:-flying-podcast-console}"

if [[ ! -f "$ROOT_DIR/scripts/server/run_podcast_console.sh" ]]; then
  echo "missing run script: $ROOT_DIR/scripts/server/run_podcast_console.sh"
  exit 2
fi

chmod +x "$ROOT_DIR/scripts/server/run_podcast_console.sh"

cat >"/etc/systemd/system/${SERVICE_NAME}.service" <<EOF
[Unit]
Description=Flying Podcast Web Console
After=network.target

[Service]
Type=simple
WorkingDirectory=${ROOT_DIR}
Environment=FLYING_PODCAST_ROOT=${ROOT_DIR}
Environment=PODCAST_WEB_HOST=${HOST}
Environment=PODCAST_WEB_PORT=${PORT}
ExecStart=${ROOT_DIR}/scripts/server/run_podcast_console.sh
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now "${SERVICE_NAME}.service"
systemctl --no-pager --full status "${SERVICE_NAME}.service"

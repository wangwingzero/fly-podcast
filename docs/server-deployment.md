# Server Deployment

The daily digest now runs on the BaoTa-managed server instead of GitHub Actions.

## Runtime Layout

- Project root: `/www/wwwroot/flying-podcast`
- Virtual environment: `/www/wwwroot/flying-podcast/.venv`
- Logs: `/www/wwwlogs/flying-podcast/daily_YYYY-MM-DD.log`
- Lock file: `/tmp/flying-podcast-daily.lock`
- Daily runner: `/www/wwwroot/flying-podcast/scripts/server/run_daily_digest.sh`

## Schedule

Run once per day at Beijing 03:00:

```cron
0 3 * * * flock -xn /www/server/cron/flying_podcast_daily.lock -c /www/server/cron/flying_podcast_daily >> /www/server/cron/flying_podcast_daily.log 2>&1
```

The runner first downloads `history/recent_published.json` from R2 when available,
then executes the same logical stages as the old `daily-digest` workflow:

```text
sync-history -> ingest -> rank -> compose -> verify -> publish -> upload-r2 -> notify
```

`upload-r2` replaces the GitHub Actions R2 upload steps for `web_YYYY-MM-DD.html`,
`static/copyright.html`, `static/beian_icon.png`, and `recent_published.json`.

## Production Prerequisites

- Add the server public IP `154.9.27.44` to the WeChat Official Account API IP whitelist.
- Keep `WECHAT_PROXY=` empty on the server when the server IP is whitelisted; the old proxy can time out from the server.
- Run `python run.py healthcheck --json` after changing model, image, or WeChat credentials.
- Image generation is a warning-level health check. The publish pipeline first tries stock/public images and only uses AI generation for missing images.

## BaoTa Management

In BaoTa, manage the job as a shell scheduled task:

- Task name: `flying-podcast-daily`
- Type: Shell script
- Schedule: daily at `03:00`
- Script:

```bash
/www/wwwroot/flying-podcast/scripts/server/run_daily_digest.sh
```

The deployed server also keeps a BaoTa-compatible shell wrapper at:

```bash
/www/server/cron/flying_podcast_daily
```

Manual rerun for a specific date:

```bash
/www/wwwroot/flying-podcast/scripts/server/run_daily_digest.sh 2026-04-27
```

## SSH Keepalive

SSH remains the preferred management protocol for this Ubuntu server. If an SSH client drops idle sessions, add a small server-side keepalive file:

```bash
cat >/etc/ssh/sshd_config.d/99-flying-podcast-keepalive.conf <<'EOF'
ClientAliveInterval 30
ClientAliveCountMax 6
TCPKeepAlive yes
EOF
sshd -t && systemctl reload ssh
```

For local clients, enable their keepalive option or add:

```sshconfig
Host 154.9.27.44
  Port 7668
  ServerAliveInterval 30
  ServerAliveCountMax 6
  TCPKeepAlive yes
```

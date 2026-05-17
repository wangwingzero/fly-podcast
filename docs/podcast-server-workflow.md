# Podcast Server Workflow

This workflow runs podcast production on the BaoTa-managed server through a web
console. The local workstation is no longer part of the production path.

## Is MinerU Required?

No. The podcast pipeline uses MinerU only when the `MINERU` environment variable
is configured.

- With `MINERU`: PDF extraction uses MinerU VLM first and caches Markdown in the
  episode work directory.
- Without `MINERU`: extraction falls back to local `pdfplumber`.
- For text-native PDFs, `pdfplumber` is usually enough.
- For scanned PDFs, image-heavy tables, or complex regulatory layouts, MinerU is
  strongly recommended because `pdfplumber` may extract little or no text.

The server should still have `ffmpeg` available because TTS segments are
converted and concatenated with ffmpeg.

## Web Console

The server exposes a password-protected web console:

```text
https://podcast.hudawang.cn
```

The console supports:

- Login with `PODCAST_WEB_PASSWORD`.
- Upload one PDF and start generation.
- Optionally create the WeChat draft after audio generation.
- Watch live logs in the browser.
- Download generated files or the full episode directory as a zip.
- Create a WeChat draft from a completed job.

Generated jobs are stored under:

```text
/www/wwwroot/flying-podcast/data/podcast_web/jobs/
/www/wwwroot/flying-podcast/data/output/podcast/
```

## Service Commands

Start or update the systemd service:

```bash
cd /www/wwwroot/flying-podcast
bash scripts/server/install_podcast_console_service.sh
```

Check status and logs:

```bash
systemctl status flying-podcast-console --no-pager
journalctl -u flying-podcast-console -f
```

Manual foreground run:

```bash
cd /www/wwwroot/flying-podcast
bash scripts/server/run_podcast_console.sh
```

## Server-Side Requirements

The script assumes the existing production layout:

- Repository root: `/www/wwwroot/flying-podcast`
- Virtual environment: `/www/wwwroot/flying-podcast/.venv`
- Static root: `/www/wwwroot/flighttoolbox-static/current`
- Static public base URL: `https://flighttoolbox.hudawang.cn`
- Web console port: `8091`

Required server environment variables are the same ones used by the current
Python pipeline:

- `LLM_API_KEY`, `LLM_BASE_URL`, `LLM_MODEL`
- `STATIC_ROOT`, `STATIC_PUBLIC_BASE_URL`
- `WECHAT_APP_ID`, `WECHAT_APP_SECRET` when using `-Publish`
- `PODCAST_WEB_PASSWORD`
- `PODCAST_WEB_SECRET` for stable sessions across restarts
- `PODCAST_WEB_PUBLIC_URL=https://podcast.hudawang.cn`
- `PODCAST_WEB_MAX_UPLOAD_MB=1024` for large PDF uploads
- `DASHSCOPE_API_KEY` plus `TTS_ENABLE_DASHSCOPE=true` if DashScope fallback is
  desired
- `TTS_ENABLE_EDGE=true` if Edge TTS fallback is desired
- `MINERU` only if MinerU extraction and full-document narration are desired

Keep the server IP on the WeChat Official Account API whitelist when publishing
from the server.

## BaoTa Reverse Proxy Notes

For the `podcast.hudawang.cn` reverse proxy, set the proxy target to
`http://127.0.0.1:8091`. Large PDFs need a longer client-body timeout than the
Nginx default 60 seconds. Add an extension config for this site with:

```nginx
client_max_body_size 1024m;
client_body_timeout 3600s;
client_header_timeout 3600s;
send_timeout 3600s;
client_body_buffer_size 16m;

proxy_connect_timeout 60s;
proxy_send_timeout 3600s;
proxy_read_timeout 3600s;
proxy_buffering off;
proxy_request_buffering on;
```

## Output

Each run creates one episode directory:

```text
data/output/podcast/{date}_{uploaded_pdf_stem}/
```

Expected files include:

- `script.json`
- `dialogue.html`
- `cover.jpg`
- `metadata.json`
- `segments/seg_*.mp3`
- `{podcast_title}.mp3`

When `-Publish` is used, `publish_result.json` is also saved after the WeChat
draft is created.

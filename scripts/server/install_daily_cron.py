#!/usr/bin/env python3
"""Install the server-local daily digest cron entry.

The production server runs in Asia/Shanghai. Keep this cron entry in server
local time so the daily digest starts at Beijing 07:00, not UTC 23:00.
"""

from __future__ import annotations

import argparse
import os
import stat
import subprocess
import tempfile
from pathlib import Path


CRON_COMMENT = "# flying-podcast daily digest: Beijing 07:00, auto-publishes non-empty WeChat digest"
CRON_COMMAND = (
    "flock -xn /www/server/cron/flying_podcast_daily.lock "
    "-c /www/server/cron/flying_podcast_daily "
    ">> /www/server/cron/flying_podcast_daily.log 2>&1"
)
CRON_LINE = f"0 7 * * *  {CRON_COMMAND}"
WRAPPER_NAME = "flying_podcast_daily"


def build_crontab(existing: str) -> str:
    """Return crontab text with one Beijing 07:00 daily digest entry."""
    kept: list[str] = []
    for line in existing.splitlines():
        if "flying-podcast daily digest" in line:
            continue
        if f"/www/server/cron/{WRAPPER_NAME}" in line:
            continue
        kept.append(line.rstrip())

    while kept and kept[-1] == "":
        kept.pop()

    if kept:
        kept.append("")
    kept.extend([CRON_COMMENT, CRON_LINE])
    return "\n".join(kept) + "\n"


def build_wrapper(root_dir: str) -> str:
    """Return the BaoTa-compatible shell wrapper content."""
    return "\n".join(
        [
            "#!/usr/bin/env bash",
            "set -Eeuo pipefail",
            "export LANG=en_US.UTF-8",
            "export LC_ALL=en_US.UTF-8",
            "export TZ=Asia/Shanghai",
            "export WECHAT_ENABLE_PUBLISH=true",
            "export WECHAT_AUTO_PUBLISH=true",
            "export WECHAT_PROXY=",
            "export MAX_ARTICLE_AGE_HOURS=48",
            "export MAX_TIER_A_ARTICLE_AGE_HOURS=48",
            f"{root_dir}/scripts/server/run_daily_digest.sh",
            "",
        ]
    )


def read_crontab() -> str:
    proc = subprocess.run(["crontab", "-l"], capture_output=True, text=True, check=False)
    if proc.returncode == 0:
        return proc.stdout
    return ""


def install_crontab(text: str) -> None:
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as handle:
        handle.write(text)
        path = handle.name
    try:
        subprocess.run(["crontab", path], check=True)
    finally:
        Path(path).unlink(missing_ok=True)


def write_wrapper(cron_dir: Path, root_dir: str) -> Path:
    cron_dir.mkdir(parents=True, exist_ok=True)
    wrapper = cron_dir / WRAPPER_NAME
    wrapper.write_text(build_wrapper(root_dir), encoding="utf-8")
    mode = wrapper.stat().st_mode
    wrapper.chmod(mode | stat.S_IXUSR)
    return wrapper


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root-dir", default=os.environ.get("FLYING_PODCAST_ROOT", "/www/wwwroot/flying-podcast"))
    parser.add_argument("--cron-dir", default="/www/server/cron")
    parser.add_argument("--dry-run", action="store_true", help="print the updated crontab instead of installing it")
    args = parser.parse_args()

    updated = build_crontab(read_crontab())
    if args.dry_run:
        print(updated, end="")
        return

    wrapper = write_wrapper(Path(args.cron_dir), args.root_dir.rstrip("/"))
    install_crontab(updated)
    print(f"installed wrapper: {wrapper}")
    print(CRON_COMMENT)
    print(CRON_LINE)


if __name__ == "__main__":
    main()

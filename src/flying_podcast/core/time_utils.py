from __future__ import annotations

from datetime import datetime, timedelta, timezone

# China Standard Time (UTC+08:00), no DST.
BEIJING_TZ = timezone(timedelta(hours=8))


def beijing_now() -> datetime:
    return datetime.now(BEIJING_TZ)


def beijing_today_str() -> str:
    return beijing_now().strftime("%Y-%m-%d")


def beijing_now_iso() -> str:
    return beijing_now().isoformat()

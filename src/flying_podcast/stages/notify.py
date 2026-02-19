from __future__ import annotations

from datetime import datetime

import requests

from flying_podcast.core.config import settings
from flying_podcast.core.email_notify import send_pipeline_report
from flying_podcast.core.io_utils import load_json
from flying_podcast.core.logging_utils import get_logger

logger = get_logger("notify")


def _build_message(day: str, quality: dict, publish: dict) -> str:
    compose_mode = publish.get("compose_mode", "-")
    lines = [
        f"飞行播客日报 {day}",
        f"质量分: {quality.get('total_score', '-')}",
        f"决策: {quality.get('decision', '-')}",
        f"成稿模式: {compose_mode}",
        f"发布状态: {publish.get('status', '-')}",
        f"原因: {', '.join(quality.get('reasons', [])) or '无'}",
        f"链接: {publish.get('url', '-')}",
    ]
    return "\n".join(lines)


def run(target_date: str | None = None) -> None:
    day = target_date or datetime.now().strftime("%Y-%m-%d")
    quality = load_json(settings.processed_dir / f"quality_{day}.json")
    publish = load_json(settings.output_dir / f"publish_{day}.json")
    composed = load_json(settings.processed_dir / f"composed_{day}.json")
    publish["compose_mode"] = composed.get("meta", {}).get("compose_mode", "-")

    # --- Email: pipeline process report ---
    raw = load_json(settings.raw_dir / f"{day}.json")
    ingest_count = len(raw) if isinstance(raw, list) else 0

    ranked = load_json(settings.processed_dir / f"ranked_{day}.json")
    rank_meta = ranked.get("meta", {}) if isinstance(ranked, dict) else {}

    compose_meta = {
        "domestic_count": composed.get("domestic_count", 0),
        "international_count": composed.get("international_count", 0),
        "entry_count": len(composed.get("entries", [])),
    }

    send_pipeline_report(day, ingest_count, rank_meta, compose_meta, quality, publish)

    # --- Webhook notification ---
    msg = _build_message(day, quality, publish)
    if settings.dry_run or not settings.alert_webhook_url:
        logger.info("[DRY_RUN notify]\n%s", msg)
        return

    resp = requests.post(
        settings.alert_webhook_url,
        json={"msgtype": "text", "text": {"content": msg}},
        timeout=15,
    )
    if not resp.ok:
        logger.error("notify failed: %s %s", resp.status_code, resp.text)
    else:
        logger.info("notify sent")


if __name__ == "__main__":
    run()

"""Publish podcast articles to WeChat Official Account as drafts.

Reads podcast output directories (script.json, dialogue.html, cover.jpg)
and creates WeChat drafts with the dialogue content and cover image.
"""
from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import quote

from flying_podcast.core.config import settings
from flying_podcast.core.io_utils import dump_json, load_json
from flying_podcast.core.logging_utils import get_logger
from flying_podcast.core.time_utils import beijing_today_str
from flying_podcast.core.wechat import WeChatClient

logger = get_logger("publish_podcast")

# CAAC document prefixes — PDFs with these prefixes get "阅读原文" link
_CAAC_PREFIXES = ("AC-", "IB-", "CCAR-", "AP-", "MD-", "MH-")


def _build_article_html(title: str, dialogue_html: str,
                        mp3_url: str = "") -> str:
    """Build complete article HTML for WeChat — MP3 URL + dialogue card.

    Audio is added manually in the WeChat editor.
    MP3 URL shown as plain text at top for easy copy-paste.
    """
    parts: list[str] = []

    if mp3_url:
        parts.append(
            '<section style="text-align:center;margin:10px auto 15px;'
            'max-width:420px;padding:8px 16px;font-size:12px;color:#999;'
            'word-break:break-all;">'
            f'{mp3_url}'
            '</section>'
        )

    parts.append(dialogue_html)
    return "".join(parts)


def _resolve_source_url(meta: dict) -> str:
    """Resolve the best source URL for the "阅读原文" link.

    Priority: metadata download_url > R2 normative PDF URL (CAAC docs only).
    Non-CAAC documents (e.g. Airbus, Boeing) return empty string.
    """
    # 1. Explicit download_url from metadata (set by podcast_inbox)
    download_url = meta.get("download_url", "")
    if download_url:
        return download_url

    # 2. For CAAC documents, construct R2 PDF URL from pdf_source
    pdf_source = meta.get("pdf_source", "")
    if not pdf_source:
        return ""

    pdf_name = Path(pdf_source).name  # e.g. "AC-121-FS-138R2循证证训练（EBT）实施管理规定.pdf"

    # Check if it's a CAAC document
    if not any(pdf_name.startswith(prefix) for prefix in _CAAC_PREFIXES):
        return ""

    # Construct R2 normative URL
    r2_url = f"https://{settings.r2_domain}/normative/{quote(pdf_name)}"
    logger.info("Auto-resolved CAAC source URL: %s", r2_url)
    return r2_url


def run(target_date: str | None = None, *,
        podcast_dir: str | None = None) -> list[str]:
    """Publish podcast episodes as WeChat drafts.

    Args:
        target_date: Date prefix to match podcast directories (YYYY-MM-DD)
        podcast_dir: Specific podcast output directory to publish (optional)

    Returns:
        List of created draft media_ids
    """
    day = target_date or beijing_today_str()
    output_base = settings.output_dir / "podcast"

    # Find podcast directories to publish
    if podcast_dir:
        dirs_to_publish = [Path(podcast_dir)]
    else:
        # Find all podcast dirs for the target date
        dirs_to_publish = sorted(
            d for d in output_base.iterdir()
            if d.is_dir() and d.name.startswith(day)
        )

    if not dirs_to_publish:
        logger.info("No podcast directories found for %s", day)
        return []

    logger.info("Found %d podcast episode(s) to publish", len(dirs_to_publish))

    client = WeChatClient()
    draft_ids: list[str] = []

    for ep_dir in dirs_to_publish:
        logger.info("Publishing: %s", ep_dir.name)

        # Load content
        meta_path = ep_dir / "metadata.json"
        script_path = ep_dir / "script.json"
        html_path = ep_dir / "dialogue.html"
        cover_path = ep_dir / "cover.jpg"

        if not script_path.exists():
            logger.warning("Skip %s: no script.json", ep_dir.name)
            continue

        script = load_json(script_path)
        title = script.get("title", ep_dir.name)

        # Load metadata for MP3 CDN URL and source document link
        meta = load_json(meta_path) if meta_path.exists() else {}
        mp3_url = meta.get("mp3_cdn_url", "")
        source_url = _resolve_source_url(meta)

        # Read dialogue HTML
        if html_path.exists():
            dialogue_html = html_path.read_text("utf-8")
        else:
            logger.warning("Skip %s: no dialogue.html", ep_dir.name)
            continue

        # Upload cover image as thumb material
        thumb_media_id = ""
        if cover_path.exists():
            cover_bytes = cover_path.read_bytes()
            cover_name = f"{title}.jpg"
            thumb_media_id = client.upload_thumb_image_bytes(cover_bytes, file_name=cover_name)
            if thumb_media_id:
                logger.info("Cover uploaded: %s", thumb_media_id[:30])
            else:
                logger.warning("Cover upload failed, using default thumb")

        # Build article HTML
        article_html = _build_article_html(title, dialogue_html, mp3_url=mp3_url)

        # Create digest summary (just the title)
        lines = script.get("dialogue", [])
        total_chars = sum(len(l.get("text", "")) for l in lines)
        digest = title
        if len(digest) > 120:
            digest = digest[:117] + "..."

        # Create draft
        try:
            media_id = client.create_draft(
                title=title,
                author="飞行播客",
                content_html=article_html,
                digest=digest,
                source_url=source_url,
                thumb_media_id=thumb_media_id,
            )
            logger.info("Draft created: %s (media_id: %s)", title, media_id[:30])
            draft_ids.append(media_id)

            # Save publish result
            result = {
                "date": day,
                "title": title,
                "media_id": media_id,
                "thumb_media_id": thumb_media_id,
                "source_url": source_url,
                "dialogue_lines": len(lines),
                "total_chars": total_chars,
            }
            dump_json(ep_dir / "publish_result.json", result)

        except Exception as e:
            logger.error("Failed to create draft for '%s': %s", title, e)
            continue

    logger.info("Published %d/%d podcast drafts", len(draft_ids), len(dirs_to_publish))
    return draft_ids

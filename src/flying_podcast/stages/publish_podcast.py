"""Publish podcast articles to WeChat Official Account as drafts.

Reads podcast output directories (script.json, dialogue.html, cover.jpg)
and creates WeChat drafts with the dialogue content and cover image.
"""
from __future__ import annotations

import json
from pathlib import Path

from flying_podcast.core.config import settings
from flying_podcast.core.io_utils import dump_json, load_json
from flying_podcast.core.logging_utils import get_logger
from flying_podcast.core.time_utils import beijing_today_str
from flying_podcast.core.wechat import WeChatClient

logger = get_logger("publish_podcast")


def _build_article_html(title: str, dialogue_html: str,
                        has_audio: bool = False) -> str:
    """Build complete article HTML for WeChat with intro + dialogue.

    Args:
        title: Podcast episode title
        dialogue_html: Pre-rendered scrollable dialogue HTML fragment
        has_audio: Whether an audio player page is linked via "阅读原文"
    """
    # Intro section above the dialogue
    intro = (
        '<section style="margin:15px auto;max-width:420px;padding:0 8px;">'
        '<section style="font-size:14px;color:#666;line-height:1.8;'
        'margin-bottom:12px;">'
        '虎机长和千羽带你用最轻松的方式，读懂民航局最新通告。'
        '</section>'
    )

    if has_audio:
        intro += (
            '<section style="background:linear-gradient(135deg,#1e6fff,#4e95ff);'
            'border-radius:8px;padding:12px 16px;margin-bottom:12px;'
            'text-align:center;">'
            '<section style="font-size:15px;color:#fff;font-weight:bold;'
            'margin-bottom:4px;">点击底部「阅读原文」收听音频</section>'
            '<section style="font-size:12px;color:rgba(255,255,255,0.8);">'
            '边听边看文字稿，体验更佳</section>'
            '</section>'
        )

    intro += '</section>'

    return intro + dialogue_html


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

        # Load metadata and content
        meta_path = ep_dir / "metadata.json"
        script_path = ep_dir / "script.json"
        html_path = ep_dir / "dialogue.html"
        cover_path = ep_dir / "cover.jpg"

        if not script_path.exists():
            logger.warning("Skip %s: no script.json", ep_dir.name)
            continue

        script = load_json(script_path)
        title = script.get("title", ep_dir.name)

        # Load metadata for player URL
        meta = load_json(meta_path) if meta_path.exists() else {}
        player_url = meta.get("player_url", "")

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
            thumb_media_id = client.upload_thumb_image_bytes(cover_bytes)
            if thumb_media_id:
                logger.info("Cover uploaded: %s", thumb_media_id[:30])
            else:
                logger.warning("Cover upload failed, using default thumb")

        # Build article HTML
        article_html = _build_article_html(
            title, dialogue_html, has_audio=bool(player_url)
        )

        # Create digest summary
        lines = script.get("dialogue", [])
        total_chars = sum(len(l.get("text", "")) for l in lines)
        digest = f"飞行播客 | 虎机长x千羽 | {len(lines)}段对话 | {title}"
        if len(digest) > 120:
            digest = digest[:117] + "..."

        # Create draft
        try:
            media_id = client.create_draft(
                title=f"飞行播客 | {title}",
                author="飞行播客",
                content_html=article_html,
                digest=digest,
                source_url=player_url,
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
                "dialogue_lines": len(lines),
                "total_chars": total_chars,
            }
            dump_json(ep_dir / "publish_result.json", result)

        except Exception as e:
            logger.error("Failed to create draft for '%s': %s", title, e)
            continue

    logger.info("Published %d/%d podcast drafts", len(draft_ids), len(dirs_to_publish))
    return draft_ids

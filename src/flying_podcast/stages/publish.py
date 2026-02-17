from __future__ import annotations

from datetime import datetime
from html import escape
from pathlib import Path

from dateutil import parser as dt_parser

from flying_podcast.core.config import ensure_dirs, settings
from flying_podcast.core.io_utils import dump_json, load_json
from flying_podcast.core.logging_utils import get_logger
from flying_podcast.core.wechat import WeChatClient, WeChatPublishError

logger = get_logger("publish")

_TIER_LABEL = {"A": "核心来源", "B": "媒体来源", "C": "参考来源"}
_REGION_DOT = {"domestic": "#34C759", "international": "#007AFF"}
_REGION_LABEL = {"domestic": "国内", "international": "国际"}


def _render_markdown(digest: dict) -> str:
    """Generate clean markdown for audit/preview purposes."""
    lines: list[str] = []
    lines.append(f"# 飞行播客日报 | {digest['date']}")
    lines.append("")
    lines.append(f"国内 {digest['domestic_count']} 条 | 国际 {digest['international_count']} 条")
    lines.append("")

    for idx, entry in enumerate(digest.get("entries", []), 1):
        title = entry["title"]
        source_name = entry.get("source_name", "")
        tier = entry.get("source_tier", "C")
        region = "国内" if entry.get("region") == "domestic" else "国际"
        citation = (entry.get("citations") or [""])[0]
        has_link = bool(citation)

        if has_link:
            lines.append(f"### {idx}. [{title}]({citation})")
        else:
            lines.append(f"### {idx}. {title}")
        facts = entry.get("facts", [])
        if facts:
            for f in facts:
                lines.append(f"- {f}")
        lines.append(f"- 影响：{entry['impact']}")
        if has_link:
            lines.append(f"- 原文：[{citation}]({citation})")
        source_parts = [source_name] if source_name else []
        source_parts.append(f"{tier}级")
        source_parts.append(region)
        lines.append(f"- 来源：{' · '.join(source_parts)}")
        lines.append("")
    return "\n".join(lines)


def _render_html(digest: dict) -> str:
    """Generate Apple-style card-based HTML, mobile-first, WeChat compatible."""
    date = digest["date"]
    dc = digest["domestic_count"]
    ic = digest["international_count"]
    entries = digest.get("entries", [])

    cards: list[str] = []
    for idx, entry in enumerate(entries, 1):
        title = escape(entry["title"])
        impact = escape(entry.get("impact", ""))
        facts = entry.get("facts", [])
        region = entry.get("region", "international")
        region_label = _REGION_LABEL.get(region, "国际")
        region_dot = _REGION_DOT.get(region, "#007AFF")
        source_name = escape(entry.get("source_name", ""))
        tier = entry.get("source_tier", "C")
        tier_label = _TIER_LABEL.get(tier, "参考来源")
        citation = str((entry.get("citations") or [""])[0]).strip()
        safe_href = escape(citation, quote=True)

        # --- Date ---
        date_str = ""
        raw_pa = entry.get("published_at", "")
        if raw_pa:
            try:
                dt = dt_parser.parse(str(raw_pa))
                date_str = f"{dt.month}月{dt.day}日 {dt.strftime('%H:%M')}"
            except (ValueError, TypeError):
                pass

        # --- Facts list ---
        facts_html = ""
        if facts:
            items = "".join(
                f'<p style="margin:0 0 6px 0;padding:0;font-size:14px;'
                f"color:#48484A;line-height:1.65;\">"
                f"{escape(f)}</p>"
                for f in facts
            )
            facts_html = (
                f'<section style="margin:10px 0 0 0;padding:10px 14px;'
                f"background:#F9F9F9;border-radius:10px;\">"
                f"{items}</section>"
            )

        # --- Impact badge ---
        impact_html = ""
        if impact:
            impact_html = (
                f'<p style="margin:12px 0 0 0;padding:0;font-size:13px;'
                f'color:#636366;line-height:1.55;">'
                f'<span style="display:inline-block;background:#F2F2F7;'
                f"color:#8E8E93;font-size:11px;padding:2px 8px;"
                f'border-radius:4px;margin-right:6px;font-weight:500;">速评</span>'
                f"{impact}</p>"
            )

        # --- Source row: only "阅读原文 →" ---
        source_meta = ""
        if citation:
            source_meta = (
                f'<section style="margin:14px 0 0 0;text-align:right;">'
                f'<a href="{safe_href}" style="font-size:12px;color:#007AFF;'
                f'text-decoration:none;font-weight:500;">阅读原文 →</a>'
                f"</section>"
            )

        # --- Date line under title ---
        date_html = ""
        if date_str:
            date_html = (
                f'<p style="margin:4px 0 0 0;font-size:12px;color:#AEAEB2;">'
                f"{escape(date_str)}</p>"
            )

        # --- Hero image ---
        image_html = ""
        image_url = entry.get("image_url", "")
        if image_url:
            safe_img = escape(image_url, quote=True)
            image_html = (
                f'<img src="{safe_img}" style="width:100%;height:auto;'
                f"border-radius:10px;margin:10px 0 0 0;display:block;"
                f'object-fit:cover;max-height:220px;" />'
            )

        # --- Title ---
        title_html = title
        if citation:
            title_html = (
                f'<a href="{safe_href}" style="color:#1C1C1E;'
                f'text-decoration:none;">{title}</a>'
            )

        card = (
            f'<section style="background:#FFFFFF;border-radius:16px;'
            f"padding:20px;margin-bottom:12px;"
            f'box-shadow:0 1px 3px rgba(0,0,0,0.04);">'
            # Number + title
            f'<p style="margin:0;font-size:17px;font-weight:600;'
            f'color:#1C1C1E;line-height:1.45;">'
            f'<span style="color:#AEAEB2;font-size:13px;'
            f'font-weight:400;margin-right:6px;">{idx:02d}</span>'
            f"{title_html}</p>"
            # Date
            f"{date_html}"
            # Hero image
            f"{image_html}"
            # Facts
            f"{facts_html}"
            # Impact
            f"{impact_html}"
            # Source row
            f"{source_meta}"
            f"</section>"
        )
        cards.append(card)

    html = (
        # Outer wrapper
        f'<section style="padding:0;margin:0;'
        f"font-family:-apple-system,BlinkMacSystemFont,"
        f"'SF Pro Display','PingFang SC','Microsoft YaHei',sans-serif;"
        f'background:#F2F2F7;-webkit-font-smoothing:antialiased;">'
        # Header
        f'<section style="padding:40px 20px 28px 20px;text-align:center;">'
        f'<p style="margin:0;font-size:11px;font-weight:600;'
        f"color:#8E8E93;letter-spacing:3px;"
        f'text-transform:uppercase;">FLYING PODCAST</p>'
        f'<p style="margin:8px 0 0 0;font-size:28px;font-weight:700;'
        f'color:#1C1C1E;letter-spacing:-0.5px;">每日简报</p>'
        f'<p style="margin:6px 0 0 0;font-size:15px;'
        f'color:#8E8E93;font-weight:400;">{date}</p>'
        f'<section style="margin:14px auto 0 auto;display:flex;'
        f'justify-content:center;gap:16px;">'
        f'<span style="font-size:13px;color:#48484A;">'
        f'<span style="display:inline-block;width:7px;height:7px;'
        f'border-radius:50%;background:#34C759;margin-right:4px;'
        f'vertical-align:middle;"></span>国内 {dc}</span>'
        f'<span style="font-size:13px;color:#48484A;">'
        f'<span style="display:inline-block;width:7px;height:7px;'
        f'border-radius:50%;background:#007AFF;margin-right:4px;'
        f'vertical-align:middle;"></span>国际 {ic}</span>'
        f"</section>"
        f"</section>"
        # Cards
        f'<section style="padding:0 12px 12px 12px;">'
        f"{''.join(cards)}"
        f"</section>"
        # Footer
        f'<section style="padding:20px 20px 32px 20px;text-align:center;">'
        f'<section style="width:36px;height:2px;background:#D1D1D6;'
        f'margin:0 auto 12px auto;border-radius:1px;"></section>'
        f'<p style="margin:0;font-size:12px;color:#AEAEB2;'
        f'line-height:1.6;">飞行播客 · 运输航空新闻精选</p>'
        f'<p style="margin:4px 0 0 0;font-size:11px;color:#C7C7CC;">'
        f"数据来源：民航局 / 航司 / 行业媒体 · 自动聚合 · 仅供参考</p>"
        f"</section>"
        f"</section>"
    )
    return html


def run(target_date: str | None = None) -> Path:
    ensure_dirs()
    day = target_date or datetime.now().strftime("%Y-%m-%d")
    digest = load_json(settings.processed_dir / f"composed_{day}.json")
    quality = load_json(settings.processed_dir / f"quality_{day}.json")

    md = _render_markdown(digest)
    html = _render_html(digest)

    result = {
        "date": day,
        "decision": quality["decision"],
        "quality_score": quality["total_score"],
        "compose_mode": digest.get("meta", {}).get("compose_mode", "unknown"),
        "status": "skipped",
        "publish_id": "",
        "url": "",
        "reasons": quality.get("reasons", []),
    }

    if quality["decision"] != "auto_publish":
        result["status"] = "hold"
    elif settings.dry_run or not settings.wechat_enable_publish:
        result["status"] = "dry_run"
        result["url"] = f"dry-run://flying-podcast/{day}"
    else:
        client = WeChatClient()
        try:
            media_id = client.create_draft(
                title=f"飞行播客日报 | {day}",
                author=settings.wechat_author,
                content_html=html,
                digest="面向航空公司职员的运输航空新闻日报",
                source_url="https://mp.weixin.qq.com",
            )
            publish = client.publish_draft(media_id)
            result["status"] = "published"
            result["publish_id"] = publish.publish_id
            result["url"] = f"wechat://publish/{publish.publish_id}"
        except WeChatPublishError as exc:
            result["status"] = "failed"
            result["reasons"].append(str(exc))

    out = settings.output_dir / f"publish_{day}.json"
    dump_json(out, result)

    # Persist human-readable draft for audit.
    dump_json(settings.output_dir / f"draft_{day}.json", {"markdown": md, "html": html})

    logger.info("Publish done. status=%s score=%.2f", result["status"], quality["total_score"])
    return out


if __name__ == "__main__":
    run()

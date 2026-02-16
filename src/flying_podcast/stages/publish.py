from __future__ import annotations

from datetime import datetime
from html import escape
from pathlib import Path

from flying_podcast.core.config import ensure_dirs, settings
from flying_podcast.core.io_utils import dump_json, load_json
from flying_podcast.core.logging_utils import get_logger
from flying_podcast.core.wechat import WeChatClient, WeChatPublishError

logger = get_logger("publish")

_SECTION_ORDER = ["运行与安全", "航司经营与网络", "机队与制造商", "监管与行业政策"]

_SECTION_STYLES = {
    "运行与安全": {"color": "#dc2626", "bg": "#fef2f2"},
    "航司经营与网络": {"color": "#2563eb", "bg": "#eff6ff"},
    "机队与制造商": {"color": "#0d9488", "bg": "#f0fdfa"},
    "监管与行业政策": {"color": "#d97706", "bg": "#fffbeb"},
}

_TIER_LABEL = {"A": "核心来源", "B": "媒体来源", "C": "参考来源"}


def _render_markdown(digest: dict) -> str:
    """Generate clean markdown for audit/preview purposes."""
    lines: list[str] = []
    lines.append(f"# 飞行播客日报 | {digest['date']}")
    lines.append("")
    lines.append(f"国内 {digest['domestic_count']} 条 | 国际 {digest['international_count']} 条")
    lines.append("")

    for section in _SECTION_ORDER:
        entries = [x for x in digest["entries"] if x["section"] == section]
        if not entries:
            continue
        lines.append(f"## {section}")
        lines.append("")
        for idx, entry in enumerate(entries, 1):
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
    """Generate WeChat Official Account compatible HTML with inline styles only."""
    date = digest["date"]
    dc = digest["domestic_count"]
    ic = digest["international_count"]
    entries = digest.get("entries", [])

    sections_html: list[str] = []
    for section_name in _SECTION_ORDER:
        section_entries = [e for e in entries if e["section"] == section_name]
        if not section_entries:
            continue

        style = _SECTION_STYLES.get(section_name, {"color": "#6b7280", "bg": "#f9fafb"})
        color = style["color"]
        bg = style["bg"]

        cards: list[str] = []
        for entry in section_entries:
            title = escape(entry["title"])
            source_name = escape(entry.get("source_name", ""))
            tier = entry.get("source_tier", "C")
            tier_label = _TIER_LABEL.get(tier, "参考来源")
            impact = escape(entry.get("impact", ""))
            facts = entry.get("facts", [])
            region_tag = "国内" if entry.get("region") == "domestic" else "国际"
            citation = str((entry.get("citations") or [""])[0]).strip()
            safe_href = escape(citation, quote=True)
            title_link = (
                f'<a href="{safe_href}" style="color:#1a202c;text-decoration:none;">{title}</a>' if citation else title
            )

            # Facts section (only if real content exists)
            facts_html = ""
            if facts:
                items = "".join(
                    f'<p style="font-size:14px;color:#4a5568;margin:2px 0 2px 0;'
                    f'line-height:1.6;padding-left:12px;">'
                    f"· {escape(f)}</p>"
                    for f in facts
                )
                facts_html = items

            # Source attribution line
            source_parts = []
            if source_name:
                source_parts.append(source_name)
            source_parts.append(tier_label)
            source_parts.append(region_tag)
            source_line = " · ".join(source_parts)
            action_html = ""
            if citation:
                action_html = (
                    f'<p style="margin:10px 0 0 0;">'
                    f'<a href="{safe_href}" style="display:inline-block;'
                    f"font-size:12px;color:#ffffff;background:{color};"
                    f'padding:6px 10px;border-radius:4px;text-decoration:none;">查看原文</a>'
                    f"</p>"
                )

            card = (
                f'<section style="background:#ffffff;border-radius:8px;'
                f"padding:14px 16px;margin-bottom:8px;"
                f'border-left:3px solid {color};">'
                f'<p style="font-size:16px;font-weight:bold;color:#1a202c;'
                f'margin:0 0 8px 0;line-height:1.4;">{title_link}</p>'
                f"{facts_html}"
                f'<p style="font-size:13px;color:#718096;'
                f'margin:6px 0 0 0;line-height:1.5;">'
                f'<span style="color:{color};font-weight:bold;">▎</span>'
                f"{impact}</p>"
                f"{action_html}"
                f'<p style="font-size:12px;color:#a0aec0;'
                f'margin:6px 0 0 0;">{source_line}</p>'
                f"</section>"
            )
            cards.append(card)

        section_html = (
            f'<section style="margin-bottom:20px;">'
            f'<section style="margin-bottom:10px;">'
            f'<span style="background:{color};color:#ffffff;'
            f"padding:5px 12px;border-radius:4px;"
            f'font-size:14px;font-weight:bold;">'
            f"{escape(section_name)}</span>"
            f"</section>"
            f"{''.join(cards)}"
            f"</section>"
        )
        sections_html.append(section_html)

    html = (
        f'<section style="padding:12px;background:#f7f8fa;'
        f"font-family:-apple-system,BlinkMacSystemFont,"
        f"'PingFang SC','Microsoft YaHei',sans-serif;\">"
        # Header
        f'<section style="background:#1a365d;border-radius:12px;'
        f'padding:24px 16px;margin-bottom:20px;text-align:center;">'
        f'<p style="color:#ffffff;font-size:22px;font-weight:bold;'
        f'margin:0;letter-spacing:2px;">飞行播客日报</p>'
        f'<p style="color:#93c5fd;font-size:15px;'
        f'margin:10px 0 0 0;">{date}</p>'
        f'<p style="color:#cbd5e1;font-size:13px;'
        f'margin:6px 0 0 0;">国内 {dc} 条 · 国际 {ic} 条</p>'
        f"</section>"
        # Body
        f"{''.join(sections_html)}"
        # Footer
        f'<section style="text-align:center;padding:16px 0 8px 0;">'
        f'<section style="border-top:1px solid #e2e8f0;'
        f'padding-top:12px;margin:0 20px;">'
        f'<p style="font-size:12px;color:#a0aec0;margin:0;">'
        f"飞行播客 · 运输航空新闻精选</p>"
        f'<p style="font-size:11px;color:#cbd5e0;'
        f'margin:4px 0 0 0;">'
        f"数据来源：民航局 / 航司 / 行业媒体 · 自动聚合 · 仅供参考</p>"
        f"</section></section>"
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

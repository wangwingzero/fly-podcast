from __future__ import annotations

import re
from datetime import datetime
from html import escape
from pathlib import Path
from urllib.parse import urlparse

from dateutil import parser as dt_parser

from flying_podcast.core.config import ensure_dirs, settings
from flying_podcast.core.io_utils import dump_json, load_json
from flying_podcast.core.logging_utils import get_logger
from flying_podcast.core.wechat import WeChatClient, WeChatPublishError

logger = get_logger("publish")

_TIER_LABEL = {"A": "核心来源", "B": "媒体来源", "C": "参考来源"}
_REGION_COLOR = {"domestic": "#30D158", "international": "#0A84FF"}
_REGION_LABEL = {"domestic": "国内", "international": "国际"}
_REGION_SECTION = {"domestic": "国内动态", "international": "国际动态"}
_IMPACT_GRADIENT = {
    "domestic": "linear-gradient(135deg,#0A84FF,#30D158)",
    "international": "linear-gradient(135deg,#0A84FF,#5E5CE6)",
}

_WEEKDAY_CN = ["星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日"]

_STRIP_IMPACT_RE = re.compile(r"^速评[：:]\s*")


def _publisher_domain(entry: dict) -> str:
    """Extract short publisher domain from entry metadata."""
    domain = entry.get("publisher_domain", "")
    if domain:
        return domain.removeprefix("www.")
    for url_field in ("canonical_url", "url"):
        raw = entry.get(url_field, "")
        if raw:
            host = urlparse(raw).netloc or ""
            if host and "news.google.com" not in host:
                return host.removeprefix("www.")
    citation = (entry.get("citations") or [""])[0]
    if citation:
        host = urlparse(citation).netloc or ""
        return host.removeprefix("www.")
    return ""


def _format_date_cn(date_str: str) -> str:
    """Format YYYY-MM-DD into '2026 年 2 月 17 日 · 星期二'."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        wd = _WEEKDAY_CN[dt.weekday()]
        return f"{dt.year} 年 {dt.month} 月 {dt.day} 日 · {wd}"
    except (ValueError, TypeError):
        return date_str


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
    """Generate Apple-style card-based HTML, mobile-first, WeChat compatible.

    Design features:
    - Section headers separating domestic / international news
    - Hero accent border on the first card of each section
    - Source domain + region pill on every card
    - Gradient 速评 badge, stripped "速评：" prefix from impact text
    - Left-accent facts block
    """
    date = digest["date"]
    dc = digest["domestic_count"]
    ic = digest["international_count"]
    entries = digest.get("entries", [])
    date_long = _format_date_cn(date)

    # Split entries by region, preserving order.
    domestic: list[tuple[int, dict]] = []
    international: list[tuple[int, dict]] = []
    for idx, entry in enumerate(entries, 1):
        region = entry.get("region", "international")
        if region == "domestic":
            domestic.append((idx, entry))
        else:
            international.append((idx, entry))

    def _build_card(idx: int, entry: dict, is_hero: bool = False) -> str:
        title = escape(entry["title"])
        impact_raw = entry.get("impact", "")
        impact = escape(_STRIP_IMPACT_RE.sub("", impact_raw))
        facts = entry.get("facts", [])
        region = entry.get("region", "international")
        region_label = _REGION_LABEL.get(region, "国际")
        region_color = _REGION_COLOR.get(region, "#0A84FF")
        citation = str((entry.get("citations") or [""])[0]).strip()
        safe_href = escape(citation, quote=True)
        domain = escape(_publisher_domain(entry))
        gradient = _IMPACT_GRADIENT.get(region, _IMPACT_GRADIENT["international"])

        # Date
        date_html = ""
        raw_pa = entry.get("published_at", "")
        if raw_pa:
            try:
                dt = dt_parser.parse(str(raw_pa))
                date_html = (
                    f'<p style="margin:5px 0 0 0;font-size:12px;color:#86868B;">'
                    f"{dt.month}月{dt.day}日 {dt.strftime('%H:%M')}</p>"
                )
            except (ValueError, TypeError):
                pass

        # Hero image
        image_html = ""
        image_url = entry.get("image_url", "")
        if image_url:
            safe_img = escape(image_url, quote=True)
            image_html = (
                f'<img src="{safe_img}" style="width:100%;height:auto;'
                f"border-radius:10px;margin:10px 0 0 0;display:block;"
                f'object-fit:contain;" />'
            )

        # Facts
        facts_html = ""
        if facts:
            items = "".join(
                f'<p style="margin:0 0 5px 0;font-size:13.5px;'
                f'color:#48484A;line-height:1.6;">{escape(f)}</p>'
                for f in facts
            )
            facts_html = (
                f'<section style="margin:12px 0 0 0;padding:10px 12px;'
                f"background:#F5F5F7;border-radius:10px;"
                f'border-left:3px solid #E5E5EA;">'
                f"{items}</section>"
            )

        # Impact badge
        impact_html = ""
        if impact:
            impact_html = (
                f'<p style="margin:10px 0 0 0;font-size:13px;'
                f'color:#636366;line-height:1.55;">'
                f'<span style="display:inline-block;background:{gradient};'
                f"color:#FFF;font-size:10px;padding:2px 7px;border-radius:4px;"
                f'margin-right:5px;font-weight:600;">速评</span>'
                f"{impact}</p>"
            )

        # Read-more link
        source_meta = ""
        if citation:
            source_meta = (
                f'<section style="margin:12px 0 0 0;text-align:right;">'
                f'<a href="{safe_href}" style="font-size:12px;color:#0A84FF;'
                f'text-decoration:none;font-weight:500;">阅读原文 →</a>'
                f"</section>"
            )

        # Title (linked or plain)
        title_size = "17px" if is_hero else "16px"
        title_html = title
        if citation:
            title_html = (
                f'<a href="{safe_href}" style="color:#1D1D1F;'
                f'text-decoration:none;">{title}</a>'
            )

        # Number badge
        if is_hero:
            num_badge = (
                f'<span style="display:inline-flex;align-items:center;'
                f"justify-content:center;width:22px;height:22px;"
                f"border-radius:6px;background:{region_color};"
                f'color:#FFF;font-size:11px;font-weight:700;">{idx}</span>'
            )
        else:
            num_badge = (
                f'<span style="display:inline-flex;align-items:center;'
                f"justify-content:center;width:22px;height:22px;"
                f"border-radius:6px;background:#F2F2F7;"
                f'color:#86868B;font-size:11px;font-weight:700;">{idx}</span>'
            )

        # Top border accent for hero card
        border_top = f"border-top:3px solid {region_color};" if is_hero else ""

        card = (
            f'<section style="background:#FFFFFF;border-radius:14px;'
            f"padding:18px;margin-bottom:10px;"
            f"box-shadow:0 1px 3px rgba(0,0,0,0.06);"
            f'{border_top}">'
            # Meta row: number badge + domain + region pill
            f'<section style="display:flex;align-items:center;'
            f'gap:6px;margin:0 0 8px 0;">'
            f"{num_badge}"
            f'<span style="font-size:11px;color:#86868B;'
            f'font-weight:500;">{domain}</span>'
            f'<span style="margin-left:auto;font-size:10px;color:#FFFFFF;'
            f"background:{region_color};padding:2px 7px;"
            f'border-radius:4px;font-weight:500;">{region_label}</span>'
            f"</section>"
            # Title
            f'<p style="margin:0;font-size:{title_size};font-weight:600;'
            f'color:#1D1D1F;line-height:1.5;">{title_html}</p>'
            f"{date_html}"
            f"{image_html}"
            f"{facts_html}"
            f"{impact_html}"
            f"{source_meta}"
            f"</section>"
        )
        return card

    def _section_header(label: str, color: str) -> str:
        return (
            f'<section style="display:flex;align-items:center;'
            f'gap:8px;padding:4px 8px 10px 8px;">'
            f'<span style="display:inline-block;width:3px;height:14px;'
            f'border-radius:2px;background:{color};"></span>'
            f'<span style="font-size:13px;font-weight:600;'
            f'color:#1D1D1F;letter-spacing:0.5px;">{label}</span>'
            f'<span style="flex:1;height:1px;background:#E5E5EA;"></span>'
            f"</section>"
        )

    # Build sections
    parts: list[str] = []

    if domestic:
        parts.append(
            f'<section style="padding:0 12px;">'
            f'{_section_header(_REGION_SECTION["domestic"], _REGION_COLOR["domestic"])}'
        )
        for i, (idx, entry) in enumerate(domestic):
            parts.append(_build_card(idx, entry, is_hero=(i == 0)))
        parts.append("</section>")

    if international:
        top_pad = "14px" if domestic else "0"
        parts.append(
            f'<section style="padding:0 12px;">'
            f'<section style="padding:{top_pad} 0 0 0;"></section>'
            f'{_section_header(_REGION_SECTION["international"], _REGION_COLOR["international"])}'
        )
        for i, (idx, entry) in enumerate(international):
            parts.append(_build_card(idx, entry, is_hero=(i == 0)))
        parts.append("</section>")

    html = (
        # Outer wrapper
        f'<section style="padding:0;margin:0 auto;max-width:420px;'
        f"font-family:-apple-system,BlinkMacSystemFont,"
        f"'SF Pro Display','PingFang SC','Helvetica Neue',"
        f"'Microsoft YaHei',sans-serif;"
        f'background:#F2F2F7;-webkit-font-smoothing:antialiased;">'
        # Header
        f'<section style="padding:36px 20px 24px 20px;text-align:center;">'
        f'<section style="width:40px;height:3px;'
        f"background:linear-gradient(90deg,#0A84FF,#30D158);"
        f'margin:0 auto 18px auto;border-radius:2px;"></section>'
        f'<p style="margin:0;font-size:10px;font-weight:600;'
        f"color:#86868B;letter-spacing:4px;"
        f'text-transform:uppercase;">FLYING PODCAST</p>'
        f'<p style="margin:6px 0 0 0;font-size:26px;font-weight:700;'
        f'color:#1D1D1F;letter-spacing:-0.3px;line-height:1.2;">每日航空简报</p>'
        f'<p style="margin:8px 0 0 0;font-size:13px;'
        f'color:#86868B;font-weight:400;">{date_long}</p>'
        f'<section style="margin:16px auto 0 auto;display:flex;'
        f'justify-content:center;gap:20px;">'
        # Domestic pill
        f'<span style="display:inline-flex;align-items:center;'
        f"font-size:12px;color:#1D1D1F;background:#FFFFFF;"
        f"padding:4px 10px;border-radius:20px;"
        f'box-shadow:0 1px 2px rgba(0,0,0,0.06);">'
        f'<span style="display:inline-block;width:6px;height:6px;'
        f'border-radius:50%;background:#30D158;margin-right:5px;"></span>'
        f"国内 {dc}</span>"
        # International pill
        f'<span style="display:inline-flex;align-items:center;'
        f"font-size:12px;color:#1D1D1F;background:#FFFFFF;"
        f"padding:4px 10px;border-radius:20px;"
        f'box-shadow:0 1px 2px rgba(0,0,0,0.06);">'
        f'<span style="display:inline-block;width:6px;height:6px;'
        f'border-radius:50%;background:#0A84FF;margin-right:5px;"></span>'
        f"国际 {ic}</span>"
        f"</section>"
        f"</section>"
        # Cards by section
        f"{''.join(parts)}"
        # Footer
        f'<section style="padding:24px 20px 36px 20px;text-align:center;">'
        f'<section style="width:40px;height:3px;'
        f"background:linear-gradient(90deg,#0A84FF,#30D158);"
        f'margin:0 auto 14px auto;border-radius:2px;"></section>'
        f'<p style="margin:0;font-size:12px;color:#86868B;'
        f'line-height:1.6;font-weight:500;">飞行播客 · 运输航空新闻精选</p>'
        f'<p style="margin:6px 0 0 0;font-size:11px;color:#AEAEB2;'
        f'line-height:1.5;">数据来源：民航局 / 航司 / 行业媒体'
        f"<br/>自动聚合 · 仅供参考</p>"
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

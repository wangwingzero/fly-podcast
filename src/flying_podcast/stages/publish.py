from __future__ import annotations

import re
from datetime import datetime
from html import escape
from pathlib import Path
from urllib.parse import urlparse

import requests
from dateutil import parser as dt_parser

from flying_podcast.core.config import ensure_dirs, settings
from flying_podcast.core.image_gen import generate_article_image
from flying_podcast.core.io_utils import dump_json, load_json
from flying_podcast.core.logging_utils import get_logger
from flying_podcast.core.wechat import WeChatClient, WeChatPublishError

logger = get_logger("publish")

_TIER_LABEL = {"A": "核心来源", "B": "媒体来源", "C": "参考来源"}
_REGION_COLOR = {"domestic": "#30D158", "international": "#0A84FF"}
_REGION_LABEL = {"domestic": "国内", "international": "国际"}
_REGION_SECTION = {"domestic": "国内动态", "international": "国际动态"}
_WEEKDAY_CN = ["星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日"]


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
        region = "国内" if entry.get("region") == "domestic" else "国际"
        citation = (entry.get("citations") or [""])[0]
        has_link = bool(citation)

        if has_link:
            lines.append(f"### {idx}. [{title}]({citation})")
        else:
            lines.append(f"### {idx}. {title}")
        body = entry.get("body", "")
        if body:
            lines.append(f"\n{body}")
        else:
            facts = entry.get("facts", [])
            if facts:
                for f in facts:
                    lines.append(f"- {f}")
        lines.append(f"- 来源：{region}")
        lines.append("")
    return "\n".join(lines)


def _render_html(digest: dict) -> str:
    """Generate Apple-style card-based HTML, mobile-first, WeChat compatible.

    Design features:
    - Section headers separating domestic / international news
    - Hero accent border on the first card of each section
    - Clickable title linking to source
    - Journalist-style body paragraph for reading and listening
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
        region = entry.get("region", "international")
        region_label = _REGION_LABEL.get(region, "国际")
        region_color = _REGION_COLOR.get(region, "#0A84FF")
        citation = str((entry.get("citations") or [""])[0]).strip()
        safe_href = escape(citation, quote=True)

        # Date
        date_html = ""
        raw_pa = entry.get("published_at", "")
        if raw_pa:
            try:
                dt = dt_parser.parse(str(raw_pa))
                date_html = (
                    f'<p style="margin:5px 0 0 0;font-size:12px;color:#6E6E73;">'
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

        # Body paragraph (fall back to joining facts)
        body_text = entry.get("body", "")
        if not body_text:
            facts = entry.get("facts", [])
            if facts:
                body_text = "".join(
                    f if f.rstrip().endswith(("。", ".", "!", "?", "！", "？"))
                    else f + "。"
                    for f in facts if f
                )
        body_html = ""
        if body_text:
            body_html = (
                f'<p style="margin:12px 0 0 0;font-size:14px;'
                f'color:#333333;line-height:1.75;">'
                f"{escape(body_text)}</p>"
            )

        # Title (plain text — WeChat personal accounts strip <a> tags)
        title_size = "17px" if is_hero else "16px"
        title_html = title

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
                f'color:#6E6E73;font-size:11px;font-weight:700;">{idx}</span>'
            )

        # Top border accent for hero card
        border_top = f"border-top:3px solid {region_color};" if is_hero else ""

        card = (
            f'<section style="background:#FFFFFF;border-radius:14px;'
            f"padding:18px;margin-bottom:10px;"
            f"box-shadow:0 1px 3px rgba(0,0,0,0.06);"
            f'{border_top}">'
            # Meta row: number badge + region pill
            f'<section style="display:flex;align-items:center;'
            f'gap:6px;margin:0 0 8px 0;">'
            f"{num_badge}"
            f'<span style="margin-left:auto;font-size:10px;color:#FFFFFF;'
            f"background:{region_color};padding:2px 7px;"
            f'border-radius:4px;font-weight:500;">{region_label}</span>'
            f"</section>"
            # Title
            f'<p style="margin:0;font-size:{title_size};font-weight:600;'
            f'color:#1D1D1F;line-height:1.5;">{title_html}</p>'
            f"{date_html}"
            f"{image_html}"
            f"{body_html}"
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
        f"color:#6E6E73;letter-spacing:4px;"
        f'text-transform:uppercase;">FLYING PODCAST</p>'
        f'<p style="margin:6px 0 0 0;font-size:26px;font-weight:700;'
        f'color:#1D1D1F;letter-spacing:-0.3px;line-height:1.2;">每日航空简报</p>'
        f'<p style="margin:8px 0 0 0;font-size:13px;'
        f'color:#6E6E73;font-weight:400;">{date_long}</p>'
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
        f'<p style="margin:0;font-size:12px;color:#6E6E73;'
        f'line-height:1.6;font-weight:500;">飞行播客 · 运输航空新闻精选</p>'
        f'<p style="margin:6px 0 0 0;font-size:11px;color:#AEAEB2;'
        f'line-height:1.5;">数据来源：民航局 / 航司 / 行业媒体'
        f"<br/>内容请看原文 · 仅供参考</p>"
        f"</section>"
        f"</section>"
    )
    return html


_WEB_INTRO_PROMPT = (
    "你是资深航空新闻编辑。根据以下今日航空新闻列表，写一段3-5句话的导读概述。"
    "要求：\n"
    "1. 专业、简洁、有信息量\n"
    "2. 概括今日最重要的2-3条核心要点\n"
    "3. 语气类似新闻联播导语，客观中立\n"
    "4. 不要用'本期'、'本日'开头，直接说内容\n"
    "5. 不超过150字\n"
    "只输出这段导读，不要任何其他内容。\n\n今日新闻：\n{entries}"
)


def _generate_web_intro(digest: dict) -> str:
    """Use LLM to generate a 3-5 sentence overview for the web digest page."""
    if not settings.llm_api_key:
        return ""

    parts = []
    for e in digest.get("entries", [])[:10]:
        title = e.get("title", "")
        body = e.get("body", "") or ""
        if not body:
            facts = e.get("facts", [])
            body = "；".join(facts[:3]) if facts else ""
        parts.append(f"- {title}：{body[:100]}")
    entries_text = "\n".join(parts)
    if not entries_text:
        return ""

    try:
        resp = requests.post(
            settings.llm_base_url,
            headers={
                "Authorization": f"Bearer {settings.llm_api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": settings.llm_model,
                "messages": [{"role": "user", "content": _WEB_INTRO_PROMPT.format(entries=entries_text)}],
                "max_tokens": 300,
                "temperature": 0.5,
            },
            timeout=30,
        )
        resp.raise_for_status()
        text = (resp.json()["choices"][0]["message"].get("content") or "").strip()
        if text and len(text) <= 300:
            logger.info("Web intro generated: %s", text[:60])
            return text
    except Exception as exc:
        logger.warning("Web intro generation failed: %s", exc)

    return ""


def _web_summary_block(summary: str, intro: str) -> str:
    """Build the summary + intro HTML block for the web page header."""
    if not summary and not intro:
        return ""

    parts = []
    parts.append(
        '<section style="padding:0 20px 12px 20px;">'
        '<section style="background:#FFFFFF;border-radius:14px;'
        'padding:16px 18px;box-shadow:0 1px 3px rgba(0,0,0,0.06);">'
    )

    if summary:
        parts.append(
            f'<p style="margin:0;font-size:15px;font-weight:600;'
            f'color:#1D1D1F;line-height:1.6;text-align:center;">'
            f'{escape(summary)}</p>'
        )

    if intro:
        top_margin = "12px" if summary else "0"
        parts.append(
            f'<p style="margin:{top_margin} 0 0 0;font-size:13px;'
            f'color:#6E6E73;line-height:1.8;">'
            f'{escape(intro)}</p>'
        )

    parts.append('</section></section>')
    return "".join(parts)


def _render_web_html(digest: dict, summary: str = "", intro: str = "") -> str:
    """Generate standalone HTML page with clickable links for hosting on external domain.

    Same Apple-style card design as _render_html(), but:
    - Full <!DOCTYPE html> standalone page
    - Titles wrapped in <a> tags linking to citation URLs
    - Source domain shown on each card
    - Optional LLM-generated summary and intro paragraph
    """
    date = digest["date"]
    dc = digest["domestic_count"]
    ic = digest["international_count"]
    entries = digest.get("entries", [])
    date_long = _format_date_cn(date)

    domestic: list[tuple[int, dict]] = []
    international: list[tuple[int, dict]] = []
    for idx, entry in enumerate(entries, 1):
        region = entry.get("region", "international")
        if region == "domestic":
            domestic.append((idx, entry))
        else:
            international.append((idx, entry))

    def _build_web_card(idx: int, entry: dict, is_hero: bool = False) -> str:
        title = escape(entry["title"])
        region = entry.get("region", "international")
        region_label = _REGION_LABEL.get(region, "国际")
        region_color = _REGION_COLOR.get(region, "#0A84FF")
        citation = str((entry.get("citations") or [""])[0]).strip()
        domain = _publisher_domain(entry)

        # Date
        date_html = ""
        raw_pa = entry.get("published_at", "")
        if raw_pa:
            try:
                dt = dt_parser.parse(str(raw_pa))
                date_html = (
                    f'<p style="margin:5px 0 0 0;font-size:12px;color:#6E6E73;">'
                    f"{dt.month}月{dt.day}日 {dt.strftime('%H:%M')}</p>"
                )
            except (ValueError, TypeError):
                pass

        # Image
        image_html = ""
        image_url = entry.get("image_url", "")
        if image_url:
            safe_img = escape(image_url, quote=True)
            image_html = (
                f'<img src="{safe_img}" style="width:100%;height:auto;'
                f"border-radius:10px;margin:10px 0 0 0;display:block;"
                f'object-fit:contain;" />'
            )

        # Body
        body_text = entry.get("body", "")
        if not body_text:
            facts = entry.get("facts", [])
            if facts:
                body_text = "".join(
                    f if f.rstrip().endswith(("。", ".", "!", "?", "！", "？"))
                    else f + "。"
                    for f in facts if f
                )
        body_html = ""
        if body_text:
            body_html = (
                f'<p style="margin:12px 0 0 0;font-size:14px;'
                f'color:#333333;line-height:1.75;">'
                f"{escape(body_text)}</p>"
            )

        # Title — clickable link if citation exists
        title_size = "17px" if is_hero else "16px"
        if citation:
            safe_href = escape(citation, quote=True)
            title_html = (
                f'<a href="{safe_href}" target="_blank" rel="noopener" '
                f'style="color:#1D1D1F;text-decoration:none;'
                f'border-bottom:1px solid #D1D1D6;">{title}</a>'
            )
        else:
            title_html = title

        # Source domain link
        source_html = ""
        if domain:
            if citation:
                safe_href = escape(citation, quote=True)
                source_html = (
                    f'<p style="margin:8px 0 0 0;font-size:11px;color:#8E8E93;">'
                    f'来源：<a href="{safe_href}" target="_blank" rel="noopener" '
                    f'style="color:#0A84FF;text-decoration:none;">{escape(domain)}</a></p>'
                )
            else:
                source_html = (
                    f'<p style="margin:8px 0 0 0;font-size:11px;color:#8E8E93;">'
                    f"来源：{escape(domain)}</p>"
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
                f'color:#6E6E73;font-size:11px;font-weight:700;">{idx}</span>'
            )

        border_top = f"border-top:3px solid {region_color};" if is_hero else ""

        card = (
            f'<section style="background:#FFFFFF;border-radius:14px;'
            f"padding:18px;margin-bottom:10px;"
            f"box-shadow:0 1px 3px rgba(0,0,0,0.06);"
            f'{border_top}">'
            f'<section style="display:flex;align-items:center;'
            f'gap:6px;margin:0 0 8px 0;">'
            f"{num_badge}"
            f'<span style="margin-left:auto;font-size:10px;color:#FFFFFF;'
            f"background:{region_color};padding:2px 7px;"
            f'border-radius:4px;font-weight:500;">{region_label}</span>'
            f"</section>"
            f'<p style="margin:0;font-size:{title_size};font-weight:600;'
            f'color:#1D1D1F;line-height:1.5;">{title_html}</p>'
            f"{date_html}"
            f"{image_html}"
            f"{body_html}"
            f"{source_html}"
            f"</section>"
        )
        return card

    def _web_section_header(label: str, color: str) -> str:
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
            f'{_web_section_header(_REGION_SECTION["domestic"], _REGION_COLOR["domestic"])}'
        )
        for i, (idx, entry) in enumerate(domestic):
            parts.append(_build_web_card(idx, entry, is_hero=(i == 0)))
        parts.append("</section>")

    if international:
        top_pad = "14px" if domestic else "0"
        parts.append(
            f'<section style="padding:0 12px;">'
            f'<section style="padding:{top_pad} 0 0 0;"></section>'
            f'{_web_section_header(_REGION_SECTION["international"], _REGION_COLOR["international"])}'
        )
        for i, (idx, entry) in enumerate(international):
            parts.append(_build_web_card(idx, entry, is_hero=(i == 0)))
        parts.append("</section>")

    body = (
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
        f"color:#6E6E73;letter-spacing:4px;"
        f'text-transform:uppercase;">FLYING PODCAST</p>'
        f'<p style="margin:6px 0 0 0;font-size:26px;font-weight:700;'
        f'color:#1D1D1F;letter-spacing:-0.3px;line-height:1.2;">每日航空简报</p>'
        f'<p style="margin:8px 0 0 0;font-size:13px;'
        f'color:#6E6E73;font-weight:400;">{date_long}</p>'
        f'<section style="margin:16px auto 0 auto;display:flex;'
        f'justify-content:center;gap:20px;">'
        f'<span style="display:inline-flex;align-items:center;'
        f"font-size:12px;color:#1D1D1F;background:#FFFFFF;"
        f"padding:4px 10px;border-radius:20px;"
        f'box-shadow:0 1px 2px rgba(0,0,0,0.06);">'
        f'<span style="display:inline-block;width:6px;height:6px;'
        f'border-radius:50%;background:#30D158;margin-right:5px;"></span>'
        f"国内 {dc}</span>"
        f'<span style="display:inline-flex;align-items:center;'
        f"font-size:12px;color:#1D1D1F;background:#FFFFFF;"
        f"padding:4px 10px;border-radius:20px;"
        f'box-shadow:0 1px 2px rgba(0,0,0,0.06);">'
        f'<span style="display:inline-block;width:6px;height:6px;'
        f'border-radius:50%;background:#0A84FF;margin-right:5px;"></span>'
        f"国际 {ic}</span>"
        f"</section>"
        f"</section>"
        # Summary + Intro block
        f"{_web_summary_block(summary, intro)}"
        # Cards
        f"{''.join(parts)}"
        # Footer
        f'<section style="padding:24px 20px 36px 20px;text-align:center;">'
        f'<section style="width:40px;height:3px;'
        f"background:linear-gradient(90deg,#0A84FF,#30D158);"
        f'margin:0 auto 14px auto;border-radius:2px;"></section>'
        f'<p style="margin:0;font-size:12px;color:#6E6E73;'
        f'line-height:1.6;font-weight:500;">飞行播客 · 运输航空新闻精选</p>'
        f'<p style="margin:6px 0 0 0;font-size:11px;color:#AEAEB2;'
        f'line-height:1.5;">数据来源：民航局 / 航司 / 行业媒体'
        f"<br/>点击标题可查看原文出处</p>"
        f"</section>"
        f"</section>"
    )

    return (
        f"<!DOCTYPE html>\n"
        f'<html lang="zh-CN">\n<head>\n'
        f'<meta charset="UTF-8">\n'
        f'<meta name="viewport" content="width=device-width,initial-scale=1.0">\n'
        f"<title>飞行播客日报 | {escape(date)}</title>\n"
        f"<style>body{{margin:0;padding:0;background:#F2F2F7;}}"
        f"a:hover{{opacity:0.7;}}</style>\n"
        f"</head>\n<body>\n{body}\n</body>\n</html>"
    )


def _fill_missing_images(digest: dict, client: WeChatClient) -> dict:
    """Generate AI images for entries that have no image_url, upload to WeChat CDN."""
    if not settings.image_gen_api_key:
        return digest

    token = client._access_token()
    entries = digest.get("entries", [])

    for entry in entries:
        if entry.get("image_url"):
            continue
        title = entry.get("title", "")
        body = entry.get("body", "") or ""
        if not body:
            facts = entry.get("facts", [])
            body = " ".join(facts) if facts else ""

        image_data = generate_article_image(title, body)
        if not image_data:
            logger.info("AI image generation failed for: %s", title[:40])
            continue

        wx_url = client.upload_content_image_bytes(image_data, token=token)
        if wx_url:
            # Ensure https
            wx_url = wx_url.replace("http://mmbiz.qpic.cn", "https://mmbiz.qpic.cn")
            entry["image_url"] = wx_url
            logger.info("AI image set for: %s", title[:40])
        else:
            logger.info("AI image upload failed for: %s", title[:40])

    return digest


_DIGEST_SUMMARY_PROMPT = (
    "你是航空新闻编辑。根据以下新闻标题列表，用一句话（不超过30个字）概括今天的播报要点。"
    "要求：简洁有信息量，像新闻导语，不要用'本期'、'今日'等开头，直接说内容。"
    "只输出这一句话，不要任何其他内容。\n\n标题列表：\n{titles}"
)


def _generate_digest_summary(digest: dict) -> str:
    """Use LLM to generate a one-line summary of today's digest."""
    fallback = "今日热点精选速读"
    if not settings.llm_api_key:
        return fallback

    titles = "\n".join(
        f"- {e.get('title', '')}" for e in digest.get("entries", []) if e.get("title")
    )
    if not titles:
        return fallback

    try:
        resp = requests.post(
            settings.llm_base_url,
            headers={
                "Authorization": f"Bearer {settings.llm_api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": settings.llm_model,
                "messages": [{"role": "user", "content": _DIGEST_SUMMARY_PROMPT.format(titles=titles)}],
                "max_tokens": 80,
                "temperature": 0.7,
            },
            timeout=30,
        )
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"].get("content") or ""
        text = raw.strip().strip("\"'""''")
        if text and len(text) <= 60:
            logger.info("Digest summary: %s", text)
            return text
    except Exception as exc:
        logger.warning("Digest summary generation failed: %s", exc)

    return fallback


_COVER_PROMPT_TEMPLATE = (
    "你是航空新闻视觉编辑。根据以下新闻摘要，写一段英文 Grok AI 画图提示词，"
    "用于生成今天航空日报的封面图。要求：\n"
    "1. 宽屏构图(16:9)，高端大气的航空主题\n"
    "2. 摄影级画质，电影感光影，专业航空杂志风格\n"
    "3. 不要文字、水印、UI元素\n"
    "4. 结合今天新闻的核心主题选择场景（如机场、客机、驾驶舱、航线等）\n"
    "只输出英文提示词，不要任何其他内容。\n\n今日新闻：\n{summary}"
)


def _generate_cover_image(digest: dict, client: WeChatClient) -> str:
    """Generate a cover image via LLM prompt + Grok, upload as permanent material.

    Returns thumb_media_id, or empty string on failure.
    """
    if not settings.llm_api_key or not settings.image_gen_api_key:
        return ""

    # Build summary from titles + bodies
    parts = []
    for e in digest.get("entries", [])[:10]:
        title = e.get("title", "")
        body = e.get("body", "") or ""
        parts.append(f"- {title}: {body[:80]}")
    summary = "\n".join(parts)
    if not summary:
        return ""

    # Step 1: Ask LLM to write a Grok prompt
    try:
        resp = requests.post(
            settings.llm_base_url,
            headers={
                "Authorization": f"Bearer {settings.llm_api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": settings.llm_model,
                "messages": [{"role": "user", "content": _COVER_PROMPT_TEMPLATE.format(summary=summary)}],
                "max_tokens": 300,
                "temperature": 0.7,
            },
            timeout=30,
        )
        resp.raise_for_status()
        prompt = (resp.json()["choices"][0]["message"].get("content") or "").strip()
        logger.info("Cover prompt: %s", prompt[:120])
    except Exception as exc:
        logger.warning("Cover prompt generation failed: %s", exc)
        return ""

    # Step 2: Call Grok to generate image
    from flying_podcast.core.image_gen import _call_grok_api

    image_data = _call_grok_api(
        settings.image_gen_base_url,
        settings.image_gen_api_key,
        settings.image_gen_model,
        prompt,
    )
    if not image_data and settings.image_gen_backup_api_key:
        logger.info("Cover: primary Grok failed, trying backup")
        image_data = _call_grok_api(
            settings.image_gen_backup_base_url,
            settings.image_gen_backup_api_key,
            settings.image_gen_backup_model,
            prompt,
        )
    if not image_data:
        logger.warning("Cover image generation failed")
        return ""

    # Step 3: Upload as permanent material
    thumb_id = client.upload_thumb_image_bytes(image_data)
    if thumb_id:
        logger.info("Cover image uploaded: %s", thumb_id[:40])
    return thumb_id


def run(target_date: str | None = None) -> Path:
    ensure_dirs()
    day = target_date or datetime.now().strftime("%Y-%m-%d")
    digest = load_json(settings.processed_dir / f"composed_{day}.json")
    quality = load_json(settings.processed_dir / f"quality_{day}.json")

    md = _render_markdown(digest)
    html = _render_html(digest)

    # Generate LLM content for web version (works in all modes, only needs LLM key)
    summary = _generate_digest_summary(digest)
    intro = _generate_web_intro(digest)

    # Initial web render (without AI images, used for dry_run/hold)
    web_html = _render_web_html(digest, summary=summary, intro=intro)

    # Build web URL for content_source_url
    web_filename = f"web_{day}.html"
    base = settings.web_digest_base_url.rstrip("/")
    web_url = f"{base}/{web_filename}" if base else ""

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

    if quality["decision"] != "auto_publish" and not settings.wechat_force_publish:
        result["status"] = "hold"
    elif settings.dry_run or not settings.wechat_enable_publish:
        result["status"] = "dry_run"
        result["url"] = f"dry-run://flying-podcast/{day}"
    else:
        client = WeChatClient()
        try:
            # Fill missing images with AI-generated ones
            digest = _fill_missing_images(digest, client)
            # Re-render HTML with new images
            html = _render_html(digest)
            html = client.replace_external_images(html)
            # Re-render web HTML with AI images + LLM content
            web_html = _render_web_html(digest, summary=summary, intro=intro)
            # Generate AI cover image, fallback to default thumb
            cover_thumb_id = _generate_cover_image(digest, client)
            media_id = client.create_draft(
                title=f"飞行播客日报 | {day}",
                author=settings.wechat_author,
                content_html=html,
                digest=summary,
                source_url=web_url or "https://mp.weixin.qq.com",
                thumb_media_id=cover_thumb_id,
            )
            result["status"] = "draft_created"
            result["publish_id"] = media_id
            result["url"] = f"https://mp.weixin.qq.com"
            try:
                publish = client.publish_draft(media_id)
                result["status"] = "published"
                result["publish_id"] = publish.publish_id
                result["url"] = f"wechat://publish/{publish.publish_id}"
            except WeChatPublishError:
                logger.info("Auto-publish not available, draft saved to WeChat backend")
        except WeChatPublishError as exc:
            result["status"] = "failed"
            result["reasons"].append(str(exc))

    # Save standalone web page for "阅读原文"
    web_path = settings.output_dir / web_filename
    web_path.write_text(web_html, encoding="utf-8")
    logger.info("Web version saved: %s", web_path)

    out = settings.output_dir / f"publish_{day}.json"
    dump_json(out, result)

    # Persist human-readable draft for audit.
    dump_json(settings.output_dir / f"draft_{day}.json", {
        "markdown": md,
        "html": html,
        "web_html": web_html,
        "web_url": web_url,
    })

    logger.info("Publish done. status=%s score=%.2f", result["status"], quality["total_score"])
    return out


if __name__ == "__main__":
    run()

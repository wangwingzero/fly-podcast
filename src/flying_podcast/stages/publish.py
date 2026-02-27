from __future__ import annotations

import json
import base64
import re
import time
from datetime import datetime
from html import escape
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from dateutil import parser as dt_parser

from flying_podcast.core.config import ensure_dirs, settings
from flying_podcast.core.image_gen import generate_article_image, search_public_image_url
from flying_podcast.core.io_utils import dump_json, load_json
from flying_podcast.core.logging_utils import get_logger
from flying_podcast.core.time_utils import beijing_now, beijing_today_str
from flying_podcast.core.wechat import WeChatClient, WeChatPublishError

logger = get_logger("publish")


def _extract_llm_text(data: dict) -> str:
    """Extract text content from an LLM response (OpenAI or Anthropic format).

    Handles multiple content formats: string, list of content blocks, dict, None.
    Mirrors the robust extraction logic in llm_client.py.
    """
    # Anthropic native format: {"content": [{"type": "text", "text": "..."}]}
    if "content" in data and "choices" not in data:
        content_blocks = data.get("content") or []
        if isinstance(content_blocks, list):
            parts = []
            for block in content_blocks:
                if isinstance(block, dict) and block.get("type") == "text":
                    parts.append(block.get("text", ""))
                elif isinstance(block, str):
                    parts.append(block)
            return "\n".join([x for x in parts if x.strip()])

    # OpenAI format
    choices = data.get("choices") or []
    if not choices:
        return ""
    msg = choices[0].get("message") or {}
    content = msg.get("content")

    if content is None:
        # Legacy: some providers put text at choice.text
        content = choices[0].get("text") or ""

    if isinstance(content, list):
        text_parts: list[str] = []
        for block in content:
            if isinstance(block, str):
                text_parts.append(block)
            elif isinstance(block, dict):
                if isinstance(block.get("text"), str):
                    text_parts.append(block["text"])
                elif block.get("type") == "text" and isinstance(block.get("content"), str):
                    text_parts.append(block["content"])
        content = "\n".join([x for x in text_parts if x.strip()])
    elif isinstance(content, dict):
        content = content.get("text") or content.get("content") or ""

    return str(content).strip() if content else ""


def _llm_chat(messages: list[dict], max_tokens: int = 200,
              temperature: float = 0.5, retries: int = 2,
              timeout: int = 30) -> str:
    """Make an LLM chat completion request with retry and robust extraction.

    Auto-detects Anthropic native API (sk-ant- key) vs OpenAI-compatible format.
    Returns the extracted text content, or "" on failure.
    """
    if not settings.llm_api_key:
        return ""

    is_anthropic = settings.llm_api_key.startswith("sk-ant-")

    for attempt in range(1, retries + 1):
        try:
            if is_anthropic:
                # Anthropic native Messages API
                base = settings.llm_base_url.rstrip("/")
                if base.endswith("/messages"):
                    url = base
                elif base.endswith("/v1"):
                    url = f"{base}/messages"
                else:
                    url = f"{base}/v1/messages"
                headers = {
                    "x-api-key": settings.llm_api_key,
                    "anthropic-version": "2023-06-01",
                    "Content-Type": "application/json",
                }
                # Extract system prompt from messages
                system_text = ""
                user_messages = []
                for m in messages:
                    if m.get("role") == "system":
                        system_text += m.get("content", "") + "\n"
                    else:
                        user_messages.append(m)
                body: dict[str, Any] = {
                    "model": settings.llm_model,
                    "max_tokens": max_tokens,
                    "temperature": temperature,
                    "messages": user_messages or [{"role": "user", "content": ""}],
                }
                if system_text.strip():
                    body["system"] = system_text.strip()
            else:
                # OpenAI-compatible
                url = settings.llm_base_url
                headers = {
                    "Authorization": f"Bearer {settings.llm_api_key}",
                    "Content-Type": "application/json",
                }
                body = {
                    "model": settings.llm_model,
                    "messages": messages,
                    "max_tokens": max_tokens,
                    "temperature": temperature,
                }

            resp = requests.post(url, headers=headers, json=body, timeout=timeout)
            resp.raise_for_status()
            text = _extract_llm_text(resp.json())
            if text:
                return text
            logger.warning("LLM returned empty content (attempt %d/%d)", attempt, retries)
        except Exception as exc:
            logger.warning("LLM request failed (attempt %d/%d): %s", attempt, retries, exc)
        if attempt < retries:
            time.sleep(min(2 ** attempt, 8))

    return ""


_TIER_LABEL = {"A": "Core", "B": "Media", "C": "Reference"}
_ACCENT_COLOR = "#0A84FF"
_WEEKDAY_CN = ["星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日"]
_GOOGLE_NEWS_HOSTS = {"news.google.com"}
_WECHAT_IMAGE_HOST_SUFFIXES = (
    "mmbiz.qpic.cn",
    "mmbiz.qlogo.cn",
    "mmbiz.qpic.cn.cn",
)
_MPS_BEIAN_URL = "https://beian.mps.gov.cn/#/query/webSearch?code=31011502405233"
_COMMENT_PREFIX = "划重点："


def _format_body_html(body_text: str) -> str:
    """Format article body with styled editorial comment if present.

    Splits on '划重点：' and renders the comment in a distinct accent style.
    """
    if not body_text:
        return ""
    if _COMMENT_PREFIX in body_text:
        parts = body_text.split(_COMMENT_PREFIX, 1)
        main_text = parts[0].strip()
        comment_text = parts[1].strip()
        html = ""
        if main_text:
            html += (
                f'<p style="margin:12px 0 0 0;font-size:14px;'
                f'color:#333333;line-height:1.75;">'
                f"{escape(main_text)}</p>"
            )
        if comment_text:
            html += (
                f'<p style="margin:8px 0 0 0;font-size:13px;'
                f"color:#6E6E73;line-height:1.7;padding:8px 12px;"
                f"background:#F2F2F7;border-radius:8px;"
                f'border-left:3px solid {_ACCENT_COLOR};">'
                f'<span style="font-weight:600;color:{_ACCENT_COLOR};">划重点</span>'
                f"｜{escape(comment_text)}</p>"
            )
        return html
    return (
        f'<p style="margin:12px 0 0 0;font-size:14px;'
        f'color:#333333;line-height:1.75;">'
        f"{escape(body_text)}</p>"
    )


def _is_google_news_url(url: str) -> bool:
    try:
        parsed = urlparse(str(url))
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
        return host in _GOOGLE_NEWS_HOSTS and path.startswith("/rss/articles/")
    except Exception:  # noqa: BLE001
        return False


def _is_blocked_wechat_image(url: str) -> bool:
    try:
        host = (urlparse(str(url)).netloc or "").lower()
        return any(host.endswith(sfx) for sfx in _WECHAT_IMAGE_HOST_SUFFIXES)
    except Exception:  # noqa: BLE001
        return False


def _resolve_google_news_url(url: str) -> str:
    """Resolve a Google News RSS article redirect URL to final source URL."""
    if not _is_google_news_url(url):
        return url
    try:
        resp = requests.get(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
                    "AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148"
                )
            },
            allow_redirects=True,
            timeout=12,
        )
        final_url = (resp.url or "").strip()
        if final_url and not _is_google_news_url(final_url):
            return final_url
    except Exception as exc:  # noqa: BLE001
        logger.info("Resolve google redirect failed: %s", exc)
    return ""


def _pick_click_url(entry: dict) -> str:
    citation = str((entry.get("citations") or [""])[0]).strip()
    candidates = [
        citation,
        str(entry.get("canonical_url", "")).strip(),
        str(entry.get("url", "")).strip(),
    ]
    for raw in candidates:
        if raw and not _is_google_news_url(raw):
            return raw
    for raw in candidates:
        if raw and _is_google_news_url(raw):
            resolved = _resolve_google_news_url(raw)
            if resolved:
                return resolved
    return ""


def _load_beian_icon_data_uri() -> str:
    """Load local public-security beian icon as data URI for stable rendering."""
    try:
        root = Path(__file__).resolve().parents[3]
        candidate_paths = [
            root / "static" / "beian_icon.png",
            root / "备案" / "备案图标.png",
        ]
        for icon_path in candidate_paths:
            if not icon_path.exists():
                continue
            raw = icon_path.read_bytes()
            if not raw:
                continue
            b64 = base64.b64encode(raw).decode("ascii")
            return f"data:image/png;base64,{b64}"
    except Exception:  # noqa: BLE001
        return ""
    return ""


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
    citation = _pick_click_url(entry)
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
    lines.append(f"{digest.get('article_count', len(digest.get('entries', [])))} articles")
    lines.append("")

    for idx, entry in enumerate(digest.get("entries", []), 1):
        title = entry["title"]
        citation = _pick_click_url(entry)
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
        lines.append(f"- Source: international")
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
    notice_url = str(digest.get("copyright_notice_url", "")).strip()
    entries = digest.get("entries", [])
    date_long = _format_date_cn(date)

    all_entries = [(idx, entry) for idx, entry in enumerate(entries, 1)]

    # Build TOC (plain list, no anchor links — WeChat doesn't support them)
    toc_rows: list[str] = []
    for idx, entry in all_entries:
        t = escape(entry["title"])
        toc_rows.append(
            f'<section style="display:flex;align-items:center;'
            f"gap:8px;padding:7px 0;"
            f'border-bottom:1px solid #F2F2F7;">'
            f'<span style="display:inline-flex;align-items:center;'
            f"justify-content:center;min-width:20px;height:20px;"
            f"border-radius:5px;background:#F2F2F7;"
            f'color:#6E6E73;font-size:10px;font-weight:700;">{idx}</span>'
            f'<span style="font-size:13px;line-height:1.4;'
            f'font-weight:500;color:#1D1D1F;flex:1;">{t}</span>'
            f"</section>"
        )
    if toc_rows:
        toc_rows[-1] = toc_rows[-1].replace(
            "border-bottom:1px solid #F2F2F7;", ""
        )
    toc_html = (
        f'<section style="padding:0 16px 10px 16px;">'
        f'<section style="background:#FFFFFF;border-radius:14px;'
        f"padding:14px 16px;box-shadow:0 1px 3px rgba(0,0,0,0.06);"
        f'margin-bottom:4px;">'
        f'<p style="margin:0 0 6px 0;font-size:11px;font-weight:600;'
        f"color:#6E6E73;letter-spacing:1px;"
        f'text-transform:uppercase;">目录 INDEX</p>'
        f"{''.join(toc_rows)}"
        f"</section></section>"
    ) if toc_rows else ""

    def _build_card(idx: int, entry: dict, is_hero: bool = False) -> str:
        title = escape(entry["title"])
        color = _ACCENT_COLOR
        citation = _pick_click_url(entry)
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
                f'<img src="{safe_img}" referrerpolicy="no-referrer" style="width:100%;height:auto;'
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
        body_html = _format_body_html(body_text)

        # Title (plain text — WeChat personal accounts strip <a> tags)
        title_size = "17px" if is_hero else "16px"
        title_html = title

        # Number badge
        if is_hero:
            num_badge = (
                f'<span style="display:inline-flex;align-items:center;'
                f"justify-content:center;width:22px;height:22px;"
                f"border-radius:6px;background:{color};"
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
        border_top = f"border-top:3px solid {color};" if is_hero else ""

        card = (
            f'<section style="background:#FFFFFF;border-radius:14px;'
            f"padding:18px;margin-bottom:10px;"
            f"box-shadow:0 1px 3px rgba(0,0,0,0.06);"
            f'{border_top}">'
            # Meta row: number badge
            f'<section style="display:flex;align-items:center;'
            f'gap:6px;margin:0 0 8px 0;">'
            f"{num_badge}"
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

    # Build cards section
    parts: list[str] = []
    if all_entries:
        parts.append(f'<section style="padding:0 12px;">')
        for i, (idx, entry) in enumerate(all_entries):
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
        f"background:linear-gradient(90deg,#0A84FF,#5AC8FA);"
        f'margin:0 auto 18px auto;border-radius:2px;"></section>'
        f'<p style="margin:0;font-size:10px;font-weight:600;'
        f"color:#6E6E73;letter-spacing:4px;"
        f'text-transform:uppercase;">GLOBAL AVIATION DIGEST</p>'
        f'<p style="margin:6px 0 0 0;font-size:22px;font-weight:700;'
        f'color:#1D1D1F;letter-spacing:-0.3px;line-height:1.2;">飞行播客日报</p>'
        f'<p style="margin:8px 0 0 0;font-size:13px;'
        f'color:#6E6E73;font-weight:400;">{date_long}</p>'
        f"</section>"
        # TOC
        f"{toc_html}"
        # Cards by section
        f"{''.join(parts)}"
        # Footer
        f'<section style="padding:24px 20px 36px 20px;text-align:center;">'
        f'<section style="width:40px;height:3px;'
        f"background:linear-gradient(90deg,#0A84FF,#5AC8FA);"
        f'margin:0 auto 14px auto;border-radius:2px;"></section>'
        f'<p style="margin:0;font-size:12px;color:#6E6E73;'
        f'line-height:1.6;font-weight:500;">飞行播客日报</p>'
        f'<p style="margin:6px 0 0 0;font-size:10px;color:#AEAEB2;'
        f'line-height:1.6;">版权归原作者及原发机构所有 · 仅供行业信息交流'
        f"<br/>如有版权疑问请联系我</p>"
        f"</section>"
        f"</section>"
    )
    return html


_WEB_INTRO_PROMPT = (
    "你是资深国际航空新闻编辑。根据以下今日国际航空新闻列表，写一段3-5句话的导读概述。"
    "要求：\n"
    "1. 专业、简洁、有信息量\n"
    "2. 概括今日最重要的2-3条核心要点\n"
    "3. 语气客观中立，适合专业航空从业者阅读\n"
    "4. 不要用'本期'、'本日'开头，直接说内容\n"
    "5. 外国航空公司名称保留英文原名\n"
    "6. 不超过150字\n"
    "只输出这段导读，不要任何其他内容。\n\n今日新闻：\n{entries}"
)


def _generate_web_intro(digest: dict) -> str:
    """Use LLM to generate a 3-5 sentence overview for the web digest page."""
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

    text = _llm_chat(
        messages=[{"role": "user", "content": _WEB_INTRO_PROMPT.format(entries=entries_text)}],
        max_tokens=300,
        temperature=0.5,
        timeout=30,
    )
    if text and len(text) <= 500:
        logger.info("Web intro generated: %s", text[:60])
        return text
    if text:
        logger.warning("Web intro too long (%d chars), using truncated", len(text))
        return text[:500]
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


_TRANSLATE_PROMPT = (
    "将以下英文航空新闻标题翻译成简洁的中文标题。"
    "要求：航空专业术语必须使用ICAO/民航标准中文译法"
    "（如Rejected Takeoff=中断起飞，Diversion=备降，Go-Around=复飞，Turbulence=颠簸）。"
    "外国航空公司名称必须保留英文原名（如 Delta、United、Lufthansa、Emirates 等），"
    "其他专有名词（机型等）也保留英文，简洁通顺，不超过40字。"
    "只输出翻译结果，不要任何其他内容。\n\n"
    "英文标题：{title}"
)

_TRANSLATE_BODY_PROMPT = (
    "将以下英文航空新闻正文翻译成简洁的中文。"
    "要求：航空专业术语使用ICAO/民航标准中文译法，"
    "外国航空公司名称保留英文原名（如Delta、United等），"
    "飞机型号保留英文（如Boeing 737、Airbus A320等）。"
    "用2-3句叙述体中文概括核心内容，适合朗读收听。"
    "只输出翻译结果，不要任何其他内容。\n\n"
    "正文：{body}"
)


def _translate_title(title: str) -> str:
    """Translate an English title to Chinese via LLM."""
    # Skip if already mostly Chinese
    cn_chars = sum(1 for c in title if '\u4e00' <= c <= '\u9fff')
    if cn_chars > len(title) * 0.3:
        return title

    text = _llm_chat(
        messages=[{"role": "user", "content": _TRANSLATE_PROMPT.format(title=title)}],
        max_tokens=100,
        temperature=0.3,
        retries=2,
        timeout=20,
    )
    if text:
        text = text.strip("\"'""''")
    if text and len(text) <= 120:
        logger.info("Translated: %s -> %s", title[:30], text[:30])
        return text
    if text:
        logger.warning("Translation too long (%d chars): %s", len(text), text[:80])
    return title


def _translate_body(body: str) -> str:
    """Translate an English body to Chinese via LLM. Last-resort safety net."""
    if not body or not body.strip():
        return body
    # Skip if already mostly Chinese
    cn_chars = sum(1 for c in body if '\u4e00' <= c <= '\u9fff')
    if cn_chars > len(body) * 0.15:
        return body

    text = _llm_chat(
        messages=[{"role": "user", "content": _TRANSLATE_BODY_PROMPT.format(body=body)}],
        max_tokens=400,
        temperature=0.3,
        retries=2,
        timeout=30,
    )
    if text:
        text = text.strip("\"'""''")
    cn_result = sum(1 for c in (text or "") if '\u4e00' <= c <= '\u9fff')
    if text and cn_result > 5 and len(text) <= 500:
        logger.info("Body translated: %s -> %s", body[:30], text[:30])
        return text
    if text:
        logger.warning("Body translation failed quality check, keeping original")
    return body


def _enhance_web_entries(digest: dict) -> dict:
    """Enhance digest entries for the web version.

    - Fill missing images with public stock photo URLs
    - Translate international (English) titles to Chinese
    """
    import copy
    digest = copy.deepcopy(digest)

    for entry in digest.get("entries", []):
        click_url = _pick_click_url(entry)
        if click_url:
            entry["canonical_url"] = click_url
            citations = entry.get("citations") or []
            if citations:
                citations[0] = click_url
            else:
                citations = [click_url]
            entry["citations"] = citations
        elif _is_google_news_url(str((entry.get("citations") or [""])[0]).strip()):
            # Do not send users to Google News redirect pages when unresolved.
            entry["citations"] = []
            if _is_google_news_url(str(entry.get("canonical_url", "")).strip()):
                entry["canonical_url"] = ""
            if _is_google_news_url(str(entry.get("url", "")).strip()):
                entry["url"] = ""

        # Fill missing images with publicly accessible URLs
        if (not entry.get("image_url")) or _is_blocked_wechat_image(str(entry.get("image_url", ""))):
            url = search_public_image_url(entry.get("title", ""))
            if url:
                entry["image_url"] = url

        # Translate English titles for international entries
        title = entry.get("title", "")
        if title:
            translated = _translate_title(title)
            if translated != title:
                entry["title"] = translated
                entry["original_title"] = title

    return digest


def _render_web_html(
    digest: dict, summary: str = "", intro: str = "", copyright_notice_url: str = ""
) -> str:
    """Generate standalone HTML page with clickable links for hosting on external domain.

    Same Apple-style card design as _render_html(), but:
    - Full <!DOCTYPE html> standalone page
    - Titles wrapped in <a> tags linking to citation URLs
    - Source domain shown on each card
    - Optional LLM-generated summary and intro paragraph
    """
    date = digest["date"]
    entries = digest.get("entries", [])
    date_long = _format_date_cn(date)

    all_entries_indexed = [(idx, entry) for idx, entry in enumerate(entries, 1)]

    def _build_web_card(idx: int, entry: dict, is_hero: bool = False) -> str:
        title = escape(entry["title"])
        color = _ACCENT_COLOR
        citation = _pick_click_url(entry)
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
                f'<img src="{safe_img}" referrerpolicy="no-referrer" style="width:100%;height:auto;'
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
        body_html = _format_body_html(body_text)

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
                f"border-radius:6px;background:{color};"
                f'color:#FFF;font-size:11px;font-weight:700;">{idx}</span>'
            )
        else:
            num_badge = (
                f'<span style="display:inline-flex;align-items:center;'
                f"justify-content:center;width:22px;height:22px;"
                f"border-radius:6px;background:#F2F2F7;"
                f'color:#6E6E73;font-size:11px;font-weight:700;">{idx}</span>'
            )

        border_top = f"border-top:3px solid {color};" if is_hero else ""

        card = (
            f'<section id="article-{idx}" style="background:#FFFFFF;border-radius:14px;'
            f"padding:18px;margin-bottom:10px;"
            f"box-shadow:0 1px 3px rgba(0,0,0,0.06);"
            f'{border_top}">'
            f'<section style="display:flex;align-items:center;'
            f'gap:6px;margin:0 0 8px 0;">'
            f"{num_badge}"
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

    def _build_toc(all_entries: list[tuple[int, dict]]) -> str:
        """Build a clickable table of contents for the web page."""
        if not all_entries:
            return ""
        rows: list[str] = []
        for idx, entry in all_entries:
            title = escape(entry["title"])
            rows.append(
                f'<a href="#article-{idx}" style="display:flex;align-items:center;'
                f"gap:8px;padding:8px 0;"
                f"border-bottom:1px solid #F2F2F7;text-decoration:none;"
                f'color:#1D1D1F;">'
                f'<span style="display:inline-flex;align-items:center;'
                f"justify-content:center;min-width:20px;height:20px;"
                f"border-radius:5px;background:#F2F2F7;"
                f'color:#6E6E73;font-size:10px;font-weight:700;">{idx}</span>'
                f'<span style="font-size:13px;line-height:1.4;'
                f'font-weight:500;flex:1;">{title}</span>'
                f"</a>"
            )
        # Remove bottom border from last item
        if rows:
            rows[-1] = rows[-1].replace(
                "border-bottom:1px solid #F2F2F7;", ""
            )
        return (
            f'<section style="padding:0 16px 10px 16px;">'
            f'<section style="background:#FFFFFF;border-radius:14px;'
            f"padding:14px 16px;box-shadow:0 1px 3px rgba(0,0,0,0.06);"
            f'margin-bottom:4px;">'
            f'<p style="margin:0 0 6px 0;font-size:11px;font-weight:600;'
            f"color:#6E6E73;letter-spacing:1px;"
            f'text-transform:uppercase;">目录 INDEX</p>'
            f"{''.join(rows)}"
            f"</section></section>"
        )

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

    # Build cards section
    parts: list[str] = []
    if all_entries_indexed:
        parts.append(f'<section style="padding:0 12px;">')
        for i, (idx, entry) in enumerate(all_entries_indexed):
            parts.append(_build_web_card(idx, entry, is_hero=(i == 0)))
        parts.append("</section>")

    # Build TOC from all entries in order
    toc_html = _build_toc(all_entries_indexed)
    beian_icon_src = _load_beian_icon_data_uri()
    if beian_icon_src:
        beian_line = (
            f'<a href="{_MPS_BEIAN_URL}" target="_blank" rel="noreferrer" '
            f'style="display:inline-flex;align-items:center;gap:6px;'
            f'justify-content:center;color:#AEAEB2;text-decoration:none;">'
            f'<img src="{beian_icon_src}" alt="公安备案图标" '
            f'style="width:14px;height:14px;vertical-align:middle;" />'
            f"沪公网安备31011502405233号</a>"
        )
    else:
        beian_line = (
            f'<a href="{_MPS_BEIAN_URL}" target="_blank" rel="noreferrer" '
            f'style="color:#AEAEB2;text-decoration:none;">沪公网安备31011502405233号</a>'
        )

    notice_line = "如有版权疑问请联系我"
    notice_url = str(copyright_notice_url).strip()
    if notice_url:
        safe_notice = escape(notice_url, quote=True)
        notice_line = (
            f'如有版权疑问请联系我'
            f' · <a href="{safe_notice}" target="_blank" rel="noopener" '
            f'style="color:#AEAEB2;text-decoration:underline;">版权声明</a>'
        )

    body = (
        f'<section style="padding:0;margin:0 auto;max-width:420px;'
        f"font-family:-apple-system,BlinkMacSystemFont,"
        f"'SF Pro Display','PingFang SC','Helvetica Neue',"
        f"'Microsoft YaHei',sans-serif;"
        f'background:#F2F2F7;-webkit-font-smoothing:antialiased;">'
        # Header
        f'<section style="padding:36px 20px 24px 20px;text-align:center;">'
        f'<section style="width:40px;height:3px;'
        f"background:linear-gradient(90deg,#0A84FF,#5AC8FA);"
        f'margin:0 auto 18px auto;border-radius:2px;"></section>'
        f'<p style="margin:0;font-size:10px;font-weight:600;'
        f"color:#6E6E73;letter-spacing:4px;"
        f'text-transform:uppercase;">GLOBAL AVIATION DIGEST</p>'
        f'<p style="margin:6px 0 0 0;font-size:22px;font-weight:700;'
        f'color:#1D1D1F;letter-spacing:-0.3px;line-height:1.2;">飞行播客日报</p>'
        f'<p style="margin:8px 0 0 0;font-size:13px;'
        f'color:#6E6E73;font-weight:400;">{date_long}</p>'
        f"</section>"
        # Summary + Intro block
        f"{_web_summary_block(summary, intro)}"
        # Table of contents
        f"{toc_html}"
        # Cards
        f"{''.join(parts)}"
        # Footer
        f'<section style="padding:24px 20px 36px 20px;text-align:center;">'
        f'<section style="width:40px;height:3px;'
        f"background:linear-gradient(90deg,#0A84FF,#5AC8FA);"
        f'margin:0 auto 14px auto;border-radius:2px;"></section>'
        f'<p style="margin:0;font-size:12px;color:#6E6E73;'
        f'line-height:1.6;font-weight:500;">飞行播客日报</p>'
        f'<p style="margin:6px 0 0 0;font-size:10px;color:#AEAEB2;'
        f'line-height:1.6;">版权归原作者及原发机构所有 · 仅供行业信息交流'
        f"<br/>{notice_line}</p>"
        f"</section>"
        f"</section>"
    )

    return (
        f"<!DOCTYPE html>\n"
        f'<html lang="zh-CN">\n<head>\n'
        f'<meta charset="UTF-8">\n'
        f'<meta name="viewport" content="width=device-width,initial-scale=1.0">\n'
        f'<meta name="referrer" content="no-referrer">\n'
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
    "你是国际航空新闻编辑。根据以下新闻标题列表，用一句话（不超过30个字）概括今天的播报要点。"
    "要求：简洁有信息量，像新闻导语，不要用'本期'、'今日'等开头，直接说内容。"
    "外国航空公司名称保留英文原名。"
    "只输出这一句话，不要任何其他内容。\n\n标题列表：\n{titles}"
)


def _generate_digest_summary(digest: dict) -> str:
    """Use LLM to generate a one-line summary of today's digest."""
    fallback = "国际航空要闻速览"

    titles = "\n".join(
        f"- {e.get('title', '')}" for e in digest.get("entries", []) if e.get("title")
    )
    if not titles:
        return fallback

    text = _llm_chat(
        messages=[{"role": "user", "content": _DIGEST_SUMMARY_PROMPT.format(titles=titles)}],
        max_tokens=80,
        temperature=0.7,
        timeout=30,
    )
    if text:
        text = text.strip("\"'""''")
    if text and len(text) <= 100:
        logger.info("Digest summary: %s", text)
        return text
    if text:
        logger.warning("Digest summary too long (%d chars), truncating", len(text))
        return text[:100]
    return fallback



def _download_first_article_image(digest: dict) -> bytes | None:
    """Download the first article's image as cover.

    Returns image bytes, or None on failure.
    """
    entries = digest.get("entries", [])
    for entry in entries:
        url = entry.get("image_url", "")
        if not url:
            continue
        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code == 200 and len(resp.content) > 1000:
                logger.info("Cover image downloaded from first article: %s", url[:80])
                return resp.content
        except Exception as exc:
            logger.warning("Failed to download cover image from %s: %s", url[:80], exc)
    logger.warning("No article image available for cover")
    return None


def _upload_cover_image(image_data: bytes, client: WeChatClient,
                        file_name: str = "cover.jpg") -> str:
    """Upload cover image bytes to WeChat as permanent material.

    Returns thumb_media_id, or empty string on failure.
    """
    thumb_id = client.upload_thumb_image_bytes(image_data, file_name=file_name)
    if thumb_id:
        logger.info("Cover image uploaded: %s", thumb_id[:40])
    return thumb_id


def _save_recent_published(digest: dict, day: str) -> None:
    """Save today's published titles to history for cross-day dedup.

    Keeps the last N days of published titles in data/history/recent_published.json.
    """
    history_dir = settings.history_dir
    history_dir.mkdir(parents=True, exist_ok=True)
    history_path = history_dir / "recent_published.json"

    existing: dict = {}
    if history_path.exists():
        try:
            raw = json.loads(history_path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                existing = raw
        except Exception:  # noqa: BLE001
            logger.warning("Failed to read recent_published.json, starting fresh")

    days = existing.get("days", {})
    if not isinstance(days, dict):
        days = {}

    # Add today's entries
    today_entries = []
    for entry in digest.get("entries", []):
        title = str(entry.get("title", "")).strip()
        item_id = str(entry.get("id", "")).strip()
        event_fp = str(entry.get("event_fingerprint", "")).strip()
        url = ""
        citations = entry.get("citations", [])
        if citations:
            url = citations[0] if isinstance(citations[0], str) else str(citations[0])
        elif entry.get("canonical_url"):
            url = entry["canonical_url"]
        elif entry.get("url"):
            url = entry["url"]
        url = str(url).strip()
        if title or url or item_id or event_fp:
            today_entries.append(
                {
                    "title": title,
                    "url": url,
                    "id": item_id,
                    "event_fingerprint": event_fp,
                }
            )
    days[day] = today_entries

    # Keep only the last N days
    keep_days = max(1, int(settings.recent_published_days))
    sorted_days = sorted(days.keys(), reverse=True)[:keep_days]
    days = {k: days[k] for k in sorted_days}

    payload = {"days": days, "updated_at": day}
    history_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Recent published history saved: %d days, %d entries today", len(days), len(today_entries))


def _copyright_web_fallback_url() -> str:
    base = settings.web_digest_base_url.rstrip("/")
    return f"{base}/copyright.html" if base else "https://mp.weixin.qq.com"


def _load_saved_copyright_notice_url() -> str:
    if settings.copyright_notice_url.strip():
        return settings.copyright_notice_url.strip()
    path = settings.history_dir / "copyright_notice.json"
    if not path.exists():
        return ""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return ""
    url = str(data.get("url", "")).strip()
    if url.startswith("https://mp.weixin.qq.com"):
        return url
    return ""


def _save_copyright_notice_url(url: str, publish_id: str = "", article_id: str = "") -> None:
    if not url:
        return
    path = settings.history_dir / "copyright_notice.json"
    payload = {
        "url": url,
        "publish_id": publish_id,
        "article_id": article_id,
        "updated_at": beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _extract_body_html(full_html: str) -> str:
    m = re.search(r"<body[^>]*>(.*?)</body>", full_html, flags=re.IGNORECASE | re.DOTALL)
    return (m.group(1).strip() if m else full_html.strip())


def _extract_article_id(status: dict[str, Any]) -> str:
    direct = status.get("article_id")
    if isinstance(direct, str) and direct.strip():
        return direct.strip()
    if isinstance(direct, int):
        return str(direct)
    detail = status.get("article_detail")
    if isinstance(detail, dict):
        aid = detail.get("article_id")
        if isinstance(aid, str) and aid.strip():
            return aid.strip()
        if isinstance(aid, int):
            return str(aid)
    return ""


def _extract_article_url(article_detail: dict[str, Any]) -> str:
    for k in ("url", "article_url", "link"):
        v = article_detail.get(k)
        if isinstance(v, str) and v.startswith("https://mp.weixin.qq.com"):
            return v
    items = article_detail.get("news_item")
    if isinstance(items, list):
        for it in items:
            if not isinstance(it, dict):
                continue
            for k in ("url", "article_url", "link"):
                v = it.get(k)
                if isinstance(v, str) and v.startswith("https://mp.weixin.qq.com"):
                    return v
    return ""


def _ensure_wechat_copyright_notice_url(client: WeChatClient) -> str:
    existing = _load_saved_copyright_notice_url()
    if existing:
        return existing

    fallback_url = _copyright_web_fallback_url()
    static_path = Path(__file__).resolve().parents[3] / "static" / "copyright.html"
    if not static_path.exists():
        return fallback_url

    content_html = static_path.read_text(encoding="utf-8")
    icon_data = _load_beian_icon_data_uri()
    if icon_data:
        content_html = content_html.replace('src="beian_icon.png"', f'src="{icon_data}"')
    content_html = _extract_body_html(content_html)
    content_html = client.replace_external_images(content_html)

    media_id = client.create_draft(
        title="版权声明与侵权处理说明",
        author=settings.wechat_author,
        content_html=content_html,
        digest="版权声明、侵权处理流程与联系方式说明。",
        source_url=fallback_url,
        thumb_media_id="",
    )
    publish = client.publish_draft(media_id)

    article_id = ""
    for _ in range(30):
        status = client.get_publish_status(publish.publish_id)
        article_id = _extract_article_id(status)
        if article_id:
            break
        publish_status = status.get("publish_status")
        # 0 means success in WeChat docs; if succeeded without article_id, keep polling briefly.
        if publish_status in (2, "2"):
            raise WeChatPublishError(f"Copyright publish failed: {status}")
        time.sleep(3)

    if not article_id:
        return fallback_url

    detail = client.get_article_detail(article_id)
    article_url = _extract_article_url(detail)
    if article_url:
        _save_copyright_notice_url(article_url, publish.publish_id, article_id)
        return article_url
    return fallback_url


def run(target_date: str | None = None) -> Path:
    ensure_dirs()
    day = target_date or beijing_today_str()
    digest = load_json(settings.processed_dir / f"composed_{day}.json")
    quality = load_json(settings.processed_dir / f"quality_{day}.json")
    copyright_notice_url = _load_saved_copyright_notice_url() or _copyright_web_fallback_url()
    digest["copyright_notice_url"] = copyright_notice_url

    # Translate any remaining English titles and bodies before rendering
    for entry in digest.get("entries", []):
        title = entry.get("title", "")
        if title:
            translated = _translate_title(title)
            if translated != title:
                entry["title"] = translated
                entry["original_title"] = title
        # Safety net: translate English bodies that slipped through compose
        body = entry.get("body", "")
        if body:
            translated_body = _translate_body(body)
            if translated_body != body:
                entry["body"] = translated_body

    md = _render_markdown(digest)
    html = _render_html(digest)

    # Generate LLM content for web version (works in all modes, only needs LLM key)
    summary = _generate_digest_summary(digest)
    intro = _generate_web_intro(digest)

    # Enhance entries for web: public image URLs + title translation
    web_digest = _enhance_web_entries(digest)

    # Initial web render (with public images + translated titles)
    web_html = _render_web_html(
        web_digest,
        summary=summary,
        intro=intro,
        copyright_notice_url=copyright_notice_url,
    )

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
        "copyright_notice_url": copyright_notice_url,
        "reasons": quality.get("reasons", []),
    }

    def _cleanup_old_drafts(wc: WeChatClient, keep_media_id: str) -> None:
        """Delete old daily-digest drafts, keep podcasts and the one just created."""
        podcast_authors = {"飞行播客"}
        try:
            drafts = wc.list_drafts(count=20)
            deleted = 0
            for item in drafts:
                mid = item.get("media_id", "")
                if not mid or mid == keep_media_id:
                    continue
                # Skip podcast drafts (author = "飞行播客")
                news_items = item.get("content", {}).get("news_item", [])
                if news_items and news_items[0].get("author") in podcast_authors:
                    continue
                wc.delete_draft(mid)
                deleted += 1
            if deleted:
                logger.info("Cleaned up %d old digest draft(s)", deleted)
        except Exception:
            logger.warning("Draft cleanup failed, continuing")

    # Use the first article's image as cover
    cover_image_data = _download_first_article_image(digest)
    if cover_image_data:
        cover_path = settings.output_dir / f"cover_{day}.jpg"
        cover_path.write_bytes(cover_image_data)
        logger.info("Cover image saved locally: %s (%d bytes)", cover_path, len(cover_image_data))

    if settings.dry_run or not settings.wechat_enable_publish:
        result["status"] = "dry_run"
        result["url"] = f"dry-run://flying-podcast/{day}"
    else:
        client = WeChatClient()
        try:
            try:
                copyright_notice_url = _ensure_wechat_copyright_notice_url(client)
            except WeChatPublishError as exc:
                logger.warning("Use fallback copyright notice URL: %s", exc)
                copyright_notice_url = _copyright_web_fallback_url()
            digest["copyright_notice_url"] = copyright_notice_url
            result["copyright_notice_url"] = copyright_notice_url
            # Fill missing images with AI-generated ones
            digest = _fill_missing_images(digest, client)
            # Re-render HTML with new images
            html = _render_html(digest)
            html = client.replace_external_images(html)
            # Re-render web HTML with AI images + LLM content
            web_digest = _enhance_web_entries(digest)
            web_html = _render_web_html(
                web_digest,
                summary=summary,
                intro=intro,
                copyright_notice_url=copyright_notice_url,
            )
            # Upload cover image, fallback to default thumb
            cover_thumb_id = ""
            if cover_image_data:
                cover_thumb_id = _upload_cover_image(
                    cover_image_data, client,
                    file_name=f"飞行播客日报_{day}.jpg",
                )
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
            # Clean up old drafts, keep only the one just created
            _cleanup_old_drafts(client, keep_media_id=media_id)
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
        "copyright_notice_url": copyright_notice_url,
    })

    # Save published titles for cross-day dedup
    _save_recent_published(digest, day)

    logger.info("Publish done. status=%s score=%.2f", result["status"], quality["total_score"])
    return out


if __name__ == "__main__":
    run()

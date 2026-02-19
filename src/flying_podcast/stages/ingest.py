from __future__ import annotations

import hashlib
import html
import re
import sys
import os
from html.parser import HTMLParser
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import asyncio
import inspect
from urllib.parse import urlparse

import feedparser
import requests
from dateutil import parser as dt_parser

from flying_podcast.core.config import ensure_dirs, settings
from flying_podcast.core.io_utils import append_lines, dump_json, load_json, load_yaml, read_lines
from flying_podcast.core.logging_utils import get_logger
from flying_podcast.core.models import NewsItem
from flying_podcast.core.time_utils import beijing_today_str
from flying_podcast.stages.web_parser_registry import parse_web_source_entries

logger = get_logger("ingest")


def _hash_id(*parts: str) -> str:
    text = "||".join(parts)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _normalize_title_for_fp(title: str) -> str:
    text = title.strip().lower()
    text = re.sub(r"\s+[-–—]\s+[^-–—]{1,40}$", "", text)
    text = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _normalize_time(raw_value: Any) -> str:
    if not raw_value:
        return datetime.now(timezone.utc).isoformat()
    if isinstance(raw_value, datetime):
        return raw_value.astimezone(timezone.utc).isoformat()
    try:
        return dt_parser.parse(str(raw_value)).astimezone(timezone.utc).isoformat()
    except (ValueError, TypeError):
        return datetime.now(timezone.utc).isoformat()


def _normalize_time_strict(raw_value: Any) -> str:
    """Normalize to UTC ISO string; return empty string when parse fails."""
    if not raw_value:
        return ""
    if isinstance(raw_value, datetime):
        if raw_value.tzinfo is None:
            raw_value = raw_value.replace(tzinfo=timezone.utc)
        return raw_value.astimezone(timezone.utc).isoformat()
    try:
        parsed = dt_parser.parse(str(raw_value))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat()
    except (ValueError, TypeError):
        return ""


def _entry_text(entry: dict[str, Any]) -> str:
    summary = entry.get("summary", "")
    content = ""
    if entry.get("content"):
        first = entry["content"][0]
        content = first.get("value", "") if isinstance(first, dict) else str(first)
    return (summary or content or "").strip()


def _extract_image_url(entry: dict[str, Any]) -> str:
    """Extract the best image URL from an RSS entry.

    Priority: enclosures (image/*) > media_content > media_thumbnail > <img> in HTML.
    """
    # 1. enclosures with image type
    for enc in entry.get("enclosures", []):
        etype = str(enc.get("type", "")).lower()
        href = str(enc.get("href", "")).strip()
        if href and ("image" in etype or href.lower().endswith((".jpg", ".jpeg", ".png", ".webp"))):
            return href

    # 2. media_content
    for mc in entry.get("media_content", []):
        url = str(mc.get("url", "")).strip()
        medium = str(mc.get("medium", "")).lower()
        mtype = str(mc.get("type", "")).lower()
        if url and ("image" in medium or "image" in mtype or url.lower().endswith((".jpg", ".jpeg", ".png", ".webp"))):
            return url

    # 3. media_thumbnail
    for mt in entry.get("media_thumbnail", []):
        url = str(mt.get("url", "")).strip()
        if url:
            return url

    # 4. First <img src="..."> in summary or content HTML
    summary = entry.get("summary", "")
    content_val = ""
    if entry.get("content"):
        first = entry["content"][0]
        content_val = first.get("value", "") if isinstance(first, dict) else str(first)
    for html_text in [summary, content_val]:
        if not html_text:
            continue
        m = re.search(r'<img[^>]+src=["\']([^"\'>{]+)', html_text, re.IGNORECASE)
        if m:
            src = m.group(1).strip()
            if src.startswith(("http://", "https://")):
                return src

    return ""


def _extract_domain(url: str) -> str:
    try:
        return (urlparse(url).netloc or "").lower()
    except Exception:  # noqa: BLE001
        return ""


def _is_google_redirect(url: str) -> bool:
    domain = _extract_domain(url)
    path = (urlparse(url).path or "").lower() if domain else ""
    return domain.endswith("news.google.com") and path.startswith("/rss/articles/")


def _extract_published_at_from_url(url: str, patterns: list[str]) -> str:
    decoded = html.unescape(url)
    default_patterns = [
        r"t(20\d{2})(0[1-9]|1[0-2])(3[01]|[12]\d|0?[1-9])_",
        r"/(20\d{2})/(0[1-9]|1[0-2])/(3[01]|[12]\d|0?[1-9])(?!\d)",
        r"(20\d{2})-(0[1-9]|1[0-2])-(3[01]|[12]\d|0?[1-9])(?!\d)",
        r"(20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])",
        r"/(20\d{2})(0[1-9]|1[0-2])/",
    ]
    for pat in patterns + default_patterns:
        m = re.search(pat, decoded)
        if not m:
            continue
        groups = m.groups()
        if len(groups) >= 3:
            yyyy, mm, dd = groups[0], groups[1], groups[2]
            ts = _normalize_time_strict(f"{yyyy}-{mm}-{dd}T00:00:00+00:00")
            if ts:
                return ts
        elif len(groups) == 2:
            yyyy, mm = groups[0], groups[1]
            ts = _normalize_time_strict(f"{yyyy}-{mm}-01T00:00:00+00:00")
            if ts:
                return ts
    return ""


def _extract_published_at_from_text(text: str, formats: list[str]) -> str:
    txt = html.unescape(text or "")
    regex_list = [
        r"(20\d{2}[-/](?:0?[1-9]|1[0-2])[-/](?:3[01]|[12]\d|0?[1-9]))",
        r"(20\d{2}年(?:0?[1-9]|1[0-2])月(?:3[01]|[12]\d|0?[1-9])日)",
    ]
    for pat in regex_list:
        m = re.search(pat, txt)
        if not m:
            continue
        candidate = m.group(1).replace("年", "-").replace("月", "-").replace("日", "")
        ts = _normalize_time_strict(candidate)
        if ts:
            return ts

    # Optional strict format parsing from source config.
    if formats:
        for fmt in formats:
            for token in re.split(r"\s+", txt):
                try:
                    parsed = datetime.strptime(token.strip(), fmt).replace(tzinfo=timezone.utc)
                    return parsed.astimezone(timezone.utc).isoformat()
                except ValueError:
                    continue
    return ""


def _extract_published_at_for_web(source: dict[str, Any], url: str, title_text: str) -> str:
    patterns = [str(x) for x in source.get("published_at_patterns", [])]
    formats = [str(x) for x in source.get("published_at_formats", [])]

    from_url = _extract_published_at_from_url(url, patterns=patterns)
    if from_url:
        return from_url

    from_text = _extract_published_at_from_text(title_text, formats=formats)
    if from_text:
        return from_text
    return ""


class _AnchorParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._current_href = ""
        self._current_text: list[str] = []
        self.links: list[tuple[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        attr_map = dict(attrs)
        self._current_href = (attr_map.get("href") or "").strip()
        self._current_text = []

    def handle_data(self, data: str) -> None:
        if self._current_href:
            self._current_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a":
            return
        if self._current_href:
            text = re.sub(r"\s+", " ", "".join(self._current_text)).strip()
            self.links.append((self._current_href, text))
        self._current_href = ""
        self._current_text = []


def _fetch_html_requests(url: str, user_agent: str, timeout: int = 20) -> str:
    resp = requests.get(url, timeout=timeout, headers={"User-Agent": user_agent})
    if not resp.ok:
        raise RuntimeError(f"http_{resp.status_code}")
    if not resp.encoding or resp.encoding.lower() in {"iso-8859-1", "ascii"}:
        try:
            resp.encoding = resp.apparent_encoding
        except Exception:  # noqa: BLE001
            pass
    return resp.text


def _fetch_html_playwright(url: str, timeout_ms: int = 30000) -> str:
    try:
        from playwright.sync_api import sync_playwright  # type: ignore
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("playwright_not_installed") from exc
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            page.goto(url, wait_until="networkidle", timeout=timeout_ms)
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            html = page.content()
        finally:
            browser.close()
    return html


def _fetch_html_nodriver(url: str, timeout_ms: int = 30000, use_xvfb: bool = True) -> str:
    display = None
    if use_xvfb and sys.platform.startswith("linux"):
        if not (os.getenv("DISPLAY") or os.getenv("WAYLAND_DISPLAY")):
            try:
                from pyvirtualdisplay import Display  # type: ignore

                display = Display(visible=False, size=(1366, 768))
                display.start()
            except Exception:  # noqa: BLE001
                display = None

    async def _run() -> str:
        import nodriver as uc  # type: ignore

        async def _maybe_await(value: Any) -> Any:
            if inspect.isawaitable(value):
                return await value
            return value

        browser = await uc.start(headless=True)
        try:
            page = await _maybe_await(browser.get(url))
            await _maybe_await(page.wait(1.5) if hasattr(page, "wait") else None)
            content = await _maybe_await(page.get_content())
            return content
        finally:
            await _maybe_await(browser.stop())

    try:
        return asyncio.run(asyncio.wait_for(_run(), timeout=timeout_ms / 1000))
    finally:
        if display is not None:
            try:
                display.stop()
            except Exception:  # noqa: BLE001
                pass


def _fetch_html(source: dict[str, Any], url: str) -> str:
    mode = str(source.get("fetch_mode", "auto")).lower()
    user_agent = source.get(
        "user_agent",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    )
    use_xvfb = bool(source.get("xvfb", True))
    fallback_order = [str(x).lower() for x in source.get("fallback_order", ["playwright", "nodriver"])]
    if sys.platform.startswith("win") and mode != "nodriver":
        fallback_order = [x for x in fallback_order if x != "nodriver"]
    if mode == "nodriver":
        return _fetch_html_nodriver(url, use_xvfb=use_xvfb)
    if mode == "playwright":
        return _fetch_html_playwright(url)

    try:
        html = _fetch_html_requests(url, user_agent=user_agent)
        if mode == "auto" and len(html) < 800:
            last_error = "empty_shell"
            for fallback in fallback_order:
                try:
                    if fallback == "playwright":
                        return _fetch_html_playwright(url)
                    if fallback == "nodriver":
                        return _fetch_html_nodriver(url, use_xvfb=use_xvfb)
                except Exception as exc:  # noqa: BLE001
                    last_error = str(exc)
            logger.warning("Fallback failed for %s, keep requests html: %s", source.get("id"), last_error)
            return html
        return html
    except Exception:  # noqa: BLE001
        if mode == "requests":
            raise
        last_error = "fetch_failed"
        for fallback in fallback_order:
            try:
                if fallback == "playwright":
                    return _fetch_html_playwright(url)
                if fallback == "nodriver":
                    return _fetch_html_nodriver(url, use_xvfb=use_xvfb)
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
        raise RuntimeError(last_error)


def _collect_rss_entries(source: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        parsed = feedparser.parse(source["url"])
    except Exception as exc:  # noqa: BLE001
        logger.warning("Fetch feed failed %s: %s", source["id"], exc)
        return rows
    if parsed.bozo:
        logger.warning("Feed parse warning %s: %s", source["id"], parsed.bozo_exception)
        if not parsed.entries:
            return rows

    for entry in parsed.entries:
        title = (entry.get("title") or "").strip()
        link = (entry.get("link") or source["url"]).strip()
        raw_text = _entry_text(entry)
        if not title or not raw_text:
            continue
        publisher_domain = ""
        if isinstance(entry.get("source"), dict):
            publisher_domain = _extract_domain(entry["source"].get("href", ""))
        rows.append(
            {
                "title": title,
                "url": link,
                "canonical_url": link,
                "raw_text": raw_text,
                "published_at": _normalize_time(entry.get("published") or entry.get("updated")),
                "lang": (entry.get("language") or source.get("lang") or "unknown").lower(),
                "publisher_domain": publisher_domain or _extract_domain(link),
                "is_google_redirect": _is_google_redirect(link),
                "image_url": _extract_image_url(entry),
            }
        )
    return rows


def _collect_web_entries(source: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    list_url = source.get("list_url") or source.get("url")
    if not list_url:
        return rows
    try:
        html = _fetch_html(source, list_url)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Fetch web source failed %s: %s", source.get("id"), exc)
        return rows

    link_patterns = [re.compile(p) for p in source.get("link_patterns", [])]
    exclude_patterns = [re.compile(p) for p in source.get("exclude_patterns", [])]
    include_keywords = [x.lower() for x in source.get("article_include_keywords", [])]
    max_items = int(source.get("max_items", 40))
    strict_published_at = settings.strict_web_published_at
    parsed_entries = parse_web_source_entries(
        str(source.get("id") or ""),
        list_url,
        html,
        max_items=max_items * 3,
    )

    seen_links: set[str] = set()
    for parsed in parsed_entries:
        if len(rows) >= max_items:
            break
        text = parsed.title.strip()
        abs_url = parsed.url.strip()
        if not abs_url or not text or len(text) < 8:
            continue
        if not abs_url.startswith(("http://", "https://")):
            continue
        if abs_url in seen_links:
            continue
        combined = f"{text} {abs_url}"
        if link_patterns and not any(p.search(combined) for p in link_patterns):
            continue
        if any(p.search(combined) for p in exclude_patterns):
            continue
        if include_keywords and not any(k in text.lower() for k in include_keywords):
            continue
        published_at = _normalize_time_strict(parsed.published_hint) or _extract_published_at_for_web(source, abs_url, text)
        if not published_at and strict_published_at:
            continue
        seen_links.add(abs_url)
        rows.append(
            {
                "title": text,
                "url": abs_url,
                "canonical_url": abs_url,
                "raw_text": parsed.raw_text or text,
                "published_at": published_at or _normalize_time(None),
                "lang": (source.get("lang") or "unknown").lower(),
                "publisher_domain": _extract_domain(abs_url),
                "is_google_redirect": _is_google_redirect(abs_url),
                "image_url": "",
            }
        )
    return rows


def run(target_date: str | None = None) -> Path:
    ensure_dirs()
    day = target_date or beijing_today_str()
    out = settings.raw_dir / f"{day}.json"

    config = load_yaml(settings.sources_config)
    sources = [s for s in config.get("sources", []) if s.get("enabled", True)]
    seen_ids = read_lines(settings.history_dir / "seen_ids.txt")

    existing: dict[str, dict[str, Any]] = {}
    if out.exists():
        for row in load_json(out):
            existing[row["id"]] = row

    items: list[NewsItem] = []
    new_ids: list[str] = []

    for source in sources:
        stype = str(source.get("type", "rss")).lower()
        if stype == "rss":
            entries = _collect_rss_entries(source)
        elif stype == "web":
            entries = _collect_web_entries(source)
        else:
            logger.warning("Skip unsupported source type: %s", source.get("id"))
            continue

        for entry in entries:
            title = entry["title"]
            link = entry["url"]
            raw_text = entry["raw_text"]
            canonical_url = entry["canonical_url"]
            publisher_domain = entry["publisher_domain"]
            is_google_redirect = entry["is_google_redirect"]

            item_id = _hash_id(title.lower(), canonical_url.lower())
            if item_id in seen_ids:
                continue
            if item_id in existing:
                continue
            event_fingerprint = _hash_id(_normalize_title_for_fp(title))

            item = NewsItem(
                id=item_id,
                title=title,
                source_id=source["id"],
                source_name=source["name"],
                source_url=source["url"],
                url=link,
                source_tier=source.get("source_tier", "C"),
                region=source.get("region", "international"),
                published_at=entry["published_at"],
                lang=entry["lang"],
                raw_text=raw_text,
                canonical_url=canonical_url,
                publisher_domain=publisher_domain,
                is_google_redirect=is_google_redirect,
                event_fingerprint=event_fingerprint,
                image_url=entry.get("image_url", ""),
            )
            items.append(item)
            new_ids.append(item_id)

    merged = list(existing.values()) + [i.to_dict() for i in items]
    dump_json(out, merged)
    append_lines(settings.history_dir / "seen_ids.txt", new_ids)
    logger.info("Ingest done. sources=%s new_items=%s merged_items=%s", len(sources), len(items), len(merged))
    return out


if __name__ == "__main__":
    run()

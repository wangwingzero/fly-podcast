from __future__ import annotations

import html
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Callable
from urllib.parse import urljoin, urlparse


@dataclass(frozen=True)
class ParsedWebEntry:
    url: str
    title: str
    raw_text: str
    published_hint: str = ""


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


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(text or "")).strip()


def _looks_like_article_title(text: str) -> bool:
    if not text or len(text) < 8:
        return False
    low = text.lower()
    skip_terms = {
        "read more",
        "learn more",
        "more",
        "view all",
        "about",
        "contact",
        "home",
        "新闻中心",
        "更多",
        "返回",
        "登录",
        "注册",
    }
    if low in skip_terms:
        return False
    return True


def _extract_date_hint(text: str) -> str:
    patterns = [
        r"(20\d{2}-[01]?\d-[0-3]?\d)",
        r"(20\d{2}/[01]?\d/[0-3]?\d)",
        r"(20\d{2}年[01]?\d月[0-3]?\d日)",
        r"((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+[0-3]?\d,\s*20\d{2})",
        r"(t(20\d{2})(0[1-9]|1[0-2])([0-3]\d)_)",
    ]
    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if not m:
            continue
        token = m.group(1)
        compact = re.match(r"t(20\d{2})(0[1-9]|1[0-2])([0-3]\d)_", token, flags=re.IGNORECASE)
        if compact:
            return f"{compact.group(1)}-{compact.group(2)}-{compact.group(3)}"
        return token
    return ""


def _collect_context_snippets(html_text: str, token: str, window: int = 260) -> list[str]:
    if not token:
        return []
    keys = {token, html.escape(token), html.escape(token, quote=True)}
    out: list[str] = []
    for key in keys:
        start = 0
        while True:
            idx = html_text.find(key, start)
            if idx < 0:
                break
            left = max(0, idx - window)
            right = min(len(html_text), idx + len(key) + window)
            out.append(html_text[left:right])
            start = idx + len(key)
            if len(out) >= 4:
                break
    return out


def _find_date_near_anchor(html_text: str, href: str, title: str) -> str:
    snippets = _collect_context_snippets(html_text, href)
    if title:
        snippets.extend(_collect_context_snippets(html_text, title[:40]))
    for snippet in snippets:
        hint = _extract_date_hint(snippet)
        if hint:
            return hint
    return ""


def _has_allowed_domain(url: str, allowed_domains: set[str]) -> bool:
    if not allowed_domains:
        return True
    host = (urlparse(url).netloc or "").lower()
    return any(host.endswith(d) for d in allowed_domains)


def _match_path_hints(url: str, path_hints: set[str]) -> bool:
    if not path_hints:
        return True
    low = url.lower()
    return any(h in low for h in path_hints)


def _validate_entry(entry: ParsedWebEntry) -> bool:
    if not entry.title or len(entry.title) < 8:
        return False
    if not entry.url.startswith(("http://", "https://")):
        return False
    host = (urlparse(entry.url).netloc or "").lower()
    return bool(host)


def _parse_generic(
    *,
    list_url: str,
    html_text: str,
    allowed_domains: set[str] | None = None,
    path_hints: set[str] | None = None,
) -> list[ParsedWebEntry]:
    parser = _AnchorParser()
    parser.feed(html_text)

    domains = allowed_domains or set()
    hints = {x.lower() for x in (path_hints or set())}
    out: list[ParsedWebEntry] = []
    seen: set[str] = set()
    for href, text in parser.links:
        title = _normalize_text(text)
        if not _looks_like_article_title(title):
            continue
        abs_url = _normalize_text(urljoin(list_url, href))
        if not abs_url.startswith(("http://", "https://")):
            continue
        if abs_url in seen:
            continue
        if not _has_allowed_domain(abs_url, domains):
            continue
        if not _match_path_hints(abs_url, hints):
            continue
        if "javascript:" in abs_url.lower():
            continue
        published_hint = _find_date_near_anchor(html_text, href, title)
        candidate = ParsedWebEntry(
            url=abs_url,
            title=title,
            raw_text=title,
            published_hint=_normalize_text(published_hint),
        )
        if not _validate_entry(candidate):
            continue
        out.append(candidate)
        seen.add(abs_url)
    return out


def _parse_caacnews(list_url: str, html_text: str) -> list[ParsedWebEntry]:
    return _parse_generic(
        list_url=list_url,
        html_text=html_text,
        allowed_domains={"caacnews.com.cn"},
        path_hints={"/1/2/20", "t20"},
    )


def _parse_iata(list_url: str, html_text: str) -> list[ParsedWebEntry]:
    return _parse_generic(
        list_url=list_url,
        html_text=html_text,
        allowed_domains={"iata.org"},
        path_hints={"/pressroom/"},
    )


def _parse_faa(list_url: str, html_text: str) -> list[ParsedWebEntry]:
    return _parse_generic(
        list_url=list_url,
        html_text=html_text,
        allowed_domains={"faa.gov"},
        path_hints={"/newsroom/"},
    )


def _parse_airbus(list_url: str, html_text: str) -> list[ParsedWebEntry]:
    return _parse_generic(
        list_url=list_url,
        html_text=html_text,
        allowed_domains={"airbus.com"},
        path_hints={"/newsroom/"},
    )


def _parse_boeing(list_url: str, html_text: str) -> list[ParsedWebEntry]:
    return _parse_generic(
        list_url=list_url,
        html_text=html_text,
        allowed_domains={"boeing.mediaroom.com"},
        path_hints={"20", "news"},
    )


def _parse_flightglobal(list_url: str, html_text: str) -> list[ParsedWebEntry]:
    return _parse_generic(
        list_url=list_url,
        html_text=html_text,
        allowed_domains={"flightglobal.com"},
        path_hints={"/news", ".article"},
    )


def _parse_caac_gov_mhyw(list_url: str, html_text: str) -> list[ParsedWebEntry]:
    return _parse_generic(
        list_url=list_url,
        html_text=html_text,
        allowed_domains={"caac.gov.cn"},
        path_hints={"/xwzx/mhyw/", ".shtml", ".html", "t20"},
    )


def _parse_carnoc(list_url: str, html_text: str) -> list[ParsedWebEntry]:
    return _parse_generic(
        list_url=list_url,
        html_text=html_text,
        allowed_domains={"news.carnoc.com", "carnoc.com"},
        path_hints={"news.carnoc.com/list/", ".html"},
    )


Parser = Callable[[str, str], list[ParsedWebEntry]]

_WEB_PARSER_REGISTRY: dict[str, Parser] = {
    "carnoc_web_list": _parse_carnoc,
    "caacnews_web_list": _parse_caacnews,
    "caac_gov_web_mhyw": _parse_caac_gov_mhyw,
    "iata_press_web": _parse_iata,
    "faa_newsroom_web": _parse_faa,
    "airbus_newsroom_web": _parse_airbus,
    "boeing_newsroom_web": _parse_boeing,
    "flightglobal_news_web": _parse_flightglobal,
}


def get_web_parser(source_id: str) -> Parser:
    return _WEB_PARSER_REGISTRY.get(source_id, lambda list_url, html_text: _parse_generic(list_url=list_url, html_text=html_text))


def parse_web_source_entries(source_id: str, list_url: str, html_text: str, max_items: int = 120) -> list[ParsedWebEntry]:
    parser = get_web_parser(source_id)
    parsed = parser(list_url, html_text)
    out: list[ParsedWebEntry] = []
    seen: set[str] = set()
    for row in parsed:
        if row.url in seen:
            continue
        if not _validate_entry(row):
            continue
        out.append(row)
        seen.add(row.url)
        if len(out) >= max_items:
            break
    return out

from __future__ import annotations

import html
import re
from dataclasses import dataclass
from datetime import datetime
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
        r"((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+[0-3]?\d(?:st|nd|rd|th)?\s+20\d{2})",
        r"(t(20\d{2})(0[1-9]|1[0-2])([0-3]\d)_)",
        r"([0-3]?\d\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+20\d{2})",
        r"((?:NR|MA|MR)(20\d{2})(0[1-9]|1[0-2])([0-3]\d))",
    ]
    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if not m:
            continue
        token = m.group(1)
        compact = re.match(r"t(20\d{2})(0[1-9]|1[0-2])([0-3]\d)_", token, flags=re.IGNORECASE)
        if compact:
            return f"{compact.group(1)}-{compact.group(2)}-{compact.group(3)}"
        ntsb = re.match(r"(?:NR|MA|MR)(20\d{2})(0[1-9]|1[0-2])([0-3]\d)", token, flags=re.IGNORECASE)
        if ntsb:
            return f"{ntsb.group(1)}-{ntsb.group(2)}-{ntsb.group(3)}"
        ordinal = re.match(
            r"((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*)\s+([0-3]?\d)(?:st|nd|rd|th)?\s+(20\d{2})",
            token,
            flags=re.IGNORECASE,
        )
        if ordinal:
            try:
                parsed = datetime.strptime(
                    f"{ordinal.group(1)} {int(ordinal.group(2))} {ordinal.group(3)}",
                    "%b %d %Y",
                )
            except ValueError:
                parsed = datetime.strptime(
                    f"{ordinal.group(1)[:3]} {int(ordinal.group(2))} {ordinal.group(3)}",
                    "%b %d %Y",
                )
            return parsed.strftime("%Y-%m-%d")
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


def _parse_reuters_aerospace(list_url: str, html_text: str) -> list[ParsedWebEntry]:
    return _parse_generic(
        list_url=list_url,
        html_text=html_text,
        allowed_domains={"reuters.com"},
        path_hints={"/business/aerospace-defense/"},
    )


def _parse_ain_online(list_url: str, html_text: str) -> list[ParsedWebEntry]:
    return _parse_generic(
        list_url=list_url,
        html_text=html_text,
        allowed_domains={"ainonline.com"},
        path_hints={"/aviation-news/"},
    )


def _parse_ntsb(list_url: str, html_text: str) -> list[ParsedWebEntry]:
    return _parse_generic(
        list_url=list_url,
        html_text=html_text,
        allowed_domains={"ntsb.gov"},
        path_hints={"/news/press-releases/"},
    )


def _parse_easa(list_url: str, html_text: str) -> list[ParsedWebEntry]:
    return _parse_generic(
        list_url=list_url,
        html_text=html_text,
        allowed_domains={"easa.europa.eu"},
        path_hints={"/newsroom-and-events/", "/news/"},
    )


def _strip_tags(text: str) -> str:
    clean = re.sub(r"<[^>]+>", " ", text or "")
    return _normalize_text(clean)


def _parse_avherald(list_url: str, html_text: str) -> list[ParsedWebEntry]:
    parser = _AnchorParser()
    parser.feed(html_text)
    out: list[ParsedWebEntry] = []
    seen: set[str] = set()
    for href, title in parser.links:
        title = _normalize_text(title)
        if not title or "/h?article=" not in href:
            continue
        abs_url = _normalize_text(urljoin(list_url, href))
        if abs_url in seen:
            continue
        # AvHerald titles usually contain "on Apr 26th 2026".
        published_hint = _extract_date_hint(title)
        out.append(
            ParsedWebEntry(
                url=abs_url,
                title=title,
                raw_text=f"AvHerald safety event: {title}",
                published_hint=published_hint,
            )
        )
        seen.add(abs_url)
    return out


def _parse_asn_year(list_url: str, html_text: str) -> list[ParsedWebEntry]:
    out: list[ParsedWebEntry] = []
    seen: set[str] = set()
    for row in re.findall(r"<tr[^>]+class=[\"']list[\"'][^>]*>(.*?)</tr>", html_text, flags=re.I | re.S):
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, flags=re.I | re.S)
        if len(cells) < 8:
            continue
        href_match = re.search(r"href\s*=\s*[\"']?([^\"'\s>]+)", cells[0], flags=re.I)
        if not href_match:
            continue
        url = _normalize_text(urljoin(list_url, href_match.group(1)))
        if url in seen:
            continue
        acc_date = _strip_tags(cells[0])
        aircraft_type = _strip_tags(cells[1])
        registration = _strip_tags(cells[2])
        operator = _strip_tags(cells[3])
        fatalities = _strip_tags(cells[4])
        location = _strip_tags(cells[5])
        damage = _strip_tags(cells[7])
        title_parts = [
            "ASN accident record",
            acc_date,
            operator,
            aircraft_type,
            registration,
            location,
        ]
        title = " - ".join(x for x in title_parts if x)
        raw_text = (
            f"Aviation Safety Network accident database entry. Date: {acc_date}. "
            f"Aircraft: {aircraft_type}. Registration: {registration}. Operator: {operator}. "
            f"Fatalities: {fatalities}. Location: {location}. Damage: {damage}."
        )
        out.append(ParsedWebEntry(url=url, title=title, raw_text=raw_text, published_hint=acc_date))
        seen.add(url)
    return out


def _parse_easa_ad(list_url: str, html_text: str) -> list[ParsedWebEntry]:
    out: list[ParsedWebEntry] = []
    seen: set[str] = set()
    for row in re.findall(r"<tr[^>]*showStatus\('https://ad\.easa\.europa\.eu/ad/[^']+'[^>]*>(.*?)</tr>", html_text, flags=re.I | re.S):
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, flags=re.I | re.S)
        if len(cells) < 6:
            continue
        link_match = re.search(r"<a[^>]+href=[\"']([^\"']+/ad/[^\"']+)[\"'][^>]*>(.*?)</a>", cells[0], flags=re.I | re.S)
        if not link_match:
            continue
        url = _normalize_text(link_match.group(1))
        if url in seen:
            continue
        ad_number = _strip_tags(link_match.group(2))
        issue_date = _strip_tags(cells[2])
        subject = _strip_tags(cells[3])
        subject = re.sub(r"\bsend comment\b.*$", "", subject, flags=re.I).strip()
        effective_date = _strip_tags(cells[5]) if len(cells) > 5 else ""
        tree_text = _strip_tags(cells[4]) if len(cells) > 4 else ""
        title = f"EASA AD {ad_number}: {subject}"
        if tree_text:
            title = f"{title} - {tree_text}"
        raw_text = (
            f"EASA safety publication. AD: {ad_number}. Issue date: {issue_date}. "
            f"Effective date: {effective_date}. Subject: {subject}. Affected products: {tree_text}."
        )
        out.append(ParsedWebEntry(url=url, title=title, raw_text=raw_text, published_hint=issue_date))
        seen.add(url)
    return out


def _parse_faa_operator_bulletins(list_url: str, html_text: str) -> list[ParsedWebEntry]:
    out: list[ParsedWebEntry] = []
    seen: set[str] = set()
    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", html_text, flags=re.I | re.S):
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, flags=re.I | re.S)
        if len(cells) < 2:
            continue
        link_match = re.search(r"<a[^>]+href=[\"']([^\"']+\.pdf)[\"'][^>]*>(.*?)</a>", cells[0], flags=re.I | re.S)
        if not link_match:
            continue
        href = link_match.group(1)
        url = _normalize_text(urljoin(list_url, href))
        if url in seen:
            continue
        number = _strip_tags(link_match.group(2))
        title_text = _strip_tags(cells[1])
        if not number or not title_text:
            continue
        kind = "FAA SAFO" if "safo" in list_url.lower() else "FAA InFO"
        title = f"{kind} {number}: {title_text}"
        # FAA bulletin numbers use YYNNN, e.g. 26005 -> 2026.
        year_hint = ""
        m = re.match(r"(\d{2})", number)
        if m:
            year_hint = f"20{m.group(1)}-01-01"
        raw_text = f"{kind} operator bulletin. Number: {number}. Title: {title_text}."
        out.append(ParsedWebEntry(url=url, title=title, raw_text=raw_text, published_hint=year_hint))
        seen.add(url)
    return out


def _parse_asrs_callback(list_url: str, html_text: str) -> list[ParsedWebEntry]:
    out: list[ParsedWebEntry] = []
    seen: set[str] = set()
    pattern = re.compile(
        r'<a[^>]+href=["\']([^"\']*callback/cb_\d+\.html)["\'][^>]*>.*?</a>.*?'
        r'<div class=["\']fileDescription["\']>\s*Issue\s+(\d+)\s*<span>\s*-\s*([^<]+)<br\s*/?>\s*([^<]+)',
        flags=re.I | re.S,
    )
    for href, issue, month_text, title_text in pattern.findall(html_text):
        url = _normalize_text(urljoin(list_url, href))
        if url in seen:
            continue
        month_text = _normalize_text(month_text)
        title_text = _normalize_text(title_text)
        title = f"NASA ASRS CALLBACK Issue {issue}: {title_text}"
        raw_text = (
            f"NASA ASRS CALLBACK safety learning bulletin. Issue: {issue}. "
            f"Date: {month_text}. Topic: {title_text}."
        )
        out.append(ParsedWebEntry(url=url, title=title, raw_text=raw_text, published_hint=month_text))
        seen.add(url)
    return out


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
    "reuters_aerospace_web": _parse_reuters_aerospace,
    "ain_online_web": _parse_ain_online,
    "ntsb_press_web": _parse_ntsb,
    "easa_newsroom_web": _parse_easa,
    "avherald_web": _parse_avherald,
    "asn_2026_web": _parse_asn_year,
    "easa_ad_web": _parse_easa_ad,
    "faa_safo_web": _parse_faa_operator_bulletins,
    "faa_info_web": _parse_faa_operator_bulletins,
    "nasa_asrs_callback_web": _parse_asrs_callback,
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

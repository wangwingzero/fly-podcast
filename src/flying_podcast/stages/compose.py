from __future__ import annotations

import html
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit

import requests

from flying_podcast.core.config import settings
from flying_podcast.core.llm_client import LLMError, OpenAICompatibleClient
from flying_podcast.core.io_utils import dump_json, load_json
from flying_podcast.core.logging_utils import get_logger
from flying_podcast.core.models import DailyDigest, DigestEntry
from flying_podcast.core.scoring import readability_score, weighted_quality
from flying_podcast.core.time_utils import beijing_today_str

logger = get_logger("compose")


def _load_recent_published(*, exclude_date: str | None = None) -> list[dict]:
    """Load recently published entries from history for cross-day dedup.

    Returns a flat list of dicts with fields:
    - date
    - title
    - url
    - id
    - event_fingerprint
    Returns empty list if the file doesn't exist or is corrupted.
    """
    history_path = settings.processed_dir.parent / "history" / "recent_published.json"
    if not history_path.exists():
        logger.info("No recent_published.json found, skipping cross-day dedup")
        return []
    try:
        raw = json.loads(history_path.read_text(encoding="utf-8"))
        days = raw.get("days", {})
        result = []
        skipped_same_day = 0
        for date_str, entries in days.items():
            if exclude_date and str(date_str).strip() == exclude_date:
                skipped_same_day += len(entries) if isinstance(entries, list) else 0
                continue
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                title = str(entry.get("title", "")).strip()
                url = str(entry.get("url", "")).strip()
                item_id = str(entry.get("id", "")).strip()
                event_fp = str(entry.get("event_fingerprint", "")).strip()
                if not any([title, url, item_id, event_fp]):
                    continue
                result.append(
                    {
                        "date": date_str,
                        "title": title,
                        "url": url,
                        "id": item_id,
                        "event_fingerprint": event_fp,
                    }
                )
        if exclude_date and skipped_same_day:
            logger.info(
                "Cross-day dedup: ignored %d same-day titles for %s",
                skipped_same_day,
                exclude_date,
            )
        logger.info("Cross-day dedup: %d recent titles loaded", len(result))
        return result
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to load recent_published.json: %s", exc)
        return []


def _normalize_title_for_recent_dedup(title: str) -> str:
    text = str(title or "").strip().lower()
    if not text:
        return ""
    text = re.sub(r"\s+", "", text)
    text = re.sub(r"[^\w\u4e00-\u9fff]+", "", text)
    return text


# ---------------------------------------------------------------------------
# Fuzzy title similarity for cross-day dedup
# ---------------------------------------------------------------------------

# Regex: split into CJK characters, English words, or digit sequences
_TOKEN_RE = re.compile(r"[\u4e00-\u9fff]|[a-zA-Z][a-zA-Z0-9]*|[0-9]+")

# Short / stop tokens to ignore (too common to be meaningful)
_STOP_TOKENS: set[str] = {
    "a", "an", "the", "of", "in", "on", "at", "to", "for", "and", "or",
    "is", "are", "was", "were", "be", "by", "with", "from", "as", "its",
    "的", "了", "在", "与", "和", "将", "为", "被", "已", "于", "从",
    "向", "至", "等", "及", "上", "下", "中", "后", "前", "新", "大",
}


def _tokenize_title_for_fuzzy(title: str) -> tuple[frozenset[str], frozenset[str]]:
    """Extract tokens from a title, split into (english+numbers, cjk) sets.

    Returns two frozensets: (en_num_tokens, cjk_tokens).
    English words and numbers are high-signal (airline names, model numbers).
    CJK characters carry event-specific meaning but are more granular.
    """
    text = str(title or "").strip()
    if not text:
        return frozenset(), frozenset()
    raw_tokens = _TOKEN_RE.findall(text)
    en_num: set[str] = set()
    cjk: set[str] = set()
    for t in raw_tokens:
        if len(t) == 1 and "\u4e00" <= t <= "\u9fff":
            if t not in _STOP_TOKENS:
                cjk.add(t)
        else:
            low = t.lower()
            if low in _STOP_TOKENS:
                continue
            if len(low) == 1 and low.isascii():
                continue
            en_num.add(low)
    return frozenset(en_num), frozenset(cjk)


def _jaccard_similarity(a: frozenset[str], b: frozenset[str]) -> float:
    if not a or not b:
        return 0.0
    intersection = len(a & b)
    union = len(a | b)
    return intersection / union if union else 0.0


def _is_fuzzy_title_match(
    tokens_a: tuple[frozenset[str], frozenset[str]],
    tokens_b: tuple[frozenset[str], frozenset[str]],
) -> float | None:
    """Check if two titles are fuzzy duplicates using two-tier Jaccard.

    Returns the similarity score if matched, None otherwise.

    Rule 1: English entities strongly overlap (>=0.8) AND some CJK overlap
            (>=0.15) → same event reported by different outlets.
    Rule 2: CJK overlap >= 0.35 AND neither title has many English tokens
            (max 2) → pure-CJK titles about the same event.
    """
    en_a, cjk_a = tokens_a
    en_b, cjk_b = tokens_b
    en_sim = _jaccard_similarity(en_a, en_b)
    cjk_sim = _jaccard_similarity(cjk_a, cjk_b)
    # Rule 1: strong English entity match + some CJK overlap
    if en_sim >= 0.8 and cjk_sim >= 0.15:
        return en_sim * 0.5 + cjk_sim * 0.5
    # Rule 2: high CJK overlap for titles with few/no English tokens
    if cjk_sim >= 0.35 and max(len(en_a), len(en_b)) <= 2:
        return cjk_sim
    return None


def _normalize_url_for_recent_dedup(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    try:
        parts = urlsplit(raw)
    except Exception:  # noqa: BLE001
        return raw.lower()
    host = (parts.netloc or "").lower().strip()
    path = (parts.path or "").rstrip("/").lower().strip()
    if not host:
        return path or raw.lower()

    keep_query: list[tuple[str, str]] = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        lk = key.lower().strip()
        if lk == "utm" or lk.startswith("utm_") or lk in {"spm", "from", "source", "gclid", "fbclid"}:
            continue
        keep_query.append((lk, value.strip()))
    keep_query.sort()
    query = urlencode(keep_query, doseq=True)
    base = f"{host}{path}"
    if query:
        return f"{base}?{query}"
    return base


def _build_recent_dedup_index(recent_published: list[dict]) -> dict[str, Any]:
    ids: set[str] = set()
    event_fps: set[str] = set()
    urls: set[str] = set()
    titles: set[str] = set()
    title_tokens_list: list[tuple[str, tuple[frozenset[str], frozenset[str]]]] = []
    for row in recent_published:
        if not isinstance(row, dict):
            continue
        item_id = str(row.get("id", "")).strip()
        if item_id:
            ids.add(item_id)
        fp = str(row.get("event_fingerprint", "")).strip()
        if fp:
            event_fps.add(fp)
        url_key = _normalize_url_for_recent_dedup(str(row.get("url", "")))
        if url_key:
            urls.add(url_key)
        raw_title = str(row.get("title", "")).strip()
        title_key = _normalize_title_for_recent_dedup(raw_title)
        if title_key:
            titles.add(title_key)
        tokens = _tokenize_title_for_fuzzy(raw_title)
        if tokens:
            title_tokens_list.append((raw_title, tokens))
    return {
        "ids": ids,
        "event_fps": event_fps,
        "urls": urls,
        "titles": titles,
        "title_tokens": title_tokens_list,
    }


def _is_recent_duplicate(
    *,
    item_id: str,
    event_fingerprint: str,
    title: str,
    canonical_url: str,
    recent_index: dict[str, Any],
) -> bool:
    if not recent_index:
        return False
    ids = recent_index.get("ids", set())
    event_fps = recent_index.get("event_fps", set())
    urls = recent_index.get("urls", set())
    titles = recent_index.get("titles", set())

    row_id = str(item_id or "").strip()
    if row_id and row_id in ids:
        return True
    row_fp = str(event_fingerprint or "").strip()
    if row_fp and row_fp in event_fps:
        return True
    row_url = _normalize_url_for_recent_dedup(canonical_url)
    if row_url and row_url in urls:
        return True
    row_title = _normalize_title_for_recent_dedup(title)
    if row_title and row_title in titles:
        return True

    # Fuzzy title similarity check — catches same-event articles with
    # different wording from different sources.
    title_tokens_list = recent_index.get("title_tokens", [])
    if title_tokens_list:
        candidate_tokens = _tokenize_title_for_fuzzy(title)
        total_tokens = len(candidate_tokens[0]) + len(candidate_tokens[1])
        if total_tokens >= 3:  # skip very short titles
            for recent_title, recent_tokens in title_tokens_list:
                sim = _is_fuzzy_title_match(candidate_tokens, recent_tokens)
                if sim is not None:
                    logger.info(
                        "Cross-day fuzzy dedup: %.2f similarity — [%s] ≈ [%s]",
                        sim, title[:50], recent_title[:50],
                    )
                    return True
    return False


def _prioritize_non_recent_candidates(candidates: list[dict], recent_index: dict[str, Any]) -> list[dict]:
    """Remove candidates that match recently published entries.

    Hard-filters duplicates when enough fresh candidates exist (>= target count).
    Falls back to appending duplicates at the end only when the fresh pool is too small.
    """
    if not candidates:
        return []
    if not any(recent_index.values()):
        return list(candidates)

    fresh: list[dict] = []
    repeated: list[dict] = []
    for row in candidates:
        if _is_recent_duplicate(
            item_id=str(row.get("id", "")),
            event_fingerprint=str(row.get("event_fingerprint", "")),
            title=str(row.get("title", "")),
            canonical_url=str(row.get("canonical_url") or row.get("url") or ""),
            recent_index=recent_index,
        ):
            repeated.append(row)
        else:
            fresh.append(row)
    if repeated:
        logger.info(
            "Cross-day dedup: removed %d repeated candidates (fresh=%d total=%d)",
            len(repeated),
            len(fresh),
            len(candidates),
        )
    # Only fall back to repeated entries when a positive article-count target is configured.
    min_needed = max(0, int(getattr(settings, "target_article_count", 0) or 0))
    if min_needed <= 0:
        return fresh
    if len(fresh) >= min_needed:
        return fresh
    logger.warning(
        "Cross-day dedup: fresh pool (%d) < target (%d), adding %d repeated as fallback",
        len(fresh), min_needed, len(repeated),
    )
    return fresh + repeated

_GLOSSARY_PATH = Path(__file__).resolve().parents[3] / "AirbusTermbase.js"

_OG_IMAGE_RE = re.compile(
    r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
    re.IGNORECASE,
)
_OG_IMAGE_RE2 = re.compile(
    r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']',
    re.IGNORECASE,
)
_BAD_IMAGE_TOKEN_RE = re.compile(r"(^|[-_/])(?:logo|favicon|icon|placeholder|sprite|avatar|blank)(?:[-_.?/]|$)", re.IGNORECASE)


def _is_usable_article_image_url(url: str) -> bool:
    """Reject site chrome images before they reach WeChat drafts."""
    clean = html.unescape(str(url or "")).strip()
    if not clean.startswith(("http://", "https://")):
        return False
    lower = clean.lower()
    if lower.startswith("data:image") or lower.split("?", 1)[0].endswith(".svg"):
        return False
    path = urlsplit(lower).path
    if any("logo" in segment for segment in path.split("/")):
        return False
    return _BAD_IMAGE_TOKEN_RE.search(path) is None


def _entry_candidate_urls(entry: Any) -> set[str]:
    urls: set[str] = set()
    for attr in ("canonical_url", "url"):
        value = getattr(entry, attr, "") if hasattr(entry, attr) else entry.get(attr, "")
        if value:
            urls.add(str(value).strip())
    citations = getattr(entry, "citations", []) if hasattr(entry, "citations") else entry.get("citations", [])
    if isinstance(citations, list):
        urls.update(str(u).strip() for u in citations if str(u).strip())
    return {u for u in urls if u}


def _image_index_from_pool(pool: list[dict]) -> dict[str, str]:
    image_by_url: dict[str, str] = {}
    for candidate in pool:
        img = str(candidate.get("image_url", "")).strip()
        if not _is_usable_article_image_url(img):
            continue
        for key in (candidate.get("canonical_url"), candidate.get("url")):
            if key:
                image_by_url.setdefault(str(key).strip(), img)
    return image_by_url


def _set_entry_image(entry: Any, image_url: str) -> None:
    if hasattr(entry, "image_url"):
        entry.image_url = image_url
    else:
        entry["image_url"] = image_url


def _fetch_og_image(url: str, timeout: int = 8) -> str:
    """Fetch og:image from a URL. Returns empty string on failure."""
    if not url or not url.startswith(("http://", "https://")):
        return ""
    try:
        resp = requests.get(
            url,
            timeout=timeout,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
            },
            allow_redirects=True,
        )
        if not resp.ok:
            logger.debug("og:image fetch failed %s: HTTP %s", url[:60], resp.status_code)
            return ""
        # Only read first 50KB to find og:image quickly
        html = resp.text[:50000]
        m = _OG_IMAGE_RE.search(html) or _OG_IMAGE_RE2.search(html)
        if m:
            img = m.group(1).strip()
            if img.startswith(("http://", "https://")):
                return img
    except Exception as exc:  # noqa: BLE001
        logger.debug("og:image fetch error %s: %s", url[:60], exc)
    return ""


def _resolve_google_url_single(page: Any, gurl: str, max_attempts: int = 3) -> str:
    """Try to resolve a Google News redirect URL with retries and increasing wait times.

    Returns the resolved real URL, or empty string if all attempts fail.
    """
    wait_times = [3000, 5000, 8000]
    for attempt in range(max_attempts):
        try:
            page.goto(gurl, wait_until="domcontentloaded", timeout=15000)
            wait_ms = wait_times[min(attempt, len(wait_times) - 1)]
            page.wait_for_timeout(wait_ms)
            final_url = page.url
            if "news.google.com" not in final_url:
                return final_url
            # Try clicking through consent/redirect pages
            try:
                page.wait_for_url(lambda u: "news.google.com" not in u, timeout=5000)
                final_url = page.url
                if "news.google.com" not in final_url:
                    return final_url
            except Exception:  # noqa: BLE001
                pass
        except Exception:  # noqa: BLE001
            pass
    return ""


def _resolve_google_url_requests(gurl: str, timeout: int = 10) -> str:
    if "news.google.com" not in gurl:
        return gurl
    try:
        resp = requests.get(
            gurl,
            timeout=timeout,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
            },
            allow_redirects=True,
        )
        final_url = (resp.url or "").strip()
        if final_url and "news.google.com" not in final_url:
            return final_url
    except Exception as exc:  # noqa: BLE001
        logger.debug("Google redirect requests resolve failed %s: %s", gurl[:80], exc)
    return ""


def _apply_resolved_candidate_url(candidate: dict, final_url: str) -> None:
    candidate["canonical_url"] = final_url
    current_url = str(candidate.get("url", "")).strip()
    if not current_url or "news.google.com" in current_url:
        candidate["url"] = final_url
    host = (urlsplit(final_url).netloc or "").lower().strip()
    if host:
        candidate["publisher_domain"] = host
    candidate["is_google_redirect"] = False


def _resolve_google_candidate_urls(candidates: list[dict]) -> None:
    pending: list[tuple[dict, str]] = []
    resolved_count = 0
    failed_count = 0

    for candidate in candidates:
        page_url = str(candidate.get("canonical_url") or candidate.get("url") or "").strip()
        if "news.google.com" not in page_url:
            continue
        final_url = _resolve_google_url_requests(page_url)
        if final_url:
            _apply_resolved_candidate_url(candidate, final_url)
            resolved_count += 1
        else:
            pending.append((candidate, page_url))

    if pending:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            logger.warning("Playwright not available, skipping Google pre-resolution fallback")
            failed_count += len(pending)
            pending = []

    if pending:
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                try:
                    for candidate, gurl in pending:
                        final_url = _resolve_google_url_single(page, gurl)
                        if final_url:
                            _apply_resolved_candidate_url(candidate, final_url)
                            resolved_count += 1
                        else:
                            failed_count += 1
                finally:
                    browser.close()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Google pre-resolution fallback failed: %s", exc)
            failed_count += len(pending)

    if resolved_count or failed_count:
        logger.info("Google candidate pre-resolution: resolved=%d failed=%d", resolved_count, failed_count)


def _resolve_google_urls_and_fetch_images(entries: list, pool: list[dict]) -> None:
    """Resolve Google News redirect URLs via Playwright, then fetch og:image."""
    image_by_url = _image_index_from_pool(pool)
    google_entries = []
    for entry in entries:
        existing_img = entry.image_url if hasattr(entry, "image_url") else entry.get("image_url", "")
        if existing_img and _is_usable_article_image_url(existing_img):
            continue
        if existing_img:
            _set_entry_image(entry, "")
        inherited = next((image_by_url[u] for u in _entry_candidate_urls(entry) if u in image_by_url), "")
        if inherited:
            _set_entry_image(entry, inherited)
            logger.info(
                "article image inherited for %s: %s",
                (entry.id if hasattr(entry, "id") else entry.get("id", ""))[:12],
                inherited[:80],
            )
            continue
        page_url = entry.canonical_url if hasattr(entry, "canonical_url") else entry.get("canonical_url", "")
        if not page_url:
            page_url = entry.url if hasattr(entry, "url") else entry.get("url", "")
        if "news.google.com" not in page_url:
            # Direct URL — use simple requests
            img = _fetch_og_image(page_url)
            if _is_usable_article_image_url(img):
                _set_entry_image(entry, img)
                logger.info("og:image found for %s: %s", (entry.id if hasattr(entry, "id") else entry.get("id", ""))[:12], img[:80])
            elif img:
                logger.info("og:image rejected for %s: %s", (entry.id if hasattr(entry, "id") else entry.get("id", ""))[:12], img[:80])
            continue
        google_entries.append((entry, page_url))

    if not google_entries:
        return

    # Use Playwright to resolve Google News redirects in batch
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.warning("Playwright not available, skipping Google redirect resolution")
        return

    resolved_count = 0
    failed_count = 0
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            try:
                for entry, gurl in google_entries:
                    eid = entry.id if hasattr(entry, "id") else entry.get("id", "")
                    final_url = _resolve_google_url_single(page, gurl)
                    if not final_url:
                        failed_count += 1
                        logger.info("Google redirect failed after retries: %s", eid[:12])
                        continue
                    resolved_count += 1
                    # Update canonical_url to the real URL
                    if hasattr(entry, "canonical_url"):
                        entry.canonical_url = final_url
                    # Update citations to use real URL instead of Google redirect
                    if hasattr(entry, "citations") and entry.citations:
                        entry.citations = [final_url]
                    # Try og:image from the page JS context first
                    try:
                        og = page.evaluate(
                            '() => { const m = document.querySelector(\'meta[property="og:image"]\'); return m ? m.content : ""; }'
                        )
                    except Exception:  # noqa: BLE001
                        og = ""
                    if _is_usable_article_image_url(og):
                        _set_entry_image(entry, og)
                        logger.info("og:image found (playwright) for %s: %s", eid[:12], og[:80])
                    else:
                        # Fallback: fetch og:image via requests from resolved URL
                        img = _fetch_og_image(final_url)
                        if _is_usable_article_image_url(img):
                            _set_entry_image(entry, img)
                            logger.info("og:image found (resolved) for %s: %s", eid[:12], img[:80])
                        else:
                            logger.info("og:image not found for %s from %s", eid[:12], final_url[:60])
            finally:
                browser.close()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Playwright launch failed: %s", exc)
    logger.info("Google redirect resolution: resolved=%d failed=%d", resolved_count, failed_count)


def _load_aviation_glossary() -> dict[str, str]:
    """Load aviation terminology from AirbusTermbase.js → {english: chinese}."""
    if not _GLOSSARY_PATH.exists():
        return {}
    raw = _GLOSSARY_PATH.read_text(encoding="utf-8")
    # Strip JS module wrapper: module.exports = [...]
    start = raw.find("[")
    end = raw.rfind("]")
    if start < 0 or end < 0:
        return {}
    arr = json.loads(raw[start : end + 1])
    glossary: dict[str, str] = {}
    for item in arr:
        en = str(item.get("english_full", "")).strip()
        zh = str(item.get("chinese_translation", "")).strip()
        # Skip trivial entries (math comparisons, single chars)
        if en and zh and len(en) > 3 and " is " not in en.lower():
            glossary[en] = zh
    return glossary


def _normalize_for_match(text: str) -> str:
    """Normalize text for fuzzy glossary matching: lowercase, hyphens→spaces."""
    text = text.lower().replace("-", " ").replace("_", " ")
    text = re.sub(r"\s+", " ", text)
    return text


def _stem_word(w: str) -> str:
    """Naive English stemming: strip common suffixes for matching purposes."""
    for suffix in ("ting", "ning", "ring", "ing", "ied", "ies", "ed", "es", "s"):
        if w.endswith(suffix) and len(w) - len(suffix) >= 3:
            return w[: -len(suffix)]
    return w


def _stem_phrase(phrase: str) -> str:
    """Stem each word in a phrase."""
    return " ".join(_stem_word(w) for w in phrase.split())


# Trivial words to skip when doing constituent-word matching for compound terms
_STOP_WORDS = frozenset({
    "a", "an", "the", "of", "for", "and", "or", "in", "on", "to", "at",
    "by", "is", "it", "no", "not", "all", "any", "up", "out", "off",
})


def _match_glossary_for_candidates(candidates: list[dict], glossary: dict[str, str]) -> str:
    """Find glossary terms that appear in candidate texts and return as prompt string.

    Three-tier matching strategy (from strictest to most flexible):
    1. Exact normalized match: term appears verbatim in corpus
    2. Stemmed match: stemmed term appears in stemmed corpus
    3. Constituent-word match (compound terms only): every significant word
       of the term appears somewhere in the corpus (not necessarily adjacent)

    This ensures terms like 'Rejected Takeoff' match 'Rejects Takeoff',
    and 'Emergency Landing' matches articles mentioning both words separately.
    """
    if not glossary:
        return ""
    raw_corpus = _normalize_for_match(
        " ".join(f"{c.get('title', '')} {c.get('raw_text', '')}" for c in candidates)
    )
    stemmed_corpus = _stem_phrase(raw_corpus)
    # Build a set of stemmed words for constituent-word matching (tier 3)
    stemmed_word_set = set(stemmed_corpus.split())

    exact: list[str] = []        # tier 1+2: exact/stemmed phrase match
    constituent: list[str] = []  # tier 3: all constituent words present

    for en, zh in glossary.items():
        normalized = _normalize_for_match(en)
        entry_str = f"{en} → {zh}"

        # Tier 1: exact normalized substring
        if normalized in raw_corpus:
            exact.append(entry_str)
            continue

        stemmed = _stem_phrase(normalized)

        # Tier 2: stemmed phrase substring (single-word: require len>=5 to avoid noise)
        if " " in normalized:
            if stemmed in stemmed_corpus:
                exact.append(entry_str)
                continue
        elif len(normalized) >= 5:
            if stemmed in stemmed_corpus:
                exact.append(entry_str)
                continue

        # Tier 3: for compound terms (2+ words), check if every significant
        # constituent word (stemmed) appears anywhere in the corpus
        if " " in normalized:
            significant_stems = [
                _stem_word(w) for w in normalized.split()
                if w not in _STOP_WORDS and len(w) >= 3
            ]
            if significant_stems and all(s in stemmed_word_set for s in significant_stems):
                constituent.append(entry_str)

    # Prioritize exact/stemmed matches, then add constituent matches up to cap
    matched = exact + constituent
    if not matched:
        return ""
    return "\n".join(matched[:80])
_TITLE_NOISE_PATTERNS = [
    r"\bPress Release\b",
    r"\bCommercial Aircraft\b",
    r"\b\d+\s*min read\b",
    r"\bRead more\b",
    r"\bRead more about\b",
]


def _strip_html(text: str) -> str:
    """Remove HTML tags and return plain text."""
    return re.sub(r"<[^>]+>", "", text).strip()


def _clean_title(title: str) -> tuple[str, str]:
    """Strip trailing ' - SourceName' suffix common in Google News titles.

    Returns (clean_title, source_name).
    """
    best_idx = -1
    best_sep = ""
    for sep in [" - ", " – ", " — "]:
        idx = title.rfind(sep)
        if idx > best_idx:
            best_idx = idx
            best_sep = sep
    if best_idx > 0:
        left = title[:best_idx].strip()
        right = title[best_idx + len(best_sep) :].strip()
        if len(right) < 40 and left:
            title = left
            source_name = right
        else:
            source_name = ""
    else:
        source_name = ""

    cleaned = title.strip()
    for pat in _TITLE_NOISE_PATTERNS:
        cleaned = re.sub(pat, " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b\d{1,2}\s+[A-Za-z]+\s+20\d{2}\b", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -|")

    # Deduplicate repeated first phrase in long English titles.
    words = cleaned.split()
    if len(words) >= 12:
        for n in range(6, min(18, len(words) // 2 + 1)):
            if words[:n] == words[n : 2 * n]:
                cleaned = " ".join(words[:n])
                break
    return cleaned.strip(), source_name


def _is_noisy_title(title: str) -> bool:
    if len(title.strip()) > 110:
        return True
    for pat in _TITLE_NOISE_PATTERNS:
        if re.search(pat, title, flags=re.IGNORECASE):
            return True
    return False


def _split_facts(raw_text: str, title: str = "") -> list[str]:
    plain = _strip_html(raw_text)
    # Decode HTML entities leftover from RSS
    plain = plain.replace("\xa0", " ").replace("&nbsp;", " ").strip()
    plain = re.sub(r"The post .*? appeared first on .*", " ", plain, flags=re.IGNORECASE)
    plain = re.sub(r"本文.*?仅供参考", " ", plain, flags=re.IGNORECASE)
    if not plain or len(plain) < 15:
        return []
    # Google News raw_text often echoes the title — skip if too similar.
    title_prefix = title.lower().strip()[:30]
    if title_prefix and plain.lower().strip().startswith(title_prefix):
        return []
    parts = re.split(r"[。.!?]\s*", plain)
    facts = [p.strip() for p in parts if len(p.strip()) > 10]
    return facts[:3]


def _is_title_like(text: str, title: str) -> bool:
    t = re.sub(r"\s+", "", text.lower())
    k = re.sub(r"\s+", "", title.lower())
    if not t or not k:
        return False
    return t == k or t in k or k in t


def _ensure_min_facts(facts: list[str], raw_text: str, title: str, min_count: int = 2) -> list[str]:
    out: list[str] = []
    seen_keys: set[str] = set()
    for x in facts:
        x = x.strip()
        key = re.sub(r"\s+", " ", x.lower())
        if len(x) <= 10 or key in seen_keys or _is_title_like(x, title):
            continue
        out.append(x)
        seen_keys.add(key)

    plain = _strip_html(raw_text).replace("\xa0", " ").replace("&nbsp;", " ").strip()
    plain = re.sub(r"The post .*? appeared first on .*", " ", plain, flags=re.IGNORECASE)
    if len(out) < min_count and plain:
        for seg in re.split(r"[。.!?;；,，]\s*", plain):
            seg = seg.strip()
            key = re.sub(r"\s+", " ", seg.lower())
            if len(seg) <= 10 or key in seen_keys or _is_title_like(seg, title):
                continue
            out.append(seg)
            seen_keys.add(key)
            if len(out) >= 3:
                break
    if len(out) < min_count:
        # Avoid title repetition when source only has headline-level snippet.
        if len(out) == 0:
            out.append("本条为新闻摘要，请点击原文链接查看完整运行细节。")
        if len(out) < min_count:
            out.append("附原始来源链接，供机组和签派核实关键信息。")
    return out[:3]


def _build_conclusion(title: str) -> str:
    clean, _ = _clean_title(title)
    return clean[:80]


def _build_impact() -> str:
    return "详见原始来源。"


_META_SENTENCE_PATTERNS = [
    r"新闻标题所述事件核心为[^。！？!?]*[。！？!?]?",
    r"标题所述事件核心为[^。！？!?]*[。！？!?]?",
    r"报道标题所述事件核心为[^。！？!?]*[。！？!?]?",
    r"原文未(?:列出|提供|说明)[^。！？!?]*[。！？!?]?",
    r"(?:标题涉及|原文提到|原文列明|原文同时提到|原文称|原文显示)",
]

_META_SENTENCE_PHRASES = [
    "报道提到",
    "报道指出",
    "报道显示",
    "报道称",
    "新闻称",
    "新闻指出",
    "新闻显示",
    "据报道",
    "报道所述事件核心为",
    "新闻标题所述事件核心为",
    "标题所述事件核心为",
    "事件核心",
    "事件核心是",
    "事件核心为",
    "信息显示",
    "现有信息显示",
    "仍令人费解",
    "被形容为",
    "事件性质为",
    "事件性质是",
    "严重后果",
    "险些酿成",
    "短时运行中断",
    "已明确的时间节点为",
    "时间节点为",
    "时间点为",
    "主体为",
    "主体包括",
    "原始信息中明确的主体包括",
    "根据标题信息",
    "相关措施发生在",
    "受影响范围为",
    "相关主管部门此后",
]


def _sanitize_body_text(body: str) -> str:
    text = html.unescape((body or "").strip())
    if not text:
        return ""

    comment = ""
    if "划重点：" in text:
        main, comment_part = text.split("划重点：", 1)
        text = main.strip()
        comment = "划重点：" + comment_part.strip()

    for pattern in _META_SENTENCE_PATTERNS:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE)

    sentences = re.split(r"(?<=[。！？!?])", text)
    cleaned_parts: list[str] = []
    for sentence in sentences:
        sentence = sentence.strip()
        if not sentence:
            continue
        lowered = sentence.lower()
        if any(phrase.lower() in lowered for phrase in _META_SENTENCE_PHRASES):
            continue
        cleaned_parts.append(sentence)

    text = "".join(cleaned_parts).strip()
    if comment:
        return f"{text}\n{comment}".strip() if text else comment
    return text


def _pick_final_entries(candidates: list[dict], total: int, domestic_ratio: float) -> list[dict]:
    if domestic_ratio <= 0.0:
        candidates = [c for c in candidates if c.get("region") != "domestic"]
    if total <= 0:
        return candidates
    return candidates[:total]


def _to_digest_entry(item: dict[str, Any], title: str, conclusion: str, facts: list[str], impact: str, body: str = "") -> DigestEntry:
    source_name = item.get("source_name", "")
    pilot_value = item.get("pilot_value") if isinstance(item.get("pilot_value"), dict) else {}
    section = str(item.get("section") or pilot_value.get("category") or "").strip()
    clean_title = html.unescape(title.strip()) or _clean_title(item.get("title", ""))[0]
    if _is_noisy_title(clean_title):
        clean_title = _clean_title(item.get("title", ""))[0]
    conclusion = html.unescape(conclusion.strip()[:120]) or clean_title[:80]
    normalized_facts = _ensure_min_facts(
        [f.strip() for f in facts if str(f).strip()],
        raw_text=item.get("raw_text", ""),
        title=clean_title,
        min_count=2,
    )
    impact = impact.strip() or _build_impact()
    # If body is provided but facts are empty, derive facts from body for scoring
    body = _sanitize_body_text(body)
    if body and not facts:
        normalized_facts = _ensure_min_facts(
            [s.strip() for s in re.split(r"[。.!?；]\s*", body) if s.strip()],
            raw_text=item.get("raw_text", ""),
            title=clean_title,
            min_count=2,
        )
    citation = item.get("canonical_url") or item.get("url") or item.get("source_url") or ""
    read_score = readability_score(conclusion, normalized_facts, impact)
    score = item.get("score_breakdown", {})
    total_score = weighted_quality(
        factual=90.0,
        relevance=float(score.get("relevance", 70.0)),
        authority=float(score.get("authority", 70.0)),
        timeliness=float(score.get("timeliness", 70.0)),
        readability=read_score,
    )
    score_breakdown = {
        "factual": 90.0,
        "relevance": float(score.get("relevance", 70.0)),
        "authority": float(score.get("authority", 70.0)),
        "timeliness": float(score.get("timeliness", 70.0)),
        "readability": read_score,
        "total": total_score,
    }
    return DigestEntry(
        id=item["id"],
        source_id=item.get("source_id", ""),
        section=section,
        title=clean_title,
        conclusion=conclusion,
        facts=normalized_facts,
        impact=impact,
        citations=[citation] if citation else [],
        source_tier=item.get("source_tier", "C"),
        region=item.get("region", "international"),
        score_breakdown=score_breakdown,
        source_name=source_name,
        url=item.get("url", ""),
        canonical_url=item.get("canonical_url", citation),
        publisher_domain=item.get("publisher_domain", ""),
        event_fingerprint=item.get("event_fingerprint", ""),
        published_at=item.get("published_at", ""),
        image_url=item.get("image_url", ""),
        source_role=item.get("source_role", ""),
        body=body,
    )


# DEPRECATED: replaced by _build_selection_prompt + _build_composition_prompt (two-phase flow)
def _build_llm_prompts(candidates_pool: list[dict], total: int, domestic_quota: int, intl_quota: int, recent_published: list[dict] | None = None) -> tuple[str, str]:
    payload = []
    for row in candidates_pool:
        payload.append(
            {
                "ref_id": row["id"],
                "title": row.get("title", ""),
                "raw_text": _strip_html(row.get("raw_text", ""))[:500],
                "region": row.get("region", "international"),
                "source_tier": row.get("source_tier", "C"),
                "source_name": row.get("source_name", ""),
                "publisher_domain": row.get("publisher_domain", ""),
            }
        )

    glossary = _load_aviation_glossary()
    glossary_block = _match_glossary_for_candidates(candidates_pool, glossary)
    glossary_instruction = ""
    if glossary_block:
        glossary_instruction = (
            "\n\n以下是航空专业术语对照表，翻译时必须使用这些标准译法：\n"
            + glossary_block
        )

    dedup_instruction = ""
    if recent_published:
        dedup_instruction = (
            "\n\n【去重 - 最高优先级】recently_published列表包含最近已发布的文章标题。"
            "绝对不能选择与已发布内容相同或主题高度相似的文章。"
            "宁可少选几条，也不要选重复的。"
        )

    system_prompt = (
        "你是服务于飞行员的国际航空新闻编辑。"
        "你的首要任务是筛掉与飞行运行无关的泛社会、娱乐、旅游、财经新闻。"
        "【选稿原则】优先选择有真正信息增量的内容——新事实、新数据、新政策、新事件。"
        "坚决过滤：政治宣传/政绩报道、企业软文/广告、空洞的口号式报道、"
        "领导视察/会议通稿、无实质内容的表态类新闻。"
        "判断标准：读完这条新闻，飞行员能获得什么具体的、可操作的信息？"
        "如果答案是「没有」，就不要选。"
        "像新航线开通、机型投放、机队扩张、订单交付、常规排班和计划维护这类行业背景稿，"
        "除非直接涉及飞行程序、运行限制、训练要求或安全风险，否则不要选。"
        "必须只基于输入内容改写，不得引入外部事实，不得编造链接或ID。"
        "【重要】所有输出内容（title、conclusion、body）必须是中文。"
        "英文新闻必须翻译成专业、准确的中文，航空专业术语使用标准译法。"
        "【重要】外国航空公司名称必须保留英文原名，不要翻译。"
        "例如：Delta、United、American Airlines、Lufthansa、Emirates、Singapore Airlines、"
        "Qatar Airways、British Airways、Cathay Pacific、Ryanair 等保持英文。"
        "body 字段是正文：像一个记者一样，用4-6句话把核心事实讲清楚、讲明白。"
        "读者可能通过微信「听文章」功能收听，所以正文必须仅靠听就能听懂，"
        "不要用项目符号或编号列表，用自然的叙述语言连贯表达。"
        "正文要紧凑但不能过短，优先写到180-260字；如果原文信息明显不足，也至少写出3句完整事实。"
        "不要写空话套话，不要加「总之」「综上」等总结语。"
        "输出必须是 JSON object，且仅包含 entries 字段。"
        + glossary_instruction
        + dedup_instruction
    )
    user_data: dict[str, Any] = {
        "task": "从候选国际航空新闻中生成日报条目（中文输出，航司名保留英文）",
        "audience": "飞行员",
        "rules": {
            "total": total,
            "domestic_quota": domestic_quota,
            "international_quota": intl_quota,
            "must_keep_ref_id": True,
            "must_not_generate_links": True,
            "pilot_relevance_first": True,
            "language": "zh-CN（中文输出，外国航司名保留英文原名）",
            "body_style": "记者叙述体，4-6句话讲清核心事实，优先180-260字，适合朗读收听，航司名用英文",
            "hard_reject_topics": [
                "股价/财报/融资",
                "明星娱乐",
                "旅游生活方式",
                "泛科技八卦",
                "与飞行运行无关的社会新闻",
                "政治宣传/政绩报道/领导视察",
                "企业软文/品牌广告/营销推广",
                "空洞的会议通稿/表态式报道",
                "无实质信息增量的口号式新闻",
            ],
            "prefer_topics": [
                "运行安全（事故/事件/安全通报）",
                "适航与监管（新规/适航指令/罚单）",
                "空域与航班运行（流控/航路/NOTAM）",
                "机队与机务维护（故障/AD/SB）",
                "气象与运行影响（极端天气/火山灰）",
                "训练与资质要求变更",
                "机场/空域/程序限制变化",
            ],
            "selection_principle": "每条新闻必须有信息增量——新事实、新数据、新政策或新事件，拒绝空话套话",
        },
        "output_schema": {
            "entries": [
                {
                    "ref_id": "string（保持原始ID不变）",
                    "title": "string（中文标题）",
                    "conclusion": "string（中文结论，一句话概括）",
                    "body": "string（正文：记者叙述体，4-6句话讲清核心事实，优先180-260字）",
                }
            ]
        },
        "candidates": payload,
    }
    if recent_published:
        user_data["recently_published"] = recent_published
        user_data["rules"]["avoid_recent_duplicates"] = "避免选择与recently_published中标题相同或主题高度相似的文章"
    user_prompt = json.dumps(user_data, ensure_ascii=False)
    return system_prompt, user_prompt


def _build_entries_with_rules(selected: list[dict]) -> list[DigestEntry]:
    entries: list[DigestEntry] = []
    for item in selected:
        raw_title = item.get("title", "")
        clean_title, source_name = _clean_title(raw_title)
        item = dict(item)
        item["source_name"] = item.get("source_name") or source_name
        facts = _split_facts(item.get("raw_text", ""), title=clean_title)
        conclusion = clean_title[:80]
        impact = _build_impact()
        body = "".join(
            f if f.rstrip().endswith(("。", ".", "!", "?", "！", "？")) else f + "。"
            for f in facts if f
        ) if facts else ""
        entry = _to_digest_entry(item, clean_title, conclusion, facts, impact, body=body)
        if not entry.citations:
            continue
        entries.append(entry)
    return entries


# ---------------------------------------------------------------------------
# Two-phase LLM compose: Phase 1 (selection) + Phase 2 (per-article composition)
# ---------------------------------------------------------------------------

_COMPOSE_RAW_TEXT_LIMIT = 2000
_COMPOSE_MAX_WORKERS = 1
_THIN_CONTENT_THRESHOLD = 200  # chars — below this, try to fetch full article


def _fetch_article_text(url: str, timeout: int = 12) -> str:
    """Fetch article page and extract main text content.

    Used to enrich articles that only have title/short snippet from RSS.
    Chain: requests (fast) → nodriver+Xvfb (JS rendering fallback).
    Returns extracted text (up to 3000 chars) or empty string on failure.
    """
    if not url or not url.startswith(("http://", "https://")):
        return ""
    # Skip Google News redirect URLs (need special handling)
    if "news.google.com" in url:
        return ""

    def _extract_paragraphs(html_text: str) -> str:
        """Extract readable text from <p> tags, skip boilerplate."""
        html_text = re.sub(r"<(script|style|nav|header|footer|aside)[^>]*>.*?</\1>", "", html_text, flags=re.DOTALL | re.IGNORECASE)
        avherald_blocks = re.findall(
            r'<span[^>]+class=["\']sitetext["\'][^>]*>(.*?)</span>',
            html_text,
            flags=re.DOTALL | re.IGNORECASE,
        )
        if avherald_blocks:
            text = " ".join(_strip_html(block) for block in avherald_blocks)
            text = re.sub(r"\s+", " ", html.unescape(text)).strip()
            if "List by: Filter:" in text:
                text = text.split("List by: Filter:", 1)[1].strip()
            if len(text) > 100:
                return text
        paragraphs = re.findall(r"<p[^>]*>(.*?)</p>", html_text, flags=re.DOTALL | re.IGNORECASE)
        text_parts = []
        for p in paragraphs:
            clean = _strip_html(p).strip()
            if len(clean) < 30:
                continue
            if any(kw in clean.lower() for kw in [
                "cookie", "subscribe", "newsletter", "sign up", "log in",
                "privacy policy", "terms of", "copyright", "all rights reserved",
                "advertisement", "read more about",
            ]):
                continue
            text_parts.append(clean)
        return " ".join(text_parts)

    # Step 1: Try requests (fast, works for static pages)
    text = ""
    try:
        resp = requests.get(
            url, timeout=timeout,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
            },
            allow_redirects=True,
        )
        if resp.ok:
            text = _extract_paragraphs(resp.text[:100000])
    except Exception as exc:  # noqa: BLE001
        logger.debug("Article requests fetch failed %s: %s", url[:60], exc)

    if len(text) >= 200:
        logger.info("Fetched article text (%d chars) via requests from %s", len(text[:3000]), url[:60])
        return text[:3000]

    # Step 2: Fallback to nodriver + Xvfb for JS-rendered pages
    try:
        from flying_podcast.stages.ingest import _fetch_html_nodriver
        html_text = _fetch_html_nodriver(url, timeout_ms=20000, use_xvfb=True)
        if html_text:
            text = _extract_paragraphs(html_text[:100000])
            if len(text) >= 100:
                logger.info("Fetched article text (%d chars) via nodriver from %s", len(text[:3000]), url[:60])
                return text[:3000]
    except Exception as exc:  # noqa: BLE001
        logger.debug("Article nodriver fetch failed %s: %s", url[:60], exc)

    if len(text) >= 100:
        return text[:3000]
    return ""


def _enrich_thin_candidates(candidates: list[dict]) -> None:
    """Enrich candidates with thin raw_text by fetching full article content.

    Modifies candidates in place. Only fetches for articles that have
    very short raw_text (likely RSS title-only or brief snippet).
    """
    enriched = 0
    for cand in candidates:
        raw_text = _strip_html(cand.get("raw_text", ""))
        title = cand.get("title", "")
        content_len = len(raw_text.strip())
        title_len = len(title.strip())
        # Only fetch if content is very thin
        if content_len >= _THIN_CONTENT_THRESHOLD and content_len >= title_len * 2:
            continue
        # Try canonical_url first, then url
        url = cand.get("canonical_url") or cand.get("url") or ""
        if not url:
            continue
        fetched = _fetch_article_text(url)
        if fetched and len(fetched) > content_len * 2:
            cand["raw_text"] = fetched
            enriched += 1
    if enriched:
        logger.info("Enriched %d thin articles with fetched content", enriched)


def _build_selection_prompt(
    candidates_pool: list[dict],
    total: int,
    recent_published: list[dict] | None = None,
) -> tuple[str, str]:
    """Build prompts for Phase 1: article selection only (no content generation).

    Uses a prompt structure similar to the original compose prompt to ensure
    compatibility with the LLM API. Output schema requests only ref_id per entry.
    """
    payload = []
    for row in candidates_pool:
        payload.append(
            {
                "ref_id": row["id"],
                "title": row.get("title", ""),
                "raw_text": _strip_html(row.get("raw_text", ""))[:600],
                "source_tier": row.get("source_tier", "C"),
                "source_name": row.get("source_name", ""),
                "publisher_domain": row.get("publisher_domain", ""),
                "source_role": row.get("source_role", ""),
                "rank_score": row.get("rank_score", 0),
                "pilot_value": row.get("pilot_value", {}),
            }
        )

    dedup_instruction = ""
    if recent_published:
        dedup_instruction = (
            "\n\n【去重 - 最高优先级】recently_published列表包含最近已发布的文章标题。"
            "绝对不能选择与已发布内容相同或主题高度相似的文章，即使它看起来很有价值。"
            "判断相似的标准：同一事件的不同报道、同一主题的更新报道、"
            "标题换了措辞但讲的是同一件事——这些都算重复，必须跳过。"
            "宁可少选几条，也不要选重复的。"
        )

    system_prompt = (
        "你是国际航空行业新闻编辑，负责给飞行员读者选一份全方位的国际航空日报。\n"
        "从候选新闻中选出最有价值的文章。候选列表已经过初步筛选，全部是航空相关新闻，但仍可能混入行业背景稿、软文或局部事故稿。\n\n"
        "你的判断标准：读者是否能获得关于国际航空产业、运行环境、机队、监管、机场、空域、供应链或航司经营的具体新事实。\n"
        "日报主体应覆盖航司战略、机队、订单/交付、监管、机场、空域、MRO、供应链、联盟、劳资、SAF 和重大国际运营变化。\n"
        "可以充分利用你的理解能力做排序，但不能脑补事实；只根据候选标题和摘要判断。\n\n"
        "【选稿优先级——从高到低】\n"
        "1. 国际行业主线：航司战略、机队规划、订单/交付、租赁、联盟、航线网络、劳资、财务压力对运营的影响。\n"
        "2. 监管与运行环境：FAA/EASA/IATA/ICAO、适航、机场、空域、ATC、slot、航班大范围调整和跨国运行限制。\n"
        "3. 制造与供应链：OEM、发动机、MRO、产能、交付延误、供应链问题、SAF 和排放规则。\n"
        "4. 重大国际宏观事件：只有明确影响航司、航班、机场、空域、机队或供应链时才可入选。\n"
        "5. 事故、严重事故、空难调查默认不作为日报主体；只有造成停飞、监管动作、机队检查、跨国航班/机场/空域影响或主要国际航司受影响时才保留。\n\n"
        "【版面平衡】如果候选足够，必须形成国际航空行业简报，而不是事故简报：\n"
        "- 行业媒体来源优先，Reuters/Bloomberg 只补重大国际事件。\n"
        "- 同一来源不要连续堆太多；避免被事故源、数据库记录或单一媒体刷屏。\n"
        "- 事故类最多作为例外保留，不能成为主体。\n"
        "- 纯适航指令或检查类 AD 最多2条，除非当天没有足够行业、监管和运营类内容。\n"
        "- 软文、营销稿、泛旅游稿和没有航空后果的宏观新闻一律不选。\n\n"
        "【直接排除】航空公司官网软文、品牌宣传、管理层表态、机场商业服务、旅游生活方式、"
        "没有具体运营/监管/机队/供应链影响的股价财报和宏观政治经济新闻。\n\n"
        "【不选】股价财报、明星娱乐、旅游生活方式、政治宣传、企业软文广告、"
        "eVTOL/电动飞机/氢能飞机/超音速客机/空中出租车（这些与线运行飞行员关联弱，本期不收）。\n\n"
        + (
            f"最多选择{total}条。宁缺毋滥，如果没有足够高价值的飞行员相关内容，可以少选。\n"
            if total > 0
            else "不设数量上限。选择所有达到高价值标准的文章；没有价值就不选，禁止凑数。\n"
        )
        + "输出必须是 JSON object，且仅包含 entries 字段。"
        + dedup_instruction
    )
    user_data: dict[str, Any] = {
        "task": "从候选国际航空新闻中选出最有价值的文章",
        "audience": "飞行员",
        "rules": {
            "total": total if total > 0 else "unlimited",
            "must_keep_ref_id": True,
            "pilot_relevance_first": True,
            "selection_only": "只需返回选中文章的ref_id，不需要翻译或改写任何内容",
            "allow_fewer_entries": "没有足够高价值的国际航空行业内容时允许少选，禁止用事故流水、软文或泛旅游稿凑数",
            "hard_reject_topics": [
                "股价/财报/融资/市值",
                "明星娱乐", "旅游生活方式", "泛科技八卦",
                "与航空运行或航空产业完全无关的社会新闻",
                "政治宣传/政绩报道/领导视察",
                "纯企业软文/品牌广告/营销推广",
                "无明确航空后果的宏观政治经济新闻",
                "没有行业信息增量的航空公司官网通稿",
                "eVTOL/电动飞机/氢能飞机/超音速客机/空中出租车",
            ],
            "prefer_topics": [
                "航司战略、航线网络、联盟、劳资和运营结构变化",
                "机队、订单、交付、租赁、发动机、MRO、OEM和供应链",
                "监管、适航、机场、空域、slot、ATC和跨国运行限制",
                "Reuters/Bloomberg中的重大国际航空影响事件",
                "事故/调查仅在有停飞、监管、机队检查、跨国运行影响时保留",
            ],
            "balance": {
                "prefer_primary_industry_sources": True,
                "max_accident_exception_if_available": 1,
                "max_macro_supplement_if_available": 2,
                "max_pure_airworthiness_directives_if_available": 2,
                "max_industry_novelty_if_available": 2,
                "reject_accident_flow_as_digest_backbone": True,
            },
        },
        "output_schema": {
            "entries": [
                {
                    "ref_id": "string（保持原始ID不变）",
                }
            ]
        },
        "candidates": payload,
    }
    if recent_published:
        user_data["recently_published"] = recent_published
        user_data["rules"]["avoid_recent_duplicates"] = "避免选择与recently_published中标题相同或主题高度相似的文章"
    return system_prompt, json.dumps(user_data, ensure_ascii=False)


def _llm_select_articles(
    client: "OpenAICompatibleClient",
    candidates_pool: list[dict],
    total: int,
    recent_published: list[dict] | None = None,
) -> list[str]:
    """Phase 1: Use LLM to select which articles to include.

    Returns ordered list of ref_ids.  Raises on failure.
    """
    system_prompt, user_prompt = _build_selection_prompt(
        candidates_pool, total, recent_published,
    )
    response = client.complete_json(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        max_tokens=settings.llm_max_tokens,
        temperature=settings.llm_temperature,
        retries=3,
        timeout=180,
    )
    # Parse entries format: {"entries": [{"ref_id": "..."}, ...]}
    raw_entries = response.payload.get("entries")
    if not isinstance(raw_entries, list):
        # Also try selected_ids format as fallback
        raw_ids = response.payload.get("selected_ids")
        if isinstance(raw_ids, list):
            raw_entries = [{"ref_id": rid} for rid in raw_ids]
        else:
            raise ValueError("selection_missing_entries")

    valid_ids = {c["id"] for c in candidates_pool}
    selected = []
    for entry in raw_entries:
        if isinstance(entry, dict):
            rid = str(entry.get("ref_id", "")).strip()
        else:
            rid = str(entry).strip()
        if rid and rid in valid_ids and rid not in selected:
            selected.append(rid)

    if len(selected) < 1:
        raise ValueError(
            f"selection_insufficient: got {len(selected)}, need >= 1"
        )
    logger.info("Phase 1 selection: %d entries returned, %d valid", len(raw_entries), len(selected))
    return selected


def _is_thin_database_record(candidate: dict) -> bool:
    source_id = str(candidate.get("source_id", ""))
    raw_text = _strip_html(candidate.get("raw_text", ""))
    return source_id.startswith("asn_") and len(raw_text) < 260


def _is_editorial_anchor(candidate: dict) -> bool:
    """High-value line-pilot stories that should not be lost by LLM taste."""
    if _is_thin_database_record(candidate):
        return False
    pilot_value = candidate.get("pilot_value") if isinstance(candidate.get("pilot_value"), dict) else {}
    category = str(pilot_value.get("category") or candidate.get("section") or "")
    rank_score = float(candidate.get("rank_score") or 0.0)
    if category in {"safety_event", "ops_environment", "human_factors_training"} and rank_score >= 85.0:
        return True
    text = f"{candidate.get('title', '')} {candidate.get('raw_text', '')}".lower()
    must_keep_terms = [
        "runway collision",
        "runway incursion",
        "runway excursion",
        "engine smoke",
        "cockpit smoke",
        "cabin smoke",
        "cpdlc",
        "runway data",
        "spatial disorientation",
    ]
    return rank_score >= 80.0 and any(term in text for term in must_keep_terms)


def _pick_novelty_anchors(
    candidates_pool: list[dict],
    selected_ids: list[str],
    min_count: int,
) -> list[str]:
    """选择强制保底的趣闻锚点：ranked 池中分数最高的 industry_novelty。

    LLM 选稿是非确定性的，KC-46/777-9 这类趣闻容易被忽略。
    本函数确保至少 min_count 条 industry_novelty 进入选稿池。
    """
    if min_count <= 0:
        return []
    selected_set = set(selected_ids)
    novelty_candidates: list[tuple[float, str]] = []
    for row in candidates_pool:
        pv = row.get("pilot_value") if isinstance(row.get("pilot_value"), dict) else {}
        cat = str(pv.get("category") or row.get("section") or "")
        if cat != "industry_novelty":
            continue
        rid = str(row.get("id") or "")
        if not rid:
            continue
        novelty_candidates.append((float(row.get("rank_score") or 0.0), rid))
    novelty_candidates.sort(key=lambda x: -x[0])
    # 已经在选稿里的 novelty 计入配额
    already = sum(1 for _, rid in novelty_candidates if rid in selected_set)
    if already >= min_count:
        return []
    need = min_count - already
    extras: list[str] = []
    for _, rid in novelty_candidates:
        if rid in selected_set:
            continue
        extras.append(rid)
        if len(extras) >= need:
            break
    return extras


def _blend_selection_with_editorial_anchors(
    selected_ids: list[str],
    candidates_pool: list[dict],
    total: int,
) -> list[str]:
    """Merge LLM taste with deterministic top-ranked pilot-safety anchors."""
    if not candidates_pool:
        return selected_ids
    if total <= 0:
        anchor_limit = len(candidates_pool)
    else:
        target = max(0, int(getattr(settings, "target_article_count", 0) or 0))
        anchor_limit = min(len(candidates_pool), max(target + 4, total // 2))
    anchor_ids = [
        str(row.get("id", ""))
        for row in candidates_pool
        if _is_editorial_anchor(row)
    ][:anchor_limit]
    novelty_min = max(0, int(getattr(settings, "min_novelty_articles", 0) or 0))
    novelty_anchor_ids = _pick_novelty_anchors(candidates_pool, selected_ids, novelty_min)
    combined: list[str] = []
    # 顺序：严肃锚点 → LLM 选稿 → 趣闻锚点（趣闻放在严肃稿之后，不抢前排）
    for rid in anchor_ids + selected_ids + novelty_anchor_ids:
        if rid and rid not in combined:
            combined.append(rid)
        if total > 0 and len(combined) >= total:
            break
    if combined != selected_ids[: len(combined)]:
        logger.info(
            "Phase 1 editorial anchors merged: anchors=%d novelty_anchors=%d selected=%d final=%d",
            len(anchor_ids),
            len(novelty_anchor_ids),
            len(selected_ids),
            len(combined),
        )
    return combined


def _match_glossary_for_single(candidate: dict, glossary: dict[str, str]) -> str:
    """Match glossary terms for a single candidate article."""
    return _match_glossary_for_candidates([candidate], glossary)


def _build_composition_prompt(
    candidate: dict,
    glossary_terms: str,
) -> tuple[str, str]:
    """Build prompts for Phase 2: compose ONE article in isolation."""
    raw_text = _strip_html(candidate.get("raw_text", ""))[:_COMPOSE_RAW_TEXT_LIMIT]

    glossary_block = ""
    if glossary_terms:
        glossary_block = f"\n\n航空专业术语对照表（翻译时必须使用标准译法）：\n{glossary_terms}"

    # Determine content type based on raw_text length
    title_text = candidate.get("title", "")
    content_len = len(raw_text.strip())
    title_len = len(title_text.strip())
    # If raw_text is short (< 200 chars) or barely longer than the title,
    # it's likely just a summary/headline from RSS, not a full article.
    is_summary_only = content_len < 200 or (content_len < title_len * 2)

    if is_summary_only:
        mode_instruction = (
            "【篇幅模式：短讯】原文只有标题或简短摘要，按 2-4 句的中文短讯撕写。\n"
            "- 第一句是新闻导语：用一句话讲清最重要的 5W1H 要素（何时、何地、谁、发生了什么、结果如何）\n"
            "- 后续句子按重要性递减，只补充标题或摘要里已经明确出现的时间、地点、机型、系统、处置动作和后果\n"
            "- 信息少就写短，宁短勿假；不得为达字数推测、扩写或补充原文不存在的细节\n"
            "- 不得编造原文不存在的数据、原因、日期、人名、机组判断、调查结论\n"
            "- 留白即专业：原文没有的事实一律不写——以任何形式陈述「原文里缺什么」都禁止（未提供/未给出/未说明/未披露/未涉及/未涵盖/未列出/未交代/未点明 都禁止），不要枚举原文缺失的字段（航班号、机上人数、伤亡、跑道编号、飞机受损情况、原因 等都不要点名缺失）\n"
            "- 只写看得见、点得出的事实：谁、何时、何地、做了什么、结果如何\n"
        )
    else:
        mode_instruction = (
            "【篇幅模式：标准报道】原文较完整，按180-260字的标准中文报道撕写。\n"
            "- 采用倒金字塔结构：第一句是导语，浓缩最重要的 5W1H 要素于一句\n"
            "- 后续4-6句话按重要性递减依次展开过程、机组处置、初步影响和后续动作，不要压缩成两三句\n"
            "- 选词专业克制：发生、经历、随即、随后、确认、宣布、调查、要求、披露、修订 等专业报道动词；避开口语化、感叹化表达\n"
            "- 不引入外部事实，不替原文做判断、总结或定性\n"
            "- 留白即专业：原文没有的事实一律不写，不点名「缺失了什么」\n"
        )

    system_prompt = (
        "你身兼两个角色，写一篇两段式稿件：\n"
        "  角色 A（正文）：资深航空新闻记者，笔法标杆是路透社、Flightglobal、AIN、AVHerald——\n"
        "    事实先行、笔触干净、克制内敛、不夹带个人观点，让事实自己说话。\n"
        "  角色 B（划重点）：飞了二十年的老机长，在飞行员群里转发新闻随手配一句话——\n"
        "    调侃、吐槽、自嘲、黑色幽默，像群聊不像播报。\n"
        "任务：将下面这条英文航空新闻按上述两段式标准改写为中文稿件，并按新闻价值打分。\n"
        "重要：以下【报道写作准则】【核心原则】【高风险事实】【绝对禁止的写法】等"
        "约束**仅适用于正文（角色 A）**；结尾「划重点：」一句（角色 B）走自己的风格规则，"
        "不受正文的克制语气约束，允许反讽、夸张、口语化。\n\n"
        f"{mode_instruction}\n"
        "【报道写作准则 — 仅适用于正文】\n"
        "- 用客观第三人称陈述，时态以过去时为主，调查、影响、持续状态可用现在时\n"
        "- 不出现「值得关注」「引发热议」「值得深思」「这表明」这类记者自评\n"
        "- 不出现「据报道」「报道指出」「据悉」「新闻称」这类二手转述——你就是写这篇报道的人，事实直接陈述\n"
        "- 不写感叹号、反问句、修辞性提问、夸张比喻；语气专业、克制、内敛\n"
        "- 数字、跑道号、航班号、机型代号、航空公司名按原文出现形式精确保留\n"
        "- 报道与点评严格分离：正文是冷静的事实陈述，「划重点：」之后才允许出现观点和老机长口吻\n\n"
        "【核心原则】所有事实必须能在 source_title 或 raw_text 中找到依据。\n"
        "不引入外部事实、常识、背景知识、个人猜测；可以将英文事实翻译为中文，但不能补任何原文没有的内容。\n"
        "如果原文没有写原因、责任、处置依据、伤情程度、调查结论，就不要写。\n\n"
        "【高风险事实】涉及人数、伤亡、撤离方式、速度、高度、跑道、故障原因、处置结果时，\n"
        "禁止使用「所有」「全部」「均」「只有」「仅」「已经确认」这类全称或排他性措辞，\n"
        "除非这些词在原文中有明确依据。遇到撤离方式、人数口径不完全一致时，按原文分开写，不能合并成一个绝对表述。\n\n"
        "【绝对禁止的写法 — 仅适用于正文】\n"
        "- 禁止以任何形式陈述原文里没有什么信息（未提供/未给出/未说明/未披露/未涉及/未涵盖/未列出/未交代/未点明 + 更多细节/伤亡/原因/航班号/机上人数/跑道编号/飞机受损 等任意具体项，都禁止），也禁止「具体原因尚不清楚」「详情有待进一步报道」这种烂尾话；禁止枚举原文缺失的字段。原文没有的就在稿子里直接不写，绝不做缺失声明\n"
        "- 禁止写「报道提到」「报道指出」「新闻称」「据报道」「新闻标题所述事件核心为」「原文未列出」「标题涉及」「原文提到」这类转述原文的元叙述\n"
        "- 禁止评论原文的信息量、质量或完整程度\n"
        "- 禁止添加「值得关注」「引发关注」等空话套话\n"
        "- 禁止写总结句、判断句、评价句，例如「事件性质为…」「被形容为…」「险些酿成严重后果」「这说明…」\n"
        "- 禁止写字段抽取口吻，例如「时间节点为…」「时间点为…」「主体为…」「主体包括…」\n"
        "- 禁止写来源提示语或整理素材口吻，例如「根据标题信息…」「受影响范围为…」「相关措施发生在…」\n"
        "- 只写原文中有的事实，不需要凑字数，但也不要为了求稳把正文压成过短摘要\n"
        "- 英文占位符（unk/unknown/tbd/n/a/null/none/-- 等）不要直译——原文这样写表示「不详」，"
        "你要么把这个字段整个不写，要么写成「损伤情况不详」之类的中文表达，绝不能让 unk / n/a 出现在中文成稿里\n\n"
        "【术语要求】航空专业术语必须使用ICAO/民航标准中文译法。\n"
        "如果下方提供了术语对照表，必须严格按照对照表翻译，不得自行发挥。\n"
        "例如：Rejected Takeoff=中断起飞，Diversion=备降，Go-Around=复飞，"
        "Turbulence=颠簸，NOTAM=航行通告，METAR=例行天气报告。\n\n"
        "【打分要求 — 核心标准：信息增量】\n"
        "一个飞行员读完这条新闻后，是否获得了一个他之前不知道的具体事实？\n"
        "如果「看完像没看」，就是低分。如果获得了新的具体认知，就是高分。\n\n"
        "根据以下维度打score分（1-10分）：\n"
        "- 信息增量（占50%）：飞行员读完后能获得什么新认知？\n"
        "  高分示例：具体的安全事件细节、新的适航指令、SAFO/InFO、运行限制变更、机型系统问题、程序或训练要求\n"
        "  也算具体新事实（不是空洞概述）：新机型首飞日期/地点、退役机长告别航班、首位女机长任职、纪念涂装首航、驾驶舱新航电启用\n"
        "  低分示例（必须给1-3分）：\n"
        "    · 空洞趋势分析：「XX技术将改变航空业」— 没有具体数据或时间节点\n"
        "    · 通用科普文：「航空法律如何塑造全球航空旅行」— 没有具体新事实，没具体型号或事件\n"
        "    · 企业软文/宣传稿：某航司宣布战略愿景、某机场获奖 — 无运行相关信息\n"
        "    · 会议通稿：领导讲话、签约仪式、合作备忘录 — 飞行员看完等于没看\n"
        "    · 「某航司订购了飞机」「某航司开了新航线」「机场提升旅客体验」— 无具体运行影响\n"
        "- 运行相关性（占30%）：与飞行运行、安全、训练和机型操作的关联程度\n"
        "- 类别加分（占20%）：\n"
        "  飞行技术类（新技术、新系统、飞行操作、驾驶舱相关）→ 额外+2分\n"
        "  事故/事件类（安全事件、事故调查、紧急情况、备降返航）→ 额外+2分\n"
        "  适航指令/安全通报类 → 额外+1分\n"
        "  航空趣闻类（首飞/试飞里程碑、纪念飞行、退役/首位机长故事、罕见任务、驾驶舱创新）→ 额外+1分\n"
        "打分标准：7-10=值得发布，4-6=可发可不发，1-3=不值得发布\n"
        "注意：即使原文较短，只要涉及飞行技术或安全事件的具体事实，也应给高分。\n\n"
        "【趣闻类评分专属指引 — 重要】\n"
        "对航空趣闻类（首飞、纪念飞行、退役/首位机长、罕见任务、驾驶舱新装）评分时，\n"
        "判断标准应该是「飞行员在群里看到这条会不会停下来读一眼或转发」，\n"
        "不要按「能否直接指导今天的运行」来评——那是事故和适航指令的标准。\n"
        "趣闻类的合格分是 6-7 分（不是 5 以下），只要满足以下任一条件即可给 6 分及以上：\n"
        "- 提到具体型号 + 具体地点/日期，例如「Boeing 777-9 首架生产型在 Paine Field 首飞」\n"
        "- 提到具体航司即将运营该机型，飞行员可能会面临改装（例：「Lufthansa 即将接收」）\n"
        "- 涉及驾驶舱可见的新装备或操作变化（HUD、合成视景、AI 副驾、单飞行员等）\n"
        "- 涉及具体人物 + 飞行经历（首位女机长、传奇老机长退役）\n"
        "趣闻类只在内容是空泛宣传或纯财经/订单时给 1-3 分。\n\n"
        "【输出要求】\n"
        "- title: 中文标题，简洁准确，忠实于原文\n"
        "- conclusion: 一句话中文结论概括\n"
        "- body: 必须由两部分组成：\n"
        "    (1) 正文：纯客观报道，只写事实（谁、什么、何时、何地），不加任何评论、分析、解读、评价；不要用项目符号或编号列表\n"
        "    (2) 另起一行，以「划重点：」开头的老机长式锐评（详细规则见下方【划重点】段）\n"
        "  两部分都必须出现，缺一不可\n"
        "- score: 1-10的整数评分\n"
        "- score_reason: 一句话说明打分理由\n"
        "- 外国航空公司名称保留英文原名（如Delta、United、Lufthansa、Emirates等）\n"
        "- 飞机型号保留英文（如Boeing 737 MAX、Airbus A350等）\n"
        "- 正文简洁但不能过短，不写空话套话，不加「总之」「综上」等总结语\n"
        "- 正文中禁止出现：「反映出」「体现了」「意味着」「表明」「显示出」「值得注意的是」「被形容为」「事件性质为」「时间节点为」「主体包括」「根据标题信息」「受影响范围为」等分析、定性、字段抽取或来源提示口吻\n"
        "- 正文只回答：发生了什么事？谁？什么时候？在哪？涉及什么？——仅此而已\n"
        "- 正文结构：先写事实摘要（完整原文写4-6句话，摘要稿写2-4句），然后另起一行写「划重点：」开头的编辑锐评。\n"
        "  【划重点 — 这是本文最重要的部分，必须写好】\n"
        "  风格：你是一个飞了二十年的老机长，在飞行员群里转发新闻时随手配的一句话。\n"
        "  语气要求：保留调侃、吐槽、自嘲、或黑色幽默，像飞行员群里的老机长，不像新闻播音员。\n"
        "  事实边界：点评只能围绕正文已经写出的事实发散，不能补原因、补责任、补处置建议、补调查结论。\n"
        "  安全边界：涉及伤亡、严重受伤、紧急撤离、火警、失压、跑道事件时，可以冷幽默，但不能拿伤亡开玩笑，不能轻佻嘲笑当事机组或旅客。\n"
        "  可以用的手法：反讽、夸张、类比日常飞行生活、自嘲行业现状、抖机灵、接地气的比喻。\n"
        "  字数：1-2句话，不超过50字，越短越好，像发微博不像写报告。\n\n"
        "  好的示例（学习这种味道）：\n"
        "  「划重点：PW又出幺蛾子了，建议各位查查下个月排班表上有多少NEO，心里好有个数。」\n"
        "  「划重点：地面碰坏轮椅不赔、飞机晚点不赔，就是股价跌了赔得最快。」\n"
        "  「划重点：模拟机都批了，离真飞机交付还远着呢——不过至少以后面试可以多个机型选了。」\n"
        "  「划重点：RTO这种事，字越短心跳越快，尤其还带着发动机和刹车一起上场。」\n"
        "  「划重点：又订飞机了，建议各位先别高兴——你猜谁来飞？」\n"
        "  「划重点：737 MAX的复飞之路，比你早高峰堵在三环上还漫长。」\n"
        "  「划重点：FAA终于动手了，就是这速度，黄花菜都凉了。」\n"
        "  「划重点：说是自愿检查，但你敢不查试试？」\n"
        "  「划重点：CPDLC上行路线装错，听起来像小失误，飞起来就是大麻烦。」\n\n"
        "  绝对禁止的写法（出现以下任何一种，整条作废）：\n"
        "  X「此事值得持续关注」X「让我们拭目以待」X「这一趋势值得深思」\n"
        "  X「这无疑是一个积极信号」X「未来发展值得期待」X「业界应高度重视」\n"
        "  X「这对行业具有重要意义」X「这一举措将产生深远影响」\n"
        "  X 任何以「这」开头的总结性句子 X 任何官话套话新闻腔 X 对伤亡和受伤开玩笑\n"
        "  X 评论原文信息量/完整度/缺失——例如「信息少」「字段不全」「细节欠奉」「全靠脑补」「机长群已经在脑补」等都禁止。\n"
        "    划重点必须针对事实本身（机型、动作、后果、行业现象）发散，不针对「这条新闻信息够不够」发散\n"
        + glossary_block
        + "\n\n输出JSON：{\"title\": \"...\", \"conclusion\": \"...\", \"body\": \"...\", \"score\": 8, \"score_reason\": \"...\"}"
    )
    user_data = {
        "source_title": candidate.get("title", ""),
        "source_name": candidate.get("source_name", ""),
        "source_tier": candidate.get("source_tier", "C"),
        "publisher_domain": candidate.get("publisher_domain", ""),
        "raw_text": raw_text,
    }
    return system_prompt, json.dumps(user_data, ensure_ascii=False)


# Minimum score threshold for articles to be published
_MIN_PUBLISH_SCORE = 7
_MIN_BACKFILL_SCORE = 3
# 趣闻/新奇类（首飞、纪念飞行、人物故事）本质是行业资讯+调味剂，
# LLM 用严肃运行价值标准评分通常偏低（5-6/10），用稍低阈值放行。
_MIN_PUBLISH_SCORE_NOVELTY = 5


_TRANSLATE_BODY_PROMPT = (
    "你是航空新闻编辑。将下面的英文航空新闻翻译为简洁的中文。\n"
    "要求：\n"
    "1. 航空专业术语使用ICAO/民航标准中文译法\n"
    "2. 外国航空公司名称保留英文原名（如Delta、United、Lufthansa等）\n"
    "3. 飞机型号保留英文（如Boeing 737、Airbus A320等）\n"
    "4. 只基于原文写核心事实，纯客观报道，不加评论分析解读；原文信息少就短写，宁短勿假\n"
    "5. 正文禁止出现「反映出」「体现了」「意味着」「表明」「被形容为」「事件性质为」等分析、总结或定性语言\n"
    "5.1 禁止出现「报道提到」「报道指出」「新闻称」「据报道」「新闻标题所述事件核心为」「原文未列出」「标题涉及」「原文提到」这类元叙述\n"
    "5.2 只写客观事实，不要替原文下结论，不要写「险些酿成严重后果」「短时运行中断」这类概括性判断\n"
    "5.3 禁止写字段抽取口吻，不要出现「时间节点为…」「时间点为…」「主体为…」「主体包括…」\n"
    "5.4 禁止写来源提示语或整理素材口吻，不要出现「根据标题信息…」「受影响范围为…」「相关措施发生在…」\n"
    "6. 正文结构：先写事实摘要，然后另起一行写「划重点：」开头的编辑锐评。\n"
    "   锐评风格：飞了二十年的老机长在群里转发新闻配的一句话，可以调侃或吐槽，但必须围绕原文事实。\n"
    "   涉及伤亡、严重受伤、紧急撤离、火警、失压、跑道事件时，不得拿伤亡开玩笑，不得嘲笑当事机组或旅客。\n"
    "   禁止写「值得关注」「拭目以待」「值得深思」「积极信号」等官话套话。\n"
    "只输出JSON，不要任何其他内容。\n\n"
    "标题：{title}\n内容：{text}"
)


def _llm_translate_fallback(
    client: "OpenAICompatibleClient",
    candidate: dict,
) -> dict[str, str] | None:
    """Simplified LLM call to translate an article when Phase 2 compose fails.

    Returns {title, conclusion, body, score, score_reason} or None.
    """
    try:
        raw_title = candidate.get("title", "")
        raw_text = _strip_html(candidate.get("raw_text", ""))[:1500]
        if not raw_text or len(raw_text) < 20:
            return None

        prompt = _TRANSLATE_BODY_PROMPT.format(title=raw_title, text=raw_text)
        response = client.complete_json(
            system_prompt=(
                "你是航空新闻翻译。输出JSON：{\"title\": \"中文标题\", \"body\": \"中文正文\"}\n"
                "body格式：先写纯客观事实（不加评论分析，原文信息少就短写），然后另起一行写「划重点：」开头的编辑锐评。"
                "正文只写事实，禁止「反映出」「体现了」「意味着」等分析性语言。"
                "锐评风格：老机长在群里配的一句话，可以调侃或吐槽，但不能补事实；涉及伤亡或严重安全事件时不得轻佻。"
            ),
            user_prompt=prompt,
            max_tokens=800,
            temperature=0.1,
            retries=2,
            timeout=45,
        )
        result = response.payload
        title = str(result.get("title", "")).strip()
        body = str(result.get("body", "")).strip()
        if not title or not body:
            return None
        if not _has_chinese(body):
            return None
        logger.info("Phase 2 translate fallback ok for %s: %s", candidate.get("id", "")[:12], title[:30])
        return {
            "title": title, "conclusion": title[:80], "body": body,
            "score": 3, "score_reason": "LLM compose failed, translated via fallback",
        }
    except Exception as exc:  # noqa: BLE001
        logger.warning("Phase 2 translate fallback failed for %s: %s", candidate.get("id", "")[:12], exc)
        return None


def _llm_compose_single(
    client: "OpenAICompatibleClient",
    candidate: dict,
    glossary: dict[str, str],
) -> dict[str, str] | None:
    """Phase 2: Compose ONE article using an isolated LLM call.

    Returns {title, conclusion, body, score, score_reason} on success, or None on failure.
    """
    try:
        glossary_terms = _match_glossary_for_single(candidate, glossary)
        system_prompt, user_prompt = _build_composition_prompt(candidate, glossary_terms)
        response = client.complete_json(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            max_tokens=1500,
            temperature=0.1,
            retries=2,
            timeout=60,
        )
        result = response.payload
        title = str(result.get("title", "")).strip()
        conclusion = str(result.get("conclusion", "")).strip()
        body = str(result.get("body", "")).strip()
        score = int(result.get("score", 0))
        score_reason = str(result.get("score_reason", "")).strip()
        if not title or not body:
            logger.warning("Phase 2 empty response for %s", candidate.get("id", "")[:12])
            return None
        logger.info(
            "Phase 2 scored %s: %d/10 (%s) — %s",
            candidate.get("id", "")[:12], score, score_reason, title,
        )
        return {
            "title": title, "conclusion": conclusion, "body": body,
            "score": score, "score_reason": score_reason,
        }
    except Exception as exc:  # noqa: BLE001
        logger.warning("Phase 2 compose failed for %s: %s", candidate.get("id", "")[:12], exc)
        return None


def _llm_compose_entries(
    client: "OpenAICompatibleClient",
    selected_ids: list[str],
    candidates_pool: list[dict],
    glossary: dict[str, str],
    max_workers: int = _COMPOSE_MAX_WORKERS,
) -> list[DigestEntry]:
    """Phase 2: Compose all selected articles in parallel, isolated LLM calls.

    Falls back to LLM translate (simplified) → rules-based for individual failures.
    """
    by_id = {c["id"]: c for c in candidates_pool}
    candidates_ordered = [by_id[rid] for rid in selected_ids if rid in by_id]

    results: dict[str, dict | None] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_id = {
            executor.submit(_llm_compose_single, client, cand, glossary): cand["id"]
            for cand in candidates_ordered
        }
        for future in as_completed(future_to_id):
            cand_id = future_to_id[future]
            try:
                results[cand_id] = future.result()
            except Exception as exc:  # noqa: BLE001
                logger.warning("Phase 2 thread error for %s: %s", cand_id[:12], exc)
                results[cand_id] = None

    entries: list[DigestEntry] = []
    filtered_entries: list[tuple[int, DigestEntry]] = []  # (score, entry) for backfill
    n_llm_ok = 0
    n_fallback = 0
    n_translate_fallback = 0
    n_filtered = 0
    for cand in candidates_ordered:
        llm_result = results.get(cand["id"])
        if llm_result is not None:
            score = llm_result.get("score", 0)
            entry = _to_digest_entry(
                cand,
                llm_result["title"],
                llm_result["conclusion"],
                [],
                "",
                body=llm_result["body"],
            )
            n_llm_ok += 1
        else:
            # Phase 2 compose failed — try simplified LLM translation first
            translate_result = _llm_translate_fallback(client, cand)
            if translate_result is not None:
                score = translate_result.get("score", 3)
                entry = _to_digest_entry(
                    cand,
                    translate_result["title"],
                    translate_result["conclusion"],
                    [],
                    "",
                    body=translate_result["body"],
                )
                n_translate_fallback += 1
                logger.info("Phase 2 using translate fallback for %s", cand.get("id", "")[:12])
            else:
                score = 2
                fallback = _build_entries_with_rules([cand])
                entry = fallback[0] if fallback else None
                n_fallback += 1

        # Paywall / incomplete content detection — demote to score 1
        if entry:
            _all_text = " ".join([entry.body or "", entry.conclusion or ""] + list(entry.facts or []))
            _paywall_phrases = [
                "付费墙", "内容不完整", "未能获取", "正文截断",
                "paywall", "content unavailable", "subscription required",
                "详见原始来源", "可惜正文没给答案",
            ]
            if any(p in _all_text for p in _paywall_phrases):
                logger.info(
                    "Phase 2 paywall/incomplete content detected, demoting: %s",
                    cand.get("id", "")[:12],
                )
                score = 1

        # Unified score gate — applies to ALL paths (LLM, translate, rules)
        # 趣闻类（industry_novelty）用更宽松的阈值，其他类保持严格。
        cand_pv = cand.get("pilot_value") if isinstance(cand.get("pilot_value"), dict) else {}
        cand_category = str(cand_pv.get("category") or cand.get("section") or "")
        publish_threshold = (
            _MIN_PUBLISH_SCORE_NOVELTY
            if cand_category == "industry_novelty"
            else _MIN_PUBLISH_SCORE
        )
        if entry and entry.citations:
            if score < publish_threshold:
                logger.info(
                    "Phase 2 filtered (score %d < %d): %s",
                    score, publish_threshold, cand.get("id", "")[:12],
                )
                n_filtered += 1
                filtered_entries.append((score, entry))
                continue
            entries.append(entry)

    # Limited backfill only applies when a positive article-count target is configured.
    # In unlimited mode, low-score articles stay filtered; the issue publishes fewer items.
    article_limit = max(0, int(getattr(settings, "target_article_count", 0) or 0))
    if article_limit > 0 and len(entries) < article_limit and filtered_entries:
        filtered_entries.sort(key=lambda x: x[0], reverse=True)
        for score, entry in filtered_entries:
            if len(entries) >= article_limit:
                break
            if score < _MIN_BACKFILL_SCORE:
                continue
            entries.append(entry)
            n_filtered -= 1
            logger.info("Phase 2 backfill (score %d): %s", score, entry.title[:40])
    if n_filtered:
        logger.info("Phase 2: dropped %d low-score articles", n_filtered)

    logger.info(
        "Phase 2 compose: %d LLM ok, %d filtered (score<%d), %d translate-fallback, %d rules-fallback",
        n_llm_ok, n_filtered, _MIN_PUBLISH_SCORE, n_translate_fallback, n_fallback,
    )
    return entries


def _has_chinese(text: str) -> bool:
    """Check if text contains at least one CJK character."""
    return bool(re.search(r"[\u4e00-\u9fff]", text))


def _post_compose_review(
    entries: list[DigestEntry],
    client: "OpenAICompatibleClient | None",
    candidates_pool: list[dict],
) -> list[DigestEntry]:
    """Final quality review: ensure all entries have Chinese content.

    Catches untranslated entries that slipped through rules-fallback
    and retries translation via LLM.
    """
    if not client:
        return entries
    by_id = {c["id"]: c for c in candidates_pool}
    reviewed = []
    n_fixed = 0
    n_dropped = 0
    for entry in entries:
        # Check if body has Chinese content
        if entry.body and _has_chinese(entry.body):
            reviewed.append(entry)
            continue
        # Body is missing or English — try to translate
        cand = by_id.get(entry.id)
        if not cand:
            reviewed.append(entry)
            continue
        logger.warning(
            "Post-compose review: entry %s has non-Chinese body, retrying translation",
            entry.id[:12],
        )
        translate_result = _llm_translate_fallback(client, cand)
        if translate_result and _has_chinese(translate_result.get("body", "")):
            entry.title = translate_result["title"]
            entry.conclusion = translate_result["conclusion"]
            entry.body = translate_result["body"]
            n_fixed += 1
            logger.info("Post-compose review: fixed entry %s", entry.id[:12])
            reviewed.append(entry)
        else:
            n_dropped += 1
            logger.warning(
                "Post-compose review: dropping entry %s (non-Chinese, translation failed)",
                entry.id[:12],
            )
    if n_fixed:
        logger.info("Post-compose review: fixed %d untranslated entries", n_fixed)
    if n_dropped:
        logger.warning("Post-compose review: dropped %d untranslatable entries", n_dropped)
    return reviewed


def _enforce_constraints(
    entries: list[DigestEntry],
    pool: list[DigestEntry],
    total: int,
    domestic_ratio: float,
) -> list[DigestEntry]:
    required_sections: list[str] = []
    max_per_source = max(0, int(getattr(settings, "max_entries_per_source", 0) or 0))
    unlimited = total <= 0

    # Use already-composed overflow entries first, then rules entries as a last resort.
    # Rules entries often keep English source titles, so only Chinese rules entries are
    # eligible as replacements.
    composed_pool = [e for e in entries if _has_chinese(e.title)]
    rules_pool = [p for p in pool if _has_chinese(p.title)]
    pool = composed_pool + [p for p in rules_pool if p.id not in {e.id for e in composed_pool}]

    # Filter domestic entries from pool when domestic_ratio is 0
    if domestic_ratio <= 0.0:
        pool = [p for p in pool if p.region != "domestic"]

    uniq: list[DigestEntry] = []
    used_ids: set[str] = set()
    for e in entries:
        if e.id in used_ids:
            continue
        if domestic_ratio <= 0.0 and e.region == "domestic":
            continue
        uniq.append(e)
        used_ids.add(e.id)
    out = uniq if unlimited else uniq[:total]
    used_ids = {e.id for e in out}

    if not unlimited:
        for p in pool:
            if len(out) >= total:
                break
            if p.id not in used_ids:
                out.append(p)
                used_ids.add(p.id)
    effective_total = len(out)

    def _section_counts(rows: list[DigestEntry]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for r in rows:
            counts[r.section] = counts.get(r.section, 0) + 1
        return counts

    def _source_counts(rows: list[DigestEntry]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for r in rows:
            key = r.source_id or r.source_name
            counts[key] = counts.get(key, 0) + 1
        return counts

    # section coverage first
    for section in required_sections:
        if len(out) < len(required_sections):
            break
        if any(x.section == section for x in out):
            continue
        replacement = next((p for p in pool if p.section == section and p.id not in used_ids), None)
        if replacement is None:
            continue
        counts = _section_counts(out)
        replace_idx = None
        for i in range(len(out) - 1, -1, -1):
            if counts.get(out[i].section, 0) > 1:
                replace_idx = i
                break
        if replace_idx is None:
            replace_idx = len(out) - 1
        used_ids.discard(out[replace_idx].id)
        out[replace_idx] = replacement
        used_ids.add(replacement.id)

    # source concentration cap
    if max_per_source > 0:
        source_guard = 0
        while source_guard < 80:
            source_guard += 1
            s_counts = _source_counts(out)
            over = next((k for k, v in s_counts.items() if v > max_per_source), "")
            if not over:
                break
            section_counts = _section_counts(out)
            victim_idx = None
            for i in range(len(out) - 1, -1, -1):
                key = out[i].source_id or out[i].source_name
                if key != over:
                    continue
                victim_idx = i
                break
            if victim_idx is None:
                break
            victim = out[victim_idx]
            replacement = next(
                (
                    p
                    for p in pool
                    if p.id not in used_ids
                    and (p.source_id or p.source_name) != over
                    and s_counts.get((p.source_id or p.source_name), 0) < max_per_source
                ),
                None,
            )
            if replacement is not None and section_counts.get(victim.section, 0) <= 1 and replacement.section != victim.section:
                replacement = None
            if replacement is None:
                break
            used_ids.discard(out[victim_idx].id)
            out[victim_idx] = replacement
            used_ids.add(replacement.id)

    def _entry_category(row: DigestEntry) -> str:
        return (row.section or "").strip()

    def _is_pure_airworthiness_ad(row: DigestEntry) -> bool:
        title_l = row.title.lower()
        return row.source_id == "easa_ad_web" or (
            "适航指令" in row.title and not any(word in title_l for word in ["safo", "info", "info"])
        )

    def _replace_last_matching(
        victim_predicate: Any,
        replacement_predicate: Any,
    ) -> bool:
        nonlocal out, used_ids
        victim_idx = None
        for i in range(len(out) - 1, -1, -1):
            if victim_predicate(out[i]):
                victim_idx = i
                break
        if victim_idx is None:
            return False
        replacement = next(
            (
                p
                for p in pool
                if p.id not in used_ids
                and replacement_predicate(p)
            ),
            None,
        )
        if replacement is None:
            return False
        used_ids.discard(out[victim_idx].id)
        out[victim_idx] = replacement
        used_ids.add(replacement.id)
        return True

    def _has_available(replacement_predicate: Any) -> bool:
        return any(p.id not in used_ids and replacement_predicate(p) for p in pool)

    balance_total = len(out) if unlimited else total
    max_pure_ad = min(2, max(1, balance_total // 4))
    if max_per_source > 0:
        max_pure_ad = min(max_pure_ad, max_per_source)
    guard = 0
    while guard < 20 and sum(1 for e in out if _is_pure_airworthiness_ad(e)) > max_pure_ad:
        guard += 1
        if not _replace_last_matching(
            _is_pure_airworthiness_ad,
            lambda p: not _is_pure_airworthiness_ad(p)
            and _entry_category(p) in {"safety_event", "ops_environment", "human_factors_training"},
        ):
            break

    ops_or_human = {"ops_environment", "human_factors_training"}
    min_ops_or_human = min(2, balance_total)
    guard = 0
    while (
        guard < 20
        and sum(1 for e in out if _entry_category(e) in ops_or_human) < min_ops_or_human
        and _has_available(lambda p: _entry_category(p) in ops_or_human)
    ):
        guard += 1
        if not _replace_last_matching(
            lambda e: _is_pure_airworthiness_ad(e) or _entry_category(e) == "airworthiness_technical",
            lambda p: _entry_category(p) in ops_or_human,
        ):
            break

    min_safety_events = min(3, balance_total)
    safety_count = sum(1 for e in out if _entry_category(e) == "safety_event")
    guard = 0
    while guard < 20 and safety_count < min_safety_events and _has_available(lambda p: _entry_category(p) == "safety_event"):
        guard += 1
        if not _replace_last_matching(
            lambda e: _is_pure_airworthiness_ad(e) or _entry_category(e) in {"airworthiness_technical", "human_factors_training"},
            lambda p: _entry_category(p) == "safety_event",
        ):
            break
        safety_count = sum(1 for e in out if _entry_category(e) == "safety_event")

    return out if unlimited else out[:effective_total]


def _replace_recent_duplicates(
    entries: list[DigestEntry],
    pool: list[DigestEntry],
    recent_index: dict[str, Any],
) -> list[DigestEntry]:
    """Replace or remove entries that duplicate recently published content.

    Tries to replace each duplicate with a fresh entry from the pool.
    If no replacement is available, the duplicate is dropped entirely
    (better to have fewer articles than repeat yesterday's content).
    """
    if not entries:
        return entries
    if not any(recent_index.values()):
        return entries

    out: list[DigestEntry] = []
    used_ids = {e.id for e in entries}
    replaced = 0
    dropped = 0

    def _entry_is_recent(e: DigestEntry) -> bool:
        return _is_recent_duplicate(
            item_id=e.id,
            event_fingerprint=e.event_fingerprint,
            title=e.title,
            canonical_url=e.canonical_url or (e.citations[0] if e.citations else ""),
            recent_index=recent_index,
        )

    for current in entries:
        if not _entry_is_recent(current):
            out.append(current)
            continue
        replacement = next(
            (
                p
                for p in pool
                if p.id not in used_ids
                and not _entry_is_recent(p)
            ),
            None,
        )
        if replacement is not None:
            used_ids.add(replacement.id)
            out.append(replacement)
            replaced += 1
        else:
            dropped += 1

    if replaced or dropped:
        logger.info("Cross-day dedup final: replaced %d, dropped %d (output=%d)", replaced, dropped, len(out))
    return out


# DEPRECATED: replaced by inline validation in _llm_compose_single (two-phase flow)
def _validate_llm_entries(llm_payload: dict[str, Any], selected: list[dict], total: int) -> list[DigestEntry]:
    raw_entries = llm_payload.get("entries")
    if not isinstance(raw_entries, list):
        raise ValueError("llm_payload_missing_entries")

    by_id = {x["id"]: x for x in selected}
    dedup_ids: set[str] = set()
    out: list[DigestEntry] = []
    for item in raw_entries:
        if not isinstance(item, dict):
            continue
        ref_id = str(item.get("ref_id", "")).strip()
        if not ref_id or ref_id in dedup_ids or ref_id not in by_id:
            continue
        section = str(item.get("section", "")).strip()
        source = dict(by_id[ref_id])
        source["section"] = section
        entry = _to_digest_entry(
            source,
            str(item.get("title", "")).strip(),
            str(item.get("conclusion", "")).strip(),
            item.get("facts", []) if isinstance(item.get("facts"), list) else [],
            str(item.get("impact", "")).strip(),
            body=str(item.get("body", "")).strip(),
        )
        if not entry.citations:
            continue
        out.append(entry)
        dedup_ids.add(ref_id)
        if len(out) >= total + 4:
            break
    if not out:
        raise ValueError("llm_entries_empty")
    return out


def run(target_date: str | None = None) -> Path:
    day = target_date or beijing_today_str()
    ranked_path = settings.processed_dir / f"ranked_{day}.json"
    ranked_payload = load_json(ranked_path)
    candidates = ranked_payload.get("articles", [])
    recent_published = _load_recent_published(exclude_date=day)
    recent_index = _build_recent_dedup_index(recent_published)
    candidates = _prioritize_non_recent_candidates(candidates, recent_index)
    article_limit = max(0, int(getattr(settings, "target_article_count", 0) or 0))

    ai_pool_size = len(candidates) if article_limit <= 0 else max(article_limit * 5, 60)
    ai_candidates_pool = candidates[:ai_pool_size] if candidates else []
    if article_limit > 0 and len(ai_candidates_pool) < article_limit:
        ai_candidates_pool = list(candidates)

    selected = _pick_final_entries(
        ai_candidates_pool,
        total=article_limit,
        domestic_ratio=settings.domestic_ratio,
    )
    pool_entries = _build_entries_with_rules(ai_candidates_pool or candidates)

    entries: list[DigestEntry] = []
    compose_mode = "rules"
    compose_reason = "llm_not_configured"
    compose_meta_extra: dict[str, Any] = {}
    llm_client = None
    if OpenAICompatibleClient.is_configured():
        # Positive target: request buffer entries. Unlimited mode: let value gates decide.
        llm_total = article_limit * 2 if article_limit > 0 else len(ai_candidates_pool or selected)
        glossary = _load_aviation_glossary()
        llm_client = OpenAICompatibleClient(
            api_key=settings.llm_api_key,
            base_url=settings.llm_base_url,
            model=settings.llm_model,
        )
        try:
            # Phase 1: Selection (1 LLM call — lightweight)
            selected_ids = _llm_select_articles(
                llm_client, ai_candidates_pool or selected, llm_total, recent_published,
            )
            selected_ids = _blend_selection_with_editorial_anchors(
                selected_ids, ai_candidates_pool or selected, llm_total,
            )
            compose_meta_extra["phase1_ids"] = selected_ids

            # Enrich thin articles (selected only) before Phase 2 compose
            by_id = {c["id"]: c for c in (ai_candidates_pool or selected)}
            selected_cands = [by_id[rid] for rid in selected_ids if rid in by_id]
            _resolve_google_candidate_urls(selected_cands)
            _enrich_thin_candidates(selected_cands)

            # Phase 2: Composition (N parallel isolated LLM calls)
            entries = _llm_compose_entries(
                llm_client, selected_ids, ai_candidates_pool or selected, glossary,
            )
            compose_mode = "llm_two_phase"
            compose_reason = "ok"
        except (LLMError, ValueError, KeyError) as exc:
            compose_mode = "rules_fallback"
            compose_reason = str(exc)
            logger.warning("LLM two-phase failed, fallback to rules: %s", exc)
            entries = _build_entries_with_rules(selected)
    else:
        entries = _build_entries_with_rules(selected)

    entries = _enforce_constraints(entries, pool_entries, article_limit, settings.domestic_ratio)
    entries = _replace_recent_duplicates(entries, pool_entries, recent_index)

    # Final LLM review: ensure all entries have Chinese body content
    entries = _post_compose_review(entries, llm_client, ai_candidates_pool or candidates)

    # Enrich missing images: resolve Google redirects + fetch og:image
    _resolve_google_urls_and_fetch_images(entries, ai_candidates_pool or candidates)

    digest = DailyDigest(
        date=day,
        article_count=len(entries),
        entries=entries,
        total_score=round(sum(e.score_breakdown["total"] for e in entries) / max(len(entries), 1), 2),
    )

    out = settings.processed_dir / f"composed_{day}.json"
    payload = digest.to_dict()
    payload["meta"] = {
        "compose_mode": compose_mode,
        "compose_reason": compose_reason,
        "model": settings.llm_model if OpenAICompatibleClient.is_configured() else "",
        "article_count_limit": article_limit,
        "min_publish_score": _MIN_PUBLISH_SCORE,
        **compose_meta_extra,
    }
    dump_json(out, payload)
    logger.info("Compose done. entries=%s", len(entries))
    return out


if __name__ == "__main__":
    run()

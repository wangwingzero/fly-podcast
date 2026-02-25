from __future__ import annotations

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


def _load_recent_published() -> list[dict]:
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
        for date_str, entries in days.items():
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


def _build_recent_dedup_index(recent_published: list[dict]) -> dict[str, set[str]]:
    ids: set[str] = set()
    event_fps: set[str] = set()
    urls: set[str] = set()
    titles: set[str] = set()
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
        title_key = _normalize_title_for_recent_dedup(str(row.get("title", "")))
        if title_key:
            titles.add(title_key)
    return {
        "ids": ids,
        "event_fps": event_fps,
        "urls": urls,
        "titles": titles,
    }


def _is_recent_duplicate(
    *,
    item_id: str,
    event_fingerprint: str,
    title: str,
    canonical_url: str,
    recent_index: dict[str, set[str]],
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
    return False


def _prioritize_non_recent_candidates(candidates: list[dict], recent_index: dict[str, set[str]]) -> list[dict]:
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
    # Only fall back to repeated entries when fresh pool is critically small
    min_needed = settings.target_article_count
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


def _resolve_google_urls_and_fetch_images(entries: list, pool: list[dict]) -> None:
    """Resolve Google News redirect URLs via Playwright, then fetch og:image."""
    google_entries = []
    for entry in entries:
        existing_img = entry.image_url if hasattr(entry, "image_url") else entry.get("image_url", "")
        if existing_img:
            continue
        page_url = entry.canonical_url if hasattr(entry, "canonical_url") else entry.get("canonical_url", "")
        if not page_url:
            page_url = entry.url if hasattr(entry, "url") else entry.get("url", "")
        if "news.google.com" not in page_url:
            # Direct URL — use simple requests
            img = _fetch_og_image(page_url)
            if img:
                if hasattr(entry, "image_url"):
                    entry.image_url = img
                else:
                    entry["image_url"] = img
                logger.info("og:image found for %s: %s", (entry.id if hasattr(entry, "id") else entry.get("id", ""))[:12], img[:80])
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
                    if og and og.startswith(("http://", "https://")) and not og.lower().endswith(".svg"):
                        if hasattr(entry, "image_url"):
                            entry.image_url = og
                        else:
                            entry["image_url"] = og
                        logger.info("og:image found (playwright) for %s: %s", eid[:12], og[:80])
                    else:
                        # Fallback: fetch og:image via requests from resolved URL
                        img = _fetch_og_image(final_url)
                        if img:
                            if hasattr(entry, "image_url"):
                                entry.image_url = img
                            else:
                                entry["image_url"] = img
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


def _pick_final_entries(candidates: list[dict], total: int, domestic_ratio: float) -> list[dict]:
    if domestic_ratio <= 0.0:
        candidates = [c for c in candidates if c.get("region") != "domestic"]
    return candidates[:total]


def _to_digest_entry(item: dict[str, Any], title: str, conclusion: str, facts: list[str], impact: str, body: str = "") -> DigestEntry:
    source_name = item.get("source_name", "")
    clean_title = title.strip() or _clean_title(item.get("title", ""))[0]
    if _is_noisy_title(clean_title):
        clean_title = _clean_title(item.get("title", ""))[0]
    conclusion = conclusion.strip()[:120] or clean_title[:80]
    normalized_facts = _ensure_min_facts(
        [f.strip() for f in facts if str(f).strip()],
        raw_text=item.get("raw_text", ""),
        title=clean_title,
        min_count=2,
    )
    impact = impact.strip() or _build_impact()
    # If body is provided but facts are empty, derive facts from body for scoring
    body = body.strip()
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
        section=item.get("section", ""),
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
        "你是服务于飞行员、签派和运行控制人员的国际航空新闻编辑。"
        "你的首要任务是筛掉与飞行运行无关的泛社会、娱乐、旅游、财经新闻。"
        "【选稿原则】优先选择有真正信息增量的内容——新事实、新数据、新政策、新事件。"
        "坚决过滤：政治宣传/政绩报道、企业软文/广告、空洞的口号式报道、"
        "领导视察/会议通稿、无实质内容的表态类新闻。"
        "判断标准：读完这条新闻，飞行员/签派能获得什么具体的、可操作的信息？"
        "如果答案是「没有」，就不要选。"
        "必须只基于输入内容改写，不得引入外部事实，不得编造链接或ID。"
        "【重要】所有输出内容（title、conclusion、body）必须是中文。"
        "英文新闻必须翻译成专业、准确的中文，航空专业术语使用标准译法。"
        "【重要】外国航空公司名称必须保留英文原名，不要翻译。"
        "例如：Delta、United、American Airlines、Lufthansa、Emirates、Singapore Airlines、"
        "Qatar Airways、British Airways、Cathay Pacific、Ryanair 等保持英文。"
        "body 字段是正文：像一个记者一样，用2-4句话把核心事实讲清楚、讲明白。"
        "读者可能通过微信「听文章」功能收听，所以正文必须仅靠听就能听懂，"
        "不要用项目符号或编号列表，用自然的叙述语言连贯表达。"
        "正文要简洁，不能冗长，不要写空话套话，不要加「总之」「综上」等总结语。"
        "输出必须是 JSON object，且仅包含 entries 字段。"
        + glossary_instruction
        + dedup_instruction
    )
    user_data: dict[str, Any] = {
        "task": "从候选国际航空新闻中生成日报条目（中文输出，航司名保留英文）",
        "audience": "飞行员、签派、运行控制",
        "rules": {
            "total": total,
            "domestic_quota": domestic_quota,
            "international_quota": intl_quota,
            "must_keep_ref_id": True,
            "must_not_generate_links": True,
            "pilot_relevance_first": True,
            "language": "zh-CN（中文输出，外国航司名保留英文原名）",
            "body_style": "记者叙述体，2-4句话讲清核心事实，适合朗读收听，航司名用英文",
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
                "航司运行网络变化（新航线/停航/换季）",
                "气象与运行影响（极端天气/火山灰）",
                "有具体数据或事实的行业动态",
            ],
            "selection_principle": "每条新闻必须有信息增量——新事实、新数据、新政策或新事件，拒绝空话套话",
        },
        "output_schema": {
            "entries": [
                {
                    "ref_id": "string（保持原始ID不变）",
                    "title": "string（中文标题）",
                    "conclusion": "string（中文结论，一句话概括）",
                    "body": "string（正文：记者叙述体，2-4句话讲清核心事实）",
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

    def _extract_paragraphs(html: str) -> str:
        """Extract readable text from <p> tags, skip boilerplate."""
        html = re.sub(r"<(script|style|nav|header|footer|aside)[^>]*>.*?</\1>", "", html, flags=re.DOTALL | re.IGNORECASE)
        paragraphs = re.findall(r"<p[^>]*>(.*?)</p>", html, flags=re.DOTALL | re.IGNORECASE)
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
        html = _fetch_html_nodriver(url, timeout_ms=20000, use_xvfb=True)
        if html:
            text = _extract_paragraphs(html[:100000])
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
                "raw_text": _strip_html(row.get("raw_text", ""))[:300],
                "source_tier": row.get("source_tier", "C"),
                "source_name": row.get("source_name", ""),
                "publisher_domain": row.get("publisher_domain", ""),
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
        "你是服务于飞行员、签派和运行控制人员的国际航空新闻编辑。\n"
        "从候选新闻中选出最有价值的文章。候选列表已经过初步筛选，全部是航空相关新闻。\n\n"
        "【选稿优先级——从高到低】\n"
        "1. 运行安全：事故/事件/安全通报/紧急着陆/备降/颠簸报告\n"
        "2. 适航与监管：适航指令AD/安全建议/罚单/强制检查/新规\n"
        "3. 机队与维护：发动机问题/AD/SB/停飞令/交付延迟\n"
        "4. 运行网络变化：新航线/停航/换季调整（影响签派排班）\n"
        "5. 气象与运行：极端天气/火山灰/NOTAM/TFR\n"
        "6. 行业动态：订单/技术/培训/其他航空新闻\n\n"
        "【不选】股价财报、明星娱乐、旅游生活方式、政治宣传、企业软文广告\n\n"
        f"必须选够{total}条。确保话题多样性，避免多条新闻讲同一件事。\n"
        "输出必须是 JSON object，且仅包含 entries 字段。"
        + dedup_instruction
    )
    user_data: dict[str, Any] = {
        "task": "从候选国际航空新闻中选出最有价值的文章",
        "audience": "飞行员、签派、运行控制",
        "rules": {
            "total": total,
            "must_keep_ref_id": True,
            "pilot_relevance_first": True,
            "selection_only": "只需返回选中文章的ref_id，不需要翻译或改写任何内容",
            "must_select_enough": f"必须选够{total}条，不得少选",
            "hard_reject_topics": [
                "股价/财报/融资/市值",
                "明星娱乐", "旅游生活方式", "泛科技八卦",
                "与飞行运行完全无关的社会新闻",
                "政治宣传/政绩报道/领导视察",
                "企业软文/品牌广告/营销推广",
            ],
            "prefer_topics": [
                "运行安全（事故/事件/安全通报/紧急着陆/备降）",
                "适航与监管（适航指令AD/安全建议/罚单/强制检查）",
                "空域与航班运行（流控/航路变更/NOTAM/TFR）",
                "机队与机务维护（故障/AD/SB/停飞令/发动机问题）",
                "航司运行网络变化（新航线/停航/换季）",
                "气象与运行影响（极端天气/火山灰/颠簸报告）",
                "航空器订单与交付（影响未来运力规划）",
                "培训与资质要求变更",
                "有具体数据或事实的行业动态",
            ],
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
            "【内容类型：摘要/标题级】这条新闻只有简短的摘要或标题，没有完整原文。\n"
            "处理方式：基于已有信息写出2-3句话的中文摘要。\n"
            "- 第一句概括核心事实\n"
            "- 后续句子可基于标题和摘要中的线索，补充相关背景（如涉及的机型特点、事件类型的行业意义等）\n"
            "- 不得编造具体数据、日期、人名等不存在于原文中的细节\n"
            "- 不要写「原文未提供更多细节」之类的元评论\n"
        )
    else:
        mode_instruction = (
            "【内容类型：完整原文】这条新闻有较完整的原文内容。\n"
            "处理方式：提炼核心事实，用2-4句话总结最重要的信息，去掉冗余细节。\n"
            "不得编造原文中没有的内容。\n"
        )

    system_prompt = (
        "你是服务于飞行员、签派和运行控制人员的国际航空新闻编辑。\n"
        "你的任务是将下面这一条英文航空新闻改写为中文摘要，并对其价值打分。\n\n"
        f"{mode_instruction}\n"
        "【核心原则】必须且只能基于提供的原文内容改写，不得引入外部事实，不得编造任何信息。\n\n"
        "【绝对禁止的写法】\n"
        "- 禁止写「原文未提供更多细节」「具体原因尚不清楚」「详情有待进一步报道」等元评论\n"
        "- 禁止评论原文的信息量、质量或完整程度\n"
        "- 禁止添加「值得关注」「引发关注」等空话套话\n"
        "- 只写原文中有的事实，写完就结束，不需要凑字数\n\n"
        "【术语要求】航空专业术语必须使用ICAO/民航标准中文译法。\n"
        "如果下方提供了术语对照表，必须严格按照对照表翻译，不得自行发挥。\n"
        "例如：Rejected Takeoff=中断起飞，Diversion=备降，Go-Around=复飞，"
        "Turbulence=颠簸，NOTAM=航行通告，METAR=例行天气报告。\n\n"
        "【打分要求 — 核心标准：信息增量】\n"
        "一个飞行员读完这条新闻后，是否获得了一个他之前不知道的具体事实？\n"
        "如果「看完像没看」，就是低分。如果获得了新的具体认知，就是高分。\n\n"
        "根据以下维度打score分（1-10分）：\n"
        "- 信息增量（占50%）：飞行员读完后能获得什么新认知？\n"
        "  高分示例：具体的安全事件细节、新的适航指令、运行限制变更、新技术应用\n"
        "  低分示例：「某航司订购了飞机」「某航司开了新航线」— 飞行员看完等于没看\n"
        "- 运行相关性（占30%）：与飞行运行、安全、签派工作的关联程度\n"
        "- 类别加分（占20%）：\n"
        "  飞行技术类（新技术、新系统、飞行操作、驾驶舱相关）→ 额外+2分\n"
        "  事故/事件类（安全事件、事故调查、紧急情况、备降返航）→ 额外+2分\n"
        "  适航指令/安全通报类 → 额外+1分\n"
        "打分标准：7-10=值得发布，4-6=可发可不发，1-3=不值得发布\n"
        "注意：即使原文较短，只要涉及飞行技术或安全事件的具体事实，也应给高分。\n\n"
        "【输出要求】\n"
        "- title: 中文标题，简洁准确，忠实于原文\n"
        "- conclusion: 一句话中文结论概括\n"
        "- body: 正文，记者叙述体，适合朗读收听，"
        "不要用项目符号或编号列表\n"
        "- score: 1-10的整数评分\n"
        "- score_reason: 一句话说明打分理由\n"
        "- 外国航空公司名称保留英文原名（如Delta、United、Lufthansa、Emirates等）\n"
        "- 飞机型号保留英文（如Boeing 737 MAX、Airbus A350等）\n"
        "- 正文简洁，不写空话套话，不加「总之」「综上」等总结语\n"
        "- 正文结构：先写事实摘要（2-4句），然后另起一行写「划重点：」开头的编辑锐评。\n"
        "  锐评像一个毒舌但专业的老机长发朋友圈配文，"
        "既有行业洞察又带点个人情绪——可以是吐槽、调侃、感慨、无奈、或者犀利的反问。"
        "让读者看完会心一笑或深以为然，有共鸣感。\n"
        "  好的示例：「划重点：PW又出幺蛾子了，建议各位查查自己下个月排班表上有多少NEO，心里好有个数。」\n"
        "  好的示例：「划重点：地面碰坏轮椅不赔、飞机晚点不赔，就是股价跌了赔得最快。」\n"
        "  好的示例：「划重点：模拟机都批了，离真飞机交付还远着呢——不过至少以后面试可以多个机型选了。」\n"
        "  好的示例：「划重点：一架飞机上查出弹药，这背后安检漏洞的严重程度，比新闻标题吓人多了。」\n"
        "  禁止的写法：「划重点：此事值得持续关注。」「划重点：让我们拭目以待。」「划重点：这一趋势值得深思。」「划重点：这无疑是一个积极信号。」"
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
_MIN_PUBLISH_SCORE = 4


_TRANSLATE_BODY_PROMPT = (
    "你是航空新闻编辑。将下面的英文航空新闻翻译为简洁的中文。\n"
    "要求：\n"
    "1. 航空专业术语使用ICAO/民航标准中文译法\n"
    "2. 外国航空公司名称保留英文原名（如Delta、United、Lufthansa等）\n"
    "3. 飞机型号保留英文（如Boeing 737、Airbus A320等）\n"
    "4. 用2-3句话概括核心内容，记者叙述体\n"
    "5. 不要评论原文质量，不要写空话套话\n"
    "只输出中文翻译结果，不要任何其他内容。\n\n"
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
            system_prompt="你是航空新闻翻译。输出JSON：{\"title\": \"中文标题\", \"body\": \"中文正文\"}",
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
            if score < _MIN_PUBLISH_SCORE:
                logger.info(
                    "Phase 2 filtered (score %d < %d): %s — %s",
                    score, _MIN_PUBLISH_SCORE, cand.get("id", "")[:12],
                    llm_result.get("score_reason", ""),
                )
                n_filtered += 1
                if entry and entry.citations:
                    filtered_entries.append((score, entry))
                continue
            n_llm_ok += 1
        else:
            # Phase 2 compose failed — try simplified LLM translation first
            translate_result = _llm_translate_fallback(client, cand)
            if translate_result is not None:
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
                fallback = _build_entries_with_rules([cand])
                entry = fallback[0] if fallback else None
                n_fallback += 1
        if entry and entry.citations:
            entries.append(entry)

    # Backfill from filtered articles if we don't have enough
    target = settings.target_article_count
    if len(entries) < target and filtered_entries:
        filtered_entries.sort(key=lambda x: x[0], reverse=True)
        for score, entry in filtered_entries:
            if len(entries) >= target:
                break
            entries.append(entry)
            n_llm_ok += 1
            n_filtered -= 1
            logger.info("Phase 2 backfill (score %d): %s", score, entry.title[:40])

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
    max_per_source = settings.max_entries_per_source

    # Only allow pool entries with Chinese titles (avoid replacing LLM-translated entries
    # with untranslated English entries from the rules-based pool).
    pool = [p for p in pool if _has_chinese(p.title)]

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
    out = uniq[:total]

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

    return out[:effective_total]


def _replace_recent_duplicates(
    entries: list[DigestEntry],
    pool: list[DigestEntry],
    recent_index: dict[str, set[str]],
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
    recent_published = _load_recent_published()
    recent_index = _build_recent_dedup_index(recent_published)
    candidates = _prioritize_non_recent_candidates(candidates, recent_index)

    ai_pool_size = max(settings.target_article_count * 4, 40)
    ai_candidates_pool = candidates[:ai_pool_size] if candidates else []
    if len(ai_candidates_pool) < settings.target_article_count:
        ai_candidates_pool = list(candidates)

    selected = _pick_final_entries(
        ai_candidates_pool,
        total=settings.target_article_count,
        domestic_ratio=settings.domestic_ratio,
    )
    pool_entries = _build_entries_with_rules(ai_candidates_pool or candidates)

    entries: list[DigestEntry] = []
    compose_mode = "rules"
    compose_reason = "llm_not_configured"
    compose_meta_extra: dict[str, Any] = {}
    llm_client = None
    if OpenAICompatibleClient.is_configured():
        # Request extra entries from LLM as buffer for enforce_constraints
        llm_total = settings.target_article_count * 2
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
            compose_meta_extra["phase1_ids"] = selected_ids

            # Enrich thin articles (selected only) before Phase 2 compose
            by_id = {c["id"]: c for c in (ai_candidates_pool or selected)}
            selected_cands = [by_id[rid] for rid in selected_ids if rid in by_id]
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

    entries = _enforce_constraints(entries, pool_entries, settings.target_article_count, settings.domestic_ratio)
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
        **compose_meta_extra,
    }
    dump_json(out, payload)
    logger.info("Compose done. entries=%s", len(entries))
    return out


if __name__ == "__main__":
    run()

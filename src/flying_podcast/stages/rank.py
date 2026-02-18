from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse
from dateutil import parser as dt_parser

from flying_podcast.core.config import settings
from flying_podcast.core.io_utils import dump_json, load_json, load_yaml
from flying_podcast.core.logging_utils import get_logger
from flying_podcast.core.scoring import recency_score, relevance_score, tier_score

logger = get_logger("rank")

_DEFAULT_PILOT_SIGNAL_KEYWORDS = [
    "aviation",
    "airline",
    "aircraft",
    "flight",
    "airport",
    "airspace",
    "runway",
    "notam",
    "atc",
    "faa",
    "easa",
    "iata",
    "icao",
    "ntsb",
    "airworthiness",
    "service bulletin",
    "ad ",
    "safety",
    "incident",
    "turbulence",
    "diversion",
    "go-around",
    "民航",
    "航空",
    "航司",
    "航线",
    "航班",
    "飞机",
    "机场",
    "空管",
    "适航",
    "通告",
    "飞行",
    "机组",
    "跑道",
    "复飞",
    "备降",
    "返航",
]

_DEFAULT_PILOT_ENTITY_KEYWORDS = [
    "faa",
    "easa",
    "icao",
    "iata",
    "ntsb",
    "boeing",
    "airbus",
    "delta",
    "united",
    "american airlines",
    "lufthansa",
    "emirates",
    "民航局",
    "中国民航网",
    "caac",
    "c919",
    "arj21",
    "商飞",
]

_DEFAULT_HARD_REJECT_KEYWORDS = [
    "stock",
    "shares",
    "dividend",
    "market cap",
    "ipo",
    "earnings",
    "luxury",
    "lounge opening",
    "loyalty program",
    "frequent flyer",
    "meal service",
    "celebrity",
    "旅游",
    "游客",
    "酒店",
    "餐饮",
    "娱乐",
    "明星",
    "春晚",
    "网红",
    "股价",
    "市值",
    "融资",
    "财报",
    "分红",
    "营销",
    "赞助",
    "开业",
    "积分",
    "里程计划",
    "会员福利",
]


def _load_raw(day: str) -> list[dict]:
    path = settings.raw_dir / f"{day}.json"
    if not path.exists():
        return []
    return load_json(path)


def _keyword_hits(text: str, keywords: list[str]) -> int:
    text_l = text.lower()
    return sum(1 for word in keywords if word.lower() in text_l)


def _domain(url: str) -> str:
    try:
        return (urlparse(url).netloc or "").lower()
    except Exception:  # noqa: BLE001
        return ""


def _looks_like_google_redirect(url: str) -> bool:
    dm = _domain(url)
    path = (urlparse(url).path or "").lower() if dm else ""
    return dm.endswith("news.google.com") and path.startswith("/rss/articles/")


def _is_relevant(item: dict, keyword_hits: int, all_keywords: list[str]) -> bool:
    if keyword_hits > 0:
        return True
    text = f"{item.get('title', '')} {item.get('raw_text', '')}".lower()
    hit = sum(1 for k in all_keywords if k.lower() in text)
    return hit >= 2


def _keyword_list(values: list[str] | None, defaults: list[str]) -> list[str]:
    if not values:
        return defaults
    return [str(x).strip().lower() for x in values if str(x).strip()]


def _count_hits(text: str, keywords: list[str]) -> int:
    text_l = text.lower()
    return sum(1 for k in keywords if k and k in text_l)


def _domain_allowed(domain: str, allowed_domains: set[str]) -> bool:
    if not domain or not allowed_domains:
        return False
    return any(domain.endswith(x) for x in allowed_domains)


def _is_pilot_relevant(item: dict, text: str, kw_cfg: dict) -> tuple[bool, str]:
    signal_words = _keyword_list(kw_cfg.get("pilot_signal_keywords"), _DEFAULT_PILOT_SIGNAL_KEYWORDS)
    entity_words = _keyword_list(kw_cfg.get("pilot_entity_keywords"), _DEFAULT_PILOT_ENTITY_KEYWORDS)
    reject_words = _keyword_list(kw_cfg.get("hard_reject_keywords"), _DEFAULT_HARD_REJECT_KEYWORDS)

    allowed_source_ids = {str(x).strip() for x in kw_cfg.get("pilot_allowed_source_ids", []) if str(x).strip()}
    allowed_domains = {
        str(x).strip().lower()
        for x in kw_cfg.get("pilot_allowed_domains", [])
        if str(x).strip()
    }

    canonical_url = (item.get("canonical_url") or item.get("url") or "").strip()
    domain = _domain(canonical_url)
    source_id = str(item.get("source_id") or "").strip()

    text_l = text.lower()
    signal_hits = _count_hits(text_l, signal_words)
    entity_hits = _count_hits(text_l, entity_words)
    reject_hits = _count_hits(text_l, reject_words)
    trusted_source = source_id in allowed_source_ids or _domain_allowed(domain, allowed_domains)

    # Reject obvious noise unless signal is very strong.
    if reject_hits > 0 and signal_hits < 2:
        return False, "hard_reject_keywords"

    if signal_hits <= 0:
        return False, "missing_pilot_signal"

    if entity_hits <= 0 and not trusted_source:
        return False, "missing_aviation_entity"

    return True, "ok"


def _has_valid_published_at(value: str) -> bool:
    if not value:
        return False
    try:
        dt_parser.parse(str(value))
        return True
    except (ValueError, TypeError):
        return False


def _is_too_old(value: str, max_age_hours: int) -> bool:
    """Return True if published_at is older than *max_age_hours* from now."""
    if not value or max_age_hours <= 0:
        return False
    try:
        pub = dt_parser.parse(str(value))
        if pub.tzinfo is None:
            pub = pub.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return (now - pub) > timedelta(hours=max_age_hours)
    except (ValueError, TypeError):
        return False


def _pick_by_quota(candidates: list[dict], total: int, domestic_ratio: float) -> list[dict]:
    domestic_quota = round(total * domestic_ratio)
    intl_quota = total - domestic_quota

    domestic = [x for x in candidates if x["region"] == "domestic"]
    intl = [x for x in candidates if x["region"] != "domestic"]

    chosen = domestic[:domestic_quota] + intl[:intl_quota]

    if len(chosen) < total:
        remain = [x for x in candidates if x not in chosen]
        chosen.extend(remain[: total - len(chosen)])

    return chosen[:total]


def _enforce_source_cap(candidates: list[dict], ranked_pool: list[dict], max_per_source: int) -> tuple[list[dict], bool]:
    out = list(candidates)
    used_ids = {x.get("id") for x in out}
    applied = False

    def _counts(rows: list[dict]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for r in rows:
            key = str(r.get("source_id") or r.get("source_name") or "")
            counts[key] = counts.get(key, 0) + 1
        return counts

    guard = 0
    while guard < 100:
        guard += 1
        counts = _counts(out)
        over_key = next((k for k, v in counts.items() if v > max_per_source), "")
        if not over_key:
            break

        victim_idx = None
        for i in range(len(out) - 1, -1, -1):
            key = str(out[i].get("source_id") or out[i].get("source_name") or "")
            if key == over_key:
                victim_idx = i
                break
        if victim_idx is None:
            break
        victim = out[victim_idx]

        replacement = next(
            (
                x
                for x in ranked_pool
                if x.get("id") not in used_ids
                and str(x.get("source_id") or x.get("source_name") or "") != over_key
                and counts.get(str(x.get("source_id") or x.get("source_name") or ""), 0) < max_per_source
                and x.get("region") == victim.get("region")
            ),
            None,
        )
        if replacement is None:
            replacement = next(
                (
                    x
                    for x in ranked_pool
                    if x.get("id") not in used_ids
                    and str(x.get("source_id") or x.get("source_name") or "") != over_key
                    and counts.get(str(x.get("source_id") or x.get("source_name") or ""), 0) < max_per_source
                ),
                None,
            )
        if replacement is None:
            break

        used_ids.discard(out[victim_idx].get("id"))
        out[victim_idx] = replacement
        used_ids.add(replacement.get("id"))
        applied = True

    return out, applied


def run(target_date: str | None = None) -> Path:
    day = target_date or datetime.now().strftime("%Y-%m-%d")
    rows = _load_raw(day)
    kw = load_yaml(settings.keywords_config)
    section_map = kw.get("sections", {})
    relevance_kw = kw.get("relevance_keywords", [])
    # Support both old sections format and new flat relevance_keywords
    if relevance_kw:
        all_keywords = [str(x).strip() for x in relevance_kw if str(x).strip()]
    else:
        all_keywords = [x for words in section_map.values() for x in words]
    blocked_domains = [x.lower() for x in kw.get("blocked_domains", [])]

    ranked: list[dict] = []
    dropped_non_relevant = 0
    dropped_non_pilot_relevant = 0
    dropped_hard_reject = 0
    dropped_blocked_domain = 0
    dropped_no_original_link = 0
    dropped_no_published_at = 0
    dropped_too_old = 0
    max_age = settings.max_article_age_hours
    for item in rows:
        canonical_url = (item.get("canonical_url") or item.get("url") or "").strip()
        if not canonical_url.startswith(("http://", "https://")):
            dropped_no_original_link += 1
            continue
        dm = _domain(canonical_url)
        if dm in blocked_domains:
            dropped_blocked_domain += 1
            continue
        # Google redirect URLs are kept but penalised in scoring below.
        # Dropping them would eliminate nearly all domestic news from Google News RSS.
        if not _has_valid_published_at(item.get("published_at", "")):
            dropped_no_published_at += 1
            continue
        if _is_too_old(item.get("published_at", ""), max_age):
            dropped_too_old += 1
            continue

        text = f"{item['title']} {item['raw_text']}"
        hits = _keyword_hits(text, all_keywords)
        if not _is_relevant(item, hits, all_keywords):
            dropped_non_relevant += 1
            continue
        pilot_ok, pilot_reason = _is_pilot_relevant(item, text, kw)
        if not pilot_ok:
            dropped_non_pilot_relevant += 1
            if pilot_reason == "hard_reject_keywords":
                dropped_hard_reject += 1
            continue

        pilot_signal_words = _keyword_list(kw.get("pilot_signal_keywords"), _DEFAULT_PILOT_SIGNAL_KEYWORDS)
        pilot_hits = _count_hits(text, pilot_signal_words)
        rel = relevance_score(text, hits + pilot_hits)
        auth = tier_score(item.get("source_tier", "C"))
        if str(item.get("source_id", "")).startswith("google_"):
            auth = min(auth, 80.0)
        time_score = recency_score(item.get("published_at", ""))
        google_penalty = 15.0 if _looks_like_google_redirect(canonical_url) else 0.0
        rank_score = round(rel * 0.65 + auth * 0.20 + time_score * 0.15 - google_penalty, 2)

        enriched = dict(item)
        enriched.update(
            {
                "canonical_url": canonical_url,
                "publisher_domain": item.get("publisher_domain", dm),
                "event_fingerprint": item.get("event_fingerprint") or item.get("id"),
                "is_google_redirect": item.get("is_google_redirect", _looks_like_google_redirect(canonical_url)),
                "keyword_hits": hits,
                "rank_score": rank_score,
                "score_breakdown": {
                    "relevance": rel,
                    "authority": auth,
                    "timeliness": time_score,
                },
            }
        )
        ranked.append(enriched)

    ranked.sort(key=lambda x: x["rank_score"], reverse=True)
    deduped: list[dict] = []
    seen_fp: set[str] = set()
    seen_url: set[str] = set()
    for row in ranked:
        fp = row.get("event_fingerprint", "")
        u = row.get("canonical_url", "")
        if fp in seen_fp or u in seen_url:
            continue
        seen_fp.add(fp)
        seen_url.add(u)
        deduped.append(row)

    candidate_total = max(settings.target_article_count * 3, 25)
    top_candidates = _pick_by_quota(deduped, total=candidate_total, domestic_ratio=settings.domestic_ratio)
    top_candidates, source_cap_applied = _enforce_source_cap(
        top_candidates,
        deduped,
        max_per_source=settings.max_entries_per_source,
    )

    # Ensure A-tier ratio >= configured threshold when possible.
    min_a = int(len(top_candidates) * settings.min_tier_a_ratio)
    current_a = sum(1 for x in top_candidates if x.get("source_tier") == "A")
    if current_a < min_a:
        alt_a = [x for x in deduped if x.get("source_tier") == "A" and x not in top_candidates]
        for replacement in alt_a:
            replace_idx = next(
                (i for i, v in enumerate(top_candidates[::-1]) if v.get("source_tier") != "A"),
                None,
            )
            if replace_idx is None:
                break
            real_idx = len(top_candidates) - 1 - replace_idx
            top_candidates[real_idx] = replacement
            current_a += 1
            if current_a >= min_a:
                break

    source_distribution = Counter(x.get("source_id", "") for x in top_candidates if x.get("source_id"))

    payload = {
        "date": day,
        "meta": {
            "total_candidates": len(rows),
            "dropped_non_relevant": dropped_non_relevant,
            "dropped_non_pilot_relevant": dropped_non_pilot_relevant,
            "dropped_hard_reject": dropped_hard_reject,
            "dropped_blocked_domain": dropped_blocked_domain,
            "dropped_no_original_link": dropped_no_original_link,
            "dropped_no_published_at": dropped_no_published_at,
            "dropped_too_old": dropped_too_old,
            "max_article_age_hours": max_age,
            "selected_for_compose": len(top_candidates),
            "source_cap_applied": source_cap_applied,
            "source_distribution": dict(source_distribution.most_common(10)),
        },
        "articles": top_candidates,
    }
    out = settings.processed_dir / f"ranked_{day}.json"
    dump_json(out, payload)
    logger.info("Rank done. candidates=%s selected=%s", len(rows), len(top_candidates))
    return out


if __name__ == "__main__":
    run()

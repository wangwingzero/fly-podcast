from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from dateutil import parser as dt_parser

from flying_podcast.core.config import settings
from flying_podcast.core.io_utils import dump_json, load_json, load_yaml
from flying_podcast.core.logging_utils import get_logger
from flying_podcast.core.scoring import recency_score, relevance_score, tier_score

logger = get_logger("rank")


def _load_raw(day: str) -> list[dict]:
    path = settings.raw_dir / f"{day}.json"
    if not path.exists():
        return []
    return load_json(path)


def _classify_section(text: str, sections: dict[str, list[str]]) -> tuple[str, int]:
    text_l = text.lower()
    best = ("航司经营与网络", 0)
    for section, words in sections.items():
        hits = sum(1 for word in words if word.lower() in text_l)
        if hits > best[1]:
            best = (section, hits)
    return best


def _domain(url: str) -> str:
    try:
        return (urlparse(url).netloc or "").lower()
    except Exception:  # noqa: BLE001
        return ""


def _looks_like_google_redirect(url: str) -> bool:
    dm = _domain(url)
    path = (urlparse(url).path or "").lower() if dm else ""
    return dm.endswith("news.google.com") and path.startswith("/rss/articles/")


def _is_relevant(item: dict, section_hits: int, all_keywords: list[str]) -> bool:
    text = f"{item.get('title', '')} {item.get('raw_text', '')}".lower()
    if section_hits > 0:
        return True
    hit = sum(1 for k in all_keywords if k.lower() in text)
    return hit >= 2


def _has_valid_published_at(value: str) -> bool:
    if not value:
        return False
    try:
        dt_parser.parse(str(value))
        return True
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


def _enforce_section_diversity(candidates: list[dict], ranked_pool: list[dict], total: int) -> list[dict]:
    required_sections = ["运行与安全", "航司经营与网络", "机队与制造商", "监管与行业政策"]
    out = list(candidates[:total])
    for section in required_sections:
        if any(x.get("section") == section for x in out):
            continue
        repl = next((x for x in ranked_pool if x.get("section") == section and x not in out), None)
        if repl is None:
            continue
        counts: dict[str, int] = {}
        for row in out:
            s = row.get("section", "")
            counts[s] = counts.get(s, 0) + 1
        replace_idx = None
        for i in range(len(out) - 1, -1, -1):
            if counts.get(out[i].get("section", ""), 0) > 1:
                replace_idx = i
                break
        if replace_idx is None:
            replace_idx = len(out) - 1
        out[replace_idx] = repl
    return out[:total]


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

        section_counts: dict[str, int] = {}
        for row in out:
            section_counts[row.get("section", "")] = section_counts.get(row.get("section", ""), 0) + 1

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
                and x.get("section") == victim.get("section")
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
        if (
            replacement is not None
            and section_counts.get(victim.get("section", ""), 0) <= 1
            and replacement.get("section") != victim.get("section")
        ):
            replacement = None
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
    all_keywords = [x for words in section_map.values() for x in words]
    blocked_domains = [x.lower() for x in kw.get("blocked_domains", [])]

    ranked: list[dict] = []
    dropped_non_relevant = 0
    dropped_blocked_domain = 0
    dropped_no_original_link = 0
    dropped_no_published_at = 0
    for item in rows:
        canonical_url = (item.get("canonical_url") or item.get("url") or "").strip()
        if not canonical_url.startswith(("http://", "https://")):
            dropped_no_original_link += 1
            continue
        dm = _domain(canonical_url)
        if dm in blocked_domains:
            dropped_blocked_domain += 1
            continue
        if _looks_like_google_redirect(canonical_url):
            # 无原始可验证直链，按业务规则直接丢弃。
            dropped_no_original_link += 1
            continue
        if not _has_valid_published_at(item.get("published_at", "")):
            dropped_no_published_at += 1
            continue

        text = f"{item['title']} {item['raw_text']}"
        section, hits = _classify_section(text, section_map)
        if not _is_relevant(item, hits, all_keywords):
            dropped_non_relevant += 1
            continue
        rel = relevance_score(text, hits)
        auth = tier_score(item.get("source_tier", "C"))
        if str(item.get("source_id", "")).startswith("google_"):
            auth = min(auth, 80.0)
        time_score = recency_score(item.get("published_at", ""))
        google_penalty = 15.0 if _looks_like_google_redirect(canonical_url) else 0.0
        rank_score = round(rel * 0.45 + auth * 0.35 + time_score * 0.20 - google_penalty, 2)

        enriched = dict(item)
        enriched.update(
            {
                "section": section,
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

    candidate_total = max(settings.target_article_count * 2, 12)
    top_candidates = _pick_by_quota(deduped, total=candidate_total, domestic_ratio=settings.domestic_ratio)
    top_candidates = _enforce_section_diversity(top_candidates, deduped, candidate_total)
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

    section_stats = defaultdict(int)
    for row in top_candidates:
        section_stats[row["section"]] += 1
    source_distribution = Counter(x.get("source_id", "") for x in top_candidates if x.get("source_id"))

    payload = {
        "date": day,
        "meta": {
            "total_candidates": len(rows),
            "dropped_non_relevant": dropped_non_relevant,
            "dropped_blocked_domain": dropped_blocked_domain,
            "dropped_no_original_link": dropped_no_original_link,
            "dropped_no_published_at": dropped_no_published_at,
            "selected_for_compose": len(top_candidates),
            "source_cap_applied": source_cap_applied,
            "source_distribution": dict(source_distribution.most_common(10)),
            "section_stats": dict(section_stats),
        },
        "articles": top_candidates,
    }
    out = settings.processed_dir / f"ranked_{day}.json"
    dump_json(out, payload)
    logger.info("Rank done. candidates=%s selected=%s", len(rows), len(top_candidates))
    return out


if __name__ == "__main__":
    run()

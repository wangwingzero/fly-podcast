from __future__ import annotations

from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from flying_podcast.core.config import settings
from flying_podcast.core.io_utils import dump_json, load_json, load_yaml
from flying_podcast.core.logging_utils import get_logger
from flying_podcast.core.models import QualityReport
from flying_podcast.core.scoring import has_source_conflict

logger = get_logger("verify")


def run(target_date: str | None = None) -> Path:
    day = target_date or datetime.now().strftime("%Y-%m-%d")
    composed_path = settings.processed_dir / f"composed_{day}.json"
    digest = load_json(composed_path)

    kw = load_yaml(settings.keywords_config)
    sensitive_keywords = [w.lower() for w in kw.get("sensitive_keywords", [])]
    sensational_words = [w.lower() for w in kw.get("sensational_words", [])]

    reasons: list[str] = []
    blocked: list[str] = []

    entries = digest.get("entries", [])
    if len(entries) == 0:
        reasons.append("no_entries")

    domestic_expected = round(settings.target_article_count * settings.domestic_ratio)
    if len(entries) >= settings.target_article_count:
        if digest.get("domestic_count", 0) != domestic_expected:
            reasons.append("domestic_quota_mismatch")
        if digest.get("international_count", 0) != (settings.target_article_count - domestic_expected):
            reasons.append("international_quota_mismatch")

    tier_a_ratio = sum(1 for x in entries if x.get("source_tier") == "A") / max(len(entries), 1)
    if tier_a_ratio < settings.min_tier_a_ratio:
        reasons.append("tier_a_ratio_too_low")

    factual_scores = []
    relevance_scores = []
    citation_scores = []
    timeliness_scores = []
    readability_scores = []
    seen_fp: set[str] = set()
    seen_titles: set[str] = set()
    source_counts: dict[str, int] = {}

    for entry in entries:
        eid = entry.get("id", "")
        title = (entry.get("title") or "").lower()
        conclusion = (entry.get("conclusion") or "").lower()
        facts = [x.lower() for x in entry.get("facts", [])]
        body = (entry.get("body") or "").lower()

        citations = entry.get("citations") or []
        if not citations:
            reasons.append("missing_citation")
            blocked.append(eid)
        else:
            citation = str(citations[0]).strip()
            parsed = urlparse(citation)
            if parsed.scheme not in {"http", "https"}:
                reasons.append("invalid_citation_url")
                blocked.append(eid)
            google_redirect = parsed.netloc.endswith("news.google.com") and parsed.path.startswith("/rss/articles/")
            if google_redirect and not settings.allow_google_redirect_citation:
                reasons.append("google_redirect_citation_blocked")
                blocked.append(eid)

        if any(word in title or word in conclusion for word in sensational_words):
            reasons.append("sensational_title")
            blocked.append(eid)

        sensitive_hit = any(word in title or word in body or any(word in fact for fact in facts) for word in sensitive_keywords)
        if sensitive_hit and entry.get("source_tier") != "A":
            reasons.append("sensitive_without_tier_a")
            blocked.append(eid)

        if has_source_conflict(entry):
            reasons.append("source_conflict")
            blocked.append(eid)
        source_key = str(entry.get("source_id") or entry.get("source_name") or "")
        source_counts[source_key] = source_counts.get(source_key, 0) + 1
        if source_key and source_counts[source_key] > settings.max_entries_per_source:
            reasons.append("source_concentration_exceeded")
            blocked.append(eid)
        fp = entry.get("event_fingerprint", "")
        if fp:
            if fp in seen_fp:
                reasons.append("duplicate_event")
                blocked.append(eid)
            seen_fp.add(fp)
        if title:
            if title in seen_titles:
                reasons.append("duplicate_title")
                blocked.append(eid)
            seen_titles.add(title)

        score = entry.get("score_breakdown", {})
        factual_scores.append(float(score.get("factual", 0)))
        relevance_scores.append(float(score.get("relevance", 0)))
        citation_scores.append(100.0 if citations else 0.0)
        timeliness_scores.append(float(score.get("timeliness", 0)))
        readability_scores.append(float(score.get("readability", 0)))

    factual = round(sum(factual_scores) / max(len(factual_scores), 1), 2)
    relevance = round(sum(relevance_scores) / max(len(relevance_scores), 1), 2)
    citation = round(sum(citation_scores) / max(len(citation_scores), 1), 2)
    timeliness = round(sum(timeliness_scores) / max(len(timeliness_scores), 1), 2)
    readability = round(sum(readability_scores) / max(len(readability_scores), 1), 2)

    total = round(
        factual * 0.30 + relevance * 0.35 + citation * 0.15 + timeliness * 0.10 + readability * 0.10,
        2,
    )

    if total < settings.quality_threshold:
        reasons.append("quality_below_threshold")

    decision = "auto_publish" if total >= settings.quality_threshold and not blocked and not reasons else "hold"

    report = QualityReport(
        date=day,
        total_score=total,
        factual_score=factual,
        relevance_score=relevance,
        citation_score=citation,
        timeliness_score=timeliness,
        readability_score=readability,
        decision=decision,
        reasons=sorted(set(reasons)),
        blocked_entry_ids=sorted(set([x for x in blocked if x])),
    )

    out = settings.processed_dir / f"quality_{day}.json"
    dump_json(out, report.to_dict())
    logger.info("Verify done. score=%.2f decision=%s reasons=%s", total, decision, report.reasons)
    return out


if __name__ == "__main__":
    run()

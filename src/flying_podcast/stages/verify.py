from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlparse

from flying_podcast.core.config import settings
from flying_podcast.core.io_utils import dump_json, load_json, load_yaml
from flying_podcast.core.llm_client import OpenAICompatibleClient
from flying_podcast.core.logging_utils import get_logger
from flying_podcast.core.models import QualityReport
from flying_podcast.core.scoring import has_source_conflict
from flying_podcast.core.time_utils import beijing_today_str

logger = get_logger("verify")


def _llm_information_review(
    entries: list[dict],
    client: OpenAICompatibleClient,
) -> list[str]:
    """Use LLM to review each entry for information increment.

    Returns list of entry IDs that should be blocked (no real information).
    """
    if not entries:
        return []

    # Build a batch prompt for efficiency — one LLM call for all entries
    items = []
    for i, e in enumerate(entries):
        items.append({
            "id": e.get("id", ""),
            "title": e.get("title", ""),
            "body": (e.get("body") or "")[:300],
        })

    system_prompt = (
        "你是航空新闻质量审核员。你的任务是找出真正没有信息增量的软文并删除。\n\n"
        "【判断标准】请宽松判断——只要文章包含至少一个具体事实（具体事件、具体数据、\n"
        "具体时间、具体航司/机型/航线），就应该保留。\n\n"
        "【只删除以下类型】\n"
        "- 纯概述性/科普性文章：完全没有具体事件，只讨论一般性概念\n"
        "  例如：『航空法律如何塑造全球航空旅行的未来』\n"
        "- 纯企业软文/品牌宣传：没有任何具体数据或事件\n"
        "- 完全空洞的表态/会议通稿：没有任何实质内容\n\n"
        "【必须保留】\n"
        "- 有具体数据（订单数量、金额、日期等）的新闻\n"
        "- 有具体事件（事故、停飞、新航线开通等）的新闻\n"
        "- 有具体政策/法规变更的新闻\n"
        "- 宁可多保留，不要误删。如果拿不准，就保留。\n\n"
        "对每条新闻输出：{id, keep: true/false, reason: 一句话理由}\n"
        "输出JSON：{\"reviews\": [{\"id\": \"...\", \"keep\": true, \"reason\": \"...\"}]}"
    )
    user_prompt = json.dumps({"entries": items}, ensure_ascii=False)

    try:
        response = client.complete_json(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            max_tokens=2000,
            temperature=0.0,
            retries=2,
            timeout=60,
        )
        reviews = response.payload.get("reviews", [])
        blocked_ids = []
        for review in reviews:
            if not isinstance(review, dict):
                continue
            eid = str(review.get("id", "")).strip()
            keep = review.get("keep", True)
            reason = str(review.get("reason", "")).strip()
            if not keep and eid:
                blocked_ids.append(eid)
                logger.info("LLM 信息增量审核 — 删除: %s | 理由: %s", eid[:12], reason)
            else:
                logger.debug("LLM 信息增量审核 — 保留: %s | %s", eid[:12], reason)

        # Safety cap: never delete more than 30% of entries
        max_delete = max(1, len(entries) * 3 // 10)
        # Also ensure we keep at least half of target_article_count
        min_keep = max(settings.target_article_count // 2, 1)
        max_delete_by_keep = max(0, len(entries) - min_keep)
        effective_max = min(max_delete, max_delete_by_keep)
        if len(blocked_ids) > effective_max:
            logger.warning(
                "LLM 信息增量审核: 拟删除 %d 条超过上限 %d，只删除前 %d 条",
                len(blocked_ids), effective_max, effective_max,
            )
            blocked_ids = blocked_ids[:effective_max]
        return blocked_ids
    except Exception as exc:  # noqa: BLE001
        logger.warning("LLM 信息增量审核失败，跳过: %s", exc)
        return []


def run(target_date: str | None = None) -> Path:
    day = target_date or beijing_today_str()
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

    if len(entries) < settings.target_article_count:
        reasons.append("insufficient_articles")

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

    # ---- LLM information-increment review ----
    llm_blocked: list[str] = []
    if OpenAICompatibleClient.is_configured() and entries:
        llm_client = OpenAICompatibleClient(
            api_key=settings.llm_api_key,
            base_url=settings.llm_base_url,
            model=settings.llm_model,
        )
        llm_blocked = _llm_information_review(entries, llm_client)
        if llm_blocked:
            reasons.append("llm_low_information_increment")
            blocked.extend(llm_blocked)
            # Remove blocked entries from composed output and re-save
            original_count = len(entries)
            entries = [e for e in entries if e.get("id", "") not in set(llm_blocked)]
            digest["entries"] = entries
            digest["article_count"] = len(entries)
            dump_json(composed_path, digest)
            logger.info(
                "LLM 信息增量审核: 删除 %d 条 (%d → %d)",
                original_count - len(entries), original_count, len(entries),
            )

    # Always auto_publish — quality issues are logged in reasons for reference
    # but no longer gate the publish decision.
    decision = "auto_publish"

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

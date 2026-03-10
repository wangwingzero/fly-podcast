from __future__ import annotations

import json
import re
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

_HIGH_VALUE_OPS_TERMS = (
    "rat",
    "dark cockpit",
    "divert",
    "diversion",
    "emergency",
    "go-around",
    "turnback",
    "rejected takeoff",
    "runway incursion",
    "windshear",
    "icing",
    "turbulence",
    "smoke",
    "fire",
    "hydraulic",
    "engine",
    "gear",
    "flaps",
    "radome",
    "nose cone",
    "security threat",
    "notam",
    "airspace closure",
    "gps spoofing",
    "备降",
    "返航",
    "复飞",
    "紧急下降",
    "中断起飞",
    "跑道侵入",
    "冲出跑道",
    "风切变",
    "结冰",
    "颠簸",
    "烟雾",
    "火警",
    "液压",
    "发动机",
    "起落架",
    "襟翼",
    "雷达罩",
    "气象雷达",
    "安保威胁",
    "空域关闭",
    "航路受限",
    "禁飞",
)

_HARD_REJECT_REASON_HINTS = (
    "duplicate",
    "重复报道",
    "与其他文章重复",
    "与本期其他文章重复",
    "核心事件重复",
    "机翻",
    "读不通",
    "前后矛盾",
    "标题党",
    "低俗",
    "软文",
    "广告",
    "品牌宣传",
    "事实冲突",
)


def _is_high_value_ops_entry(entry: dict) -> bool:
    text_parts = [
        str(entry.get("title", "")),
        str(entry.get("conclusion", "")),
        str(entry.get("body", "")),
    ]
    text_parts.extend(str(x) for x in entry.get("facts", []) if x)
    text = "\n".join(text_parts).lower()
    for term in _HIGH_VALUE_OPS_TERMS:
        if term.isascii():
            pattern = rf"(?<![a-z0-9]){re.escape(term)}(?![a-z0-9])"
            if re.search(pattern, text):
                return True
            continue
        if term in text:
            return True
    return False


def _should_override_editor_rejection(entry: dict, reason: str) -> bool:
    if not _is_high_value_ops_entry(entry):
        return False
    lowered_reason = str(reason or "").lower()
    return not any(term in lowered_reason for term in _HARD_REJECT_REASON_HINTS)


def _llm_editor_review(
    entries: list[dict],
    client: OpenAICompatibleClient,
) -> list[str]:
    """LLM acts as editor-in-chief for final quality gate.

    Reviews every entry for overall quality — not just soft-article filtering,
    but also translation quality, factual coherence, readability, and
    information value.  No deletion cap: quality over quantity.

    Returns list of entry IDs that should be removed.
    """
    if not entries:
        return []

    items = []
    for e in entries:
        items.append({
            "id": e.get("id", ""),
            "title": e.get("title", ""),
            "conclusion": e.get("conclusion", ""),
            "facts": [str(x) for x in e.get("facts", [])[:5]],
            "body": (e.get("body") or "")[:800],
        })
    entries_by_id = {str(e.get("id", "")).strip(): e for e in entries}

    system_prompt = (
        "你是航空媒体总编辑，对即将发布的每日简报做最终审核。\n"
        "这份简报的目标读者是飞行员。你的首要目标不是凑篇数，也不是追求新闻腔，而是保留“对飞行员飞行运行有信息增量”的稿子。\n"
        "信息增量重点看两类：技术增量、风险增量。\n"
        "技术增量：机型/系统故障、异常告警、RAT、发动机、液压、航电、雷达、起落架、程序异常、维修发现等。\n"
        "风险增量：返航、备降、复飞、跑道事件、天气风险、风切变、结冰、安保威胁、NOTAM、空域关闭、运行限制、影响机组决策的事件等。\n"
        "只要文章能提供上述任一类具体信息，就应优先保留；文字不必像官样通稿，但必须事实成立、逻辑清楚、中文可读。\n\n"
        "这些文章已经过AI选稿和翻译。文章采用固定结构：前半段是客观新闻正文，最后一行可能是以“划重点：”开头的老机长幽默点评。\n"
        "这个“划重点”是栏目固定风格，不是问题本身。只要它不低俗、不脏话、不与正文事实冲突，就应保留；"
        "严禁仅因为口语化、调侃、吐槽或像飞行员群聊转发语气，就删除整篇文章。\n\n"
        "请按以下四条标准逐篇审查：\n\n"
        "1. 不重复：与本期其他文章是否报道同一核心事件？如果重复，删除质量较差的那篇。\n"
        "2. 优先保留有运行增量的稿子：如果标题、结论、facts 或正文已经明确给出技术故障、运行风险、处置动作、受影响机型/机场/航班/系统等关键信息，应优先保留。\n"
        "   尤其不要把以下稿子误删为“太短”或“太概述”：机型系统异常、RAT放出、备降/返航、跑道事件、气象风险、安保事件、运行限制。\n"
        "3. 内容正常：站在读者角度，文章读起来是否正常？中文通顺、逻辑清楚、标题与正文一致。\n"
        "   删除：读不通、机翻严重、标题党、前后矛盾的文章。\n"
        "   但如果正文前半段是正常新闻叙述，只有最后一句“划重点：”较口语化，不要因此删除。\n"
        "4. 避免软文：是否是企业宣传、品牌广告、没有新闻事实的空洞文章？\n"
        "   删除：纯软文、纯概述/科普、纯表态口号。\n\n"
        "【重点边界】\n"
        "- 判断时优先看 title + conclusion + facts，再看 body；不要只因为正文篇幅短，就忽视标题和 facts 里的关键运行事实。\n"
        "- 允许结构：2-4句客观事实 + 1句“划重点：”幽默点评。\n"
        "- 不允许：整篇几乎都是空话、正文没有事实、点评与事实冲突、点评低俗攻击、或正文本身就像机翻/口水话。\n"
        "- 地缘政治/宏观行业新闻，只有在明确写出航班、机场、空域、运行限制、航司处置等具体运行影响时，才算高价值；只有大而空判断的，可以删除。\n"
        "- 航司航线暂停/恢复、旅客改签豁免、换季排班、运力恢复、市场投放这类运营通告型稿件，即使和空域事件有关，只要没有新增NOTAM、程序限制、绕飞策略、机场/跑道关闭等飞行员可直接使用的细节，也应删除。\n"
        "- 判断时请优先看“划重点：”之前的正文是否合格，再决定是否删除。\n\n"
        "符合以上三条的保留，不符合任何一条的删除。\n\n"
        "对每条新闻输出：{id, keep: true/false, reason: 一句话理由}\n"
        "输出JSON：{\"reviews\": [{\"id\": \"...\", \"keep\": true, \"reason\": \"...\"}]}"
    )
    user_prompt = json.dumps({"entries": items}, ensure_ascii=False)

    try:
        response = client.complete_json(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            max_tokens=3000,
            temperature=0.0,
            retries=2,
            timeout=90,
        )
        reviews = response.payload.get("reviews", [])
        blocked_ids = []
        for review in reviews:
            if not isinstance(review, dict):
                continue
            eid = str(review.get("id", "")).strip()
            keep = review.get("keep", True)
            reason = str(review.get("reason", "")).strip()
            entry = entries_by_id.get(eid, {})
            if not keep and eid and _should_override_editor_rejection(entry, reason):
                logger.info("总编辑终审 — 改判保留: %s | 高价值运行稿件，原理由: %s", eid[:12], reason)
                continue
            if not keep and eid:
                blocked_ids.append(eid)
                logger.info("总编辑终审 — 删除: %s | 理由: %s", eid[:12], reason)
            else:
                logger.info("总编辑终审 — 保留: %s | %s", eid[:12], reason)
        return blocked_ids
    except Exception as exc:  # noqa: BLE001
        logger.warning("总编辑终审失败，跳过: %s", exc)
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

    # ---- LLM editor-in-chief final review ----
    llm_blocked: list[str] = []
    if OpenAICompatibleClient.is_configured() and entries:
        llm_client = OpenAICompatibleClient(
            api_key=settings.llm_api_key,
            base_url=settings.llm_base_url,
            model=settings.llm_model,
        )
        llm_blocked = _llm_editor_review(entries, llm_client)
        if llm_blocked:
            reasons.append("llm_editor_rejected")
            blocked.extend(llm_blocked)
            # Remove blocked entries from composed output and re-save
            original_count = len(entries)
            entries = [e for e in entries if e.get("id", "") not in set(llm_blocked)]
            digest["entries"] = entries
            digest["article_count"] = len(entries)
            dump_json(composed_path, digest)
            logger.info(
                "总编辑终审: 删除 %d 条 (%d → %d)",
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

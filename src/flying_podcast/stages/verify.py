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
    # ---- airprox / loss-of-separation family ----
    "airprox",
    "loss of separation",
    "near miss",
    "near-miss",
    "tcas ra",
    "tcas resolution",
    "midair",
    "mid-air",
    "wake encounter",
    # ---- structural / engine-mount failures ----
    "pylon",
    "pylon bearing",
    "engine separation",
    "engine mount",
    "engine pylon",
    "uncontained",
    "rotor burst",
    "fan blade",
    "compressor stall",
    "engine surge",
    "engine shutdown",
    "ifsd",
    "in-flight shutdown",
    # ---- airworthiness / fleet actions ----
    "airworthiness directive",
    "emergency airworthiness directive",
    "emergency ad",
    "fleet inspection",
    "fleet-wide inspection",
    "service bulletin",
    "service difficulty report",
    "ntsb hearing",
    "ntsb investigation",
    "preliminary report",
    "factual report",
    # ---- Chinese aliases ----
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
    "空中接近",
    "间隔丧失",
    "空中相撞",
    "尾流",
    "吊架",
    "发动机分离",
    "发动机吊架",
    "非包容",
    "包容失效",
    "风扇叶片",
    "适航指令",
    "紧急适航指令",
    "机队检查",
    "服务通告",
    "听证会",
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

_DEFAULT_VERIFY_MACRO_EFFECT_TERMS = (
    "airline", "airlines", "airport", "airports", "aircraft", "airspace",
    "flight", "flights", "fleet", "faa", "easa", "iata", "icao",
    "boeing", "airbus", "engine", "supply chain", "route", "capacity",
    "航空公司", "航班", "机场", "空域", "机队", "监管", "发动机", "供应链",
)

_DEFAULT_VERIFY_MAJOR_ACCIDENT_TERMS = (
    "grounding", "grounded", "fleet-wide", "fleet wide", "inspection",
    "airworthiness directive", "regulator", "faa", "easa", "manufacturer",
    "boeing", "airbus", "engine", "airport closure", "runway closure",
    "airspace closure", "international airlines", "suspend operations",
    "停飞", "适航指令", "监管", "检查", "空域关闭", "机场关闭",
)

_PRIMARY_HEALTH_ROLES = {"primary_industry", "macro_supplement"}
_ACCIDENT_EXCEPTION_SOURCE_IDS = {
    "avherald_web",
    "asn_2026_web",
    "flightglobal_safety",
    "ntsb_press_web",
    "nasa_asrs_callback_web",
}


def _has_chinese(text: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", str(text or "")))


def _contains_any(text: str, terms: list[str] | tuple[str, ...]) -> bool:
    text_l = str(text or "").lower()
    return any(str(term).strip().lower() in text_l for term in terms if str(term).strip())


def _source_role_for_entry(entry: dict) -> str:
    role = str(entry.get("source_role") or "").strip().lower()
    if role:
        return role
    source_id = str(entry.get("source_id") or "").strip().lower()
    if source_id in _ACCIDENT_EXCEPTION_SOURCE_IDS:
        return "accident_exception"
    return ""


def _load_source_health(day: str) -> list[dict]:
    raw_dir = getattr(settings, "raw_dir", None)
    if not raw_dir:
        return []
    path = Path(raw_dir) / f"source_health_{day}.json"
    if not path.exists():
        return []
    try:
        rows = load_json(path)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Source health gate: failed to read %s: %s", path, exc)
        return []
    return rows if isinstance(rows, list) else []


def _source_health_gate_reasons(day: str, publishable_entries: list[dict]) -> list[str]:
    if not getattr(settings, "source_health_gate_enabled", True):
        return []
    health = _load_source_health(day)
    if not health:
        return []

    primary_ok = [
        row for row in health
        if str(row.get("source_role") or "").strip().lower() in _PRIMARY_HEALTH_ROLES
        and str(row.get("status") or "").strip().lower() == "ok"
        and int(row.get("item_count") or 0) > 0
    ]
    primary_items = sum(int(row.get("item_count") or 0) for row in primary_ok)
    min_sources = max(0, int(getattr(settings, "min_primary_industry_sources_ok", 2) or 0))
    min_items = max(0, int(getattr(settings, "min_primary_industry_items", 3) or 0))
    primary_unhealthy = len(primary_ok) < min_sources or primary_items < min_items
    if not primary_unhealthy:
        return []

    accident_entries = [
        entry for entry in publishable_entries
        if _source_role_for_entry(entry) == "accident_exception"
    ]
    non_accident_entries = [
        entry for entry in publishable_entries
        if _source_role_for_entry(entry) != "accident_exception"
    ]
    if accident_entries and not non_accident_entries:
        return ["primary_source_health_below_threshold", "accident_only_fallback_digest"]
    return []


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
    # 高价值运行/技术稿件即使来自 accident_exception 源也允许改判保留：
    # 这些条目早已通过 verify 主流程的 _DEFAULT_VERIFY_MAJOR_ACCIDENT_TERMS 关，
    # 编辑 LLM 用"未体现监管动作 / 跨国影响"砍掉它们属于过度教条，需要兜底。
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
            "source_role": e.get("source_role", ""),
            "facts": [str(x) for x in e.get("facts", [])[:5]],
            "body": (e.get("body") or "")[:800],
        })
    entries_by_id = {str(e.get("id", "")).strip(): e for e in entries}

    system_prompt = (
        "你是航空媒体总编辑，对即将发布的每日国际航空简报做最终审核。\n"
        "本简报的目标读者：一线民航飞行员 + 资深飞行爱好者。\n"
        "他们关心的不是只能「今天用上」的运行细节，也包括新机型、新发动机、新航电、试飞首飞、\n"
        "结构事件、适航指令、ATC 动态、罕见任务、机队变化和制造业里程碑。\n"
        "技术增量：机型、系统、发动机、航电、维修、MRO、供应链、适航或制造交付方面的新事实。\n"
        "运行增量：监管、机场、空域、航班、运行限制、停飞、机队检查或跨国运营影响方面的新事实。\n"
        "里程碑增量：首飞、试飞、新机型衍生（如双座型、加油机版本）、本土组装首架、\n"
        "退役机长告别、首位女机长、罕见任务、纪念飞行、世界纪录、新涂装首航。\n\n"
        "【重要 — 关于事故 / 安全事件类稿件】\n"
        "结构性故障调查（如发动机分离、吊架轴承故障、风扇叶片脱落）、空中接近 / TCAS RA、\n"
        "重大空中相撞调查听证会等，是飞行员和爱好者最关心的硬核内容。\n"
        "**不要**因为它们「没有触发停飞 / 机队检查 / 监管动作」就删除——\n"
        "只要事件本身有清楚的机型、地点、故障部件或动作过程，就应保留。\n"
        "判断标准：飞行员群里看到这条会不会停下来读一眼，会不会转发给同事？会就保留。\n\n"
        "Reuters/Bloomberg 这类宏观来源，只有明确写出对航司、航班、机场、空域、机队、监管或供应链的影响时才保留。\n\n"
        "这些文章已经过AI选稿和翻译。文章采用固定结构：前半段是客观新闻正文，最后一行可能是以「划重点：」开头的老机长幽默点评。\n"
        "这个「划重点」是栏目固定风格，不是问题本身。只要它不低俗、不脏话、不与正文事实冲突，就应保留；"
        "但如果它拿伤亡、严重受伤、紧急撤离或当事机组/旅客开玩笑，或补充了正文没有的原因、责任、调查结论，应删除或标记为不合格。"
        "严禁仅因为口语化、调侃、吐槽或像飞行员群聊转发语气，就删除整篇文章。\n\n"
        "请按以下四条标准逐篇审查：\n\n"
        "1. 不重复：与本期其他文章是否报道同一核心事件？如果重复，删除质量较差的那篇。\n"
        "2. 信息增量：标题、结论、facts 或正文是否给出至少一个具体的航空事实——\n"
        "   机型、人物、地点、时间、系统、订单、试飞、检查、停飞、生产里程碑、空中接近、AD 等任一具体事实即可。\n"
        "   有就保留。空话套话、纯概述 / 科普 / 宣传 / 财报 / 营销稿应删除。\n"
        "3. 内容正常：站在读者角度，文章读起来是否正常？中文通顺、逻辑清楚、标题与正文一致。\n"
        "   删除：读不通、机翻严重、标题党、前后矛盾的文章。\n"
        "   但如果正文前半段是正常新闻叙述，只有最后一句「划重点：」较口语化，不要因此删除。\n"
        "4. 避免软文：是否是企业宣传、品牌广告、没有新闻事实的空洞文章？\n"
        "   删除：纯软文、纯概述/科普、纯表态口号。\n\n"
        "【重点边界】\n"
        "- 判断时优先看 title + conclusion + facts，再看 body；不要只因为正文篇幅短，就忽视标题和 facts 里的关键事实。\n"
        "- 允许结构：2-4句客观事实 + 1句「划重点：」幽默点评。\n"
        "- 不允许：整篇几乎都是空话、正文没有事实、点评与事实冲突、点评低俗攻击、拿伤亡开玩笑、点评补充原文没有的事实、或正文本身就像机翻/口水话。\n"
        "- 地缘政治/宏观行业新闻，只有在明确写出航班、机场、空域、运行限制、航司处置、机队、监管或供应链影响时，才算高价值；只有大而空判断的，可以删除。\n"
        "- 航司航线暂停/恢复、旅客改签豁免、换季排班、运力恢复、市场投放这类运营通告型稿件，即使和空域事件有关，只要没有新增NOTAM、程序限制、绕飞策略、机场/跑道关闭等飞行员可直接使用的细节，也应删除。\n"
        "- 企业官网式软文、航空公司营销稿、泛旅游稿，即使中文通顺也应删除。\n"
        "- 判断时请优先看「划重点：」之前的正文是否合格，再决定是否删除。\n\n"
        "【明确不构成删除理由】\n"
        "- 「未体现监管动作 / 未触发停飞 / 未提及机队检查」——只要有事件本身的具体事实，就应保留。\n"
        "- 「未体现跨国运营影响」——一国监管 / 一架试飞 / 一次空中接近也是合格稿件。\n"
        "- 「缺少更大范围影响」——结构性故障调查、试飞里程碑、首单不需要更大影响才能上稿。\n\n"
        "符合 1-4 条且没有上面的「明确不构成删除理由」的，全部保留；不符合的删除。\n\n"
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
    compose_mode = str(digest.get("meta", {}).get("compose_mode", "")).strip()
    if len(entries) == 0:
        reasons.append("no_entries")

    if settings.require_llm_for_publish and compose_mode != "llm_two_phase":
        reasons.append("llm_required_for_publish")
        blocked.extend(str(e.get("id", "")) for e in entries if e.get("id"))

    article_limit = max(0, int(getattr(settings, "target_article_count", 0) or 0))
    if article_limit > 0 and len(entries) < article_limit:
        reasons.append("insufficient_articles")

    tier_a_ratio = sum(1 for x in entries if x.get("source_tier") == "A") / max(len(entries), 1)
    min_tier_a_ratio = float(getattr(settings, "min_tier_a_ratio", 0.0) or 0.0)
    if min_tier_a_ratio > 0.0 and tier_a_ratio < min_tier_a_ratio:
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
        visible_text = " ".join(
            str(x)
            for x in [
                entry.get("title", ""),
                entry.get("conclusion", ""),
                entry.get("body", ""),
                " ".join(str(f) for f in entry.get("facts", []) if f),
            ]
            if x
        )
        role = _source_role_for_entry(entry)
        macro_terms = kw.get("macro_aviation_effect_keywords") or list(_DEFAULT_VERIFY_MACRO_EFFECT_TERMS)
        accident_terms = kw.get("major_accident_impact_keywords") or list(_DEFAULT_VERIFY_MAJOR_ACCIDENT_TERMS)

        if role == "macro_supplement" and not _contains_any(visible_text, macro_terms):
            reasons.append("macro_without_explicit_aviation_effect")
            blocked.append(eid)

        if role == "accident_exception" and not _contains_any(visible_text, accident_terms):
            reasons.append("accident_without_major_impact")
            blocked.append(eid)

        if not _has_chinese(visible_text):
            reasons.append("non_chinese_content")
            blocked.append(eid)

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
        max_entries_per_source = max(0, int(getattr(settings, "max_entries_per_source", 0) or 0))
        if max_entries_per_source > 0 and source_key and source_counts[source_key] > max_entries_per_source:
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

    blocked_set = {x for x in blocked if x}
    publishable_entries = [e for e in entries if str(e.get("id", "")) not in blocked_set]
    gate_reasons = _source_health_gate_reasons(day, publishable_entries)
    if gate_reasons:
        reasons.extend(gate_reasons)

    if not entries:
        reasons.append("empty_digest")
        decision = "skip_publish"
    elif not publishable_entries:
        reasons.append("all_entries_blocked")
        decision = "skip_publish"
    elif gate_reasons:
        decision = "skip_publish"
    else:
        # Quality issues are logged for reference, but non-empty digests can still
        # proceed to publish review and delivery.
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

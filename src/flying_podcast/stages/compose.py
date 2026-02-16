from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from flying_podcast.core.config import settings
from flying_podcast.core.llm_client import LLMError, OpenAICompatibleClient
from flying_podcast.core.io_utils import dump_json, load_json
from flying_podcast.core.logging_utils import get_logger
from flying_podcast.core.models import DailyDigest, DigestEntry
from flying_podcast.core.scoring import readability_score, weighted_quality

logger = get_logger("compose")
_SECTIONS = ["运行与安全", "航司经营与网络", "机队与制造商", "监管与行业政策"]
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
    if not plain or len(plain) < 15:
        return []
    # Google News raw_text often echoes the title — skip if too similar.
    title_prefix = title.lower().strip()[:30]
    if title_prefix and plain.lower().strip().startswith(title_prefix):
        return []
    parts = re.split(r"[。.!?]\s*", plain)
    facts = [p.strip() for p in parts if len(p.strip()) > 10]
    return facts[:3]


def _ensure_min_facts(facts: list[str], raw_text: str, title: str, min_count: int = 2) -> list[str]:
    out = [x.strip() for x in facts if len(x.strip()) > 10]
    plain = _strip_html(raw_text).replace("\xa0", " ").replace("&nbsp;", " ").strip()
    if len(out) < min_count and plain:
        for seg in re.split(r"[。.!?;；,，]\s*", plain):
            seg = seg.strip()
            if len(seg) > 10 and seg not in out:
                out.append(seg)
            if len(out) >= 3:
                break
    if len(out) < min_count:
        t = title.strip()
        if t and t not in out:
            out.append(t)
    if len(out) < min_count and out:
        out.append(out[0])
    return out[:3]


def _build_conclusion(title: str) -> str:
    clean, _ = _clean_title(title)
    return clean[:80]


def _build_impact(section: str) -> str:
    mapping = {
        "运行与安全": "建议运行、签派和机组关注执行风险变化，并同步值班提示。",
        "航司经营与网络": "对航线投放、收益管理和地面保障排班存在直接影响。",
        "机队与制造商": "影响机队计划、维修排程与机型训练资源配置。",
        "监管与行业政策": "可能触发合规流程调整，需关注程序与手册更新节奏。",
    }
    return mapping.get(section, "建议相关岗位评估对运行、成本和合规的实际影响。")


def _pick_final_entries(candidates: list[dict], total: int, domestic_ratio: float) -> list[dict]:
    domestic_quota = round(total * domestic_ratio)
    intl_quota = total - domestic_quota

    domestic = [x for x in candidates if x["region"] == "domestic"]
    intl = [x for x in candidates if x["region"] != "domestic"]

    picked = domestic[:domestic_quota] + intl[:intl_quota]

    # Section diversity fill.
    existing_sections = {x["section"] for x in picked}
    for item in candidates:
        if len(picked) >= total:
            break
        if item in picked:
            continue
        if item["section"] not in existing_sections:
            picked.append(item)
            existing_sections.add(item["section"])

    if len(picked) < total:
        for item in candidates:
            if len(picked) >= total:
                break
            if item not in picked:
                picked.append(item)

    return picked[:total]


def _to_digest_entry(item: dict[str, Any], title: str, conclusion: str, facts: list[str], impact: str) -> DigestEntry:
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
    impact = impact.strip() or _build_impact(item.get("section", ""))
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
        section=item.get("section", "航司经营与网络"),
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
    )


def _build_llm_prompts(selected: list[dict], total: int, domestic_quota: int, intl_quota: int) -> tuple[str, str]:
    payload = []
    for row in selected:
        payload.append(
            {
                "ref_id": row["id"],
                "section": row.get("section", "航司经营与网络"),
                "title": row.get("title", ""),
                "raw_text": _strip_html(row.get("raw_text", ""))[:500],
                "region": row.get("region", "international"),
                "source_tier": row.get("source_tier", "C"),
            }
        )
    system_prompt = (
        "你是民航行业新闻编辑。"
        "必须只基于输入内容改写，不得引入外部事实，不得编造链接或ID。"
        "输出必须是 JSON object，且仅包含 entries 字段。"
    )
    user_prompt = json.dumps(
        {
            "task": "从候选新闻中生成日报条目",
            "rules": {
                "total": total,
                "domestic_quota": domestic_quota,
                "international_quota": intl_quota,
                "allowed_sections": _SECTIONS,
                "must_keep_ref_id": True,
                "must_not_generate_links": True,
                "facts_count": "2-3",
            },
            "output_schema": {
                "entries": [
                    {
                        "ref_id": "string",
                        "section": "string",
                        "title": "string",
                        "conclusion": "string",
                        "facts": ["string"],
                        "impact": "string",
                    }
                ]
            },
            "candidates": payload,
        },
        ensure_ascii=False,
    )
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
        impact = _build_impact(item.get("section", ""))
        entry = _to_digest_entry(item, clean_title, conclusion, facts, impact)
        if not entry.citations:
            continue
        entries.append(entry)
    return entries


def _enforce_constraints(
    entries: list[DigestEntry],
    pool: list[DigestEntry],
    total: int,
    domestic_ratio: float,
) -> list[DigestEntry]:
    required_sections = ["运行与安全", "航司经营与网络", "机队与制造商", "监管与行业政策"]
    max_per_source = settings.max_entries_per_source

    uniq: list[DigestEntry] = []
    used_ids: set[str] = set()
    for e in entries:
        if e.id in used_ids:
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
    target_dom = round(effective_total * domestic_ratio)
    target_intl = effective_total - target_dom

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

    # then region quota (scaled by effective total)
    def _dom_count(rows: list[DigestEntry]) -> int:
        return sum(1 for x in rows if x.region == "domestic")

    loop_guard = 0
    while loop_guard < 40:
        loop_guard += 1
        dom = _dom_count(out)
        intl = len(out) - dom
        if dom == target_dom and intl == target_intl:
            break
        need_domestic = dom < target_dom
        section_counts = _section_counts(out)
        if need_domestic:
            candidate = next((p for p in pool if p.region == "domestic" and p.id not in used_ids), None)
            if candidate is None:
                break
            replace_idx = None
            for i in range(len(out) - 1, -1, -1):
                if out[i].region != "domestic" and section_counts.get(out[i].section, 0) > 1:
                    replace_idx = i
                    break
            if replace_idx is None:
                break
        else:
            candidate = next((p for p in pool if p.region != "domestic" and p.id not in used_ids), None)
            if candidate is None:
                break
            replace_idx = None
            for i in range(len(out) - 1, -1, -1):
                if out[i].region == "domestic" and section_counts.get(out[i].section, 0) > 1:
                    replace_idx = i
                    break
            if replace_idx is None:
                break
        used_ids.discard(out[replace_idx].id)
        out[replace_idx] = candidate
        used_ids.add(candidate.id)

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
                and p.region == victim.region
            ),
            None,
        )
        if replacement is None:
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
        if section not in _SECTIONS:
            section = by_id[ref_id].get("section", "航司经营与网络")
        source = dict(by_id[ref_id])
        source["section"] = section
        entry = _to_digest_entry(
            source,
            str(item.get("title", "")).strip(),
            str(item.get("conclusion", "")).strip(),
            item.get("facts", []) if isinstance(item.get("facts"), list) else [],
            str(item.get("impact", "")).strip(),
        )
        if not entry.citations:
            continue
        out.append(entry)
        dedup_ids.add(ref_id)
        if len(out) >= total:
            break
    if not out:
        raise ValueError("llm_entries_empty")
    return out


def run(target_date: str | None = None) -> Path:
    day = target_date or datetime.now().strftime("%Y-%m-%d")
    ranked_path = settings.processed_dir / f"ranked_{day}.json"
    ranked_payload = load_json(ranked_path)
    candidates = ranked_payload.get("articles", [])

    selected = _pick_final_entries(
        candidates,
        total=settings.target_article_count,
        domestic_ratio=settings.domestic_ratio,
    )
    pool_entries = _build_entries_with_rules(candidates)

    entries: list[DigestEntry] = []
    compose_mode = "rules"
    compose_reason = "llm_not_configured"
    if OpenAICompatibleClient.is_configured():
        domestic_quota = round(settings.target_article_count * settings.domestic_ratio)
        intl_quota = settings.target_article_count - domestic_quota
        system_prompt, user_prompt = _build_llm_prompts(selected, settings.target_article_count, domestic_quota, intl_quota)
        client = OpenAICompatibleClient(
            api_key=settings.llm_api_key,
            base_url=settings.llm_base_url,
            model=settings.llm_model,
        )
        try:
            response = client.complete_json(system_prompt=system_prompt, user_prompt=user_prompt, retries=3)
            entries = _validate_llm_entries(response.payload, selected, settings.target_article_count)
            compose_mode = "llm"
            compose_reason = "ok"
        except (LLMError, ValueError, KeyError) as exc:
            compose_mode = "rules_fallback"
            compose_reason = str(exc)
            logger.warning("LLM compose failed, fallback to rules: %s", exc)
            entries = _build_entries_with_rules(selected)
    else:
        entries = _build_entries_with_rules(selected)

    entries = _enforce_constraints(entries, pool_entries, settings.target_article_count, settings.domestic_ratio)

    domestic_count = sum(1 for x in entries if x.region == "domestic")
    intl_count = len(entries) - domestic_count
    digest = DailyDigest(
        date=day,
        domestic_count=domestic_count,
        international_count=intl_count,
        entries=entries,
        total_score=round(sum(e.score_breakdown["total"] for e in entries) / max(len(entries), 1), 2),
    )

    out = settings.processed_dir / f"composed_{day}.json"
    payload = digest.to_dict()
    payload["meta"] = {
        "compose_mode": compose_mode,
        "compose_reason": compose_reason,
        "model": settings.llm_model if OpenAICompatibleClient.is_configured() else "",
    }
    dump_json(out, payload)
    logger.info("Compose done. entries=%s domestic=%s intl=%s", len(entries), domestic_count, intl_count)
    return out


if __name__ == "__main__":
    run()

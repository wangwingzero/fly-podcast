from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
from typing import Any
from urllib.parse import urlparse
from dateutil import parser as dt_parser

from flying_podcast.core.config import settings
from flying_podcast.core.io_utils import dump_json, load_json, load_yaml
from flying_podcast.core.logging_utils import get_logger
from flying_podcast.core.scoring import recency_score, relevance_score, tier_score
from flying_podcast.core.time_utils import beijing_today_str

logger = get_logger("rank")

_TITLE_TOKEN_RE = re.compile(r"[a-zA-Z][a-zA-Z0-9]+|[0-9]+|[\u4e00-\u9fff]")
_TITLE_STOP_TOKENS = {
    "a", "an", "the", "and", "or", "of", "to", "in", "on", "at", "for",
    "with", "from", "after", "before", "into", "near",
    "about", "this", "that", "will", "has", "have", "had", "its", "their",
    "new", "first", "more", "says", "said", "seeks", "learn", "why",
}
_TITLE_TOKEN_SYNONYMS = {
    "crash": "collision",
    "collided": "collision",
    "collides": "collision",
    "colliding": "collision",
}

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
    "singapore airlines",
    "qatar airways",
    "british airways",
    "cathay pacific",
    "ryanair",
    "southwest",
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
]

_DEFAULT_PILOT_DIRECT_OPERATION_KEYWORDS = [
    "incident",
    "accident",
    "emergency",
    "diversion",
    "go-around",
    "airworthiness",
    "directive",
    "service bulletin",
    "inspection",
    "fault",
    "failure",
    "defect",
    "crack",
    "engine issue",
    "engine fault",
    "smoke",
    "fire",
    "runway",
    "notam",
    "tfr",
    "atc",
    "airspace",
    "weather",
    "turbulence",
    "windshear",
    "icing",
    "volcanic ash",
    "closure",
    "closed",
    "restriction",
    "restricted",
    "grounding",
    "grounded",
    "training",
    "simulator",
    "fatigue",
    "procedure",
    "checklist",
    "mel",
    "etops",
    "cpdlc",
    "navigation",
    "gps interference",
    "spoofing",
    "jamming",
    "事故",
    "事件",
    "紧急",
    "备降",
    "复飞",
    "适航",
    "检查",
    "故障",
    "失效",
    "裂纹",
    "跑道",
    "航行通告",
    "空域",
    "天气",
    "颠簸",
    "风切变",
    "结冰",
    "火山灰",
    "关闭",
    "限制",
    "停飞",
    "训练",
    "疲劳",
    "程序",
    "检查单",
    "导航",
    "干扰",
]

_DEFAULT_PILOT_BACKGROUND_ONLY_KEYWORDS = [
    "new route",
    "new routes",
    "route launch",
    "network",
    "schedule",
    "timetable",
    "frequency",
    "frequencies",
    "additional flights",
    "extra flights",
    "more flights",
    "adds flights",
    "increase flights",
    "increases flights",
    "capacity",
    "expansion",
    "demand",
    "market",
    "fleet",
    "order",
    "orders",
    "delivery",
    "deliveries",
    "takes delivery",
    "receives",
    "received",
    "deploy",
    "deployment",
    "assigned to",
    "to serve",
    "service to",
    "nonstop service",
    "planned maintenance",
    "scheduled maintenance",
    "maintenance rotation",
    "aircraft assignment",
    "widebody assignment",
    "livery",
    "inaugural",
    "launch ceremony",
    "新航线",
    "增班",
    "加班",
    "航线安排",
    "航线调整",
    "时刻",
    "排班",
    "停航",
    "复航",
    "暂停运营",
    "恢复运营",
    "恢复时间",
    "恢复时间推迟",
    "延长停飞",
    "航线停飞",
    "机型安排",
    "执飞",
    "订单",
    "交付",
    "机队",
    "扩张",
    "计划维护",
    "定检",
    "停场",
    "首航",
]

_DEFAULT_PILOT_SCHEDULE_ADVISORY_KEYWORDS = [
    "flight suspension",
    "service suspension",
    "suspension of",
    "suspend flights",
    "suspends flights",
    "suspended flights",
    "return of flights",
    "return of service",
    "service return",
    "pause flights",
    "pauses flights",
    "paused flights",
    "resume flights",
    "resumes flights",
    "resumed flights",
    "resume service",
    "resumes service",
    "service resumption",
    "operations update",
    "travel waiver",
    "rebooking",
    "恢复运营",
    "恢复航班",
    "恢复时间",
    "恢复时间推迟",
    "暂停运营",
    "暂停航班",
    "停飞安排",
    "延长停飞",
    "航线停飞",
]

_DEFAULT_PILOT_SPECIFIC_OPS_KEYWORDS = [
    "notam",
    "tfr",
    "procedure",
    "procedures",
    "reroute",
    "rerouting",
    "alternate",
    "slot restriction",
    "airport closure",
    "runway closure",
    "gps interference",
    "spoofing",
    "jamming",
    "atc restriction",
    "航行通告",
    "程序限制",
    "航路变更",
    "绕飞",
    "备降机场",
    "跑道关闭",
    "机场关闭",
]

_DEFAULT_PILOT_PRIORITY_SOURCES = [
    "avherald_web",
    "asn_2026_web",
    "easa_ad_web",
    "faa_safo_web",
    "faa_info_web",
    "nasa_asrs_callback_web",
    "ntsb_press_web",
    "flightglobal_safety",
    "flightglobal_engines",
]

_PILOT_CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "safety_event": [
        "accident", "incident", "serious incident", "emergency", "rejected takeoff",
        "aborted takeoff", "go-around", "diversion", "turnback", "evacuation",
        "runway excursion", "runway incursion", "runway overrun", "loss of separation",
        "tcas", " ra ", "smoke", "fire", "engine failure", "hydraulic", "electrical",
        "pressurization", "unreliable air data", "incapacitated", "turbulence",
        "windshear", "icing", "fuel reserve", "tail strike", "malfunction",
        "事故", "事件", "严重征候", "紧急",
        "中断起飞", "复飞", "备降", "返航", "紧急撤离", "跑道侵入", "冲出跑道",
        "间隔丧失", "烟雾", "火警", "发动机", "液压", "增压", "颠簸", "风切变",
    ],
    "airworthiness_technical": [
        "airworthiness directive", "safety directive", "service bulletin", "inspection",
        "replacement", "defect", "crack", "fault", "failure", "rudder", "stabilizer",
        "flight controls", "fuel shut-off", "brake", "landing gear", "gear", "flap",
        "hud", "window malfunction", "engine smoke", "leap", "gtf", "适航指令", "安全通告", "检查", "更换",
        "故障", "失效", "裂纹", "飞控", "起落架",
    ],
    "ops_environment": [
        "notam", "tfr", "airspace", "airspace closure", "airspace restriction",
        "procedure", "procedures", "reroute", "alternate", "runway data", "overrun",
        "cpdlc", "route uplink", "gps interference", "spoofing", "jamming",
        "volcanic ash", "航行通告", "空域", "程序", "绕飞", "备降机场", "导航干扰",
    ],
    "human_factors_training": [
        "spatial disorientation", "fatigue", "crm", "training", "simulator",
        "pilot training", "crew resource", "incapacitated", "疲劳", "训练", "模拟机",
    ],
}

_CATEGORY_BONUS = {
    "safety_event": 22.0,
    "airworthiness_technical": 18.0,
    "ops_environment": 14.0,
    "human_factors_training": 10.0,
}


def _load_raw(day: str) -> list[dict]:
    path = settings.raw_dir / f"{day}.json"
    if not path.exists():
        return []
    return load_json(path)


def _load_source_health(day: str) -> list[dict]:
    path = settings.raw_dir / f"source_health_{day}.json"
    if not path.exists():
        return []
    try:
        data = load_json(path)
    except Exception:  # noqa: BLE001
        return []
    return data if isinstance(data, list) else []


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


def _title_tokens_for_event(title: str) -> frozenset[str]:
    text = str(title or "").lower().replace("la guardia", "laguardia")
    tokens: set[str] = set()
    for raw in _TITLE_TOKEN_RE.findall(text):
        token = raw.lower().strip()
        if not token or token in _TITLE_STOP_TOKENS:
            continue
        if len(token) == 1 and token.isascii():
            continue
        token = _TITLE_TOKEN_SYNONYMS.get(token, token)
        tokens.add(token)
    return frozenset(tokens)


def _looks_like_same_event_title(a: frozenset[str], b: frozenset[str]) -> bool:
    if not a or not b:
        return False
    shared = len(a & b)
    if shared < 3:
        return False
    return shared / max(min(len(a), len(b)), 1) >= 0.33


def _is_pilot_relevant(item: dict, text: str, kw_cfg: dict) -> tuple[bool, str]:
    signal_words = _keyword_list(kw_cfg.get("pilot_signal_keywords"), _DEFAULT_PILOT_SIGNAL_KEYWORDS)
    entity_words = _keyword_list(kw_cfg.get("pilot_entity_keywords"), _DEFAULT_PILOT_ENTITY_KEYWORDS)
    reject_words = _keyword_list(kw_cfg.get("hard_reject_keywords"), _DEFAULT_HARD_REJECT_KEYWORDS)
    strict_reject_words = _keyword_list(kw_cfg.get("strict_hard_reject_keywords"), [])
    direct_operation_words = _keyword_list(
        kw_cfg.get("pilot_direct_operation_keywords"),
        _DEFAULT_PILOT_DIRECT_OPERATION_KEYWORDS,
    )
    background_only_words = _keyword_list(
        kw_cfg.get("pilot_background_only_keywords"),
        _DEFAULT_PILOT_BACKGROUND_ONLY_KEYWORDS,
    )
    schedule_advisory_words = _keyword_list(
        kw_cfg.get("pilot_schedule_advisory_keywords"),
        _DEFAULT_PILOT_SCHEDULE_ADVISORY_KEYWORDS,
    )
    specific_ops_words = _keyword_list(
        kw_cfg.get("pilot_specific_ops_keywords"),
        _DEFAULT_PILOT_SPECIFIC_OPS_KEYWORDS,
    )
    non_aviation_patterns = _keyword_list(
        kw_cfg.get("non_aviation_reject_patterns"), [],
    )

    allowed_source_ids = {str(x).strip() for x in kw_cfg.get("pilot_allowed_source_ids", []) if str(x).strip()}
    allowed_domains = {
        str(x).strip().lower()
        for x in kw_cfg.get("pilot_allowed_domains", [])
        if str(x).strip()
    }

    canonical_url = (item.get("canonical_url") or item.get("url") or "").strip()
    domain = _domain(canonical_url)
    source_id = str(item.get("source_id") or "").strip()

    title_l = item.get("title", "").lower()
    text_l = text.lower()
    if source_id.startswith("asn_") and _looks_like_non_transport_asn_record(text_l):
        return False, "non_transport_accident_record"
    if source_id == "easa_ad_web" and _looks_like_non_transport_easa_ad(text_l):
        return False, "non_transport_airworthiness_record"
    signal_hits = _count_hits(text_l, signal_words)
    entity_hits = _count_hits(text_l, entity_words)
    reject_hits = _count_hits(text_l, reject_words)
    trusted_source = source_id in allowed_source_ids or _domain_allowed(domain, allowed_domains)
    direct_operation_hits = _count_hits(title_l, direct_operation_words) + _count_hits(text_l, direct_operation_words)
    background_only_hits = _count_hits(title_l, background_only_words) + _count_hits(text_l, background_only_words)
    schedule_advisory_hits = _count_hits(title_l, schedule_advisory_words) + _count_hits(text_l, schedule_advisory_words)
    specific_ops_hits = _count_hits(title_l, specific_ops_words) + _count_hits(text_l, specific_ops_words)

    # Hard-reject known non-aviation entities (e.g. "Minnesota United" soccer)
    if non_aviation_patterns and _count_hits(title_l, non_aviation_patterns) > 0:
        return False, "non_aviation_entity"
    if strict_reject_words and _count_hits(text_l, strict_reject_words) > 0:
        return False, "strict_hard_reject_keywords"

    # Reject obvious noise unless signal is very strong.
    if reject_hits > 0 and signal_hits < 2:
        return False, "hard_reject_keywords"

    if signal_hits <= 0:
        return False, "missing_pilot_signal"

    # Trusted sources still need either an aviation entity OR strong signal (2+)
    # to prevent travel/lifestyle content from slipping through.
    if entity_hits <= 0:
        if not trusted_source:
            return False, "missing_aviation_entity"
        if signal_hits < 2 and direct_operation_hits <= 0:
            return False, "trusted_source_weak_signal"

    if background_only_hits > 0 and direct_operation_hits <= 0:
        return False, "background_only_story"
    if schedule_advisory_hits > 0 and specific_ops_hits <= 0:
        return False, "schedule_advisory_story"

    return True, "ok"


def _looks_like_non_transport_asn_record(text_l: str) -> bool:
    transport_markers = [
        "airbus", "boeing", "embraer emb-120", "embraer erj", "embraer e1",
        "embraer e2", "bombardier crj", "de havilland", "atr ", "airlines",
        "air lines", "airways", "cargo", "regional", "express",
    ]
    if any(marker in text_l for marker in transport_markers):
        return False
    non_transport_markers = [
        "private", "air force", "navy", "army", "idf/af", "drone", "uav",
        "mod hur", "fuerza aérea", "self-defense force", "defense force", "military",
        "cessna 172", "cessna 182", "piper pa-", "beechcraft", "bonanza",
        "citabria", "husky", "jonker", "glider", "super cub", "elbit hermes",
        "air tractor", "robinson r44", "robinson r22", "robinson", "bell 206",
        "skyranger", "agustawestland", " oh-1 ", "helicopter",
    ]
    return any(marker in text_l for marker in non_transport_markers)


def _looks_like_non_transport_easa_ad(text_l: str) -> bool:
    transport_markers = [
        "airbus s.a.s. a319", "airbus s.a.s. a320", "airbus s.a.s. a321",
        "airbus a330", "airbus a350", "boeing 737", "boeing 747", "boeing 757",
        "boeing 767", "boeing 777", "boeing 787", "embraer emb-120",
        "embraer erj", "atr ", "de havilland", "bombardier crj",
    ]
    if any(marker in text_l for marker in transport_markers):
        return False
    non_transport_markers = [
        "helicopter", "helicopters", "rotorcraft", "ec135", "ec145", "mbb-bk",
        "grob", "g 109", "continental aerospace", "tae125", "rotax",
        "agustawestland", "bell helicopter", "robinson", "sailplane", "glider",
    ]
    return any(marker in text_l for marker in non_transport_markers)


def _pilot_value_profile(item: dict, text: str, kw_cfg: dict) -> dict[str, Any]:
    """Classify how directly a story maps to cockpit/line operations."""
    text_l = text.lower()
    title_l = str(item.get("title", "")).lower()
    combined = f"{title_l} {text_l}"
    priority_sources = {
        str(x).strip()
        for x in kw_cfg.get("pilot_priority_sources", _DEFAULT_PILOT_PRIORITY_SOURCES)
        if str(x).strip()
    }
    source_id = str(item.get("source_id") or "").strip()

    category_hits = {
        category: _count_hits(combined, words)
        for category, words in _PILOT_CATEGORY_KEYWORDS.items()
    }
    category = max(category_hits, key=lambda x: category_hits[x]) if category_hits else "other"
    category_hit_count = category_hits.get(category, 0)
    if category_hit_count <= 0:
        category = "other"

    direct_words = _keyword_list(
        kw_cfg.get("pilot_direct_operation_keywords"),
        _DEFAULT_PILOT_DIRECT_OPERATION_KEYWORDS,
    )
    background_words = _keyword_list(
        kw_cfg.get("pilot_background_only_keywords"),
        _DEFAULT_PILOT_BACKGROUND_ONLY_KEYWORDS,
    )
    direct_hits = _count_hits(combined, direct_words)
    background_hits = _count_hits(combined, background_words)
    priority_source = source_id in priority_sources
    raw_len = len(str(item.get("raw_text", "") or ""))

    value = 0.0
    value += min(direct_hits * 8.0, 40.0)
    value += _CATEGORY_BONUS.get(category, 0.0)
    value += 12.0 if priority_source else 0.0
    value += 8.0 if raw_len >= 300 else 0.0
    value -= min(background_hits * 8.0, 28.0)
    value = max(0.0, min(100.0, 35.0 + value))
    return {
        "category": category,
        "category_hits": category_hits,
        "direct_hits": direct_hits,
        "background_hits": background_hits,
        "priority_source": priority_source,
        "pilot_value_score": round(value, 2),
    }


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


def _max_age_for_item(item: dict) -> int:
    if str(item.get("source_tier", "")).upper() == "A":
        return max(settings.max_article_age_hours, settings.max_tier_a_article_age_hours)
    return settings.max_article_age_hours


def _pick_by_quota(candidates: list[dict], total: int, domestic_ratio: float) -> list[dict]:
    if domestic_ratio <= 0.0:
        candidates = [c for c in candidates if c.get("region") != "domestic"]
    return candidates[:total]


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
            ),
            None,
        )
        if replacement is None:
            used_ids.discard(out[victim_idx].get("id"))
            del out[victim_idx]
            applied = True
            continue

        used_ids.discard(out[victim_idx].get("id"))
        out[victim_idx] = replacement
        used_ids.add(replacement.get("id"))
        applied = True

    return out, applied


def _dedupe_ranked_events(ranked: list[dict]) -> list[dict]:
    deduped: list[dict] = []
    seen_fp: set[str] = set()
    seen_url: set[str] = set()
    seen_titles: list[frozenset[str]] = []
    for row in ranked:
        fp = row.get("event_fingerprint", "")
        u = row.get("canonical_url", "")
        if fp in seen_fp or u in seen_url:
            continue
        title_tokens = _title_tokens_for_event(str(row.get("title", "")))
        if any(_looks_like_same_event_title(title_tokens, old) for old in seen_titles):
            continue
        seen_fp.add(fp)
        seen_url.add(u)
        if title_tokens:
            seen_titles.append(title_tokens)
        deduped.append(row)
    return deduped


def run(target_date: str | None = None) -> Path:
    day = target_date or beijing_today_str()
    rows = _load_raw(day)
    source_health = _load_source_health(day)
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
        max_age = _max_age_for_item(item)
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
        pilot_profile = _pilot_value_profile(item, text, kw)
        if pilot_profile["category"] == "other" and float(pilot_profile["pilot_value_score"]) < 70.0:
            dropped_non_pilot_relevant += 1
            continue
        rel = max(
            relevance_score(text, hits + pilot_hits),
            float(pilot_profile["pilot_value_score"]),
        )
        auth = tier_score(item.get("source_tier", "C"))
        if str(item.get("source_id", "")).startswith("google_"):
            auth = min(auth, 80.0)
        time_score = recency_score(item.get("published_at", ""))
        google_penalty = 15.0 if _looks_like_google_redirect(canonical_url) else 0.0
        priority_bonus = 8.0 if pilot_profile["priority_source"] else 0.0
        category_bonus = {
            "safety_event": 8.0,
            "airworthiness_technical": 6.0,
            "ops_environment": 4.0,
            "human_factors_training": 2.0,
        }.get(str(pilot_profile["category"]), 0.0)
        rank_score = round(
            rel * 0.70 + auth * 0.10 + time_score * 0.12 + priority_bonus + category_bonus - google_penalty,
            2,
        )

        enriched = dict(item)
        enriched.update(
            {
                "canonical_url": canonical_url,
                "publisher_domain": item.get("publisher_domain", dm),
                "event_fingerprint": item.get("event_fingerprint") or item.get("id"),
                "is_google_redirect": item.get("is_google_redirect", _looks_like_google_redirect(canonical_url)),
                "keyword_hits": hits,
                "pilot_value": pilot_profile,
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
    deduped = _dedupe_ranked_events(ranked)

    candidate_total = max(settings.target_article_count * 6, 50)
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
        top_candidates, a_tier_source_cap_applied = _enforce_source_cap(
            top_candidates,
            deduped,
            max_per_source=settings.max_entries_per_source,
        )
        source_cap_applied = source_cap_applied or a_tier_source_cap_applied
    top_candidates.sort(key=lambda x: x["rank_score"], reverse=True)

    source_distribution = Counter(x.get("source_id", "") for x in top_candidates if x.get("source_id"))
    source_health_summary = Counter(str(x.get("status", "unknown")) for x in source_health)
    source_failures = [
        {
            "source_id": str(x.get("source_id", "")),
            "source_name": str(x.get("source_name", "")),
            "status": str(x.get("status", "")),
            "item_count": int(x.get("item_count", 0) or 0),
            "error": str(x.get("error", ""))[:240],
        }
        for x in source_health
        if str(x.get("status", "")) in {"failed", "empty"}
    ][:12]

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
            "max_article_age_hours": settings.max_article_age_hours,
            "max_tier_a_article_age_hours": settings.max_tier_a_article_age_hours,
            "selected_for_compose": len(top_candidates),
            "source_cap_applied": source_cap_applied,
            "source_distribution": dict(source_distribution.most_common(10)),
            "source_health_summary": dict(source_health_summary),
            "source_failures": source_failures,
        },
        "articles": top_candidates,
    }
    out = settings.processed_dir / f"ranked_{day}.json"
    dump_json(out, payload)
    logger.info("Rank done. candidates=%s selected=%s", len(rows), len(top_candidates))
    return out


if __name__ == "__main__":
    run()

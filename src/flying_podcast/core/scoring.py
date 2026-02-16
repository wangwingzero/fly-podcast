from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


def tier_score(source_tier: str) -> float:
    return {"A": 100.0, "B": 80.0, "C": 60.0}.get(source_tier.upper(), 50.0)


def recency_score(published_at: str, now: datetime | None = None) -> float:
    now = now or datetime.now(timezone.utc)
    try:
        pub = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
    except ValueError:
        return 40.0

    delta = now - pub
    if delta <= timedelta(hours=12):
        return 100.0
    if delta <= timedelta(hours=24):
        return 90.0
    if delta <= timedelta(hours=48):
        return 75.0
    if delta <= timedelta(days=4):
        return 60.0
    return 40.0


def relevance_score(text: str, keyword_hits: int) -> float:
    base = min(keyword_hits * 15, 90)
    if len(text) > 180:
        base += 10
    return min(base, 100)


def readability_score(conclusion: str, facts: list[str], impact: str) -> float:
    points = 0.0
    points += 40.0 if conclusion else 0.0
    points += 30.0 if 2 <= len(facts) <= 3 else 10.0 if facts else 0.0
    points += 30.0 if impact else 0.0
    return min(points, 100.0)


def weighted_quality(
    factual: float,
    relevance: float,
    authority: float,
    timeliness: float,
    readability: float,
) -> float:
    return round(
        factual * 0.35
        + relevance * 0.25
        + authority * 0.20
        + timeliness * 0.10
        + readability * 0.10,
        2,
    )


def has_source_conflict(entry: dict[str, Any]) -> bool:
    title = entry.get("title", "").lower()
    facts = " ".join(entry.get("facts", [])).lower()
    needles = [("increase", "decrease"), ("approved", "rejected"), ("盈利", "亏损")]
    for a, b in needles:
        if a in title and b in facts:
            return True
        if b in title and a in facts:
            return True
    return False

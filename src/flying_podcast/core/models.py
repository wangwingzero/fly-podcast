from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class NewsItem:
    id: str
    title: str
    source_id: str
    source_name: str
    source_url: str
    url: str
    source_tier: str
    region: str
    published_at: str
    lang: str
    raw_text: str
    canonical_url: str = ""
    publisher_domain: str = ""
    is_google_redirect: bool = False
    event_fingerprint: str = ""
    image_url: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class DigestEntry:
    id: str
    source_id: str
    section: str
    title: str
    conclusion: str
    facts: list[str]
    impact: str
    citations: list[str]
    source_tier: str
    region: str
    score_breakdown: dict[str, float]
    source_name: str = ""
    url: str = ""
    canonical_url: str = ""
    publisher_domain: str = ""
    event_fingerprint: str = ""
    published_at: str = ""
    image_url: str = ""
    body: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class DailyDigest:
    date: str
    domestic_count: int
    international_count: int
    entries: list[DigestEntry] = field(default_factory=list)
    total_score: float = 0.0
    decision: str = "hold"
    generated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["entries"] = [entry.to_dict() for entry in self.entries]
        return data


@dataclass
class QualityReport:
    date: str
    total_score: float
    factual_score: float
    relevance_score: float
    citation_score: float
    timeliness_score: float
    readability_score: float
    decision: str
    reasons: list[str]
    blocked_entry_ids: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

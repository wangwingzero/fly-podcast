from flying_podcast.core.models import DigestEntry
from flying_podcast.stages.compose import (
    _build_recent_dedup_index,
    _prioritize_non_recent_candidates,
    _replace_recent_duplicates,
)


def _candidate(i: int, title: str, url: str, fp: str = "", region: str = "domestic") -> dict:
    return {
        "id": f"id-{i}",
        "title": title,
        "canonical_url": url,
        "url": url,
        "event_fingerprint": fp,
        "region": region,
    }


def _entry(i: int, title: str, url: str, fp: str = "", region: str = "domestic") -> DigestEntry:
    return DigestEntry(
        id=f"id-{i}",
        source_id="src",
        section="",
        title=title,
        conclusion=title,
        facts=["f1", "f2"],
        impact="i",
        citations=[url] if url else [],
        source_tier="A",
        region=region,
        score_breakdown={"total": 90.0},
        canonical_url=url,
        event_fingerprint=fp,
    )


def test_prioritize_non_recent_candidates_moves_repeated_url_to_tail():
    recent = [
        {
            "date": "2026-02-18",
            "title": "old",
            "url": "https://example.com/a?utm=1",
            "id": "",
            "event_fingerprint": "",
        }
    ]
    index = _build_recent_dedup_index(recent)
    candidates = [
        _candidate(1, "repeat", "https://example.com/a"),
        _candidate(2, "fresh", "https://example.com/b"),
    ]

    out = _prioritize_non_recent_candidates(candidates, index)
    assert [x["id"] for x in out] == ["id-2", "id-1"]


def test_prioritize_non_recent_candidates_matches_normalized_title():
    recent = [
        {
            "date": "2026-02-18",
            "title": "东航“百家姓”特色航班迎春贺岁",
            "url": "",
            "id": "",
            "event_fingerprint": "",
        }
    ]
    index = _build_recent_dedup_index(recent)
    candidates = [
        _candidate(1, "东航百家姓特色航班迎春贺岁", "https://example.com/a"),
        _candidate(2, "完全不同的新闻", "https://example.com/b"),
    ]

    out = _prioritize_non_recent_candidates(candidates, index)
    assert [x["id"] for x in out] == ["id-2", "id-1"]


def test_replace_recent_duplicates_prefers_same_region_non_recent_pool_entry():
    recent = [
        {
            "date": "2026-02-18",
            "title": "old",
            "url": "https://example.com/repeat",
            "id": "id-1",
            "event_fingerprint": "fp-repeat",
        }
    ]
    index = _build_recent_dedup_index(recent)
    entries = [
        _entry(1, "重复新闻", "https://example.com/repeat", fp="fp-repeat", region="domestic"),
        _entry(2, "国际新闻", "https://example.com/intl", fp="fp-intl", region="international"),
    ]
    pool = [
        _entry(1, "重复新闻", "https://example.com/repeat", fp="fp-repeat", region="domestic"),
        _entry(3, "新的国内新闻", "https://example.com/new-dom", fp="fp-new", region="domestic"),
        _entry(2, "国际新闻", "https://example.com/intl", fp="fp-intl", region="international"),
    ]

    out = _replace_recent_duplicates(entries, pool, index)
    assert [x.id for x in out] == ["id-3", "id-2"]

from flying_podcast.stages.rank import _dedupe_ranked_events, _enforce_source_cap


def _row(i: int, source_id: str, region: str):
    return {
        "id": f"id-{i}",
        "source_id": source_id,
        "source_name": source_id,
        "region": region,
    }


def test_enforce_source_cap_reduces_dominant_source():
    selected = [
        _row(1, "s1", "domestic"),
        _row(2, "s1", "domestic"),
        _row(3, "s1", "domestic"),
        _row(4, "s1", "domestic"),
    ]
    pool = selected + [
        _row(5, "s2", "domestic"),
        _row(6, "s3", "domestic"),
    ]
    out, applied = _enforce_source_cap(selected, pool, max_per_source=3)
    assert applied is True
    assert sum(1 for x in out if x["source_id"] == "s1") <= 3


def test_dedupe_ranked_events_collapses_similar_event_titles():
    ranked = [
        {
            "id": "best",
            "title": "LaGuardia runway collision inquiry seeks to learn why truck crew unaware of landing CRJ",
            "canonical_url": "https://example.com/best",
            "event_fingerprint": "fp1",
        },
        {
            "id": "duplicate",
            "title": "Fire truck failed to heed stop order before fatal La Guardia CRJ collision",
            "canonical_url": "https://example.com/duplicate",
            "event_fingerprint": "fp2",
        },
        {
            "id": "other",
            "title": "FAA issues new NOTAM for GPS interference procedures",
            "canonical_url": "https://example.com/other",
            "event_fingerprint": "fp3",
        },
    ]

    out = _dedupe_ranked_events(ranked)

    assert [x["id"] for x in out] == ["best", "other"]

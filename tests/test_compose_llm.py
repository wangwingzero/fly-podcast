from flying_podcast.stages.compose import _validate_llm_entries


def _candidate(i: int, region: str = "domestic"):
    return {
        "id": f"id-{i}",
        "title": f"title-{i}",
        "raw_text": "sample raw text about aviation operations.",
        "section": "",
        "source_tier": "A",
        "region": region,
        "url": f"https://example.com/{i}",
        "canonical_url": f"https://example.com/{i}",
        "score_breakdown": {"relevance": 90, "authority": 90, "timeliness": 90},
        "source_name": "Example",
        "source_id": "example_src",
        "publisher_domain": "example.com",
        "event_fingerprint": f"fp-{i}",
    }


def test_validate_llm_entries_ok():
    selected = [_candidate(i, "domestic" if i < 6 else "international") for i in range(10)]
    payload = {
        "entries": [
            {
                "ref_id": f"id-{i}",
                "section": selected[i].get("section", ""),
                "title": f"T{i}",
                "conclusion": f"C{i}",
                "facts": [f"F{i}-1", f"F{i}-2"],
                "impact": f"I{i}",
            }
            for i in range(10)
        ]
    }
    out = _validate_llm_entries(payload, selected, total=10)
    assert len(out) == 10
    assert out[0].citations[0] == "https://example.com/0"


def test_validate_llm_entries_reject_unknown_ref_id():
    selected = [_candidate(i) for i in range(10)]
    payload = {
        "entries": [
            {
                "ref_id": "id-not-exist",
                "section": "",
                "title": "bad",
                "conclusion": "bad",
                "facts": ["f1", "f2"],
                "impact": "i",
            }
        ]
    }
    try:
        _validate_llm_entries(payload, selected, total=10)
        assert False, "expected ValueError"
    except ValueError as exc:
        assert "empty" in str(exc)

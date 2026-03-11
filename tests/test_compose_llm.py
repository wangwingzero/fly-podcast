import json

from flying_podcast.stages.compose import (
    _TRANSLATE_BODY_PROMPT,
    _build_composition_prompt,
    _build_llm_prompts,
    _build_selection_prompt,
    _validate_llm_entries,
)


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


def test_build_selection_prompt_targets_pilot_only_and_allows_fewer_entries():
    system_prompt, user_prompt = _build_selection_prompt([_candidate(1)], total=10)
    payload = json.loads(user_prompt)

    assert payload["audience"] == "飞行员"
    assert "must_select_enough" not in payload["rules"]
    assert "allow_fewer_entries" in payload["rules"]
    assert all("新航线" not in topic for topic in payload["rules"]["prefer_topics"])
    assert "宁缺毋滥" in system_prompt


def test_build_llm_prompts_requests_longer_body():
    system_prompt, user_prompt = _build_llm_prompts([_candidate(1)], total=10, domestic_quota=0, intl_quota=10)
    payload = json.loads(user_prompt)

    assert "4-6句话" in system_prompt
    assert "180-260字" in system_prompt
    assert "4-6句话" in payload["rules"]["body_style"]
    assert "180-260字" in payload["rules"]["body_style"]


def test_build_composition_prompt_requests_longer_body_for_full_text():
    candidate = _candidate(1)
    candidate["raw_text"] = "A" * 500
    system_prompt, _ = _build_composition_prompt(candidate, "")

    assert "4-6句话" in system_prompt
    assert "180-260字" in system_prompt
    assert "不要压缩成两三句" in system_prompt
    assert "3-4句纯事实" in _TRANSLATE_BODY_PROMPT

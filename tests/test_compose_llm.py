import json

from flying_podcast.stages.compose import (
    _TRANSLATE_BODY_PROMPT,
    _build_composition_prompt,
    _build_llm_prompts,
    _build_selection_prompt,
    _sanitize_body_text,
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
    assert "新闻标题所述事件核心为" in system_prompt


def test_sanitize_body_text_removes_meta_discourse_sentence():
    body = (
        "2026年3月10日凌晨，JetBlue报告出现系统故障。随后，美国联邦航空管理局短暂停止了JetBlue所有离港航班。"
        "新闻标题所述事件核心为系统故障触发航班出港短暂停摆。\n"
        "划重点：系统一打喷嚏，全网先趴下。"
    )
    cleaned = _sanitize_body_text(body)

    assert "新闻标题所述事件核心为" not in cleaned
    assert "2026年3月10日凌晨" in cleaned
    assert "划重点：" in cleaned


def test_sanitize_body_text_strips_meta_lead_phrase():
    body = "报道提到，主管部门公布了初步报告。划重点：这事不小。"
    cleaned = _sanitize_body_text(body)

    assert "报道提到" not in cleaned
    assert cleaned == "划重点：这事不小。"


def test_sanitize_body_text_removes_mid_sentence_meta_phrase():
    body = (
        "2026年2月初，一架Scandinavian Airlines（SAS）客机在一座大型机场发生严重运行事件。"
        "报道指出，这起事件发生过程异常，涉及一次本不应出现的重大差错。"
        "划重点：滑行道当跑道用。"
    )
    cleaned = _sanitize_body_text(body)

    assert "报道指出" not in cleaned
    assert "发生严重运行事件" in cleaned
    assert "重大差错" not in cleaned


def test_sanitize_body_text_removes_reported_core_sentence_variant():
    body = (
        "2026年3月10日清晨，JetBlue报告发生系统故障。"
        "报道所述事件核心为航司系统故障报告与美国联邦航空管理局随即实施的短时停飞措施。"
        "划重点：系统一掉线，飞机先别动。"
    )
    cleaned = _sanitize_body_text(body)

    assert "报道所述事件核心为" not in cleaned
    assert "JetBlue报告发生系统故障" in cleaned

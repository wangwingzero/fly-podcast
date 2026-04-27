import json

from flying_podcast.stages.compose import (
    _TRANSLATE_BODY_PROMPT,
    _build_composition_prompt,
    _build_llm_prompts,
    _build_selection_prompt,
    _blend_selection_with_editorial_anchors,
    _enforce_constraints,
    _sanitize_body_text,
    _validate_llm_entries,
)
from flying_podcast.core.models import DigestEntry


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
    assert "max_pure_airworthiness_directives_if_available" in payload["rules"]["balance"]
    assert "纯适航指令或检查类 AD 最多2条" in system_prompt


def test_build_selection_prompt_zero_total_is_unlimited_high_value_mode():
    system_prompt, user_prompt = _build_selection_prompt([_candidate(1)], total=0)
    payload = json.loads(user_prompt)

    assert "不设数量上限" in system_prompt
    assert "禁止凑数" in system_prompt
    assert payload["rules"]["total"] == "unlimited"
    assert "allow_fewer_entries" in payload["rules"]


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
    assert "原文信息少就短写" in _TRANSLATE_BODY_PROMPT
    assert "新闻标题所述事件核心为" in system_prompt
    assert "禁止使用「所有」「全部」「均」" in system_prompt


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


def test_sanitize_body_text_removes_summary_style_sentences():
    body = (
        "有关部门现已就此发布初步报告，对事件经过展开说明。"
        "已公开的信息显示，这起差错的严重程度很高，且初步报告公布后，事件为何会发生仍令人费解。"
        "事件核心是航司系统故障触发了临时限制。"
        "划重点：这事不能只怪运气。"
    )
    cleaned = _sanitize_body_text(body)

    assert "信息显示" not in cleaned
    assert "仍令人费解" not in cleaned
    assert "事件核心" not in cleaned
    assert "有关部门现已就此发布初步报告" in cleaned


def test_sanitize_body_text_removes_judgment_style_sentences():
    body = (
        "2026年2月上旬，Scandinavian Airlines（SAS）一架飞机在一座大型机场尝试从滑行道起飞。"
        "该机在起飞滑跑中速度达到每小时120多英里，事件被形容为险些酿成严重后果。"
        "随后相关放行限制被解除，事件性质为一次短时运行中断。"
        "划重点：这事不小。"
    )
    cleaned = _sanitize_body_text(body)

    assert "被形容为" not in cleaned
    assert "严重后果" not in cleaned
    assert "事件性质为" not in cleaned
    assert "短时运行中断" not in cleaned
    assert "Scandinavian Airlines" in cleaned


def test_sanitize_body_text_removes_field_extraction_style_sentences():
    body = (
        "相关部门现已发布这起事件的初步报告。"
        "已明确的时间节点为2026年2月初，主体为SAS，事件地点为一座大型机场，涉及滑行道与起飞操作。"
        "原始信息中明确的主体包括美国联邦航空管理局和JetBlue，时间点为3月10日早间。"
        "划重点：这种话别写进正文。"
    )
    cleaned = _sanitize_body_text(body)

    assert "已明确的时间节点为" not in cleaned
    assert "主体为" not in cleaned
    assert "主体包括" not in cleaned
    assert "时间点为" not in cleaned
    assert "相关部门现已发布这起事件的初步报告" in cleaned


def test_sanitize_body_text_removes_source_hint_style_sentences():
    body = (
        "2026年3月10日早些时候，美国联邦航空管理局短暂停止了JetBlue所有离港航班。"
        "根据标题信息，该措施发生在JetBlue报告系统故障之后，受影响范围为JetBlue全部离港航班。"
        "相关主管部门此后结束了这一临时限制。"
        "划重点：系统掉链子，飞机先等等。"
    )
    cleaned = _sanitize_body_text(body)

    assert "根据标题信息" not in cleaned
    assert "受影响范围为" not in cleaned
    assert "相关主管部门此后" not in cleaned
    assert "美国联邦航空管理局短暂停止了JetBlue所有离港航班" in cleaned


def test_sanitize_body_text_removes_source_meta_variants():
    body = (
        "原文列明机上有232名旅客和13名机组。"
        "标题涉及Leap发动机烟雾事件，原文提到相关问题与发动机有关。"
        "原文未列出航班号、运营方或受损情况。"
        "划重点：别把整理口吻写进正文。"
    )
    cleaned = _sanitize_body_text(body)

    assert "原文列明" not in cleaned
    assert "标题涉及" not in cleaned
    assert "原文提到" not in cleaned
    assert "原文未列出" not in cleaned
    assert "232名旅客" in cleaned
    assert "Leap发动机烟雾事件" in cleaned
    assert "航班号" not in cleaned


def _digest_entry(i: int, *, source_id: str, section: str, title: str) -> DigestEntry:
    return DigestEntry(
        id=f"entry-{i}",
        source_id=source_id,
        section=section,
        title=title,
        conclusion=title,
        facts=["事实一", "事实二"],
        impact="",
        citations=[f"https://example.com/{i}"],
        source_tier="A",
        region="international",
        score_breakdown={
            "factual": 90,
            "relevance": 90,
            "authority": 90,
            "timeliness": 90,
            "readability": 100,
            "total": 92,
        },
        body=f"{title}。划重点：测试。",
    )


def test_enforce_constraints_uses_composed_overflow_for_pilot_balance():
    entries = [
        _digest_entry(1, source_id="avherald_web", section="safety_event", title="Swiss中断起飞"),
        _digest_entry(2, source_id="flightglobal_safety", section="safety_event", title="Kenyan冲出跑道"),
        _digest_entry(3, source_id="easa_ad_web", section="airworthiness_technical", title="EASA发布ATR适航指令"),
        _digest_entry(4, source_id="easa_ad_web", section="airworthiness_technical", title="EASA发布A320适航指令"),
        _digest_entry(5, source_id="easa_ad_web", section="airworthiness_technical", title="EASA发布Legacy适航指令"),
        _digest_entry(6, source_id="easa_ad_web", section="airworthiness_technical", title="EASA发布DHC适航指令"),
        _digest_entry(7, source_id="flightglobal_safety", section="safety_event", title="A321擦机尾指导"),
        _digest_entry(8, source_id="flightglobal_engines", section="airworthiness_technical", title="GE9X密封问题"),
        _digest_entry(9, source_id="faa_info_web", section="ops_environment", title="FAA发布CPDLC航路上行通告"),
        _digest_entry(10, source_id="nasa_asrs_callback_web", section="safety_event", title="ASRS跑道侵入案例"),
    ]

    out = _enforce_constraints(entries, [], total=8, domestic_ratio=0.0)

    assert len(out) == 8
    assert sum(1 for e in out if e.source_id == "easa_ad_web") <= 2
    assert any(e.section == "ops_environment" for e in out)
    assert any(e.title == "FAA发布CPDLC航路上行通告" for e in out)


def test_enforce_constraints_zero_total_does_not_truncate_entries():
    entries = [
        _digest_entry(i, source_id=f"source_{i}", section="safety_event", title=f"高价值安全事件{i}")
        for i in range(1, 7)
    ]

    out = _enforce_constraints(entries, [], total=0, domestic_ratio=0.0)

    assert len(out) == len(entries)
    assert [e.id for e in out] == [e.id for e in entries]


def test_blend_selection_keeps_high_value_editorial_anchors():
    candidates = [_candidate(i, "international") for i in range(1, 8)]
    candidates[0].update(
        {
            "title": "Swiss A330 rejected takeoff due engine failure",
            "source_id": "avherald_web",
            "rank_score": 106,
            "pilot_value": {"category": "safety_event"},
        }
    )
    candidates[1].update(
        {
            "title": "LaGuardia runway collision inquiry",
            "raw_text": "Runway collision inquiry with landing CRJ and airport vehicle.",
            "source_id": "flightglobal_safety",
            "rank_score": 103,
            "pilot_value": {"category": "safety_event"},
        }
    )
    candidates[2].update(
        {
            "title": "ASN accident record - thin",
            "raw_text": "Aviation Safety Network accident database entry.",
            "source_id": "asn_2026_web",
            "rank_score": 102,
            "pilot_value": {"category": "safety_event"},
        }
    )
    selected = [candidates[3]["id"], candidates[4]["id"]]

    out = _blend_selection_with_editorial_anchors(selected, candidates, total=5)

    assert candidates[0]["id"] in out
    assert candidates[1]["id"] in out
    assert candidates[2]["id"] not in out


def test_blend_selection_zero_total_keeps_all_editorial_anchors():
    candidates = [_candidate(i, "international") for i in range(1, 8)]
    for i, candidate in enumerate(candidates, start=1):
        candidate.update(
            {
                "title": f"Runway incursion safety report {i}",
                "source_id": "flightglobal_safety",
                "rank_score": 85 + i,
                "pilot_value": {"category": "safety_event"},
            }
        )
    selected = [candidates[-1]["id"]]

    out = _blend_selection_with_editorial_anchors(selected, candidates, total=0)

    assert [c["id"] for c in candidates] == out

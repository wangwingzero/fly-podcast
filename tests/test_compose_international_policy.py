from flying_podcast.stages.compose import _build_selection_prompt


def test_selection_prompt_prioritizes_industry_briefing_not_accident_quota():
    system_prompt, user_prompt = _build_selection_prompt(
        [
            {
                "id": "1",
                "title": "Fleet story",
                "raw_text": "A fleet planning story",
                "source_role": "primary_industry",
            }
        ],
        total=0,
    )

    assert "国际航空行业新闻编辑" in system_prompt
    assert "至少3条真实运行安全事件" not in system_prompt
    assert "事故、严重事故、空难调查默认不作为日报主体" in system_prompt
    assert "航司战略、机队、订单/交付、监管、机场、空域、MRO、供应链" in system_prompt
    assert "source_role" in user_prompt

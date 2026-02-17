from flying_podcast.stages.compose import _clean_title, _ensure_min_facts


def test_clean_title_removes_press_release_noise():
    title = "Delta Air Lines grows Airbus fleet Press Release Commercial Aircraft 28 January 2026 3 min read"
    cleaned, _ = _clean_title(title)
    assert "Press Release" not in cleaned
    assert "min read" not in cleaned


def test_ensure_min_facts_fills_to_two():
    facts = _ensure_min_facts([], "仅有一条短内容", "测试标题")
    assert len(facts) >= 2


def test_ensure_min_facts_avoids_title_duplication():
    facts = _ensure_min_facts([], "2026春运精准研判风险 上海监管局部署春运安全保障工作", "2026春运精准研判风险 上海监管局部署春运安全保障工作")
    assert len(facts) >= 2
    assert not any("2026春运精准研判风险 上海监管局部署春运安全保障工作" == x for x in facts)

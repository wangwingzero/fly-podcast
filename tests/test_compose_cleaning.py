from flying_podcast.stages.compose import _clean_title, _ensure_min_facts


def test_clean_title_removes_press_release_noise():
    title = "Delta Air Lines grows Airbus fleet Press Release Commercial Aircraft 28 January 2026 3 min read"
    cleaned, _ = _clean_title(title)
    assert "Press Release" not in cleaned
    assert "min read" not in cleaned


def test_ensure_min_facts_fills_to_two():
    facts = _ensure_min_facts([], "仅有一条短内容", "测试标题")
    assert len(facts) >= 2

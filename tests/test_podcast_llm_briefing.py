from flying_podcast.stages.podcast import BRIEFING_ADDENDUM, _resolve_llm_briefing


def test_resolve_llm_briefing_prefers_file(tmp_path) -> None:
    briefing_file = tmp_path / "briefing.txt"
    briefing_file.write_text("重点讲干扰案例", encoding="utf-8")
    assert _resolve_llm_briefing(briefing="ignored", briefing_file=briefing_file) == "重点讲干扰案例"


def test_briefing_addendum_template_has_marker() -> None:
    text = BRIEFING_ADDENDUM.format(briefing="多讲 spoofing")
    assert "制作人单独强调" in text
    assert "多讲 spoofing" in text

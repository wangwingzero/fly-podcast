from flying_podcast.core.scoring import has_source_conflict
from flying_podcast.stages.verify import _llm_editor_review


def test_source_conflict_detected():
    entry = {
        "title": "Airline profit expected to increase",
        "facts": ["report says revenue will decrease"],
    }
    assert has_source_conflict(entry) is True


def test_source_conflict_not_detected():
    entry = {
        "title": "Airline expands network",
        "facts": ["new routes launched"],
    }
    assert has_source_conflict(entry) is False


class _FakeClient:
    def __init__(self, payload=None):
        self.system_prompt = ""
        self.user_prompt = ""
        self.payload = payload or {"reviews": [{"id": "a1", "keep": True, "reason": "结构正常"}]}

    def complete_json(self, *, system_prompt: str, user_prompt: str, **kwargs):
        self.system_prompt = system_prompt
        self.user_prompt = user_prompt
        return type(
            "Resp",
            (),
            {"payload": self.payload},
        )()


def test_llm_editor_review_prompt_allows_humorous_highlight():
    client = _FakeClient()
    blocked = _llm_editor_review(
        [{
            "id": "a1",
            "title": "测试标题",
            "conclusion": "结论一句。",
            "facts": ["第一句事实。", "第二句事实。"],
            "body": "第一句事实。第二句事实。\n划重点：这波操作，机长群里肯定要聊两句。",
        }],
        client,
    )

    assert blocked == []
    assert "划重点" in client.system_prompt
    assert "严禁仅因为口语化" in client.system_prompt
    assert "技术增量" in client.system_prompt
    assert "风险增量" in client.system_prompt
    assert "第一句事实" in client.user_prompt


def test_llm_editor_review_keeps_high_value_ops_story_when_reason_is_only_too_thin():
    client = _FakeClient(
        payload={
            "reviews": [
                {
                    "id": "a1",
                    "keep": False,
                    "reason": "正文只有对标题的重复性概述，缺少时间、航班等基本新闻事实，内容过于空泛。",
                }
            ]
        }
    )
    blocked = _llm_editor_review(
        [{
            "id": "a1",
            "title": "American Airlines一架Airbus A321因Dark Cockpit放出RAT并备降巴尔的摩",
            "conclusion": "一架A321在飞行中出现Dark Cockpit后放出RAT并备降。",
            "facts": ["机组报告出现Dark Cockpit。", "飞机放出RAT并改降巴尔的摩。"],
            "body": "机组报告出现Dark Cockpit，随后放出RAT并备降巴尔的摩。",
        }],
        client,
    )

    assert blocked == []


def test_llm_editor_review_still_blocks_high_value_ops_story_for_hard_quality_failure():
    client = _FakeClient(
        payload={
            "reviews": [
                {
                    "id": "a1",
                    "keep": False,
                    "reason": "正文机翻严重且前后矛盾，不适合发布。",
                }
            ]
        }
    )
    blocked = _llm_editor_review(
        [{
            "id": "a1",
            "title": "American Airlines一架Airbus A321因Dark Cockpit放出RAT并备降巴尔的摩",
            "conclusion": "一架A321在飞行中出现Dark Cockpit后放出RAT并备降。",
            "facts": ["机组报告出现Dark Cockpit。", "飞机放出RAT并改降巴尔的摩。"],
            "body": "机组报告出现Dark Cockpit，随后放出RAT并备降巴尔的摩。",
        }],
        client,
    )

    assert blocked == ["a1"]


def test_llm_editor_review_does_not_treat_emirates_as_rat_signal():
    client = _FakeClient(
        payload={
            "reviews": [
                {
                    "id": "a1",
                    "keep": False,
                    "reason": "只有区域局势与运行受扰的笼统表述，缺少具体航班、机场、空域限制或航司处置细节，信息增量不足。",
                }
            ]
        }
    )
    blocked = _llm_editor_review(
        [{
            "id": "a1",
            "title": "伊朗导弹回应导致中东航司运行受扰",
            "conclusion": "美国和以色列袭击伊朗后，伊朗发射导弹回应，已对中东航空公司运行造成严重干扰。",
            "facts": [
                "过去一周，美国和以色列袭击伊朗后，伊朗以发射导弹作出回应。",
                "受此影响，中东多家航空公司运行严重受扰，Emirates的航线网络运营也出现困难。",
            ],
            "body": "受此影响，中东多家航空公司运行严重受扰，Emirates的航线网络运营也出现困难。",
        }],
        client,
    )

    assert blocked == ["a1"]

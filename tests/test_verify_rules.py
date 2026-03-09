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
    def __init__(self):
        self.system_prompt = ""

    def complete_json(self, *, system_prompt: str, user_prompt: str, **kwargs):
        self.system_prompt = system_prompt
        return type(
            "Resp",
            (),
            {"payload": {"reviews": [{"id": "a1", "keep": True, "reason": "结构正常"}]}},
        )()


def test_llm_editor_review_prompt_allows_humorous_highlight():
    client = _FakeClient()
    blocked = _llm_editor_review(
        [{"id": "a1", "title": "测试标题", "body": "第一句事实。第二句事实。\n划重点：这波操作，机长群里肯定要聊两句。"}],
        client,
    )

    assert blocked == []
    assert "划重点" in client.system_prompt
    assert "严禁仅因为口语化" in client.system_prompt

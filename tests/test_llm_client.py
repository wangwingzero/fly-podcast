from types import SimpleNamespace

from flying_podcast.core.llm_client import OpenAICompatibleClient


def test_chat_url_from_base_prefix():
    c = OpenAICompatibleClient("k", "https://api.example.com/v1", "m")
    assert c._chat_url() == "https://api.example.com/v1/chat/completions"


def test_chat_url_keeps_full_path():
    c = OpenAICompatibleClient("k", "https://api.example.com/v1/chat/completions", "m")
    assert c._chat_url() == "https://api.example.com/v1/chat/completions"


def test_chat_urls_from_root_base_try_v1_first():
    c = OpenAICompatibleClient("k", "https://api.example.com", "m")
    assert c._chat_urls() == [
        "https://api.example.com/v1/chat/completions",
        "https://api.example.com/chat/completions",
    ]


def test_responses_urls_from_root_base_try_v1_first():
    c = OpenAICompatibleClient("k", "https://api.example.com", "gpt-5.4")
    assert c._responses_urls() == [
        "https://api.example.com/v1/responses",
        "https://api.example.com/responses",
    ]


def test_extract_json_object_from_wrapped_text():
    wrapped = "Here is output:\n```json\n{\"ok\":true}\n```"
    parsed = OpenAICompatibleClient._extract_json_object(wrapped)
    assert parsed["ok"] is True


def test_anthropic_thinking_only_response_retries_with_more_tokens(monkeypatch):
    calls = []

    class FakeResponse:
        ok = True
        text = ""

        def __init__(self, payload):
            self._payload = payload

        def json(self):
            return self._payload

    def fake_post(url, headers, json, timeout):
        calls.append(json["max_tokens"])
        if len(calls) == 1:
            return FakeResponse(
                {
                    "content": [
                        {
                            "type": "thinking",
                            "thinking": "The model used the small token budget before final text.",
                        }
                    ],
                    "stop_reason": "max_tokens",
                }
            )
        return FakeResponse(
            {
                "content": [{"type": "text", "text": "aviation test ok"}],
                "stop_reason": "end_turn",
            }
        )

    monkeypatch.setattr("flying_podcast.core.llm_client.requests.post", fake_post)

    client = OpenAICompatibleClient(
        "k",
        "https://api.deepseek.com/anthropic",
        "deepseek-v4-pro",
    )
    text = client.complete_text(
        system_prompt="Reply in plain text only.",
        user_prompt="Say aviation test ok.",
        max_tokens=40,
        temperature=0,
        retries=1,
        timeout=10,
        _allow_backup=False,
    )

    assert text == "aviation test ok"
    assert calls == [40, 80]


def test_anthropic_json_retries_after_truncated_text(monkeypatch):
    calls = []

    class FakeResponse:
        ok = True
        text = ""

        def __init__(self, payload):
            self._payload = payload

        def json(self):
            return self._payload

    def fake_post(url, headers, json, timeout):
        calls.append(json["max_tokens"])
        if len(calls) == 1:
            return FakeResponse(
                {
                    "content": [
                        {
                            "type": "thinking",
                            "thinking": "The model used the small token budget before final JSON.",
                        }
                    ],
                    "stop_reason": "max_tokens",
                }
            )
        if len(calls) == 2:
            return FakeResponse(
                {
                    "content": [{"type": "text", "text": '{"ok": true'}],
                    "stop_reason": "max_tokens",
                }
            )
        return FakeResponse(
            {
                "content": [{"type": "text", "text": '{"ok": true, "service": "llm"}'}],
                "stop_reason": "end_turn",
            }
        )

    monkeypatch.setattr("flying_podcast.core.llm_client.requests.post", fake_post)

    client = OpenAICompatibleClient(
        "k",
        "https://api.deepseek.com/anthropic",
        "deepseek-v4-pro",
    )
    result = client.complete_json(
        system_prompt="Return only a JSON object.",
        user_prompt='Return {"ok": true, "service": "llm"}.',
        max_tokens=40,
        temperature=0,
        retries=1,
        timeout=10,
        _allow_backup=False,
    )

    assert result.payload == {"ok": True, "service": "llm"}
    assert calls == [40, 80, 160]


def test_json_uses_grok_fallback_after_backup_fails(monkeypatch):
    calls = []
    fake_settings = SimpleNamespace(
        llm_api_key="primary-key",
        llm_base_url="https://primary.example/v1",
        llm_model="bad-primary",
        llm_backup_api_key="backup-key",
        llm_backup_base_url="https://backup.example/v1",
        llm_backup_model="bad-backup",
        llm_fallback_api_key="fallback-key",
        llm_fallback_base_url="https://grok.223344567.xyz/v1",
        llm_fallback_model="grok-4.20-0309-non-reasoning",
    )

    class FakeResponse:
        text = ""

        def __init__(self, status_code, payload):
            self.status_code = status_code
            self.ok = status_code < 400
            self._payload = payload
            self.text = str(payload)

        def json(self):
            return self._payload

    def fake_post(url, headers, json, timeout):
        calls.append((url, json["model"]))
        if json["model"] == "grok-4.20-0309-non-reasoning" and url.endswith("/chat/completions"):
            return FakeResponse(
                200,
                {
                    "choices": [
                        {"message": {"content": '{"ok": true, "service": "grok"}'}}
                    ]
                },
            )
        return FakeResponse(500, {"error": "forced failure"})

    monkeypatch.setattr("flying_podcast.core.llm_client.settings", fake_settings)
    monkeypatch.setattr("flying_podcast.core.llm_client.requests.post", fake_post)

    client = OpenAICompatibleClient("primary-key", "https://primary.example/v1", "bad-primary")
    result = client.complete_json(
        system_prompt="Return JSON.",
        user_prompt="Return ok.",
        max_tokens=40,
        temperature=0,
        retries=1,
        timeout=10,
    )

    assert result.payload == {"ok": True, "service": "grok"}
    assert ("https://backup.example/v1/chat/completions", "bad-backup") in calls
    assert ("https://grok.223344567.xyz/v1/chat/completions", "grok-4.20-0309-non-reasoning") in calls


def test_json_uses_secondary_backup_before_grok_fallback(monkeypatch):
    calls = []
    fake_settings = SimpleNamespace(
        llm_api_key="primary-key",
        llm_base_url="https://primary.example/v1",
        llm_model="bad-primary",
        llm_backup_api_key="backup-key",
        llm_backup_base_url="https://backup.example/v1",
        llm_backup_model="bad-backup",
        llm_secondary_backup_api_key="secondary-key",
        llm_secondary_backup_base_url="https://api.deepseek.com/anthropic",
        llm_secondary_backup_model="deepseek-v4-pro",
        llm_fallback_api_key="fallback-key",
        llm_fallback_base_url="https://grok.223344567.xyz/v1",
        llm_fallback_model="grok-4.20-0309-non-reasoning",
    )

    class FakeResponse:
        text = ""

        def __init__(self, status_code, payload):
            self.status_code = status_code
            self.ok = status_code < 400
            self._payload = payload
            self.text = str(payload)

        def json(self):
            return self._payload

    def fake_post(url, headers, json, timeout):
        calls.append((url, json["model"]))
        if json["model"] == "deepseek-v4-pro" and "api.deepseek.com" in url:
            return FakeResponse(
                200,
                {
                    "content": [{"type": "text", "text": '{"ok": true, "service": "secondary"}'}],
                    "stop_reason": "end_turn",
                },
            )
        return FakeResponse(500, {"error": "forced failure"})

    monkeypatch.setattr("flying_podcast.core.llm_client.settings", fake_settings)
    monkeypatch.setattr("flying_podcast.core.llm_client.requests.post", fake_post)

    client = OpenAICompatibleClient("primary-key", "https://primary.example/v1", "bad-primary")
    result = client.complete_json(
        system_prompt="Return JSON.",
        user_prompt="Return ok.",
        max_tokens=40,
        temperature=0,
        retries=1,
        timeout=10,
    )

    assert result.payload == {"ok": True, "service": "secondary"}
    assert ("https://backup.example/v1/chat/completions", "bad-backup") in calls
    assert ("https://api.deepseek.com/anthropic/v1/messages", "deepseek-v4-pro") in calls
    assert not any(model == "grok-4.20-0309-non-reasoning" for _, model in calls)


def test_invalid_responses_json_falls_back_to_chat(monkeypatch):
    urls = []

    class FakeResponse:
        ok = True
        status_code = 200

        def __init__(self, url):
            self.url = url
            self.text = "not json" if url.endswith("/responses") else ""

        def json(self):
            if self.url.endswith("/responses"):
                raise ValueError("invalid json")
            return {"choices": [{"message": {"content": "ok"}}]}

    def fake_post(url, headers, json, timeout):
        urls.append(url)
        return FakeResponse(url)

    monkeypatch.setattr("flying_podcast.core.llm_client.requests.post", fake_post)

    client = OpenAICompatibleClient("k", "https://api.example/v1", "m")
    text = client.complete_text(
        system_prompt="Reply briefly.",
        user_prompt="Say ok.",
        max_tokens=20,
        temperature=0,
        retries=1,
        timeout=10,
        _allow_backup=False,
    )

    assert text == "ok"
    assert urls == [
        "https://api.example/v1/responses",
        "https://api.example/v1/responses",
        "https://api.example/v1/chat/completions",
    ]

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

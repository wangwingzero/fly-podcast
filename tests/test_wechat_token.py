import json
from types import SimpleNamespace

from flying_podcast.core import wechat
from flying_podcast.core.wechat import WeChatClient


def test_wechat_client_uses_stable_token_and_disk_cache(monkeypatch, tmp_path):
    fake_settings = SimpleNamespace(
        wechat_proxy="",
        wechat_app_id="wx-test-app",
        wechat_app_secret="secret",
        wechat_use_stable_token=True,
        wechat_token_cache_path=tmp_path / "wechat_stable_token.json",
    )
    calls = []

    def fake_post_json(url, params=None, body=None, proxy="", timeout=60):
        calls.append({"url": url, "params": params, "body": body})
        return {"access_token": "stable-token", "expires_in": 7200}

    monkeypatch.setattr(wechat, "settings", fake_settings)
    monkeypatch.setattr(wechat, "_curl_post_json", fake_post_json)

    first = WeChatClient()._access_token()
    second = WeChatClient()._access_token()

    assert first == "stable-token"
    assert second == "stable-token"
    assert len(calls) == 1
    assert calls[0]["url"] == "https://api.weixin.qq.com/cgi-bin/stable_token"
    assert calls[0]["body"] == {
        "grant_type": "client_credential",
        "appid": "wx-test-app",
        "secret": "secret",
        "force_refresh": False,
    }

    cache = json.loads(fake_settings.wechat_token_cache_path.read_text(encoding="utf-8"))
    assert cache["access_token"] == "stable-token"
    assert cache["appid"] == "wx-test-app"
    assert cache["source"] == "stable_token"
    assert "secret" not in cache
    assert "secret_sha256" in cache

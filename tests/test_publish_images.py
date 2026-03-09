import importlib

from flying_podcast.core.wechat import WeChatClient

publish = importlib.import_module("flying_podcast.stages.publish")


def test_mirror_entry_images_to_r2_updates_external_urls(monkeypatch):
    entries = [{"image_url": "https://example.com/a.jpg"}]
    fake_settings = type(
        "Settings",
        (),
        {
            "r2_access_key_id": "ak",
            "r2_secret_access_key": "sk",
            "r2_endpoint": "https://r2.example.com",
            "r2_domain": "cdn.example.com",
        },
    )()
    monkeypatch.setattr(publish, "settings", fake_settings)
    monkeypatch.setattr(
        publish,
        "mirror_image_from_url",
        lambda image_url, r2_prefix: f"https://cdn.example.com/{r2_prefix}/cached.jpg",
    )

    mirrored = publish._mirror_entry_images_to_r2(entries, r2_prefix="digest/article-images")

    assert mirrored == 1
    assert entries[0]["image_url"] == "https://cdn.example.com/digest/article-images/cached.jpg"


def test_replace_external_images_unescapes_query_params():
    client = WeChatClient.__new__(WeChatClient)
    captured = {}

    client._access_token = lambda: "token"

    def _fake_upload(url: str, token: str = "") -> str:
        captured["url"] = url
        return "https://mmbiz.qpic.cn/fake.jpg"

    client.upload_content_image = _fake_upload
    html = '<img src="https://example.com/a.jpg?x=1&amp;y=2" />'

    out = client.replace_external_images(html)

    assert captured["url"] == "https://example.com/a.jpg?x=1&y=2"
    assert "https://mmbiz.qpic.cn/fake.jpg" in out

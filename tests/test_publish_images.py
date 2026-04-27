import importlib
import json
from pathlib import Path
from types import SimpleNamespace

from flying_podcast.core.wechat import WeChatClient

publish = importlib.import_module("flying_podcast.stages.publish")


def test_mirror_entry_images_to_static_updates_external_urls(monkeypatch, tmp_path):
    entries = [{"image_url": "https://example.com/a.jpg"}]
    fake_settings = type(
        "Settings",
        (),
        {
            "static_root": str(tmp_path),
            "static_public_base_url": "https://static.example.com",
        },
    )()
    monkeypatch.setattr(publish, "settings", fake_settings)
    monkeypatch.setattr(
        publish,
        "mirror_image_from_url",
        lambda image_url, static_prefix: f"https://static.example.com/{static_prefix}/cached.jpg",
    )

    mirrored = publish._mirror_entry_images_to_static(entries, static_prefix="digest/article-images")

    assert mirrored == 1
    assert entries[0]["image_url"] == "https://static.example.com/digest/article-images/cached.jpg"


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


def test_publish_skips_wechat_when_digest_is_empty(monkeypatch, tmp_path):
    processed_dir = tmp_path / "processed"
    output_dir = tmp_path / "output"
    processed_dir.mkdir()
    output_dir.mkdir()

    (processed_dir / "composed_2026-03-12.json").write_text(
        json.dumps({
            "date": "2026-03-12",
            "article_count": 0,
            "entries": [],
            "meta": {"compose_mode": "llm_two_phase"},
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    (processed_dir / "quality_2026-03-12.json").write_text(
        json.dumps({
            "date": "2026-03-12",
            "decision": "skip_publish",
            "total_score": 78.75,
            "reasons": ["empty_digest", "llm_editor_rejected"],
        }, ensure_ascii=False),
        encoding="utf-8",
    )

    fake_settings = SimpleNamespace(
        processed_dir=processed_dir,
        output_dir=output_dir,
        web_digest_base_url="https://cdn.example.com/digest",
        dry_run=False,
        wechat_enable_publish=True,
        wechat_author="",
    )
    monkeypatch.setattr(publish, "settings", fake_settings)
    monkeypatch.setattr(publish, "ensure_dirs", lambda: None)
    monkeypatch.setattr(publish, "_load_saved_copyright_notice_url", lambda: "")
    monkeypatch.setattr(publish, "_copyright_web_fallback_url", lambda: "https://example.com/copyright")
    monkeypatch.setattr(publish, "_mirror_entry_images_to_static", lambda *args, **kwargs: 0)
    monkeypatch.setattr(publish, "_render_markdown", lambda digest: "# empty")
    monkeypatch.setattr(publish, "_render_html", lambda digest: "<html>empty</html>")
    monkeypatch.setattr(publish, "_generate_digest_summary", lambda digest: "empty")
    monkeypatch.setattr(publish, "_generate_web_intro", lambda digest: "empty")
    monkeypatch.setattr(publish, "_enhance_web_entries", lambda digest: digest)
    monkeypatch.setattr(
        publish,
        "_render_web_html",
        lambda digest, summary, intro, copyright_notice_url: "<html>web</html>",
    )
    monkeypatch.setattr(publish, "_download_first_article_image", lambda digest: None)
    monkeypatch.setattr(publish, "_save_recent_published", lambda digest, day: None)

    class ExplodingWeChatClient:
        def __init__(self, *args, **kwargs):
            raise AssertionError("WeChatClient should not be called for empty digests")

    monkeypatch.setattr(publish, "WeChatClient", ExplodingWeChatClient)

    out_path = publish.run("2026-03-12")
    result = json.loads(Path(out_path).read_text(encoding="utf-8"))

    assert result["status"] == "skipped_empty_digest"
    assert "empty_digest" in result["reasons"]


def test_publish_filters_blocked_entries_and_does_not_save_history_in_dry_run(monkeypatch, tmp_path):
    processed_dir = tmp_path / "processed"
    output_dir = tmp_path / "output"
    processed_dir.mkdir()
    output_dir.mkdir()

    entries = [
        {
            "id": "blocked",
            "title": "Blocked",
            "body": "blocked",
            "citations": ["https://example.com/blocked"],
        },
        {
            "id": "kept",
            "title": "保留",
            "body": "中文正文",
            "citations": ["https://example.com/kept"],
        },
    ]
    (processed_dir / "composed_2026-03-13.json").write_text(
        json.dumps({
            "date": "2026-03-13",
            "article_count": 2,
            "entries": entries,
            "meta": {"compose_mode": "llm_two_phase"},
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    (processed_dir / "quality_2026-03-13.json").write_text(
        json.dumps({
            "date": "2026-03-13",
            "decision": "auto_publish",
            "total_score": 88.0,
            "reasons": ["non_chinese_content"],
            "blocked_entry_ids": ["blocked"],
        }, ensure_ascii=False),
        encoding="utf-8",
    )

    fake_settings = SimpleNamespace(
        processed_dir=processed_dir,
        output_dir=output_dir,
        web_digest_base_url="https://cdn.example.com/digest",
        dry_run=True,
        wechat_enable_publish=False,
        wechat_author="",
    )
    rendered_counts = []
    saved_history = []
    monkeypatch.setattr(publish, "settings", fake_settings)
    monkeypatch.setattr(publish, "ensure_dirs", lambda: None)
    monkeypatch.setattr(publish, "_load_saved_copyright_notice_url", lambda: "")
    monkeypatch.setattr(publish, "_copyright_web_fallback_url", lambda: "https://example.com/copyright")
    monkeypatch.setattr(publish, "_mirror_entry_images_to_static", lambda *args, **kwargs: 0)
    monkeypatch.setattr(publish, "_render_markdown", lambda digest: "# draft")
    monkeypatch.setattr(publish, "_render_html", lambda digest: rendered_counts.append(len(digest["entries"])) or "<html>draft</html>")
    monkeypatch.setattr(publish, "_generate_digest_summary", lambda digest: "summary")
    monkeypatch.setattr(publish, "_generate_web_intro", lambda digest: "intro")
    monkeypatch.setattr(publish, "_enhance_web_entries", lambda digest: digest)
    monkeypatch.setattr(
        publish,
        "_render_web_html",
        lambda digest, summary, intro, copyright_notice_url: "<html>web</html>",
    )
    monkeypatch.setattr(publish, "_download_first_article_image", lambda digest: None)
    monkeypatch.setattr(publish, "_save_recent_published", lambda digest, day: saved_history.append(day))

    out_path = publish.run("2026-03-13")
    result = json.loads(Path(out_path).read_text(encoding="utf-8"))

    assert result["status"] == "dry_run"
    assert result["filtered_blocked_count"] == 1
    assert result["blocked_entry_ids"] == ["blocked"]
    assert "blocked_entries_filtered" in result["reasons"]
    assert rendered_counts == [1]
    assert saved_history == []

from __future__ import annotations

import importlib
from pathlib import Path
from types import SimpleNamespace

notify_module = importlib.import_module("flying_podcast.stages.notify")


def _write_json(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def _prepare_inputs(tmp_path: Path) -> SimpleNamespace:
    raw_dir = tmp_path / "raw"
    processed_dir = tmp_path / "processed"
    output_dir = tmp_path / "output"

    _write_json(raw_dir / "2026-03-09.json", '[{"id": "a"}]')
    _write_json(
        processed_dir / "ranked_2026-03-09.json",
        '{"meta": {"accepted_count": 1, "rejected_count": 0}}',
    )
    _write_json(
        processed_dir / "composed_2026-03-09.json",
        '{"article_count": 1, "entries": [{"title": "demo"}], "meta": {"compose_mode": "llm_two_phase"}}',
    )
    _write_json(
        processed_dir / "quality_2026-03-09.json",
        '{"total_score": 81.7, "decision": "auto_publish", "reasons": ["ok"]}',
    )
    _write_json(
        output_dir / "publish_2026-03-09.json",
        '{"status": "draft_created", "url": "https://mp.weixin.qq.com"}',
    )

    return SimpleNamespace(
        raw_dir=raw_dir,
        processed_dir=processed_dir,
        output_dir=output_dir,
        alert_webhook_url="",
        dry_run=False,
    )


def test_notify_logs_dry_run_only_when_dry_run(monkeypatch, tmp_path, caplog):
    fake_settings = _prepare_inputs(tmp_path)
    fake_settings.dry_run = True
    fake_settings.alert_webhook_url = "https://example.com/webhook"

    caplog.set_level("INFO")
    monkeypatch.setattr(notify_module, "settings", fake_settings)
    monkeypatch.setattr(notify_module, "send_pipeline_report", lambda *args, **kwargs: None)

    notify_module.run("2026-03-09")

    assert "[DRY_RUN notify]" in caplog.text
    assert "[SKIP notify]" not in caplog.text


def test_notify_logs_skip_when_webhook_missing(monkeypatch, tmp_path, caplog):
    fake_settings = _prepare_inputs(tmp_path)

    caplog.set_level("INFO")
    monkeypatch.setattr(notify_module, "settings", fake_settings)
    monkeypatch.setattr(notify_module, "send_pipeline_report", lambda *args, **kwargs: None)

    notify_module.run("2026-03-09")

    assert "[SKIP notify] ALERT_WEBHOOK_URL is not configured" in caplog.text
    assert "[DRY_RUN notify]" not in caplog.text


def test_notify_posts_webhook_when_live(monkeypatch, tmp_path):
    fake_settings = _prepare_inputs(tmp_path)
    fake_settings.alert_webhook_url = "https://example.com/webhook"

    called = {}

    class FakeResponse:
        ok = True
        status_code = 200
        text = "ok"

    def fake_post(url, json, timeout):
        called["url"] = url
        called["json"] = json
        called["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(notify_module, "settings", fake_settings)
    monkeypatch.setattr(notify_module, "send_pipeline_report", lambda *args, **kwargs: None)
    monkeypatch.setattr(notify_module.requests, "post", fake_post)

    notify_module.run("2026-03-09")

    assert called == {
        "url": "https://example.com/webhook",
        "json": {
            "msgtype": "text",
            "text": {
                "content": (
                    "Global Aviation Digest 2026-03-09\n"
                    "质量分: 81.7\n"
                    "决策: auto_publish\n"
                    "成稿模式: llm_two_phase\n"
                    "发布状态: draft_created\n"
                    "原因: ok\n"
                    "链接: https://mp.weixin.qq.com"
                )
            },
        },
        "timeout": 15,
    }

from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "server" / "sync_r2_history.py"
    spec = importlib.util.spec_from_file_location("sync_r2_history", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _FakeClient:
    def __init__(self):
        self.calls = []

    def download_file(self, bucket: str, key: str, destination: str) -> None:
        self.calls.append((bucket, key, destination))
        Path(destination).write_text('{"days":[]}', encoding="utf-8")


def test_download_recent_published_writes_history_file(tmp_path):
    module = _load_module()
    client = _FakeClient()
    destination = tmp_path / "data" / "history" / "recent_published.json"

    downloaded = module._download_recent_published(client, "bucket-name", destination)

    assert downloaded is True
    assert destination.read_text(encoding="utf-8") == '{"days":[]}'
    assert client.calls == [
        ("bucket-name", "history/recent_published.json", str(destination)),
    ]


def test_download_recent_published_dry_run_does_not_call_client(tmp_path):
    module = _load_module()
    client = _FakeClient()
    destination = tmp_path / "recent_published.json"

    downloaded = module._download_recent_published(client, "bucket-name", destination, dry_run=True)

    assert downloaded is False
    assert client.calls == []
    assert not destination.exists()

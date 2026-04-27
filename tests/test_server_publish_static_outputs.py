from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "server" / "publish_static_outputs.py"
    spec = importlib.util.spec_from_file_location("publish_static_outputs", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_copy_file_publishes_to_static_root(tmp_path):
    module = _load_module()
    source = tmp_path / "web.html"
    source.write_text("<html>ok</html>", encoding="utf-8")
    static_root = tmp_path / "static"

    copied = module._copy_file(source, static_root, "digest/web.html")

    assert copied is True
    assert (static_root / "digest" / "web.html").read_text(encoding="utf-8") == "<html>ok</html>"


def test_copy_file_skips_missing_source(tmp_path):
    module = _load_module()

    copied = module._copy_file(tmp_path / "missing.html", tmp_path / "static", "digest/web.html")

    assert copied is False
    assert not (tmp_path / "static").exists()

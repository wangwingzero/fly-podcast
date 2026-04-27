import json
import importlib
from types import SimpleNamespace

from flying_podcast.stages.ingest import _AnchorParser

ingest_module = importlib.import_module("flying_podcast.stages.ingest")


def test_anchor_parser_extracts_links_and_text():
    html = """
    <html><body>
      <a href="/news/1">  航空公司新增国际航线  </a>
      <a href="https://example.com/about">关于我们</a>
    </body></html>
    """
    p = _AnchorParser()
    p.feed(html)
    assert len(p.links) == 2
    assert p.links[0][0] == "/news/1"
    assert "新增国际航线" in p.links[0][1]


def test_ingest_writes_source_health(monkeypatch, tmp_path):
    sources_config = tmp_path / "sources.yaml"
    sources_config.write_text(
        """
sources:
  - id: demo_rss
    name: Demo RSS
    url: https://example.com/feed.xml
    type: rss
    source_tier: A
    region: international
    enabled: true
  - id: empty_web
    name: Empty Web
    url: https://example.com/news
    type: web
    source_tier: B
    region: international
    enabled: true
""".strip(),
        encoding="utf-8",
    )
    raw_dir = tmp_path / "raw"
    history_dir = tmp_path / "history"
    fake_settings = SimpleNamespace(
        sources_config=sources_config,
        raw_dir=raw_dir,
        history_dir=history_dir,
    )
    monkeypatch.setattr(ingest_module, "settings", fake_settings)
    monkeypatch.setattr(ingest_module, "ensure_dirs", lambda: None)
    monkeypatch.setattr(
        ingest_module,
        "_collect_rss_entries",
        lambda source: [
            {
                "title": "FAA issues safety directive",
                "url": "https://example.com/a",
                "canonical_url": "https://example.com/a",
                "raw_text": "FAA issues safety directive for Boeing aircraft",
                "published_at": "2026-04-26T00:00:00+00:00",
                "lang": "en",
                "publisher_domain": "example.com",
                "is_google_redirect": False,
                "image_url": "",
            }
        ],
    )

    def fake_web_entries(source):
        source["_last_error"] = "http_404"
        return []

    monkeypatch.setattr(ingest_module, "_collect_web_entries", fake_web_entries)

    ingest_module.run("2026-04-26")

    health = json.loads((raw_dir / "source_health_2026-04-26.json").read_text(encoding="utf-8"))
    assert health[0]["source_id"] == "demo_rss"
    assert health[0]["status"] == "ok"
    assert health[0]["item_count"] == 1
    assert health[1]["source_id"] == "empty_web"
    assert health[1]["status"] == "failed"
    assert health[1]["error"] == "http_404"

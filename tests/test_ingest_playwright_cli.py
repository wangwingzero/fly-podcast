import json
import importlib


ingest = importlib.import_module("flying_podcast.stages.ingest")


def test_collect_playwright_cli_entries_normalizes_list_and_article_text(monkeypatch):
    calls = []

    def fake_cli(args, *, session, timeout, use_xvfb):
        calls.append((args, session, timeout, use_xvfb))
        if args == ["open"]:
            return ""
        if args[:1] == ["run-code"] and "page.route" in args[1]:
            return ""
        if args[:1] == ["run-code"] and "domcontentloaded" in args[1] and "https://example.com/aviation" in args[1]:
            return ""
        if args[:2] == ["--raw", "eval"] and "articleText" in args[2]:
            return "Airbus has delayed several A350 deliveries after a supplier disruption affected production."
        if args[:2] == ["--raw", "eval"] and "articleImage" in args[2]:
            return "https://example.com/images/a350-supplier-delay.jpg"
        if args[:2] == ["--raw", "eval"] and "querySelectorAll" in args[2]:
            return json.dumps(
                [
                    {
                        "title": "Airbus delays A350 deliveries after supplier issue",
                        "url": "https://example.com/aviation/a350-delay",
                        "summary": "Supplier disruption affects planned deliveries.",
                        "published_at": "2026-05-16T01:00:00Z",
                        "image_url": "https://example.com/images/list-card.jpg",
                    }
                ]
            )
        if args[:1] == ["run-code"] and "domcontentloaded" in args[1] and "https://example.com/aviation/a350-delay" in args[1]:
            return ""
        if args == ["close"]:
            return ""
        raise AssertionError(f"unexpected playwright-cli call: {args}")

    monkeypatch.setattr(ingest, "_run_playwright_cli", fake_cli)

    rows = ingest._collect_playwright_cli_entries(
        {
            "id": "flightglobal_air_transport_cli",
            "url": "https://example.com/aviation",
            "lang": "en",
            "max_items": 5,
            "playwright_timeout": 12,
            "xvfb": False,
        }
    )

    assert len(rows) == 1
    assert rows[0]["title"] == "Airbus delays A350 deliveries after supplier issue"
    assert rows[0]["canonical_url"] == "https://example.com/aviation/a350-delay"
    assert rows[0]["raw_text"].startswith("Airbus has delayed")
    assert rows[0]["crawl_method"] == "playwright_cli"
    assert rows[0]["article_text"].startswith("Airbus has delayed")
    assert rows[0]["image_url"] == "https://example.com/images/a350-supplier-delay.jpg"
    assert calls[0][1] == "flightglobal_air_transport_cli"
    assert calls[0][2] == 12
    assert calls[0][0] == ["open"]
    assert "page.route" in calls[1][0][1]
    assert "domcontentloaded" in calls[2][0][1]


def test_collect_playwright_cli_entries_records_error_on_bad_json(monkeypatch):
    def fake_cli(args, *, session, timeout, use_xvfb):
        if args == ["open"]:
            return ""
        if args[:1] == ["run-code"]:
            return ""
        if args[:2] == ["--raw", "eval"]:
            return "not json"
        return ""

    monkeypatch.setattr(ingest, "_run_playwright_cli", fake_cli)

    source = {
        "id": "bad_cli_source",
        "url": "https://example.com/aviation",
        "fetch_mode": "playwright_cli",
    }
    rows = ingest._collect_playwright_cli_entries(source)

    assert rows == []
    assert source["_last_error"] == "playwright_cli_bad_json"


def test_extract_image_url_skips_site_logo_in_rss_entry():
    image_url = ingest._extract_image_url(
        {
            "media_content": [
                {"url": "https://www.flightglobal.com/wp-content/uploads/2026/01/114818_fglogo_452564.jpg", "type": "image/jpeg"},
                {"url": "https://www.flightglobal.com/wp-content/uploads/2026/05/PD-8-c-United-Engine-480x320.jpeg", "type": "image/jpeg"},
            ]
        }
    )

    assert image_url.endswith("PD-8-c-United-Engine-480x320.jpeg")

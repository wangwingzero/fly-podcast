from pathlib import Path

import yaml


def _keywords_config():
    return yaml.safe_load(Path("config/keywords.yaml").read_text(encoding="utf-8"))


def _enabled_sources():
    data = yaml.safe_load(Path("config/sources.yaml").read_text(encoding="utf-8"))
    return [s for s in data["sources"] if s.get("enabled", True)]


def test_default_sources_include_fixed_international_industry_media():
    ids = {s["id"] for s in _enabled_sources()}

    # ainonline_rss disabled 2026-05-20 (upstream RSS endpoint removed in
    # ainonline.com Next.js migration). Keep AIN coverage indirectly via
    # aerotime/flightglobal and assert the replacement enthusiast/technical
    # feeds are live.
    assert "ainonline_rss" not in ids
    assert "aviation_business_news_rss" in ids
    assert "leeham_rss" in ids
    assert "avweb_rss" in ids
    assert "airdatanews_rss" in ids
    assert "verticalmag_rss" in ids
    assert "aviation_week_air_transport_cli" not in ids
    assert "flightglobal_air_transport_cli" not in ids
    assert "ain_online_web" not in ids
    assert "simple_flying_cli" not in ids
    assert "ch_aviation_cli" not in ids


def test_broken_feeds_remain_disabled():
    """Probe-confirmed broken upstream RSS endpoints must stay disabled."""
    ids = {s["id"] for s in _enabled_sources()}

    assert "ainonline_rss" not in ids  # /rss.xml 404 since site migration
    assert "flightradar24_blog" not in ids  # Cloudflare 403 to feed clients


def test_default_sources_do_not_enable_playwright_cli():
    enabled = _enabled_sources()
    playwright_count = sum(1 for s in enabled if s.get("fetch_mode") == "playwright_cli")

    assert playwright_count == 0


def test_enabled_primary_industry_sources_are_rank_allowed():
    kw = _keywords_config()
    allowed_ids = {str(x).strip() for x in kw.get("pilot_allowed_source_ids", []) if str(x).strip()}
    allowed_domains = {
        str(x).strip().lower()
        for x in kw.get("pilot_allowed_domains", [])
        if str(x).strip()
    }

    missing = []
    for source in _enabled_sources():
        if source.get("source_role") != "primary_industry":
            continue
        source_id = str(source.get("id") or "").strip()
        if source_id in allowed_ids:
            continue
        url = str(source.get("url") or "").strip().lower()
        if any(domain and domain in url for domain in allowed_domains):
            continue
        missing.append(source_id)

    assert missing == []


def test_enabled_ain_replacement_keeps_priority_weight():
    kw = _keywords_config()
    priority_ids = {str(x).strip() for x in kw.get("pilot_priority_sources", []) if str(x).strip()}

    # ainonline_rss is dormant (kept in priority list as a paper trail) but the
    # actual coverage burden has shifted to leeham_rss and avweb_rss — both must
    # carry priority weight so they survive ranking quotas.
    assert "leeham_rss" in priority_ids
    assert "avweb_rss" in priority_ids


def test_airline_and_oem_newsrooms_are_not_default_sources():
    enabled_ids = {s["id"] for s in _enabled_sources()}

    assert "airbus_newsroom_web" not in enabled_ids
    assert "boeing_newsroom_web" not in enabled_ids
    assert "delta_news" not in enabled_ids


def test_accident_sources_are_exception_sources_not_primary_industry():
    data = yaml.safe_load(Path("config/sources.yaml").read_text(encoding="utf-8"))
    by_id = {s["id"]: s for s in data["sources"]}

    assert by_id["avherald_web"]["source_role"] == "accident_exception"
    assert by_id["asn_2026_web"]["source_role"] == "accident_exception"
    assert by_id["flightglobal_safety"]["source_role"] == "accident_exception"

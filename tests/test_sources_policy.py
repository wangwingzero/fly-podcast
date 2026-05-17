from pathlib import Path

import yaml


def _enabled_sources():
    data = yaml.safe_load(Path("config/sources.yaml").read_text(encoding="utf-8"))
    return [s for s in data["sources"] if s.get("enabled", True)]


def test_default_sources_include_fixed_international_industry_media():
    ids = {s["id"] for s in _enabled_sources()}

    assert "flightglobal_air_transport_cli" in ids
    assert "ain_online_web" in ids
    assert "simple_flying_cli" in ids
    assert "ch_aviation_cli" in ids


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

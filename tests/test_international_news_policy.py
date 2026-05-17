import importlib


rank = importlib.import_module("flying_podcast.stages.rank")


def test_accident_exception_rejects_local_incident_without_major_impact():
    item = {
        "source_id": "avherald_web",
        "source_role": "accident_exception",
        "title": "Regional jet returns after smoke indication",
        "canonical_url": "https://avherald.com/h?article=local",
        "url": "https://avherald.com/h?article=local",
    }
    text = "A regional jet returned to the departure airport after smoke indication. The aircraft landed safely."

    ok, reason = rank._is_pilot_relevant(item, text, kw_cfg={})

    assert ok is False
    assert reason == "accident_without_major_impact"


def test_accident_exception_keeps_grounding_or_regulatory_impact():
    item = {
        "source_id": "avherald_web",
        "source_role": "accident_exception",
        "title": "Regulators order fleet-wide inspections after engine incident",
        "canonical_url": "https://avherald.com/h?article=global",
        "url": "https://avherald.com/h?article=global",
    }
    text = (
        "EASA and FAA ordered fleet-wide inspections after an engine incident, "
        "forcing several international airlines to ground affected aircraft."
    )

    ok, reason = rank._is_pilot_relevant(item, text, kw_cfg={})

    assert ok is True
    assert reason == "ok"


def test_primary_industry_fleet_story_is_relevant_without_safety_event():
    item = {
        "source_id": "flightglobal_air_transport_cli",
        "source_role": "primary_industry",
        "title": "Lufthansa firms A350 options as long-haul fleet plan shifts",
        "canonical_url": "https://www.flightglobal.com/air-transport/lufthansa-a350-fleet",
        "url": "https://www.flightglobal.com/air-transport/lufthansa-a350-fleet",
    }
    text = (
        "Lufthansa has firmed Airbus A350 options and adjusted long-haul fleet plans, "
        "with deliveries scheduled across its international network."
    )

    ok, reason = rank._is_pilot_relevant(item, text, kw_cfg={})

    assert ok is True
    assert reason == "ok_industry_news"


def test_macro_supplement_rejects_finance_story_without_explicit_aviation_effect():
    item = {
        "source_id": "reuters_aviation",
        "source_role": "macro_supplement",
        "title": "European stocks rise as travel shares gain",
        "canonical_url": "https://www.reuters.com/markets/stocks-travel",
        "url": "https://www.reuters.com/markets/stocks-travel",
    }
    text = "European stocks rose as travel and leisure shares gained on stronger investor sentiment."

    ok, reason = rank._is_pilot_relevant(item, text, kw_cfg={})

    assert ok is False
    assert reason == "macro_without_explicit_aviation_effect"

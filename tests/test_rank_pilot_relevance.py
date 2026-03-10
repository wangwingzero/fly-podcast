from flying_podcast.stages.rank import _is_pilot_relevant


def test_rejects_non_aviation_social_news():
    item = {
        "source_id": "google_intl_aviation",
        "canonical_url": "https://example.com/news/celebrity-travel-review",
        "url": "https://example.com/news/celebrity-travel-review",
    }
    text = "Celebrity spotted at luxury lounge during tourism trip, loyalty program review"
    ok, reason = _is_pilot_relevant(item, text, kw_cfg={})
    assert ok is False
    assert reason in {"hard_reject_keywords", "missing_pilot_signal"}


def test_accepts_faa_safety_news():
    item = {
        "source_id": "faa_newsroom_web",
        "canonical_url": "https://www.faa.gov/newsroom/faa-issues-safety-directive-2026",
        "url": "https://www.faa.gov/newsroom/faa-issues-safety-directive-2026",
    }
    text = "FAA issues emergency airworthiness directive for Boeing 737 MAX fleet inspection"
    ok, reason = _is_pilot_relevant(item, text, kw_cfg={})
    assert ok is True
    assert reason == "ok"


def test_rejects_route_network_story_without_direct_operational_impact():
    item = {
        "source_id": "aerotime",
        "canonical_url": "https://www.aerotime.aero/articles/lufthansa-787-kuala-lumpur-route",
        "url": "https://www.aerotime.aero/articles/lufthansa-787-kuala-lumpur-route",
    }
    text = "Lufthansa will deploy Boeing 787 on the Kuala Lumpur route with additional flights as part of its network expansion and summer airline schedule update"
    ok, reason = _is_pilot_relevant(item, text, kw_cfg={})
    assert ok is False
    assert reason == "background_only_story"


def test_accepts_route_story_when_direct_operational_impact_exists():
    item = {
        "source_id": "faa_newsroom_web",
        "canonical_url": "https://www.faa.gov/newsroom/route-notam-gps-interference",
        "url": "https://www.faa.gov/newsroom/route-notam-gps-interference",
    }
    text = "FAA issues new NOTAM after GPS interference on transatlantic route, with airspace restrictions and updated flight procedures"
    ok, reason = _is_pilot_relevant(item, text, kw_cfg={})
    assert ok is True
    assert reason == "ok"


def test_rejects_airline_route_suspension_update_without_pilot_actionable_detail():
    item = {
        "source_id": "aviation_source",
        "canonical_url": "https://example.com/american-airlines-middle-east-operations-update",
        "url": "https://example.com/american-airlines-middle-east-operations-update",
    }
    text = (
        "American Airlines extends the suspension of Philadelphia-Doha service through May 7 "
        "and delays the return of New York JFK-Tel Aviv flights until April 23. "
        "The airline cited continued regional airspace disruption and customer rebooking flexibility."
    )
    ok, reason = _is_pilot_relevant(item, text, kw_cfg={})
    assert ok is False
    assert reason == "schedule_advisory_story"


def test_accepts_airspace_story_when_notam_and_reroute_detail_exist():
    item = {
        "source_id": "faa_newsroom_web",
        "canonical_url": "https://www.faa.gov/newsroom/middle-east-notam-reroute",
        "url": "https://www.faa.gov/newsroom/middle-east-notam-reroute",
    }
    text = (
        "FAA issues a NOTAM for Middle East airspace with mandatory reroute procedures, "
        "alternate airport planning guidance, and runway closure windows for affected operators."
    )
    ok, reason = _is_pilot_relevant(item, text, kw_cfg={})
    assert ok is True
    assert reason == "ok"

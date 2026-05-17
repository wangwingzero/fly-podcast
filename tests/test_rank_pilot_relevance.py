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


def test_strict_reject_blocks_podcast_even_with_aviation_signals():
    item = {
        "source_id": "flightradar24_blog",
        "canonical_url": "https://www.flightradar24.com/blog/avtalk-podcast/avtalk-367/",
        "url": "https://www.flightradar24.com/blog/avtalk-podcast/avtalk-367/",
    }
    text = "AvTalk podcast episode discusses airline operations, flight safety, TCAS and summer traffic"
    ok, reason = _is_pilot_relevant(
        item,
        text,
        kw_cfg={"strict_hard_reject_keywords": ["podcast", "episode"]},
    )
    assert ok is False
    assert reason == "strict_hard_reject_keywords"


# ── Novelty / 趣闻类放行 ─────────────────────────────────────────────


def test_accepts_new_aircraft_maiden_flight_as_novelty():
    item = {
        "source_id": "flightglobal_general",
        "canonical_url": "https://www.flightglobal.com/airframers/boeing/777x/",
        "url": "https://www.flightglobal.com/airframers/boeing/777x/",
    }
    text = (
        "Boeing 777X completes maiden flight at Paine Field. "
        "The prototype aircraft conducted certification flight test profiles before "
        "returning to Everett."
    )
    ok, reason = _is_pilot_relevant(item, text, kw_cfg={})
    assert ok is True
    assert reason in {"ok", "ok_novelty"}


def test_accepts_pilot_human_interest_story_as_novelty():
    item = {
        "source_id": "simpleflying_general",
        "canonical_url": "https://example.com/news/captain-retires-after-35-years",
        "url": "https://example.com/news/captain-retires-after-35-years",
    }
    text = (
        "Captain retires after 35 years as a Boeing 747 pilot with United, "
        "the veteran pilot completed his final flight to San Francisco with the original crew."
    )
    ok, reason = _is_pilot_relevant(item, text, kw_cfg={})
    assert ok is True
    assert reason in {"ok", "ok_novelty"}


def test_accepts_retro_livery_anniversary_flight_as_novelty():
    item = {
        "source_id": "ainonline_general",
        "canonical_url": "https://example.com/news/lufthansa-retro-livery-65-years",
        "url": "https://example.com/news/lufthansa-retro-livery-65-years",
    }
    text = (
        "Lufthansa unveils retro livery on a Boeing 747 to celebrate the 65th anniversary; "
        "the aircraft will operate a commemorative flight from Frankfurt."
    )
    ok, reason = _is_pilot_relevant(item, text, kw_cfg={})
    assert ok is True
    assert reason in {"ok", "ok_novelty"}


# ── Novelty 路径明确拒绝的话题（用户不想要的范围） ─────────────────


def test_rejects_evtol_test_flight_in_novelty_excluded_topic():
    item = {
        "source_id": "ainonline_general",
        "canonical_url": "https://example.com/news/joby-evtol-test-flight",
        "url": "https://example.com/news/joby-evtol-test-flight",
    }
    text = (
        "Joby Aviation completes another eVTOL test flight in California, "
        "advancing toward urban air mobility air taxi service."
    )
    ok, reason = _is_pilot_relevant(item, text, kw_cfg={})
    assert ok is False
    assert reason == "novelty_excluded_topic"


def test_rejects_supersonic_airliner_milestone_in_novelty_excluded_topic():
    item = {
        "source_id": "flightglobal_general",
        "canonical_url": "https://example.com/news/boom-overture-rollout",
        "url": "https://example.com/news/boom-overture-rollout",
    }
    text = (
        "Boom Overture supersonic airliner rolls out for first flight test campaign at "
        "the company's Greensboro facility."
    )
    ok, reason = _is_pilot_relevant(item, text, kw_cfg={})
    assert ok is False
    assert reason == "novelty_excluded_topic"


def test_rejects_pure_route_marketing_even_when_first_flight_phrase_appears():
    """新航线宣传用了 'first flight' 字样不应被识别为趣闻——只有 1 条 novelty 命中时仍当 background_only 处理。"""
    item = {
        "source_id": "aerotime",
        "canonical_url": "https://example.com/lufthansa-jfk-first-flight",
        "url": "https://example.com/lufthansa-jfk-first-flight",
    }
    text = (
        "Lufthansa launches a new route to JFK with first flight celebrations, "
        "deploying additional flights and expanding network capacity for the summer schedule."
    )
    ok, reason = _is_pilot_relevant(item, text, kw_cfg={})
    assert ok is False
    assert reason == "background_only_story"

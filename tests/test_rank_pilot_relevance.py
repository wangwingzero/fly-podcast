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

import importlib
import json
from pathlib import Path
from types import SimpleNamespace

from flying_podcast.core.io_utils import load_yaml


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


def test_mainland_subject_rejects_caac_title():
    item = {
        "title": "CAAC orders review of airline safety reporting",
        "source_id": "reuters_aviation",
        "source_name": "Reuters",
        "region": "international",
    }

    assert rank._looks_like_mainland_china_aviation_subject(item) is True


def test_mainland_subject_rejects_mainland_carrier_network_story():
    item = {
        "title": "China Southern adjusts domestic network for summer season",
        "source_id": "flightglobal_air_transport_cli",
        "source_name": "FlightGlobal",
        "region": "international",
    }

    assert rank._looks_like_mainland_china_aviation_subject(item) is True


def test_mainland_subject_keeps_international_story_with_china_context():
    item = {
        "title": "Boeing says Chinese market recovery supports widebody aircraft demand",
        "source_id": "reuters_aviation",
        "source_name": "Reuters",
        "region": "international",
    }

    assert rank._looks_like_mainland_china_aviation_subject(item) is False


def test_mainland_subject_keeps_taiwan_china_airlines_story():
    item = {
        "title": "China Airlines adds Rome route with A350 service",
        "source_id": "flightglobal_air_transport_cli",
        "source_name": "FlightGlobal",
        "region": "international",
    }

    assert rank._looks_like_mainland_china_aviation_subject(item) is False


def test_run_filters_mainland_subject_story_before_ranking(monkeypatch, tmp_path):
    raw_dir = tmp_path / "raw"
    processed_dir = tmp_path / "processed"
    raw_dir.mkdir()
    processed_dir.mkdir()
    keywords_path = tmp_path / "keywords.yaml"
    keywords_path.write_text(
        "relevance_keywords:\n"
        "  - aviation\n"
        "  - airline\n"
        "  - aircraft\n"
        "blocked_domains: []\n"
        "sensitive_keywords: []\n"
        "sensational_words: []\n",
        encoding="utf-8",
    )
    (raw_dir / "2026-05-22.json").write_text(
        json.dumps(
            [
                {
                    "id": "drop-caac",
                    "title": "CAAC orders review of airline safety reporting",
                    "raw_text": "CAAC orders airlines to review safety reporting procedures.",
                    "published_at": "2026-05-22T08:00:00+00:00",
                    "url": "https://www.reuters.com/world/china/caac-review",
                    "canonical_url": "https://www.reuters.com/world/china/caac-review",
                    "source_id": "reuters_aviation",
                    "source_name": "Reuters",
                    "source_tier": "A",
                    "region": "international",
                },
                {
                    "id": "keep-boeing",
                    "title": "Lufthansa airline says Chinese market recovery supports A350 aircraft fleet plan",
                    "raw_text": "Lufthansa said Chinese market recovery supports its A350 aircraft fleet plan, with airline deliveries scheduled across the international network.",
                    "published_at": "2026-05-22T09:00:00+00:00",
                    "url": "https://www.flightglobal.com/air-transport/lufthansa-a350-demand",
                    "canonical_url": "https://www.flightglobal.com/air-transport/lufthansa-a350-demand",
                    "source_id": "flightglobal_air_transport_cli",
                    "source_name": "FlightGlobal",
                    "source_role": "primary_industry",
                    "source_tier": "A",
                    "region": "international",
                },
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        rank,
        "settings",
        SimpleNamespace(
            raw_dir=raw_dir,
            processed_dir=processed_dir,
            keywords_config=keywords_path,
            max_article_age_hours=72,
            max_tier_a_article_age_hours=72,
            min_rank_score_for_compose=0,
            target_article_count=0,
            domestic_ratio=0.0,
            max_entries_per_source=0,
            min_tier_a_ratio=0.0,
            min_novelty_articles=0,
        ),
    )

    out = rank.run("2026-05-22")
    payload = json.loads(out.read_text(encoding="utf-8"))

    assert [row["id"] for row in payload["articles"]] == ["keep-boeing"]


def test_military_subject_rejects_fighter_story():
    item = {
        "title": "USAF grounds T-38 military aircraft fleet after trainer jet crash in Alabama",
        "source_id": "airdatanews_rss",
        "source_name": "AirData News",
        "region": "international",
    }

    assert rank._is_pilot_relevant(item, item["title"], kw_cfg={}) == (False, "strict_hard_reject_keywords")


def test_military_subject_rejects_combat_drone_story():
    item = {
        "title": "Northrop combat drone military aircraft moves closer to first flight",
        "source_id": "airdatanews_rss",
        "source_name": "AirData News",
        "region": "international",
    }

    assert rank._is_pilot_relevant(item, item["title"], kw_cfg={}) == (False, "strict_hard_reject_keywords")


def test_soft_content_rejects_video_interview_story():
    item = {
        "title": "Video interview: Todd Jensen & Victor Lopez, Aeras Aviation",
        "source_id": "aviation_business_news_rss",
        "source_name": "Aviation Business News",
        "region": "international",
    }

    assert rank._is_pilot_relevant(item, item["title"], kw_cfg={}) == (False, "hard_reject_keywords")


def test_soft_content_rejects_press_release_partnership_story():
    item = {
        "title": "Press release announces partnership for promotional campaign demo flight",
        "source_id": "runway_girl_rss",
        "source_name": "Runway Girl Network",
        "region": "international",
    }

    assert rank._is_pilot_relevant(item, item["title"], kw_cfg={}) == (False, "strict_hard_reject_keywords")


def test_generic_airport_infrastructure_story_is_rejected():
    item = {
        "title": "Oklahoma approves five-year airport construction plan",
        "source_id": "avweb_rss",
        "source_name": "AVweb",
        "region": "international",
    }

    assert rank._is_pilot_relevant(item, item["title"], kw_cfg={}) == (False, "hard_reject_keywords")


def test_generic_enterprise_engineering_story_is_rejected():
    item = {
        "title": "Emirates breaks ground on $5.1 billion engineering complex at Dubai South",
        "source_id": "airdatanews_rss",
        "source_name": "AirData News",
        "region": "international",
    }

    assert rank._is_pilot_relevant(item, item["title"], kw_cfg={}) == (False, "hard_reject_keywords")


def test_operations_base_story_is_rejected():
    item = {
        "title": "flynas opens fifth operations base at Abha International Airport",
        "source_id": "aviation_source",
        "source_name": "Aviation Source",
        "region": "international",
    }

    assert rank._is_pilot_relevant(item, item["title"], kw_cfg={}) == (False, "hard_reject_keywords")


def test_paperless_flight_deck_story_is_kept():
    item = {
        "title": "Philippine Airlines and PAL Express adopt paperless flight deck operations",
        "source_id": "aviation_business_news_rss",
        "source_name": "Aviation Business News",
        "region": "international",
        "source_role": "primary_industry",
        "canonical_url": "https://example.com/paperless-flight-deck",
        "url": "https://example.com/paperless-flight-deck",
    }

    ok, reason = rank._is_pilot_relevant(item, item["title"], kw_cfg={})

    assert ok is True
    assert reason in {"ok_industry_news", "ok"}


def test_mainland_subject_rejects_actual_china_order_title():
    item = {
        "title": "China confirms order for 200 Boeing aircraft following Trump-Xi summit",
        "source_id": "flightglobal_news_web",
        "source_name": "FlightGlobal Air Transport",
        "region": "international",
    }

    assert rank._looks_like_mainland_china_aviation_subject(item) is True


def test_military_subject_rejects_actual_t38_story():
    item = {
        "title": "USAF grounds T-38 fleet after trainer jet crash in Alabama",
        "source_id": "airdatanews_rss",
        "source_name": "AirData News",
        "region": "international",
    }

    assert rank._is_pilot_relevant(item, item["title"], kw_cfg={}) == (False, "strict_hard_reject_keywords")


def test_military_subject_rejects_actual_spy_plane_story():
    item = {
        "title": "UK releases video of dangerous Russian interception of RAF spy plane over Black Sea",
        "source_id": "airdatanews_rss",
        "source_name": "AirData News",
        "region": "international",
    }

    assert rank._is_pilot_relevant(item, item["title"], kw_cfg={}) == (False, "strict_hard_reject_keywords")


def test_military_subject_rejects_mrtt_conversion_story():
    item = {
        "title": "Airbus to open second A330 MRTT conversion center in Spain",
        "source_id": "airdatanews_rss",
        "source_name": "AirData News",
        "region": "international",
    }

    assert rank._is_pilot_relevant(item, item["title"], kw_cfg={}) == (False, "strict_hard_reject_keywords")


def test_config_keywords_reject_real_server_leak_titles(monkeypatch, tmp_path):
    keywords_path = tmp_path / "keywords.yaml"
    keywords_path.write_text(Path(r"D:/飞行播客公众号/config/keywords.yaml").read_text(encoding="utf-8"), encoding="utf-8")
    kw = load_yaml(keywords_path)

    cases = [
        (
            "China confirms Boeing order for 200 aircraft after Trump-Xi summit",
            False,
            "hard_reject_keywords",
        ),
        (
            "China confirms order for 200 Boeing aircraft following Trump-Xi summit",
            False,
            "hard_reject_keywords",
        ),
        (
            "USAF grounds T-38 fleet after trainer jet crash in Alabama",
            False,
            "strict_hard_reject_keywords",
        ),
        (
            "UK releases video of dangerous Russian interception of RAF spy plane over Black Sea",
            False,
            "strict_hard_reject_keywords",
        ),
        (
            "Northrop's YFQ-48-A autonomous combat drone moves closer to first flight",
            False,
            "strict_hard_reject_keywords",
        ),
        (
            "Philippine Airlines and PAL Express adopt paperless flight deck operations",
            True,
            None,
        ),
    ]

    for title, expected_ok, expected_reason in cases:
        item = {
            "title": title,
            "source_id": "demo",
            "source_name": "demo",
            "region": "international",
            "source_role": "primary_industry",
            "canonical_url": "https://example.com/demo",
            "url": "https://example.com/demo",
        }
        ok, reason = rank._is_pilot_relevant(item, title, kw_cfg=kw)
        assert ok is expected_ok, title
        if expected_reason is not None:
            assert reason == expected_reason, title

import importlib
from types import SimpleNamespace

from flying_podcast.stages.rank import _looks_like_google_redirect, _max_age_for_item


rank = importlib.import_module("flying_podcast.stages.rank")


def test_google_redirect_link_detected():
    u = "https://news.google.com/rss/articles/CBMiWkFVX3lxTE9pTUNT?oc=5"
    assert _looks_like_google_redirect(u) is True


def test_non_google_link_not_detected():
    u = "https://www.caacnews.com.cn/1/2/202602/t20260216_12345.html"
    assert _looks_like_google_redirect(u) is False


def test_tier_a_uses_same_freshness_window_by_default(monkeypatch):
    monkeypatch.setattr(
        rank,
        "settings",
        SimpleNamespace(max_article_age_hours=48, max_tier_a_article_age_hours=48),
    )

    assert _max_age_for_item({"source_tier": "A"}) == 48


def test_tier_a_can_only_be_relaxed_when_configured(monkeypatch):
    monkeypatch.setattr(
        rank,
        "settings",
        SimpleNamespace(max_article_age_hours=48, max_tier_a_article_age_hours=72),
    )

    assert _max_age_for_item({"source_tier": "A"}) == 72


def test_dedupe_ranked_events_merges_duplicate_image_by_url():
    url = "https://www.flightglobal.com/archive/2026/05/il-114-300-flown-close-to-north-pole-to-test-arctic-navigation-capabilities/"

    out = rank._dedupe_ranked_events(
        [
            {
                "id": "playwright-entry",
                "title": "Il-114-300 flown close to North Pole",
                "canonical_url": url,
                "event_fingerprint": "fp-playwright",
                "image_url": "",
            },
            {
                "id": "rss-entry",
                "title": "Il-114-300 flown close to North Pole to test Arctic navigation capabilities",
                "canonical_url": url,
                "event_fingerprint": "fp-rss",
                "image_url": "https://www.flightglobal.com/wp-content/uploads/2026/05/Il-114-300-Arctic-c-United-Aircraft-480x324.jpeg",
            },
        ]
    )

    assert len(out) == 1
    assert out[0]["image_url"].endswith("Il-114-300-Arctic-c-United-Aircraft-480x324.jpeg")


def test_merge_raw_images_by_url_runs_before_relevance_filtering():
    url = "https://www.flightglobal.com/archive/2026/05/united-engine-wraps-up-certification-tests-for-pd-8-engine/"
    rows = [
        {
            "id": "playwright-entry",
            "canonical_url": url,
            "url": url,
            "image_url": "",
        },
        {
            "id": "rss-entry",
            "canonical_url": url,
            "url": url,
            "image_url": "https://www.flightglobal.com/wp-content/uploads/2026/05/PD-8-c-United-Engine-480x320.jpeg",
        },
    ]

    rank._merge_raw_images_by_url(rows)

    assert rows[0]["image_url"].endswith("PD-8-c-United-Engine-480x320.jpeg")

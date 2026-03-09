import importlib

compose = importlib.import_module("flying_podcast.stages.compose")


def test_resolve_google_candidate_urls_updates_canonical_url(monkeypatch):
    candidate = {
        "id": "id-1",
        "canonical_url": "https://news.google.com/rss/articles/abc",
        "url": "https://news.google.com/rss/articles/abc",
        "publisher_domain": "news.google.com",
        "is_google_redirect": True,
    }

    monkeypatch.setattr(
        compose,
        "_resolve_google_url_requests",
        lambda gurl, timeout=10: "https://example.com/story",
    )

    compose._resolve_google_candidate_urls([candidate])

    assert candidate["canonical_url"] == "https://example.com/story"
    assert candidate["url"] == "https://example.com/story"
    assert candidate["publisher_domain"] == "example.com"
    assert candidate["is_google_redirect"] is False

from flying_podcast.core.scoring import recency_score, tier_score, weighted_quality


def test_tier_score():
    assert tier_score("A") == 100.0
    assert tier_score("B") == 80.0
    assert tier_score("C") == 60.0


def test_weighted_quality():
    total = weighted_quality(95, 90, 100, 90, 95)
    assert total >= 90


def test_recency_score_invalid_time():
    assert recency_score("invalid") == 40.0

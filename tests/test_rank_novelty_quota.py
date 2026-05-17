"""Tests for the novelty quota logic in rank stage.

确保即使候选池里事故稿压过 novelty 评分时，配额逻辑能从 backfill_pool
（通常是 deduped）中至少补足 N 篇 industry_novelty 进入最终 ranked 池。
"""
from flying_podcast.stages.rank import _ensure_novelty_quota


def _row(rid: str, score: float, category: str, source_id: str = "test_src",
         priority: bool = False) -> dict:
    return {
        "id": rid,
        "rank_score": score,
        "source_id": source_id,
        "pilot_value": {"category": category, "priority_source": priority},
    }


def test_quota_returns_unchanged_when_min_zero():
    candidates = [_row("a1", 90, "safety_event")]
    out, applied = _ensure_novelty_quota(
        candidates, candidates, min_novelty=0, max_per_source=0,
    )
    assert applied is False
    assert [r["id"] for r in out] == ["a1"]


def test_quota_skips_when_already_enough_novelty():
    candidates = [
        _row("nov1", 85, "industry_novelty"),
        _row("a1", 90, "safety_event"),
    ]
    out, applied = _ensure_novelty_quota(
        candidates, candidates, min_novelty=1, max_per_source=0,
    )
    assert applied is False
    assert {r["id"] for r in out} == {"nov1", "a1"}


def test_quota_returns_unchanged_when_no_novelty_in_pool():
    candidates = [_row("a1", 100, "safety_event"), _row("a2", 90, "safety_event")]
    backfill = list(candidates)  # 没 novelty
    out, applied = _ensure_novelty_quota(
        candidates, backfill, min_novelty=1, max_per_source=0,
    )
    assert applied is False
    assert [r["id"] for r in out] == ["a1", "a2"]


def test_quota_replaces_lowest_when_protected_top_skipped():
    """配额逻辑应替换分数最低的非 novelty 项，而非保护 top-N 内的项。"""
    candidates = [
        _row("a_top1", 100, "safety_event", priority=True),
        _row("a_top2", 99,  "safety_event", priority=True),
        _row("a_top3", 98,  "safety_event", priority=True),
        _row("a_top4", 95,  "safety_event", priority=True),
        _row("a_top5", 92,  "safety_event", priority=True),
        _row("a_low",  85,  "safety_event", priority=True),  # ← 应被替换
    ]
    novelty = _row("nov1", 70, "industry_novelty", source_id="aerotime")
    backfill = candidates + [novelty]

    out, applied = _ensure_novelty_quota(
        candidates, backfill, min_novelty=1, max_per_source=0,
        protected_top_count=5,
    )
    assert applied is True
    ids = [r["id"] for r in out]
    assert "nov1" in ids
    assert "a_low" not in ids
    # top 5 都不被动
    for prot in ("a_top1", "a_top2", "a_top3", "a_top4", "a_top5"):
        assert prot in ids


def test_quota_respects_novelty_min_score_threshold():
    """低于 novelty_min_score 的候选应被忽略（避免凑低质量稿）。"""
    candidates = [
        _row("a1", 100, "safety_event"),
        _row("a2", 95,  "safety_event"),
        _row("a3", 90,  "safety_event"),
        _row("a4", 88,  "safety_event"),
        _row("a5", 85,  "safety_event"),
        _row("a6", 80,  "safety_event"),
    ]
    too_low_novelty = _row("nov_low", 30, "industry_novelty")
    backfill = candidates + [too_low_novelty]

    out, applied = _ensure_novelty_quota(
        candidates, backfill, min_novelty=1, max_per_source=0,
        novelty_min_score=60.0,
    )
    assert applied is False
    assert "nov_low" not in [r["id"] for r in out]


def test_quota_respects_source_cap():
    """source cap 在配额替换时应被尊重，避免某源超额。"""
    candidates = [
        _row("a1", 100, "safety_event", source_id="src_a"),
        _row("a2", 95,  "safety_event", source_id="src_a"),
        _row("a3", 90,  "safety_event", source_id="src_b"),
        _row("a4", 88,  "safety_event", source_id="src_b"),
        _row("a5", 85,  "safety_event", source_id="src_c"),
        _row("a_low", 80, "safety_event", source_id="src_d"),
    ]
    novelty_same_src = _row("nov1", 70, "industry_novelty", source_id="src_b")
    backfill = candidates + [novelty_same_src]

    # max_per_source=2: src_b 已满，无法再加 nov1
    out, applied = _ensure_novelty_quota(
        candidates, backfill, min_novelty=1, max_per_source=2,
    )
    # nov1 与 a_low 不同源，但 nov1 加入会让 src_b 变成 3 (a3+a4+nov1)
    # 所以替换应被跳过
    assert applied is False
    assert "nov1" not in [r["id"] for r in out]


def test_quota_can_add_multiple_novelty_when_pool_has_enough():
    candidates = [
        _row("a1", 100, "safety_event"),
        _row("a2", 95,  "safety_event"),
        _row("a3", 90,  "safety_event"),
        _row("a4", 88,  "safety_event"),
        _row("a5", 85,  "safety_event"),
        _row("a_low1", 80, "safety_event"),
        _row("a_low2", 78, "safety_event"),
    ]
    nov1 = _row("nov1", 75, "industry_novelty", source_id="aerotime")
    nov2 = _row("nov2", 72, "industry_novelty", source_id="airbus")
    backfill = candidates + [nov1, nov2]

    out, applied = _ensure_novelty_quota(
        candidates, backfill, min_novelty=2, max_per_source=0,
    )
    assert applied is True
    ids = [r["id"] for r in out]
    assert "nov1" in ids and "nov2" in ids
    # 末端两条最低分应被换掉
    assert "a_low1" not in ids
    assert "a_low2" not in ids


def test_quota_picks_highest_score_novelty_first():
    candidates = [
        _row("a1", 100, "safety_event"),
        _row("a2", 90,  "safety_event"),
        _row("a3", 85,  "safety_event"),
        _row("a4", 82,  "safety_event"),
        _row("a5", 80,  "safety_event"),
        _row("a_low", 78, "safety_event"),
    ]
    nov_low = _row("nov_low", 65, "industry_novelty", source_id="aerotime")
    nov_high = _row("nov_high", 75, "industry_novelty", source_id="airbus")
    backfill = candidates + [nov_low, nov_high]

    out, applied = _ensure_novelty_quota(
        candidates, backfill, min_novelty=1, max_per_source=0,
    )
    assert applied is True
    ids = [r["id"] for r in out]
    assert "nov_high" in ids
    assert "nov_low" not in ids

from flying_podcast.stages.compose import _pick_final_entries


def _item(idx: int, region: str):
    return {
        "id": str(idx),
        "region": region,
        "section": "",
    }


def test_pick_returns_requested_total():
    candidates = [_item(i, "international") for i in range(20)]
    result = _pick_final_entries(candidates, total=10, domestic_ratio=0.0)
    assert len(result) == 10


def test_pick_returns_all_when_fewer_than_total():
    candidates = [_item(i, "international") for i in range(5)]
    result = _pick_final_entries(candidates, total=10, domestic_ratio=0.0)
    assert len(result) == 5


def test_pick_preserves_order():
    candidates = [_item(i, "international") for i in range(20)]
    result = _pick_final_entries(candidates, total=10, domestic_ratio=0.0)
    assert [x["id"] for x in result] == [str(i) for i in range(10)]

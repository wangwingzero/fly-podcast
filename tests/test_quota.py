from flying_podcast.stages.compose import _pick_final_entries


def _item(idx: int, region: str):
    return {
        "id": str(idx),
        "region": region,
        "section": "",
    }


def test_quota_distribution_10_items():
    candidates = [_item(i, "domestic") for i in range(10)] + [_item(i + 100, "international") for i in range(10)]
    result = _pick_final_entries(candidates, total=10, domestic_ratio=0.6)
    domestic = sum(1 for x in result if x["region"] == "domestic")
    intl = len(result) - domestic
    assert domestic == 6
    assert intl == 4

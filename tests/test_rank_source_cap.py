from flying_podcast.stages.rank import _enforce_source_cap


def _row(i: int, source_id: str, section: str, region: str):
    return {
        "id": f"id-{i}",
        "source_id": source_id,
        "source_name": source_id,
        "section": section,
        "region": region,
    }


def test_enforce_source_cap_reduces_dominant_source():
    selected = [
        _row(1, "s1", "运行与安全", "domestic"),
        _row(2, "s1", "机队与制造商", "domestic"),
        _row(3, "s1", "监管与行业政策", "domestic"),
        _row(4, "s1", "航司经营与网络", "domestic"),
    ]
    pool = selected + [
        _row(5, "s2", "航司经营与网络", "domestic"),
        _row(6, "s3", "运行与安全", "domestic"),
    ]
    out, applied = _enforce_source_cap(selected, pool, max_per_source=3)
    assert applied is True
    assert sum(1 for x in out if x["source_id"] == "s1") <= 3

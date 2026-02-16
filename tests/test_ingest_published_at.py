from flying_podcast.stages.ingest import _extract_published_at_for_web


def test_extract_published_at_from_caacnews_url():
    source = {"published_at_patterns": []}
    url = "http://www.caacnews.com.cn/1/2/202602/t20260214_1393398.html"
    ts = _extract_published_at_for_web(source, url, "民航新闻")
    assert ts.startswith("2026-02-14")


def test_extract_published_at_from_text_date():
    source = {"published_at_patterns": []}
    url = "https://example.com/news/article"
    ts = _extract_published_at_for_web(source, url, "发布时间 2026-02-11 航空动态")
    assert ts.startswith("2026-02-11")

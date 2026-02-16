from flying_podcast.stages.rank import _looks_like_google_redirect


def test_google_redirect_link_detected():
    u = "https://news.google.com/rss/articles/CBMiWkFVX3lxTE9pTUNT?oc=5"
    assert _looks_like_google_redirect(u) is True


def test_non_google_link_not_detected():
    u = "https://www.caacnews.com.cn/1/2/202602/t20260216_12345.html"
    assert _looks_like_google_redirect(u) is False

from flying_podcast.stages.rank import _is_pilot_relevant


def test_rejects_non_aviation_social_news():
    item = {
        "source_id": "36kr_aviation",
        "canonical_url": "https://36kr.com/p/3683968039497353?f=rss",
        "url": "https://36kr.com/p/3683968039497353?f=rss",
    }
    text = "人类首遭AI网暴社死 OpenClaw改代码遭拒 春晚 网红"
    ok, reason = _is_pilot_relevant(item, text, kw_cfg={})
    assert ok is False
    assert reason in {"hard_reject_keywords", "missing_pilot_signal"}


def test_accepts_caac_safety_news():
    item = {
        "source_id": "caacnews_web_list",
        "canonical_url": "http://www.caacnews.com.cn/1/2/202602/t20260214_1393398.html",
        "url": "http://www.caacnews.com.cn/1/2/202602/t20260214_1393398.html",
    }
    text = "民航江苏监管局召开辖区通航安全警示工作会 运行安全 民航局"
    ok, reason = _is_pilot_relevant(item, text, kw_cfg={})
    assert ok is True
    assert reason == "ok"

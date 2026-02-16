from pathlib import Path

import pytest

from flying_podcast.stages.web_parser_registry import parse_web_source_entries

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "web"


@pytest.mark.parametrize(
    ("source_id", "list_url", "fixture_name", "expected_domain"),
    [
        ("carnoc_web_list", "https://www.carnoc.com/", "carnoc_web_list.html", "news.carnoc.com"),
        ("caacnews_web_list", "http://www.caacnews.com.cn/1/2/index.html", "caacnews_web_list.html", "caacnews.com.cn"),
        ("caac_gov_web_mhyw", "http://www.caac.gov.cn/XWZX/MHYW/", "caac_gov_web_mhyw.html", "caac.gov.cn"),
        ("iata_press_web", "https://www.iata.org/en/pressroom/", "iata_press_web.html", "iata.org"),
        ("faa_newsroom_web", "https://www.faa.gov/newsroom", "faa_newsroom_web.html", "faa.gov"),
        ("airbus_newsroom_web", "https://www.airbus.com/en/newsroom", "airbus_newsroom_web.html", "airbus.com"),
        ("boeing_newsroom_web", "https://boeing.mediaroom.com/news-releases-statements", "boeing_newsroom_web.html", "boeing.mediaroom.com"),
        ("flightglobal_news_web", "https://www.flightglobal.com/news", "flightglobal_news_web.html", "flightglobal.com"),
    ],
)
def test_registry_parses_enabled_web_sources(source_id: str, list_url: str, fixture_name: str, expected_domain: str):
    html_text = (FIXTURE_DIR / fixture_name).read_text(encoding="utf-8")
    rows = parse_web_source_entries(source_id, list_url, html_text)
    assert rows
    assert expected_domain in rows[0].url
    assert rows[0].title


def test_registry_filters_invalid_entries():
    html_text = """
    <html><body>
      <a href="javascript:void(0)">read more</a>
      <a href="/x">short</a>
    </body></html>
    """
    rows = parse_web_source_entries("iata_press_web", "https://www.iata.org/en/pressroom/", html_text)
    assert rows == []


@pytest.mark.parametrize(
    ("source_id", "list_url", "fixture_name"),
    [
        ("carnoc_web_list", "https://www.carnoc.com/", "carnoc_web_list.html"),
        ("caac_gov_web_mhyw", "http://www.caac.gov.cn/XWZX/MHYW/", "caac_gov_web_mhyw.html"),
    ],
)
def test_registry_extracts_published_hint(source_id: str, list_url: str, fixture_name: str):
    html_text = (FIXTURE_DIR / fixture_name).read_text(encoding="utf-8")
    rows = parse_web_source_entries(source_id, list_url, html_text)
    assert rows
    assert rows[0].published_hint

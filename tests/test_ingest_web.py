from flying_podcast.stages.ingest import _AnchorParser


def test_anchor_parser_extracts_links_and_text():
    html = """
    <html><body>
      <a href="/news/1">  航空公司新增国际航线  </a>
      <a href="https://example.com/about">关于我们</a>
    </body></html>
    """
    p = _AnchorParser()
    p.feed(html)
    assert len(p.links) == 2
    assert p.links[0][0] == "/news/1"
    assert "新增国际航线" in p.links[0][1]

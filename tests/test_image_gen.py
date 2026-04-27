from flying_podcast.core.image_gen import _call_grok_api


def test_grok_image_generation_falls_back_to_square_size(monkeypatch):
    posted_sizes = []

    class FakePostResponse:
        text = ""

        def __init__(self, status_code, payload):
            self.status_code = status_code
            self.ok = status_code < 400
            self._payload = payload
            self.text = str(payload)

        def raise_for_status(self):
            if not self.ok:
                raise RuntimeError(f"http_{self.status_code}")

        def json(self):
            return self._payload

    class FakeGetResponse:
        content = b"fake-jpeg-bytes"

        def raise_for_status(self):
            return None

    def fake_post(url, headers, json, timeout):
        posted_sizes.append(json["size"])
        if json["size"] == "1792x1024":
            return FakePostResponse(500, {"error": "unsupported size"})
        return FakePostResponse(200, {"data": [{"url": "https://example.com/image.jpg"}]})

    monkeypatch.setattr("flying_podcast.core.image_gen.requests.post", fake_post)
    monkeypatch.setattr("flying_podcast.core.image_gen.requests.get", lambda url, timeout: FakeGetResponse())

    data = _call_grok_api(
        "https://grok.223344567.xyz",
        "key",
        "grok-imagine-image-lite",
        "aviation cover",
        size="1792x1024",
    )

    assert data == b"fake-jpeg-bytes"
    assert posted_sizes == ["1792x1024", "1792x1024", "1024x1024"]

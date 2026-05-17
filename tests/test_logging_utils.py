import importlib


logging_utils = importlib.import_module("flying_podcast.core.logging_utils")


def test_configure_stdout_encoding_uses_utf8_backslashreplace(monkeypatch):
    calls = []

    class FakeStdout:
        def reconfigure(self, **kwargs):
            calls.append(kwargs)

    monkeypatch.setattr(logging_utils, "_STDOUT_CONFIGURED", False)
    monkeypatch.setattr(logging_utils.sys, "stdout", FakeStdout())

    logging_utils._configure_stdout_encoding()

    assert calls == [{"encoding": "utf-8", "errors": "backslashreplace"}]

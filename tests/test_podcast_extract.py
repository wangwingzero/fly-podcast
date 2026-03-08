from pathlib import Path
from types import SimpleNamespace

from flying_podcast.stages import podcast


def test_extract_pdf_text_prefers_mineru_and_saves_markdown(tmp_path, monkeypatch):
    pdf_path = tmp_path / "sample.pdf"
    pdf_path.write_bytes(b"%PDF-1.4 fake")
    work_dir = tmp_path / "out"
    work_dir.mkdir()

    monkeypatch.setattr(podcast, "settings", SimpleNamespace(mineru_token="token"))
    monkeypatch.setattr(
        podcast,
        "_extract_pdf_text_mineru",
        lambda pdf, work_dir=None: "第一章\n条文内容",
    )
    monkeypatch.setattr(
        podcast,
        "_extract_pdf_text_local",
        lambda pdf: "local text",
    )

    text = podcast.extract_pdf_text(pdf_path, work_dir=work_dir)
    assert text == "第一章\n条文内容"


def test_extract_pdf_text_falls_back_to_local_when_mineru_fails(tmp_path, monkeypatch):
    pdf_path = tmp_path / "sample.pdf"
    pdf_path.write_bytes(b"%PDF-1.4 fake")

    monkeypatch.setattr(podcast, "settings", SimpleNamespace(mineru_token="token"))

    def _boom(pdf, work_dir=None):
        raise RuntimeError("mineru failed")

    monkeypatch.setattr(podcast, "_extract_pdf_text_mineru", _boom)
    monkeypatch.setattr(
        podcast,
        "_extract_pdf_text_local",
        lambda pdf: "local fallback text",
    )

    text = podcast.extract_pdf_text(pdf_path)
    assert text == "local fallback text"

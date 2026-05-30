import shutil
import uuid
from pathlib import Path
from types import SimpleNamespace

from flying_podcast.core import tts_client


def test_looks_like_mp3_detects_id3_and_frame_sync() -> None:
    assert tts_client._looks_like_mp3(b"ID3" + b"\x00" * 8)
    assert tts_client._looks_like_mp3(b"\xff\xf3" + b"\x00" * 8)
    assert not tts_client._looks_like_mp3(b"RIFF" + b"\x00" * 8)


def test_response_bytes_to_mp3_passes_through_mp3() -> None:
    mp3 = b"ID3" + b"\x00" * 120
    assert tts_client._response_bytes_to_mp3(mp3, from_wav_trim=True) == mp3


def test_qwen_wav_to_mp3_can_trim_leading_artifact(monkeypatch) -> None:
    captured_cmd: list[str] = []

    class FakeResult:
        returncode = 0
        stderr = b""
        stdout = b"mp3"

    def fake_run(cmd: list[str], **kwargs) -> FakeResult:
        captured_cmd.extend(cmd)
        assert kwargs["input"] == b"wav"
        return FakeResult()

    monkeypatch.setattr(tts_client.subprocess, "run", fake_run)

    result = tts_client._wav_to_mp3(b"wav", trim_start_seconds=0.12)

    assert result == b"mp3"
    audio_filter = captured_cmd[captured_cmd.index("-af") + 1]
    assert "atrim=start=0.120" in audio_filter
    assert "asetpts=PTS-STARTPTS" in audio_filter
    assert "afade=t=in:st=0:d=0.025" in audio_filter


def test_concatenate_simple_applies_boundary_fades(monkeypatch) -> None:
    captured_cmd: list[str] = []

    def fake_duration(path: Path) -> float:
        return 1.25

    class FakeResult:
        returncode = 0
        stderr = ""

    def fake_run(cmd: list[str], **kwargs) -> FakeResult:
        captured_cmd.extend(cmd)
        Path(cmd[-1]).write_bytes(b"mp3")
        return FakeResult()

    monkeypatch.setattr(tts_client, "_get_duration", fake_duration)
    monkeypatch.setattr(tts_client.subprocess, "run", fake_run)

    run_dir = Path("tmp") / f"test_tts_audio_cleanup_{uuid.uuid4().hex}"
    run_dir.mkdir(parents=True, exist_ok=False)
    try:
        first = run_dir / "first.mp3"
        second = run_dir / "second.mp3"
        first.write_bytes(b"first")
        second.write_bytes(b"second")

        tts_client._concatenate_simple([first, second], run_dir / "combined.mp3")

        filter_str = captured_cmd[captured_cmd.index("-filter_complex") + 1]
        assert filter_str.count("afade=t=in") == 2
        assert filter_str.count("afade=t=out") == 2
        assert "concat=n=2:v=0:a=1[raw]" in filter_str
    finally:
        shutil.rmtree(run_dir, ignore_errors=True)


def _fake_tts_settings(**overrides: object) -> SimpleNamespace:
    base = dict(
        qwen_tts_primary_url="https://tts.hudawang.cn",
        qwen_tts_fallback_url="http://186.244.244.142:8825",
        qwen_tts_prefer_cloud_voices=False,
        tts_qwen_local_voice_female="serena",
        tts_qwen_local_voice_male="aiden",
        tts_qwen_cloud_voice_female="cherry",
        tts_qwen_cloud_voice_male="ethan",
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def test_qwen_speech_endpoints_prefers_cloud_when_configured(monkeypatch) -> None:
    fake_settings = _fake_tts_settings(qwen_tts_prefer_cloud_voices=True)
    monkeypatch.setattr(tts_client, "settings", fake_settings)

    labels = [row[0] for row in tts_client._qwen_speech_endpoints()]
    voices = [row[2]["千羽"] for row in tts_client._qwen_speech_endpoints()]

    assert labels == ["qwen-cloud", "qwen-local"]
    assert voices == ["cherry", "serena"]


def test_qwen_speech_endpoints_defaults_to_local_first(monkeypatch) -> None:
    fake_settings = _fake_tts_settings()
    monkeypatch.setattr(tts_client, "settings", fake_settings)

    labels = [row[0] for row in tts_client._qwen_speech_endpoints()]
    assert labels == ["qwen-local", "qwen-cloud"]

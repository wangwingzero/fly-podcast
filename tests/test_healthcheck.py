from __future__ import annotations

import json

from flying_podcast.stages import healthcheck


def test_healthcheck_mask_secret() -> None:
    assert healthcheck._mask_secret("") == "(empty)"
    assert healthcheck._mask_secret("abcdef") == "ab***"
    assert healthcheck._mask_secret("sk-1234567890") == "sk-123...7890"


def test_healthcheck_json_output_and_exit_code(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        healthcheck,
        "_llm_check",
        lambda *args, **kwargs: healthcheck.CheckResult(
            name=args[0],
            ok=True,
            required=kwargs["required"],
            detail="ok",
            latency_s=1.23,
            meta={"model": "x"},
        ),
    )
    monkeypatch.setattr(
        healthcheck,
        "_image_check",
        lambda *args, **kwargs: healthcheck.CheckResult(
            name=args[0],
            ok=False if args[0] == "backup_image_gen" else True,
            required=kwargs["required"],
            detail="bytes=0" if args[0] == "backup_image_gen" else "bytes=10",
            latency_s=2.34,
            meta={"model": "y"},
        ),
    )

    code = healthcheck.run("2026-03-10", json_output=True)
    out = capsys.readouterr().out
    payload = json.loads(out)

    assert code == 0
    assert len(payload) == 4
    assert payload[-2]["name"] == "primary_image_gen"
    assert payload[-2]["required"] is False
    assert payload[-1]["name"] == "backup_image_gen"
    assert payload[-1]["required"] is False
    assert payload[-1]["ok"] is False


def test_healthcheck_allows_main_llm_failure_when_backup_llm_is_healthy(monkeypatch, capsys) -> None:
    def fake_llm_check(name, *args, **kwargs):
        return healthcheck.CheckResult(
            name=name,
            ok=name != "main_llm",
            required=kwargs["required"],
            detail="ok" if name != "main_llm" else "timeout",
            latency_s=1.0,
            meta={"model": name},
        )

    monkeypatch.setattr(healthcheck, "_llm_check", fake_llm_check)
    monkeypatch.setattr(
        healthcheck,
        "_image_check",
        lambda *args, **kwargs: healthcheck.CheckResult(
            name=args[0],
            ok=True,
            required=kwargs["required"],
            detail="bytes=10",
            latency_s=1.0,
            meta={"model": "img"},
        ),
    )

    code = healthcheck.run("2026-03-10", json_output=True)
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload[0]["name"] == "main_llm"
    assert payload[0]["ok"] is False
    assert payload[1]["name"] == "backup_llm"
    assert payload[1]["ok"] is True

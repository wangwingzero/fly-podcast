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
    assert payload[-1]["name"] == "backup_image_gen"
    assert payload[-1]["required"] is False
    assert payload[-1]["ok"] is False

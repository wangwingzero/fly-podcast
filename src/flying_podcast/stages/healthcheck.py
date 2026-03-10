from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from typing import Any

from flying_podcast.core.config import settings
from flying_podcast.core.image_gen import _call_gemini_api, _call_grok_api
from flying_podcast.core.llm_client import OpenAICompatibleClient


SAFE_SYSTEM_PROMPT = (
    "You are a concise aviation news assistant. "
    "Reply with one neutral sentence in plain text."
)

SAFE_USER_PROMPT = (
    "Please write one short, factual summary sentence about an airport improving "
    "terminal operations, passenger flow, and baggage handling efficiency."
)


def _mask_secret(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return "(empty)"
    if len(value) <= 10:
        return value[:2] + "***"
    return value[:6] + "..." + value[-4:]


@dataclass
class CheckResult:
    name: str
    ok: bool
    required: bool
    detail: str
    latency_s: float
    meta: dict[str, Any]


def _llm_check(name: str, api_key: str, base_url: str, model: str, *, required: bool) -> CheckResult:
    started = time.monotonic()
    meta = {
        "model": model,
        "base_url": base_url,
        "api_key": _mask_secret(api_key),
    }
    try:
        client = OpenAICompatibleClient(api_key=api_key, base_url=base_url, model=model)
        text = client.complete_text(
            system_prompt=SAFE_SYSTEM_PROMPT,
            user_prompt=SAFE_USER_PROMPT,
            max_tokens=80,
            temperature=0,
            retries=1,
            timeout=25,
            _allow_backup=False,
        )
        meta["anthropic_mode"] = client._is_anthropic
        return CheckResult(
            name=name,
            ok=True,
            required=required,
            detail=text[:160],
            latency_s=round(time.monotonic() - started, 2),
            meta=meta,
        )
    except Exception as exc:  # noqa: BLE001
        return CheckResult(
            name=name,
            ok=False,
            required=required,
            detail=f"{type(exc).__name__}: {str(exc)[:220]}",
            latency_s=round(time.monotonic() - started, 2),
            meta=meta,
        )


def _image_check(name: str, *, required: bool, provider: str) -> CheckResult:
    started = time.monotonic()
    if provider == "primary":
        api_key = settings.image_gen_api_key
        base_url = settings.image_gen_base_url
        model = settings.image_gen_model
        caller = _call_gemini_api
    else:
        api_key = settings.image_gen_backup_api_key
        base_url = settings.image_gen_backup_base_url
        model = settings.image_gen_backup_model
        caller = _call_grok_api

    meta = {
        "model": model,
        "base_url": base_url,
        "api_key": _mask_secret(api_key),
    }
    try:
        data = caller(
            base_url,
            api_key,
            model,
            "Create a clean editorial aviation illustration with an airport terminal and passenger flow scene.",
        )
        size = len(data) if data else 0
        return CheckResult(
            name=name,
            ok=bool(size),
            required=required,
            detail=f"bytes={size}" if size else "No image payload returned",
            latency_s=round(time.monotonic() - started, 2),
            meta=meta,
        )
    except Exception as exc:  # noqa: BLE001
        return CheckResult(
            name=name,
            ok=False,
            required=required,
            detail=f"{type(exc).__name__}: {str(exc)[:220]}",
            latency_s=round(time.monotonic() - started, 2),
            meta=meta,
        )


def run(_: str, *, json_output: bool = False) -> int:
    checks = [
        _llm_check("main_llm", settings.llm_api_key, settings.llm_base_url, settings.llm_model, required=True),
        _llm_check(
            "backup_llm",
            settings.llm_backup_api_key,
            settings.llm_backup_base_url,
            settings.llm_backup_model,
            required=True,
        ),
        _image_check("primary_image_gen", required=True, provider="primary"),
        _image_check("backup_image_gen", required=False, provider="backup"),
    ]

    if json_output:
        print(json.dumps([asdict(item) for item in checks], ensure_ascii=False, indent=2))
    else:
        for item in checks:
            level = "PASS" if item.ok else ("FAIL" if item.required else "WARN")
            print(f"[{level}] {item.name} ({item.latency_s:.2f}s) {item.detail}")

    return 0 if all(item.ok or not item.required for item in checks) else 1

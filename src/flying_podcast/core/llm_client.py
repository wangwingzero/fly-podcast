from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

import requests

from flying_podcast.core.config import settings


class LLMError(RuntimeError):
    pass


@dataclass
class LLMResponse:
    payload: dict[str, Any]
    raw_text: str


class OpenAICompatibleClient:
    def __init__(self, api_key: str, base_url: str, model: str) -> None:
        self.api_key = api_key.strip()
        self.base_url = base_url.strip()
        self.model = model.strip()

    @staticmethod
    def is_configured() -> bool:
        return bool(settings.llm_api_key and settings.llm_base_url and settings.llm_model)

    def _chat_url(self) -> str:
        base = self.base_url.rstrip("/")
        if base.endswith("/chat/completions"):
            return base
        return f"{base}/chat/completions"

    @staticmethod
    def _extract_json_object(text: str) -> dict[str, Any]:
        text = (text or "").strip()
        if not text:
            raise LLMError("llm_empty_content")
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass

        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            snippet = text[start : end + 1]
            parsed = json.loads(snippet)
            if isinstance(parsed, dict):
                return parsed
        raise LLMError("llm_non_object_json")

    def _request_once(self, headers: dict[str, str], body: dict[str, Any], timeout: int) -> tuple[dict[str, Any], str]:
        resp = requests.post(self._chat_url(), headers=headers, json=body, timeout=timeout)
        if not resp.ok:
            raise LLMError(f"llm_http_{resp.status_code}: {resp.text[:500]}")
        data = resp.json()
        choices = data.get("choices") or []
        if not choices:
            raise LLMError("llm_empty_choices")
        msg = choices[0].get("message") or {}
        content = msg.get("content")

        # Providers in OpenAI-compatible mode may return:
        # 1) string content
        # 2) list content blocks: [{"type":"text","text":"..."}]
        # 3) dict payload with text field
        if isinstance(content, list):
            text_parts: list[str] = []
            for block in content:
                if isinstance(block, str):
                    text_parts.append(block)
                elif isinstance(block, dict):
                    if isinstance(block.get("text"), str):
                        text_parts.append(block["text"])
                    elif block.get("type") == "text" and isinstance(block.get("content"), str):
                        text_parts.append(block["content"])
            content = "\n".join([x for x in text_parts if x.strip()])
        elif isinstance(content, dict):
            content = content.get("text") or content.get("content") or ""
        elif content is None:
            # Some providers put plain text at choice.text (legacy style).
            content = choices[0].get("text") or ""

        if not str(content).strip():
            raise LLMError("llm_empty_content")
        return self._extract_json_object(str(content)), str(content)

    def complete_json(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int = 2400,
        temperature: float = 0.2,
        retries: int = 3,
        timeout: int = 45,
    ) -> LLMResponse:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        base_body = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        last_error = "unknown"
        for attempt in range(1, retries + 1):
            try:
                body = dict(base_body)
                body["response_format"] = {"type": "json_object"}
                try:
                    parsed, content = self._request_once(headers, body, timeout)
                    return LLMResponse(payload=parsed, raw_text=content)
                except LLMError:
                    # Some providers don't support response_format in OpenAI-compatible mode.
                    fallback = dict(base_body)
                    fallback["messages"] = [
                        {"role": "system", "content": system_prompt + "\n请仅输出JSON对象，不要Markdown。"},
                        {"role": "user", "content": user_prompt},
                    ]
                    parsed, content = self._request_once(headers, fallback, timeout)
                    return LLMResponse(payload=parsed, raw_text=content)
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                if attempt < retries:
                    time.sleep(min(2**attempt, 6))
        raise LLMError(last_error)

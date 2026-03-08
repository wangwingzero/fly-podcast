from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

import requests

from flying_podcast.core.config import settings
from flying_podcast.core.logging_utils import get_logger

_log = get_logger("llm")


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
        self._is_anthropic = self._detect_anthropic()

    def _detect_anthropic(self) -> bool:
        """Auto-detect Anthropic native API by key prefix, URL pattern, or model name.

        If the base URL explicitly contains '/chat/completions', always use
        OpenAI-compatible mode regardless of key prefix — the user intentionally
        chose an OpenAI-compatible endpoint.
        """
        base = self.base_url.lower()
        # Explicit OpenAI-compatible path → never use Anthropic format
        if "/chat/completions" in base:
            return False
        if self.api_key.startswith("sk-ant-"):
            return True
        if "/messages" in base or "anthropic" in base:
            return True
        # Claude models accessed via proxy (e.g. code.newcli.com/claude/aws)
        model = self.model.lower()
        if model.startswith("claude") or "/claude" in base:
            return True
        return False

    @staticmethod
    def is_configured() -> bool:
        return bool(settings.llm_api_key and settings.llm_base_url and settings.llm_model)

    @staticmethod
    def backup_configured() -> bool:
        return bool(settings.llm_backup_api_key and settings.llm_backup_base_url and settings.llm_backup_model)

    def _chat_url(self) -> str:
        base = self.base_url.rstrip("/")
        if self._is_anthropic:
            if base.endswith("/messages"):
                return base
            if base.endswith("/v1"):
                return f"{base}/messages"
            return f"{base}/v1/messages"
        if base.endswith("/chat/completions"):
            return base
        if base.endswith("/v1"):
            return f"{base}/chat/completions"
        return f"{base}/v1/chat/completions"

    def _chat_urls(self) -> list[str]:
        base = self.base_url.rstrip("/")
        if self._is_anthropic:
            return [self._chat_url()]
        if base.endswith("/chat/completions"):
            return [base]
        if base.endswith("/v1"):
            return [f"{base}/chat/completions"]
        return [f"{base}/v1/chat/completions", f"{base}/chat/completions"]

    def _responses_urls(self) -> list[str]:
        base = self.base_url.rstrip("/")
        if self._is_anthropic or "/chat/completions" in base.lower():
            return []
        if base.endswith("/responses"):
            return [base]
        if base.endswith("/v1"):
            return [f"{base}/responses"]
        return [f"{base}/v1/responses", f"{base}/responses"]

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

    @staticmethod
    def _extract_response_text(data: dict[str, Any]) -> str:
        output_text = data.get("output_text")
        if isinstance(output_text, str) and output_text.strip():
            return output_text.strip()

        output = data.get("output") or []
        text_parts: list[str] = []
        for item in output:
            if not isinstance(item, dict):
                continue
            content = item.get("content") or []
            if not isinstance(content, list):
                continue
            for block in content:
                if not isinstance(block, dict):
                    continue
                if block.get("type") == "output_text" and isinstance(block.get("text"), str):
                    text_parts.append(block["text"])
                elif isinstance(block.get("content"), str):
                    text_parts.append(block["content"])
        text = "\n".join(x for x in text_parts if x.strip()).strip()
        if text:
            return text
        raise LLMError("llm_empty_content")

    def _request_once_openai(self, url: str, headers: dict[str, str], body: dict[str, Any], timeout: int) -> tuple[dict[str, Any], str]:
        resp = requests.post(url, headers=headers, json=body, timeout=(10, timeout))
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

    def _request_once_responses(self, url: str, headers: dict[str, str], body: dict[str, Any], timeout: int) -> tuple[dict[str, Any], str]:
        resp = requests.post(url, headers=headers, json=body, timeout=(10, timeout))
        if not resp.ok:
            raise LLMError(f"llm_http_{resp.status_code}: {resp.text[:500]}")
        data = resp.json()
        return data, self._extract_response_text(data)

    def _request_once_anthropic(self, system_prompt: str, user_prompt: str, max_tokens: int, temperature: float, timeout: int) -> tuple[dict[str, Any], str]:
        """Send request using Anthropic native Messages API format."""
        headers = {
            "x-api-key": self.api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        }
        body: dict[str, Any] = {
            "model": self.model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "messages": [{"role": "user", "content": user_prompt}],
        }
        if system_prompt:
            body["system"] = system_prompt

        resp = requests.post(self._chat_url(), headers=headers, json=body, timeout=(10, timeout))
        if not resp.ok:
            raise LLMError(f"llm_http_{resp.status_code}: {resp.text[:500]}")
        data = resp.json()

        # Anthropic response: {"content": [{"type": "text", "text": "..."}], ...}
        content_blocks = data.get("content") or []
        text_parts: list[str] = []
        for block in content_blocks:
            if isinstance(block, dict) and block.get("type") == "text":
                text_parts.append(block.get("text", ""))
            elif isinstance(block, str):
                text_parts.append(block)
        content = "\n".join([x for x in text_parts if x.strip()])

        if not content.strip():
            raise LLMError("llm_empty_content")
        return self._extract_json_object(content), content

    def complete_json(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int = 2400,
        temperature: float = 0.2,
        retries: int = 5,
        timeout: int = 45,
        _allow_backup: bool = True,
    ) -> LLMResponse:
        prompt_chars = len(system_prompt) + len(user_prompt)
        _log.info("LLM 请求: model=%s, max_tokens=%d, prompt=%d 字符, timeout=%ds",
                 self.model, max_tokens, prompt_chars, timeout)
        last_error = "unknown"
        for attempt in range(1, retries + 1):
            try:
                t0 = time.monotonic()
                if self._is_anthropic:
                    # Anthropic native Messages API
                    prompt = system_prompt + "\n请仅输出JSON对象，不要Markdown。"
                    parsed, content = self._request_once_anthropic(
                        prompt, user_prompt, max_tokens, temperature, timeout,
                    )
                    elapsed = time.monotonic() - t0
                    _log.info("LLM 响应: %.1f 秒, %d 字符", elapsed, len(content))
                    return LLMResponse(payload=parsed, raw_text=content)
                else:
                    # OpenAI-compatible API, prefer Responses and fall back to chat completions.
                    headers = {
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json",
                    }
                    json_instruction = system_prompt
                    if "JSON" not in json_instruction and "json" not in json_instruction.lower():
                        json_instruction += "\n请仅输出JSON对象，不要Markdown。"

                    response_body = {
                        "model": self.model,
                        "instructions": json_instruction,
                        "input": user_prompt,
                        "text": {"format": {"type": "json_object"}},
                        "max_output_tokens": max_tokens,
                        "temperature": temperature,
                    }
                    response_errors: list[str] = []
                    for url in self._responses_urls():
                        try:
                            data, content = self._request_once_responses(url, headers, response_body, timeout)
                            parsed = self._extract_json_object(content)
                            elapsed = time.monotonic() - t0
                            _log.info("LLM 响应 (responses): %.1f 秒, %d 字符", elapsed, len(content))
                            return LLMResponse(payload=parsed, raw_text=content)
                        except LLMError as exc:
                            response_errors.append(f"{url} -> {exc}")

                    base_body = {
                        "model": self.model,
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": user_prompt},
                        ],
                        "temperature": temperature,
                        "max_tokens": max_tokens,
                    }
                    body = dict(base_body)
                    body["response_format"] = {"type": "json_object"}
                    chat_errors: list[str] = []
                    for url in self._chat_urls():
                        try:
                            parsed, content = self._request_once_openai(url, headers, body, timeout)
                            elapsed = time.monotonic() - t0
                            _log.info("LLM 响应 (chat): %.1f 秒, %d 字符", elapsed, len(content))
                            return LLMResponse(payload=parsed, raw_text=content)
                        except LLMError:
                            fallback = dict(base_body)
                            fallback["messages"] = [
                                {"role": "system", "content": system_prompt + "\n请仅输出JSON对象，不要Markdown。"},
                                {"role": "user", "content": user_prompt},
                            ]
                            try:
                                parsed, content = self._request_once_openai(url, headers, fallback, timeout)
                                elapsed = time.monotonic() - t0
                                _log.info("LLM 响应 (chat fallback): %.1f 秒, %d 字符", elapsed, len(content))
                                return LLMResponse(payload=parsed, raw_text=content)
                            except LLMError as exc:
                                chat_errors.append(f"{url} -> {exc}")
                    if response_errors or chat_errors:
                        raise LLMError("; ".join(response_errors + chat_errors))
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                if attempt < retries:
                    # Longer backoff for server errors (5xx) — proxy may be overloaded
                    is_server_error = "http_5" in last_error
                    wait = min(30 * attempt, 120) if is_server_error else min(2**attempt, 6)
                    _log.warning("[LLM] attempt %d/%d failed: %s, retry in %ds",
                                attempt, retries, last_error[:120], wait)
                    time.sleep(wait)
        # Primary model exhausted all retries – try backup model if configured
        if _allow_backup and self.backup_configured():
            _log.warning("[LLM] 主模型 %s 全部重试失败，切换备用模型 %s",
                        self.model, settings.llm_backup_model)
            backup = OpenAICompatibleClient(
                api_key=settings.llm_backup_api_key,
                base_url=settings.llm_backup_base_url,
                model=settings.llm_backup_model,
            )
            return backup.complete_json(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                max_tokens=max_tokens,
                temperature=temperature,
                retries=retries,
                timeout=timeout,
                _allow_backup=False,
            )
        raise LLMError(last_error)

    def complete_text(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int = 2400,
        temperature: float = 0.2,
        retries: int = 5,
        timeout: int = 45,
        _allow_backup: bool = True,
    ) -> str:
        prompt_chars = len(system_prompt) + len(user_prompt)
        _log.info("LLM 文本请求: model=%s, max_tokens=%d, prompt=%d 字符, timeout=%ds",
                  self.model, max_tokens, prompt_chars, timeout)
        last_error = "unknown"
        for attempt in range(1, retries + 1):
            try:
                t0 = time.monotonic()
                if self._is_anthropic:
                    _, content = self._request_once_anthropic(
                        system_prompt, user_prompt, max_tokens, temperature, timeout,
                    )
                    elapsed = time.monotonic() - t0
                    _log.info("LLM 文本响应 (anthropic): %.1f 秒, %d 字符", elapsed, len(content))
                    return content

                headers = {
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                }
                response_body = {
                    "model": self.model,
                    "instructions": system_prompt,
                    "input": user_prompt,
                    "max_output_tokens": max_tokens,
                    "temperature": temperature,
                }
                response_errors: list[str] = []
                for url in self._responses_urls():
                    try:
                        _, content = self._request_once_responses(url, headers, response_body, timeout)
                        elapsed = time.monotonic() - t0
                        _log.info("LLM 文本响应 (responses): %.1f 秒, %d 字符", elapsed, len(content))
                        return content
                    except LLMError as exc:
                        response_errors.append(f"{url} -> {exc}")

                chat_body = {
                    "model": self.model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    "temperature": temperature,
                    "max_tokens": max_tokens,
                }
                chat_errors: list[str] = []
                for url in self._chat_urls():
                    try:
                        _, content = self._request_once_openai(url, headers, chat_body, timeout)
                        elapsed = time.monotonic() - t0
                        _log.info("LLM 文本响应 (chat): %.1f 秒, %d 字符", elapsed, len(content))
                        return content
                    except LLMError as exc:
                        chat_errors.append(f"{url} -> {exc}")
                if response_errors or chat_errors:
                    raise LLMError("; ".join(response_errors + chat_errors))
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                if attempt < retries:
                    is_server_error = "http_5" in last_error
                    wait = min(30 * attempt, 120) if is_server_error else min(2**attempt, 6)
                    _log.warning("[LLM] text attempt %d/%d failed: %s, retry in %ds",
                                 attempt, retries, last_error[:120], wait)
                    time.sleep(wait)
        if _allow_backup and self.backup_configured():
            _log.warning("[LLM] 主模型 %s 全部重试失败，切换备用模型 %s",
                         self.model, settings.llm_backup_model)
            backup = OpenAICompatibleClient(
                api_key=settings.llm_backup_api_key,
                base_url=settings.llm_backup_base_url,
                model=settings.llm_backup_model,
            )
            return backup.complete_text(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                max_tokens=max_tokens,
                temperature=temperature,
                retries=retries,
                timeout=timeout,
                _allow_backup=False,
            )
        raise LLMError(last_error)

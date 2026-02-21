from __future__ import annotations

import asyncio
import json
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any

import requests

import dashscope

from flying_podcast.core.config import settings
from flying_podcast.core.logging_utils import get_logger

logger = get_logger("tts")

# Suppress noisy dashscope logs
import logging
logging.getLogger("dashscope").setLevel(logging.WARNING)


class TTSError(RuntimeError):
    pass


# ── Voice presets ──────────────────────────────────────────────

# DashScope voices
VOICE_FEMALE = "Cherry"
VOICE_MALE = "Ethan"

INSTRUCTIONS_FEMALE = (
    "语速适中，语调亲切自然，像一位专业的播客女主持人在和朋友聊天。"
)
INSTRUCTIONS_MALE = (
    "语速沉稳，语调专业可靠，像一位资深机长在分享经验。"
)

VOICE_MAP = {
    "千羽": {"voice": VOICE_FEMALE, "instructions": INSTRUCTIONS_FEMALE},
    "虎机长": {"voice": VOICE_MALE, "instructions": INSTRUCTIONS_MALE},
    "女": {"voice": VOICE_FEMALE, "instructions": INSTRUCTIONS_FEMALE},
    "男": {"voice": VOICE_MALE, "instructions": INSTRUCTIONS_MALE},
}

# Qwen direct API voice mapping (Cherry/Ethan original voices)
QWEN_DIRECT_VOICE_MAP = {
    "女": "Cherry / 芊悦",
    "男": "Ethan / 晨煦",
    "千羽": "Cherry / 芊悦",
    "虎机长": "Ethan / 晨煦",
}

# Edge TTS voice mapping
EDGE_VOICE_MAP = {
    "女": "zh-CN-XiaoxiaoNeural",
    "男": "zh-CN-YunjianNeural",
    "千羽": "zh-CN-XiaoxiaoNeural",
    "虎机长": "zh-CN-YunjianNeural",
}

# ── TTS client ─────────────────────────────────────────────────

MODEL = "qwen3-tts-instruct-flash"
MAX_CHARS_PER_REQUEST = 2000


# ── WAV → MP3 conversion ──────────────────────────────────────

def _wav_to_mp3(wav_bytes: bytes) -> bytes:
    """Convert WAV audio bytes to MP3 using ffmpeg stdin/stdout pipe."""
    result = subprocess.run(
        ["ffmpeg", "-y", "-i", "pipe:0", "-b:a", "128k", "-f", "mp3", "pipe:1"],
        input=wav_bytes,
        capture_output=True,
    )
    if result.returncode != 0:
        raise TTSError(f"ffmpeg wav→mp3 failed: {result.stderr[-300:]}")
    return result.stdout


# ── Tier 1: Qwen direct API (free, Cherry/Ethan original) ────

_QWEN_GRADIO_BASE = "https://qwen-qwen3-tts-demo.ms.show/gradio_api"
_QWEN_GRADIO_HEADERS = {
    "accept": "*/*",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
    "content-type": "application/json",
    "sec-ch-ua": '"Chromium";v="144", "Google Chrome";v="144"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "x-studio-token": "",
    "Referer": "https://qwen-qwen3-tts-demo.ms.show/",
}


def _synthesize_via_qwen_direct(text: str, role: str) -> bytes:
    """Synthesize via Qwen Gradio API directly (no Gradio Client). Returns MP3."""
    voice = QWEN_DIRECT_VOICE_MAP.get(role, "Cherry / 芊悦")
    session_hash = uuid.uuid4().hex[:12]

    # Step 1: Initialize predict
    resp = requests.post(
        f"{_QWEN_GRADIO_BASE}/run/predict",
        headers=_QWEN_GRADIO_HEADERS,
        json={
            "data": [0],
            "event_data": None,
            "fn_index": 0,
            "trigger_id": 11,
            "dataType": ["dataset"],
            "session_hash": session_hash,
        },
        timeout=30,
    )
    resp.raise_for_status()

    # Step 2: Join queue
    resp = requests.post(
        f"{_QWEN_GRADIO_BASE}/queue/join",
        headers=_QWEN_GRADIO_HEADERS,
        json={
            "data": [text[:MAX_CHARS_PER_REQUEST], voice, "Auto / 自动"],
            "event_data": None,
            "fn_index": 1,
            "trigger_id": 7,
            "dataType": ["textbox", "dropdown", "dropdown"],
            "session_hash": session_hash,
        },
        timeout=30,
    )
    resp.raise_for_status()

    # Step 3: Stream queue data (SSE) and wait for audio URL
    sse_headers = {**_QWEN_GRADIO_HEADERS, "accept": "text/event-stream"}
    resp = requests.get(
        f"{_QWEN_GRADIO_BASE}/queue/data",
        headers=sse_headers,
        params={"session_hash": session_hash, "studio_token": ""},
        stream=True,
        timeout=180,
    )
    resp.raise_for_status()

    audio_url = None
    for line in resp.iter_lines():
        if not line:
            continue
        line_str = line.decode("utf-8")
        if not line_str.startswith("data: "):
            continue
        try:
            event = json.loads(line_str[6:])
        except json.JSONDecodeError:
            continue

        msg = event.get("msg")
        if msg == "estimation":
            rank = event.get("rank", 0)
            eta = event.get("rank_eta", 0)
            logger.debug("[Qwen] queue position %d, eta %.0fs", rank + 1, eta)
        elif msg == "process_starts":
            logger.debug("[Qwen] processing started")
        elif msg == "process_completed":
            output = event.get("output", {})
            data = output.get("data", [])
            if data:
                audio_url = data[0].get("url")
            break
        elif msg == "close_stream":
            break

    if not audio_url:
        raise TTSError("Qwen direct API returned no audio URL")

    # Step 4: Download audio (WAV) and convert to MP3
    audio_resp = requests.get(
        audio_url,
        headers={k: v for k, v in _QWEN_GRADIO_HEADERS.items()
                 if k != "content-type"},
        timeout=60,
    )
    audio_resp.raise_for_status()

    if len(audio_resp.content) < 100:
        raise TTSError(f"Qwen returned too little audio ({len(audio_resp.content)} bytes)")

    return _wav_to_mp3(audio_resp.content)


# ── Tier 2: Edge TTS (free, Microsoft Edge API) ──────────────

def _synthesize_via_edge_tts(text: str, role: str) -> bytes:
    """Synthesize via edge-tts library. Returns MP3 bytes."""
    import edge_tts

    voice = EDGE_VOICE_MAP.get(role, "zh-CN-XiaoxiaoNeural")
    communicate = edge_tts.Communicate(text[:MAX_CHARS_PER_REQUEST], voice=voice)

    # edge-tts is async; run in event loop
    mp3_chunks: list[bytes] = []

    async def _generate():
        async for chunk in communicate.stream():
            if chunk["type"] == "audio":
                mp3_chunks.append(chunk["data"])

    asyncio.run(_generate())

    if not mp3_chunks:
        raise TTSError("Edge TTS returned no audio data")

    mp3_data = b"".join(mp3_chunks)
    if len(mp3_data) < 100:
        raise TTSError(f"Edge TTS returned too little data ({len(mp3_data)} bytes)")

    return mp3_data


# ── Tier 3: DashScope (paid, Alibaba Cloud) ──────────────────

def _synthesize_via_dashscope(text: str, voice: str, instructions: str) -> bytes:
    """Synthesize via paid DashScope API. Returns MP3 bytes."""
    key = settings.dashscope_api_key
    if not key:
        raise TTSError("DASHSCOPE_API_KEY is not set")
    dashscope.base_http_api_url = "https://dashscope.aliyuncs.com/api/v1"

    response = dashscope.MultiModalConversation.call(
        model=MODEL,
        api_key=key,
        text=text[:MAX_CHARS_PER_REQUEST],
        voice=voice,
        instructions=instructions,
        optimize_instructions=True,
        stream=False,
    )

    if response.status_code != 200:
        raise TTSError(f"DashScope API error {response.status_code}: {response.message}")

    audio_url = response.output["audio"]["url"]
    if not audio_url:
        raise TTSError("DashScope returned empty audio URL")

    audio_resp = requests.get(audio_url, timeout=60)
    audio_resp.raise_for_status()
    return audio_resp.content


# ── Backend chain (session-level: entire dialogue uses ONE backend) ──

BACKEND_CHAIN = ["qwen", "edge", "dashscope"]


def _synthesize_one(
    text: str,
    voice: str,
    instructions: str,
    *,
    role: str,
    backend: str,
    retries: int = 2,
) -> bytes:
    """Synthesize a single segment using exactly ONE backend (with retries)."""
    for attempt in range(1, retries + 1):
        try:
            if backend == "qwen":
                return _synthesize_via_qwen_direct(text, role)
            elif backend == "edge":
                return _synthesize_via_edge_tts(text, role)
            elif backend == "dashscope":
                return _synthesize_via_dashscope(text, voice, instructions)
            else:
                raise TTSError(f"Unknown backend: {backend}")
        except Exception as exc:
            if attempt >= retries:
                raise TTSError(f"{backend} failed after {retries} attempts: {exc}") from exc
            wait = {"qwen": min(5 * attempt, 15), "dashscope": min(2 ** attempt, 8)}.get(backend, 2)
            logger.warning("[TTS] %s attempt %d/%d failed: %s, retry in %ds",
                           backend, attempt, retries, exc, wait)
            time.sleep(wait)
    raise TTSError(f"{backend} exhausted retries")  # unreachable


def synthesize_segment(
    text: str,
    voice: str,
    instructions: str,
    *,
    role: str = "女",
    retries: int = 2,
) -> bytes:
    """
    Synthesize a single text segment to MP3 bytes.

    Tries each backend in order: Qwen → Edge → DashScope.
    NOTE: For dialogue, use synthesize_dialogue() which locks the entire
    conversation to one backend to avoid mixing different voices.
    """
    errors: dict[str, str] = {}
    for backend in BACKEND_CHAIN:
        try:
            mp3 = _synthesize_one(text, voice, instructions, role=role,
                                  backend=backend, retries=retries)
            logger.info("[TTS] segment OK via %s", backend)
            return mp3
        except TTSError as exc:
            errors[backend] = str(exc)
            logger.warning("[TTS] %s failed: %s", backend, exc)
    summary = "; ".join(f"{k}: {v}" for k, v in errors.items())
    raise TTSError(f"All TTS backends failed. {summary}")


# ── Dialogue-level synthesis (session-locked backend) ─────────

def _synthesize_dialogue_with_backend(
    dialogue: list[dict[str, str]],
    output_dir: Path,
    backend: str,
    retries: int = 2,
) -> list[Path]:
    """Synthesize ALL segments using a single backend. Raises on any failure."""
    segment_files: list[Path] = []

    for i, line in enumerate(dialogue):
        role = line["role"]
        text = line["text"]
        preset = VOICE_MAP.get(role)
        if not preset:
            logger.warning("Unknown role '%s' at line %d, defaulting to female", role, i)
            preset = VOICE_MAP["女"]

        chunks = _split_text(text, MAX_CHARS_PER_REQUEST)

        for j, chunk in enumerate(chunks):
            suffix = f"_{j}" if len(chunks) > 1 else ""
            seg_path = output_dir / f"seg_{i:03d}{suffix}.mp3"

            if seg_path.exists():
                logger.debug("Segment already exists: %s", seg_path.name)
                segment_files.append(seg_path)
                continue

            logger.info("TTS [%s][%s] seg %d%s: %s...", backend, role, i, suffix, chunk[:30])
            audio_bytes = _synthesize_one(
                chunk, preset["voice"], preset["instructions"],
                role=role, backend=backend, retries=retries,
            )

            with open(seg_path, "wb") as f:
                f.write(audio_bytes)
            segment_files.append(seg_path)

            time.sleep(0.5)

    return segment_files


def synthesize_dialogue(
    dialogue: list[dict[str, str]],
    output_dir: Path,
) -> list[Path]:
    """
    Synthesize a list of dialogue lines to individual mp3 files.

    IMPORTANT: The entire dialogue is locked to ONE TTS backend to ensure
    voice consistency. If a backend fails mid-dialogue, all partial segments
    are deleted and the entire dialogue is retried with the next backend.

    Fallback chain: Qwen (Cherry/Ethan) → Edge (晓晓/云健) → DashScope (paid)
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    errors: dict[str, str] = {}

    for backend in BACKEND_CHAIN:
        logger.info("[TTS] Attempting full dialogue (%d lines) with '%s'",
                     len(dialogue), backend)
        try:
            files = _synthesize_dialogue_with_backend(dialogue, output_dir, backend)
            logger.info("[TTS] Dialogue complete via '%s' (%d segments)", backend, len(files))
            return files
        except TTSError as exc:
            errors[backend] = str(exc)
            logger.warning("[TTS] Backend '%s' failed mid-dialogue: %s", backend, exc)
            # Clean ALL partial segments — mixing voices is unacceptable
            cleaned = 0
            for f in output_dir.glob("seg_*.mp3"):
                f.unlink()
                cleaned += 1
            if cleaned:
                logger.info("[TTS] Deleted %d partial segments from '%s' to avoid voice mixing",
                            cleaned, backend)

    summary = "; ".join(f"{k}: {v}" for k, v in errors.items())
    raise TTSError(f"All TTS backends failed for dialogue. {summary}")


def concatenate_audio(segment_files: list[Path], output_path: Path) -> Path:
    """Concatenate mp3 segments into a single mp3 file using ffmpeg."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Build ffmpeg filter to concat with silence gaps
    inputs: list[str] = []
    filter_parts: list[str] = []

    for i, seg_file in enumerate(segment_files):
        inputs.extend(["-i", str(seg_file)])
        filter_parts.append(f"[{i}:a]")

    # Concat then boost volume via loudnorm (EBU R128 broadcast standard)
    filter_str = (
        "".join(filter_parts)
        + f"concat=n={len(segment_files)}:v=0:a=1[raw];"
        + "[raw]loudnorm=I=-16:TP=-1.5:LRA=11[out]"
    )

    cmd = [
        "ffmpeg", "-y",
        *inputs,
        "-filter_complex", filter_str,
        "-map", "[out]",
        "-b:a", "128k",
        str(output_path),
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        # Extract actual error from stderr (skip ffmpeg banner)
        err_lines = [l for l in result.stderr.splitlines() if "Error" in l or "error" in l]
        err_msg = "\n".join(err_lines) if err_lines else result.stderr[-500:]
        raise TTSError(f"ffmpeg concat failed: {err_msg}")

    size_kb = output_path.stat().st_size / 1024
    logger.info("Combined audio: %s (%.1f KB)", output_path.name, size_kb)
    return output_path


def _split_text(text: str, max_len: int) -> list[str]:
    """Split text into chunks at sentence boundaries."""
    if len(text) <= max_len:
        return [text]

    chunks: list[str] = []
    current = ""
    # Split on Chinese sentence endings
    for char in text:
        current += char
        if char in "。！？；\n" and len(current) >= max_len * 0.3:
            chunks.append(current.strip())
            current = ""
    if current.strip():
        chunks.append(current.strip())
    return chunks or [text[:max_len]]

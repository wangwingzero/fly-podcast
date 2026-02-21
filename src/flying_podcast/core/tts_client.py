from __future__ import annotations

import subprocess
import time
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

# qwen-tts2api voice mapping (same voices as DashScope!)
QWEN_FREE_VOICE_MAP = {
    "女": "cherry",
    "男": "ethan",
    "千羽": "cherry",
    "虎机长": "ethan",
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


# ── Free TTS backend ──────────────────────────────────────────

def _synthesize_via_zai(text: str, role: str) -> bytes:
    """Synthesize via zai-tts2api (Zhipu GLM proxy). Returns MP3 bytes."""
    url = settings.zai_tts_url.rstrip("/") + "/v1/audio/speech"
    voice = QWEN_FREE_VOICE_MAP.get(role, "cherry")

    headers = {}
    if settings.zai_token:
        headers["Authorization"] = f"Bearer {settings.zai_token}"
    if settings.zai_userid:
        headers["X-User-Id"] = settings.zai_userid

    payload = {
        "input": text[:MAX_CHARS_PER_REQUEST],
        "voice": voice,
    }

    resp = requests.post(url, json=payload, headers=headers, timeout=120)
    resp.raise_for_status()

    if len(resp.content) < 100:
        raise TTSError(f"zai-tts2api returned too little data ({len(resp.content)} bytes)")

    return _wav_to_mp3(resp.content)


def _synthesize_via_qwen_free(text: str, role: str) -> bytes:
    """Synthesize via qwen-tts2api (Qwen Gradio demo proxy). Returns MP3 bytes."""
    url = settings.qwen_tts_url.rstrip("/") + "/v1/audio/speech"
    voice = QWEN_FREE_VOICE_MAP.get(role, "cherry")

    payload = {
        "input": text[:MAX_CHARS_PER_REQUEST],
        "voice": voice,
    }

    resp = requests.post(url, json=payload, timeout=120)
    resp.raise_for_status()

    if len(resp.content) < 100:
        raise TTSError(f"qwen-tts2api returned too little data ({len(resp.content)} bytes)")

    return _wav_to_mp3(resp.content)


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


# ── Main synthesis with 3-tier fallback ────────────────────────

def synthesize_segment(
    text: str,
    voice: str,
    instructions: str,
    *,
    role: str = "女",
    retries: int = 3,
) -> bytes:
    """
    Synthesize a single text segment to MP3 bytes.

    Fallback chain: zai-tts2api → qwen-tts2api → DashScope (paid).
    """
    # ── Tier 1: zai-tts2api (free, Zhipu GLM) ──
    try:
        mp3 = _synthesize_via_zai(text, role)
        logger.info("[TTS] using zai-tts2api (free)")
        return mp3
    except Exception as exc:
        logger.warning("[TTS] zai-tts2api failed: %s, trying qwen-tts2api", exc)

    # ── Tier 2: qwen-tts2api (free, same Cherry/Ethan voices) ──
    try:
        mp3 = _synthesize_via_qwen_free(text, role)
        logger.info("[TTS] using qwen-tts2api (free)")
        return mp3
    except Exception as exc:
        logger.warning("[TTS] qwen-tts2api failed: %s, falling back to DashScope", exc)

    # ── Tier 3: DashScope (paid) ──
    last_error = ""
    for attempt in range(1, retries + 1):
        try:
            mp3 = _synthesize_via_dashscope(text, voice, instructions)
            logger.info("[TTS] using DashScope (paid)")
            return mp3
        except TTSError:
            raise
        except Exception as exc:
            last_error = str(exc)
            if attempt < retries:
                wait = min(2 ** attempt, 8)
                logger.warning("DashScope attempt %d failed: %s, retrying in %ds", attempt, last_error, wait)
                time.sleep(wait)

    raise TTSError(f"All TTS backends failed. Last error: {last_error}")


def synthesize_dialogue(
    dialogue: list[dict[str, str]],
    output_dir: Path,
) -> list[Path]:
    """
    Synthesize a list of dialogue lines to individual mp3 files.

    Each item: {"role": "女"/"男", "text": "..."}
    Returns list of mp3 file paths in order.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    segment_files: list[Path] = []

    for i, line in enumerate(dialogue):
        role = line["role"]
        text = line["text"]
        preset = VOICE_MAP.get(role)
        if not preset:
            logger.warning("Unknown role '%s' at line %d, defaulting to female", role, i)
            preset = VOICE_MAP["女"]

        # Split long text into chunks
        chunks = _split_text(text, MAX_CHARS_PER_REQUEST)

        for j, chunk in enumerate(chunks):
            suffix = f"_{j}" if len(chunks) > 1 else ""
            seg_path = output_dir / f"seg_{i:03d}{suffix}.mp3"

            if seg_path.exists():
                logger.debug("Segment already exists: %s", seg_path.name)
                segment_files.append(seg_path)
                continue

            logger.info("TTS [%s] seg %d%s: %s...", role, i, suffix, chunk[:30])
            audio_bytes = synthesize_segment(
                chunk,
                preset["voice"],
                preset["instructions"],
                role=role,
            )

            with open(seg_path, "wb") as f:
                f.write(audio_bytes)
            segment_files.append(seg_path)

            # Rate limiting: respect QPS
            time.sleep(0.5)

    return segment_files


def concatenate_audio(segment_files: list[Path], output_path: Path) -> Path:
    """Concatenate mp3 segments into a single mp3 file using ffmpeg."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Build ffmpeg filter to concat with silence gaps
    inputs: list[str] = []
    filter_parts: list[str] = []

    for i, seg_file in enumerate(segment_files):
        inputs.extend(["-i", str(seg_file)])
        filter_parts.append(f"[{i}:a]")

    # Simple concat without silence (silence via anullsrc adds complexity)
    filter_str = "".join(filter_parts) + f"concat=n={len(segment_files)}:v=0:a=1[out]"

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

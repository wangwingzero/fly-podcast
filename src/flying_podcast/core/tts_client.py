from __future__ import annotations

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
    # Fallback for 女/男
    "女": {"voice": VOICE_FEMALE, "instructions": INSTRUCTIONS_FEMALE},
    "男": {"voice": VOICE_MALE, "instructions": INSTRUCTIONS_MALE},
}

# ── TTS client ─────────────────────────────────────────────────

MODEL = "qwen3-tts-instruct-flash"
MAX_CHARS_PER_REQUEST = 2000


def _ensure_api() -> str:
    """Return DashScope API key, raise if not set."""
    key = settings.dashscope_api_key
    if not key:
        raise TTSError("DASHSCOPE_API_KEY is not set")
    dashscope.base_http_api_url = "https://dashscope.aliyuncs.com/api/v1"
    return key


def synthesize_segment(
    text: str,
    voice: str,
    instructions: str,
    *,
    retries: int = 3,
) -> bytes:
    """Synthesize a single text segment to audio bytes (mp3)."""
    api_key = _ensure_api()

    last_error = ""
    for attempt in range(1, retries + 1):
        try:
            response = dashscope.MultiModalConversation.call(
                model=MODEL,
                api_key=api_key,
                text=text[:MAX_CHARS_PER_REQUEST],
                voice=voice,
                instructions=instructions,
                optimize_instructions=True,
                stream=False,
            )

            if response.status_code != 200:
                raise TTSError(f"TTS API error {response.status_code}: {response.message}")

            audio_url = response.output["audio"]["url"]
            if not audio_url:
                raise TTSError("TTS returned empty audio URL")

            audio_resp = requests.get(audio_url, timeout=60)
            audio_resp.raise_for_status()
            return audio_resp.content

        except TTSError:
            raise
        except Exception as exc:
            last_error = str(exc)
            if attempt < retries:
                wait = min(2 ** attempt, 8)
                logger.warning("TTS attempt %d failed: %s, retrying in %ds", attempt, last_error, wait)
                time.sleep(wait)

    raise TTSError(f"TTS failed after {retries} attempts: {last_error}")


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
            audio_bytes = synthesize_segment(chunk, preset["voice"], preset["instructions"])

            with open(seg_path, "wb") as f:
                f.write(audio_bytes)
            segment_files.append(seg_path)

            # Rate limiting: respect QPS
            time.sleep(0.5)

    return segment_files


def concatenate_audio(segment_files: list[Path], output_path: Path) -> Path:
    """Concatenate mp3 segments into a single mp3 file using ffmpeg."""
    import subprocess

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

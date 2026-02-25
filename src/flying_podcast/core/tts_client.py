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
_NO_PROXY = {"http": "", "https": "", "all": ""}
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
        proxies=_NO_PROXY,
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
        proxies=_NO_PROXY,
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
        proxies=_NO_PROXY,
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
        proxies=_NO_PROXY,
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
        language_type="Auto",
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


# ── Dialogue-level synthesis (smart fallback) ─────────────────
#
# Qwen and DashScope both use Cherry/Ethan voices → safe to mix.
# Edge uses completely different voices (晓晓/云健) → must be all-or-nothing.
#
# Strategy:
#   1. Qwen for all segments (free, best quality)
#   2. If Qwen partially fails → DashScope patches the gaps (same voice)
#   3. If Qwen fully fails → clean slate, Edge for all (different voice)
#   4. Last resort → clean slate, DashScope for all (paid)


def _try_all_segments(
    dialogue: list[dict[str, str]],
    output_dir: Path,
    backend: str,
    retries: int = 2,
) -> tuple[list[Path | None], list[dict]]:
    """Try all segments with one backend, continuing past failures.

    Returns (files, failed) where files[i] is Path or None.
    """
    files: list[Path | None] = []
    failed: list[dict] = []

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
            idx = len(files)

            if seg_path.exists():
                logger.debug("Segment already exists: %s", seg_path.name)
                files.append(seg_path)
                continue

            logger.info("TTS [%s][%s] seg %d%s: %s...", backend, role, i, suffix, chunk[:30])
            try:
                audio_bytes = _synthesize_one(
                    chunk, preset["voice"], preset["instructions"],
                    role=role, backend=backend, retries=retries,
                )
                with open(seg_path, "wb") as f:
                    f.write(audio_bytes)
                files.append(seg_path)
            except TTSError as exc:
                logger.warning("[TTS] %s failed seg %d%s: %s", backend, i, suffix, exc)
                files.append(None)
                failed.append({
                    "idx": idx, "seg_path": seg_path, "chunk": chunk,
                    "preset": preset, "role": role,
                    "line_idx": i, "suffix": suffix,
                })

            time.sleep(0.5)

    return files, failed


def _patch_failed_segments(
    files: list[Path | None],
    failed: list[dict],
    backend: str,
    retries: int = 2,
) -> list[Path | None]:
    """Retry failed segments with a compatible backend (same voice family)."""
    for item in failed:
        logger.info("TTS [%s] patching seg %d%s: %s...",
                     backend, item["line_idx"], item["suffix"], item["chunk"][:30])
        try:
            audio_bytes = _synthesize_one(
                item["chunk"], item["preset"]["voice"], item["preset"]["instructions"],
                role=item["role"], backend=backend, retries=retries,
            )
            with open(item["seg_path"], "wb") as fp:
                fp.write(audio_bytes)
            files[item["idx"]] = item["seg_path"]
            logger.info("[TTS] Patched seg %d%s via %s",
                         item["line_idx"], item["suffix"], backend)
        except TTSError as exc:
            logger.warning("[TTS] %s patch failed seg %d%s: %s",
                            backend, item["line_idx"], item["suffix"], exc)
        time.sleep(0.5)
    return files


def _clean_segments(output_dir: Path) -> int:
    """Delete all segment mp3 files in preparation for a different voice."""
    cleaned = 0
    for f in output_dir.glob("seg_*.mp3"):
        f.unlink()
        cleaned += 1
    if cleaned:
        logger.info("[TTS] Cleaned %d partial segments", cleaned)
    return cleaned


def synthesize_dialogue(
    dialogue: list[dict[str, str]],
    output_dir: Path,
) -> list[Path]:
    """
    Synthesize dialogue with smart voice-consistent fallback.

    Backend availability controlled by settings:
    - Qwen (free): always enabled
    - DashScope (paid): only if TTS_ENABLE_DASHSCOPE=true
    - Edge (free, different voice): only if TTS_ENABLE_EDGE=true

    Qwen + DashScope share Cherry/Ethan voices → partial patching OK.
    Edge uses different voices → requires clean slate, all-or-nothing.

    Fallback chain:
      1. Qwen all → if partial fail and DashScope enabled → patches gaps
      2. Edge all  (if enabled, clean slate, different voice)
      3. DashScope all (if enabled, clean slate, paid)
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    use_dashscope = settings.tts_enable_dashscope and settings.dashscope_api_key
    use_edge = settings.tts_enable_edge

    # ── Phase 1: Qwen (free, Cherry/Ethan) ──
    logger.info("[TTS] Phase 1: Qwen direct for all %d lines", len(dialogue))
    files, failed = _try_all_segments(dialogue, output_dir, "qwen")

    if not failed:
        logger.info("[TTS] All %d segments done via Qwen", len(files))
        return [f for f in files if f is not None]

    qwen_ok = sum(1 for f in files if f is not None)
    logger.warning("[TTS] Qwen: %d succeeded, %d failed", qwen_ok, len(failed))

    if qwen_ok > 0 and use_dashscope:
        # ── Phase 1b: DashScope patches Qwen gaps (same voice) ──
        logger.info("[TTS] Phase 1b: Patching %d gaps with DashScope", len(failed))
        files = _patch_failed_segments(files, failed, "dashscope")
        still_missing = sum(1 for f in files if f is None)
        if not still_missing:
            logger.info("[TTS] All segments complete (Qwen + DashScope patch)")
            return [f for f in files if f is not None]
        logger.warning("[TTS] Still %d segments missing after DashScope patch", still_missing)

    # ── Phase 2: Edge (free, different voice — clean slate) ──
    if use_edge:
        _clean_segments(output_dir)
        logger.info("[TTS] Phase 2: Edge TTS for all %d lines (different voice)", len(dialogue))
        files, failed = _try_all_segments(dialogue, output_dir, "edge")

        if not failed:
            logger.info("[TTS] All %d segments done via Edge", len(files))
            return [f for f in files if f is not None]

    # ── Phase 3: DashScope all (paid, last resort) ──
    if use_dashscope:
        _clean_segments(output_dir)
        logger.info("[TTS] Phase 3: DashScope for all %d lines (paid)", len(dialogue))
        files, failed = _try_all_segments(dialogue, output_dir, "dashscope")

        if not failed:
            logger.info("[TTS] All %d segments done via DashScope", len(files))
            return [f for f in files if f is not None]

    raise TTSError(f"All TTS backends failed. {len(failed)} segments ungenerated.")


# ── Audio helpers for music + chapter support ─────────────────

_ASSETS_DIR = Path(__file__).resolve().parent.parent.parent.parent / "assets" / "audio"


def _get_duration(path: Path) -> float:
    """Get audio file duration in seconds via ffprobe."""
    result = subprocess.run(
        ["ffprobe", "-v", "quiet", "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1", str(path)],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    if result.returncode != 0:
        raise TTSError(f"ffprobe failed on {path.name}: {result.stderr[-200:]}")
    return float(result.stdout.strip())


def _find_audio_assets() -> dict[str, Path]:
    """Check for optional intro/transition/outro mp3 files in assets/audio/.

    Returns dict with keys present only if the file exists.
    """
    assets: dict[str, Path] = {}
    for name in ("intro", "transition", "outro"):
        p = _ASSETS_DIR / f"{name}.mp3"
        if p.exists():
            assets[name] = p
    if assets:
        logger.info("[Audio] Found assets: %s", ", ".join(assets.keys()))
    return assets


def _build_line_segment_map(
    segment_files: list[Path], num_lines: int,
) -> list[list[Path]]:
    """Map line indices to their segment file(s).

    Parses filenames like seg_005.mp3, seg_005_0.mp3, seg_005_1.mp3
    and groups them by line index.

    Returns a list where result[line_idx] = [seg_file, ...].
    """
    import re

    line_map: dict[int, list[tuple[int, Path]]] = {}
    for seg in segment_files:
        m = re.match(r"seg_(\d+)(?:_(\d+))?\.mp3", seg.name)
        if not m:
            continue
        line_idx = int(m.group(1))
        chunk_idx = int(m.group(2)) if m.group(2) else 0
        line_map.setdefault(line_idx, []).append((chunk_idx, seg))

    result: list[list[Path]] = []
    for i in range(num_lines):
        segs = line_map.get(i, [])
        segs.sort(key=lambda x: x[0])
        result.append([p for _, p in segs])
    return result


def concatenate_audio(
    segment_files: list[Path],
    output_path: Path,
    *,
    chapters: list[dict] | None = None,
    num_lines: int = 0,
) -> list[dict]:
    """Concatenate mp3 segments into a single mp3 file using ffmpeg.

    When audio assets (intro/transition/outro) exist in assets/audio/:
    - Adds intro with fade-out before dialogue
    - Inserts transition sounds between chapters
    - Adds outro with fade-in after dialogue

    When no assets exist, behaves exactly as before (simple concat + loudnorm).

    Args:
        segment_files: Ordered list of segment mp3 paths.
        output_path: Final output mp3 path.
        chapters: Optional chapter info from normalize_dialogue().
        num_lines: Total number of dialogue lines (for segment mapping).

    Returns:
        List of chapter timestamps [{"title", "start", "end"}, ...].
        Empty list if no chapters provided.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    assets = _find_audio_assets()

    has_music = bool(assets)
    has_chapters = bool(chapters and len(chapters) > 1 and num_lines > 0)

    # ── Simple mode: no assets → original behavior ──
    if not has_music or not has_chapters:
        return _concatenate_simple(segment_files, output_path)

    # ── Enhanced mode: music + chapter transitions ──
    return _concatenate_with_music(segment_files, output_path, assets,
                                   chapters, num_lines)


def _concatenate_simple(segment_files: list[Path], output_path: Path) -> list[dict]:
    """Original concatenation: simple concat + loudnorm."""
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

    result = subprocess.run(cmd, capture_output=True, text=True,
                            encoding="utf-8", errors="replace")

    if result.returncode != 0:
        err_lines = [l for l in result.stderr.splitlines() if "Error" in l or "error" in l]
        err_msg = "\n".join(err_lines) if err_lines else result.stderr[-500:]
        raise TTSError(f"ffmpeg concat failed: {err_msg}")

    size_kb = output_path.stat().st_size / 1024
    logger.info("Combined audio: %s (%.1f KB)", output_path.name, size_kb)
    return []


def _run_ffmpeg(cmd: list[str], label: str = "ffmpeg") -> None:
    """Run an ffmpeg command, raise TTSError on failure."""
    result = subprocess.run(cmd, capture_output=True, text=True,
                            encoding="utf-8", errors="replace")
    if result.returncode != 0:
        err_lines = [l for l in result.stderr.splitlines() if "Error" in l or "error" in l]
        err_msg = "\n".join(err_lines) if err_lines else result.stderr[-500:]
        raise TTSError(f"{label} failed: {err_msg}")


def _normalize_to_wav(src: Path, dst: Path, fade: str | None = None) -> None:
    """Convert any audio to 44100Hz stereo wav, optionally with fade."""
    cmd = ["ffmpeg", "-y", "-i", str(src), "-ar", "44100", "-ac", "2"]
    if fade:
        cmd.extend(["-af", fade])
    cmd.append(str(dst))
    _run_ffmpeg(cmd, f"normalize {src.name}")


def _generate_silence_wav(dst: Path, duration: float) -> None:
    """Generate a silence wav file."""
    cmd = [
        "ffmpeg", "-y",
        "-f", "lavfi", "-i", f"anullsrc=r=44100:cl=stereo",
        "-t", f"{duration:.2f}",
        "-ar", "44100", "-ac", "2",
        str(dst),
    ]
    _run_ffmpeg(cmd, f"silence {duration}s")


def _concatenate_with_music(
    segment_files: list[Path],
    output_path: Path,
    assets: dict[str, Path],
    chapters: list[dict],
    num_lines: int,
) -> list[dict]:
    """Concatenate with intro/transition/outro music and chapter timestamps.

    Uses a fast 3-step approach:
      1. Pre-convert all pieces to uniform wav (parallel)
      2. Concat via ffmpeg concat demuxer (near-instant)
      3. Apply loudnorm in a single pass
    """
    import tempfile
    line_map = _build_line_segment_map(segment_files, num_lines)

    tmp_dir = Path(tempfile.mkdtemp(prefix="podcast_concat_"))
    pieces: list[Path] = []  # ordered list of wav pieces to concat
    piece_idx = 0

    def next_piece_path(tag: str) -> Path:
        nonlocal piece_idx
        p = tmp_dir / f"{piece_idx:04d}_{tag}.wav"
        piece_idx += 1
        return p

    logger.info("Step 1/3: Pre-converting %d+ segments to uniform format...", len(segment_files))
    t0 = time.time()

    # Pre-generate silence files (reuse same duration files)
    sil_cache: dict[str, Path] = {}

    def get_silence(duration: float) -> Path:
        key = f"{duration:.2f}"
        if key not in sil_cache:
            p = tmp_dir / f"silence_{key}s.wav"
            _generate_silence_wav(p, duration)
            sil_cache[key] = p
        return sil_cache[key]

    # ── Intro ──
    if "intro" in assets:
        intro_wav = next_piece_path("intro")
        intro_dur = _get_duration(assets["intro"])
        fade_start = max(0, intro_dur - 1.5)
        _normalize_to_wav(assets["intro"], intro_wav,
                          fade=f"afade=t=out:st={fade_start:.1f}:d=1.5")
        pieces.append(intro_wav)
        sil_p = next_piece_path("sil")
        _generate_silence_wav(sil_p, 0.5)
        pieces.append(sil_p)

    # ── Chapters ──
    for ch_idx, chapter in enumerate(chapters):
        start_line = chapter["start_line"]
        end_line = chapter["end_line"]

        # Transition between chapters
        if ch_idx > 0 and "transition" in assets:
            pieces.append(get_silence(0.5))
            trans_wav = next_piece_path(f"trans{ch_idx}")
            trans_dur = _get_duration(assets["transition"])
            fade_start = max(0, trans_dur - 1.0)
            _normalize_to_wav(assets["transition"], trans_wav,
                              fade=f"afade=t=out:st={fade_start:.1f}:d=1.0")
            pieces.append(trans_wav)
            pieces.append(get_silence(0.5))

        # Segments for this chapter
        for line_idx in range(start_line, end_line):
            segs = line_map[line_idx] if line_idx < len(line_map) else []
            for seg_file in segs:
                seg_wav = next_piece_path(f"seg{line_idx}")
                _normalize_to_wav(seg_file, seg_wav)
                pieces.append(seg_wav)

            # 0.1s gap between lines
            if line_idx < end_line - 1:
                pieces.append(get_silence(0.1))

    # ── Outro ──
    if "outro" in assets:
        pieces.append(get_silence(0.5))
        outro_wav = next_piece_path("outro")
        _normalize_to_wav(assets["outro"], outro_wav,
                          fade=f"afade=t=in:st=0:d=1.5")
        pieces.append(outro_wav)

    logger.info("Step 1/3 done: %d pieces in %.1fs", len(pieces), time.time() - t0)

    # ── Step 2: Concat demuxer ──
    logger.info("Step 2/3: Concatenating %d pieces...", len(pieces))
    t1 = time.time()

    concat_list = tmp_dir / "concat.txt"
    with open(concat_list, "w", encoding="utf-8") as f:
        for p in pieces:
            # ffmpeg concat demuxer needs forward slashes and escaped quotes
            safe_path = str(p).replace("\\", "/").replace("'", "'\\''")
            f.write(f"file '{safe_path}'\n")

    raw_mp3 = tmp_dir / "raw.mp3"
    cmd_concat = [
        "ffmpeg", "-y",
        "-f", "concat", "-safe", "0",
        "-i", str(concat_list),
        "-b:a", "128k",
        str(raw_mp3),
    ]
    _run_ffmpeg(cmd_concat, "concat demuxer")
    logger.info("Step 2/3 done: concat in %.1fs", time.time() - t1)

    # ── Step 3: Copy raw to output (no loudnorm — TTS audio is already consistent) ──
    import shutil
    shutil.copy2(str(raw_mp3), str(output_path))
    logger.info("Step 3/3 done: audio ready (skipped loudnorm)")

    # Cleanup temp dir
    shutil.rmtree(tmp_dir, ignore_errors=True)

    size_kb = output_path.stat().st_size / 1024
    logger.info("Combined audio: %s (%.1f KB), total pipeline: %.1fs",
                output_path.name, size_kb, time.time() - t0)

    # ── Calculate chapter timestamps ──
    chapter_timestamps = _calculate_chapter_timestamps(
        assets, chapters, line_map,
    )
    return chapter_timestamps


def _calculate_chapter_timestamps(
    assets: dict[str, Path],
    chapters: list[dict],
    line_map: list[list[Path]],
) -> list[dict]:
    """Calculate real start/end times for each chapter by measuring segment durations."""
    pos = 0.0  # current position in seconds

    # Intro
    if "intro" in assets:
        pos += _get_duration(assets["intro"])
        pos += 0.5  # silence after intro

    timestamps: list[dict] = []

    for ch_idx, chapter in enumerate(chapters):
        start_line = chapter["start_line"]
        end_line = chapter["end_line"]

        # Transition before chapter (not first)
        if ch_idx > 0 and "transition" in assets:
            pos += 0.5  # silence before transition
            pos += _get_duration(assets["transition"])
            pos += 0.5  # silence after transition

        ch_start = pos

        for line_idx in range(start_line, end_line):
            segs = line_map[line_idx] if line_idx < len(line_map) else []
            for seg_file in segs:
                pos += _get_duration(seg_file)
            # 0.1s gap between lines
            if line_idx < end_line - 1:
                pos += 0.1

        timestamps.append({
            "title": chapter.get("title", f"Chapter {ch_idx + 1}"),
            "start": round(ch_start, 1),
            "end": round(pos, 1),
        })

    return timestamps


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

"""PDF full-text narration using MinerU VLM + Azure TTS.

Generates complete audio narration of PDF documents for podcast appendix.
"""
from __future__ import annotations

import io
import os
import re
import time
import zipfile
from pathlib import Path

import requests

from flying_podcast.core.config import settings
from flying_podcast.core.logging_utils import get_logger

logger = get_logger("pdf_narration")

# ── MinerU API ────────────────────────────────────────────────

MINERU_BASE = "https://mineru.net/api/v4"
CHUNK_SIZE = 1800  # chars per TTS segment
MAX_RETRIES = 10


class MinerUError(RuntimeError):
    """MinerU API error."""


def extract_markdown(pdf_path: Path) -> str:
    """Extract PDF to Markdown via MinerU VLM API."""
    if not settings.mineru_token:
        raise MinerUError("MinerU token not configured (MINERU env var)")

    headers = {
        "Authorization": f"Bearer {settings.mineru_token}",
        "Content-Type": "application/json",
    }
    filename = pdf_path.name

    # 1. Get presigned upload URL
    logger.info("[MinerU] Requesting upload URL for: %s", filename)
    resp = requests.post(
        f"{MINERU_BASE}/file-urls/batch",
        json={
            "files": [{"name": filename, "data_id": "narration"}],
            "model_version": "vlm",
        },
        headers=headers,
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()["data"]
    batch_id = data["batch_id"]
    upload_url = data["file_urls"][0]
    logger.info("[MinerU] batch_id=%s", batch_id)

    # 2. PUT upload the file
    file_size = pdf_path.stat().st_size
    logger.info("[MinerU] Uploading %d KB ...", file_size // 1024)
    with open(pdf_path, "rb") as f:
        up_resp = requests.put(upload_url, data=f, timeout=120)
    if up_resp.status_code != 200:
        raise MinerUError(f"Upload failed: {up_resp.status_code} {up_resp.text[:200]}")
    logger.info("[MinerU] Upload OK, waiting for extraction...")

    # 3. Poll batch results
    poll_url = f"{MINERU_BASE}/extract-results/batch/{batch_id}"
    auth_header = {"Authorization": f"Bearer {settings.mineru_token}"}

    for attempt in range(180):  # max ~15 min
        time.sleep(5)
        sr = requests.get(poll_url, headers=auth_header, timeout=15)
        sr.raise_for_status()
        result = sr.json()["data"]
        extract_results = result.get("extract_result", [])

        if not extract_results:
            logger.debug("  [%ds] waiting...", attempt * 5)
            continue

        item = extract_results[0]
        state = item.get("state", "unknown")
        progress = item.get("extract_progress", {})
        extracted = progress.get("extracted_pages", "?")
        total = progress.get("total_pages", "?")

        if state == "running":
            logger.info("  [%ds] extracting %s/%s pages...", attempt * 5, extracted, total)
        elif state in ("pending", "waiting-file", "converting"):
            logger.debug("  [%ds] state=%s", attempt * 5, state)
        elif state == "done":
            zip_url = item.get("full_zip_url", "")
            if not zip_url:
                raise MinerUError(f"Done but no zip URL: {item}")
            logger.info("[MinerU] Extraction complete! Downloading results...")
            return _download_markdown_from_zip(zip_url)
        elif state == "failed":
            raise MinerUError(f"Extraction failed: {item.get('err_msg', item)}")

    raise MinerUError("Extraction timed out after 15 minutes")


def _download_markdown_from_zip(zip_url: str) -> str:
    """Download zip, find .md file inside, return its content."""
    resp = requests.get(zip_url, timeout=120)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        md_files = [n for n in zf.namelist() if n.endswith(".md")]
        if not md_files:
            raise MinerUError(f"No .md file found in zip: {zf.namelist()[:20]}")
        # Pick the largest markdown file
        md_file = max(md_files, key=lambda n: zf.getinfo(n).file_size)
        logger.info("  Found: %s (%d bytes)", md_file, zf.getinfo(md_file).file_size)
        return zf.read(md_file).decode("utf-8")


# ── Markdown cleaning ─────────────────────────────────────────

def clean_for_tts(md_text: str) -> str:
    """Strip markdown formatting for clean TTS input."""
    text = md_text
    # Remove images
    text = re.sub(r"!\[.*?\]\(.*?\)", "", text)
    # Remove links but keep text
    text = re.sub(r"\[(.*?)\]\(.*?\)", r"\1", text)
    # Remove markdown header markers
    text = re.sub(r"^#{1,6}\s+", "", text, flags=re.MULTILINE)
    # Remove bold/italic
    text = re.sub(r"\*{1,3}(.*?)\*{1,3}", r"\1", text)
    # Remove code blocks
    text = re.sub(r"```.*?```", "", text, flags=re.DOTALL)
    text = re.sub(r"`(.*?)`", r"\1", text)
    # Remove horizontal rules
    text = re.sub(r"^-{3,}$", "", text, flags=re.MULTILINE)
    # Remove table pipes
    text = re.sub(r"\|", " ", text)
    text = re.sub(r"^[\s\-:]+$", "", text, flags=re.MULTILINE)
    # Remove LaTeX
    text = re.sub(r"\$\$.*?\$\$", "", text, flags=re.DOTALL)
    text = re.sub(r"\$.*?\$", "", text)
    # Remove HTML tags (MinerU may output <table>, <tr>, <td> etc.)
    text = re.sub(r"<[^>]+>", " ", text)
    # Remove XML/SSML special chars that would break Azure TTS
    text = text.replace("&lt;", "").replace("&gt;", "").replace("&amp;", "和")
    text = text.replace("&nbsp;", " ").replace("&quot;", "")
    # Clean up whitespace
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


# ── Azure TTS synthesis ───────────────────────────────────────

def synthesize_long_text(text: str, output_path: Path, voice: str = "zh-CN-YunyangNeural"):
    """Split text and synthesize with Azure TTS. Never skips — retries until success."""
    # Import here to avoid circular dependency
    import sys
    azure_tts_path = Path(__file__).parent.parent.parent.parent / "免费TTS"
    if str(azure_tts_path) not in sys.path:
        sys.path.insert(0, str(azure_tts_path))

    from free_tts.azure_tts import synthesize
    import free_tts.azure_tts as _azure_mod

    chunks = _split_text(text, CHUNK_SIZE)
    logger.info("[TTS] %d chars -> %d chunks", len(text), len(chunks))

    audio_parts = []
    for i, chunk in enumerate(chunks):
        # Escape XML special chars to avoid SSML parse errors
        safe_chunk = chunk.replace("&", "&amp;").replace("<", "").replace(">", "")
        logger.info("  [%d/%d] %d chars ...", i + 1, len(chunks), len(safe_chunk))

        for attempt in range(MAX_RETRIES):
            try:
                if attempt > 0:
                    # Force token refresh on retry
                    _azure_mod._expired_at = None
                mp3 = synthesize(safe_chunk, voice=voice, style="general", rate="10")
                audio_parts.append(mp3)
                logger.info("    OK (%d KB)", len(mp3) // 1024)
                break
            except Exception as e:
                # Exponential backoff: 10, 20, 30, 45, 60, 60, 60...
                wait = min(10 * (attempt + 1), 60)
                if attempt < MAX_RETRIES - 1:
                    logger.warning("    retry %d/%d in %ds ...", attempt + 1, MAX_RETRIES, wait)
                    time.sleep(wait)
                else:
                    logger.error("    ALL %d RETRIES FAILED — SKIPPED", MAX_RETRIES)
                    audio_parts.append(b"")
        time.sleep(2)  # 2s between chunks to avoid rate limiting

    with open(output_path, "wb") as f:
        for part in audio_parts:
            f.write(part)

    skipped = sum(1 for p in audio_parts if len(p) == 0)
    logger.info("[Done] %s (%d KB)", output_path, output_path.stat().st_size // 1024)
    if skipped:
        logger.warning("  WARNING: %d/%d chunks failed after all retries", skipped, len(chunks))


def _split_text(text: str, max_len: int) -> list[str]:
    """Split text at sentence boundaries."""
    chunks = []
    while text:
        if len(text) <= max_len:
            chunks.append(text)
            break
        cut = max_len
        for sep in ["。\n", "。", "；", "\n\n", "\n", "，"]:
            idx = text.rfind(sep, 0, max_len)
            if idx > max_len // 2:
                cut = idx + len(sep)
                break
        chunks.append(text[:cut])
        text = text[cut:]
    return chunks


# ── Main entry point ──────────────────────────────────────────

def generate_narration(pdf_path: Path, output_dir: Path) -> Path | None:
    """Generate full PDF narration audio.

    Returns:
        Path to generated MP3 file, or None if generation failed.
    """
    try:
        # 1. Extract PDF to Markdown via MinerU
        md_path = output_dir / f"{pdf_path.stem}_narration.md"
        if md_path.exists():
            logger.info("Markdown already exists: %s", md_path)
            with open(md_path, "r", encoding="utf-8") as f:
                md_text = f.read()
        else:
            md_text = extract_markdown(pdf_path)
            with open(md_path, "w", encoding="utf-8") as f:
                f.write(md_text)
            logger.info("Saved Markdown: %s (%d chars)", md_path, len(md_text))

        # 2. Clean for TTS
        clean_text = clean_for_tts(md_text)
        logger.info("Cleaned: %d -> %d chars", len(md_text), len(clean_text))

        # 3. Synthesize audio
        mp3_path = output_dir / f"{pdf_path.stem}_narration.mp3"
        synthesize_long_text(clean_text, mp3_path)

        return mp3_path

    except Exception as e:
        logger.error("Failed to generate narration: %s", e, exc_info=True)
        return None

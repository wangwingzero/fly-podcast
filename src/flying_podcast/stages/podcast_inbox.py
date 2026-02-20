"""Podcast inbox: auto-fetch pilot-relevant docs from CCAR-workflow + batch process.

Workflow:
1. Read CCAR-workflow regulations.json (categories 13/14/15)
2. Filter for Part 121 pilot relevance
3. Download PDFs that haven't been processed yet
4. Also scan inbox/pending/ for manually added PDFs
5. Run podcast pipeline on each
6. Track processed files
"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import requests

from flying_podcast.core.config import settings
from flying_podcast.core.io_utils import dump_json, load_json
from flying_podcast.core.logging_utils import get_logger
from flying_podcast.core.pilot_filter import filter_documents
from flying_podcast.core.time_utils import beijing_today_str
from flying_podcast.stages.podcast import run as podcast_run

logger = get_logger("podcast_inbox")

PILOT_CATEGORIES = ["13", "14", "15"]


# ── Processed state ───────────────────────────────────────────

def _processed_path() -> Path:
    return settings.podcast_inbox_dir / "processed.json"


def _load_processed() -> dict[str, Any]:
    """Load processed state. Structure:
    {
        "by_url": {"http://...": {...}},     # CCAR doc URL dedup
        "by_hash": {"sha256hex": {...}},      # PDF content dedup
    }
    """
    p = _processed_path()
    if p.exists():
        data = load_json(p)
        # Migrate old flat format
        if "by_url" not in data and "by_hash" not in data:
            return {"by_url": data, "by_hash": {}}
        return data
    return {"by_url": {}, "by_hash": {}}


def _save_processed(data: dict[str, Any]) -> None:
    dump_json(_processed_path(), data)


def _file_hash(path: Path) -> str:
    """SHA256 hash of file content for deduplication."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


# ── URL map (PDF filename → download URL) ────────────────────

def _url_map_path() -> Path:
    return settings.podcast_inbox_dir / "url_map.json"


def _load_url_map() -> dict[str, str]:
    p = _url_map_path()
    if p.exists():
        return load_json(p)
    return {}


def _save_url_map(data: dict[str, str]) -> None:
    dump_json(_url_map_path(), data)


def _is_processed(processed: dict, *, url: str = "", pdf_path: Path | None = None) -> bool:
    """Check if a document has already been processed (by URL or file hash)."""
    if url and url in processed.get("by_url", {}):
        return True
    if pdf_path and pdf_path.exists():
        fh = _file_hash(pdf_path)
        if fh in processed.get("by_hash", {}):
            return True
    return False


def _mark_processed(processed: dict, *, url: str = "", pdf_path: Path | None = None,
                    title: str = "", day: str = "", mp3_path: str = "") -> None:
    """Mark a document as processed by both URL and file hash."""
    info = {"title": title, "date": day, "mp3_path": mp3_path}
    if url:
        processed.setdefault("by_url", {})[url] = info
    if pdf_path and pdf_path.exists():
        fh = _file_hash(pdf_path)
        processed.setdefault("by_hash", {})[fh] = info


# ── CCAR-workflow data loading ────────────────────────────────

def _load_ccar_docs() -> list[dict[str, Any]]:
    """Load documents from CCAR-workflow for pilot-relevant categories."""
    ccar_path = Path(settings.ccar_data_path)
    if not ccar_path.exists():
        logger.warning("CCAR data not found: %s", ccar_path)
        return []

    data = load_json(ccar_path)
    docs_by_cat = data.get("documents", {})
    all_docs = []
    for cid in PILOT_CATEGORIES:
        cat_docs = docs_by_cat.get(cid, [])
        all_docs.extend(cat_docs)

    logger.info("Loaded %d docs from CCAR-workflow (categories %s)",
                len(all_docs), ", ".join(PILOT_CATEGORIES))
    return all_docs


def _find_pdf_path(doc: dict[str, Any]) -> Path | None:
    """Find PDF for a document: try local CCAR-workflow downloads first, then R2."""
    doc_url = doc.get("url", "")
    ccar_downloads = Path(settings.ccar_downloads_path)

    # Method 1: Check CCAR-workflow local downloads
    downloads_json = ccar_downloads.parent / "data" / "downloads.json"
    if downloads_json.exists():
        dl_data = load_json(downloads_json)
        record = dl_data.get("records", {}).get(doc_url)
        if record:
            local_file = ccar_downloads / record["relative_path"]
            if local_file.exists() and local_file.suffix.lower() == ".pdf":
                return local_file

    # Method 2: Check if doc has direct pdf_url
    pdf_url = doc.get("pdf_url", "")
    if pdf_url:
        return _download_from_url(pdf_url, doc)

    # Method 3: Check R2 uploads
    r2_json = ccar_downloads.parent / "data" / "r2_uploads.json"
    if r2_json.exists():
        r2_data = load_json(r2_json)
        # Match by finding the record whose path contains the doc title
        title = doc.get("title", "")
        for rpath, info in r2_data.get("records", {}).items():
            if title and title in unquote(rpath):
                return _download_from_url(info["r2_url"], doc)

    return None


def _download_from_url(url: str, doc: dict[str, Any]) -> Path | None:
    """Download a PDF from URL to the pending folder."""
    pending_dir = settings.podcast_inbox_dir / "pending"
    pending_dir.mkdir(parents=True, exist_ok=True)

    title = doc.get("title", "unknown")


def _find_download_url(doc: dict[str, Any]) -> str:
    """Find the best download URL for a document (pdf_url > R2 > page URL)."""
    ccar_downloads = Path(settings.ccar_downloads_path)

    # Priority 1: Direct pdf_url from doc
    pdf_url = doc.get("pdf_url", "")
    if pdf_url:
        return pdf_url

    # Priority 2: R2 upload URL (direct PDF download)
    r2_json = ccar_downloads.parent / "data" / "r2_uploads.json"
    if r2_json.exists():
        r2_data = load_json(r2_json)
        title = doc.get("title", "")
        for rpath, info in r2_data.get("records", {}).items():
            if title and title in unquote(rpath):
                return info.get("r2_url", "")

    # Priority 3: CAAC page URL
    return doc.get("url", "")
    # Clean filename
    safe_name = "".join(c for c in title if c not in r'<>:"/\|?*').strip()
    filename = f"{safe_name}.pdf"
    dest = pending_dir / filename

    if dest.exists():
        return dest

    try:
        logger.info("Downloading: %s", filename)
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        dest.write_bytes(resp.content)
        return dest
    except Exception as e:
        logger.warning("Failed to download PDF for '%s': %s", title, e)
        return None


# ── Main stage ────────────────────────────────────────────────

def run(target_date: str | None = None, *,
        local_only: bool = False,
        dry_run: bool = False) -> list[Path]:
    """Podcast inbox stage: fetch, filter, and batch-process PDFs.

    Args:
        target_date: Date string (YYYY-MM-DD), defaults to today
        local_only: Only process PDFs already in pending/, skip CCAR-workflow fetch
        dry_run: Only show what would be processed, don't generate podcasts

    Returns:
        List of generated MP3 paths
    """
    day = target_date or beijing_today_str()
    pending_dir = settings.podcast_inbox_dir / "pending"
    done_dir = settings.podcast_inbox_dir / "done"
    pending_dir.mkdir(parents=True, exist_ok=True)
    done_dir.mkdir(parents=True, exist_ok=True)

    processed = _load_processed()
    url_map = _load_url_map()

    # Step 1: Fetch from CCAR-workflow (unless local_only)
    if not local_only:
        logger.info("Step 1: Scanning CCAR-workflow for pilot-relevant documents...")
        all_docs = _load_ccar_docs()
        if all_docs:
            # Use LLM client for borderline filtering
            llm_client = None
            if settings.llm_api_key:
                from flying_podcast.core.llm_client import OpenAICompatibleClient
                llm_client = OpenAICompatibleClient(
                    api_key=settings.llm_api_key,
                    base_url=settings.llm_base_url,
                    model=settings.llm_model,
                )
            relevant_docs = filter_documents(all_docs, llm_client=llm_client)

            new_count = 0
            seen_titles: set[str] = set()
            for doc in relevant_docs:
                doc_url = doc.get("url", "")
                title = doc.get("title", "")
                if _is_processed(processed, url=doc_url):
                    continue  # Already processed
                if title in seen_titles:
                    continue  # Deduplicate same-title docs
                seen_titles.add(title)

                pdf_path = _find_pdf_path(doc)
                if pdf_path:
                    if _is_processed(processed, pdf_path=pdf_path):
                        logger.debug("Skip (hash match): %s", title)
                        continue
                    # Record download URL for this PDF
                    dl_url = _find_download_url(doc)
                    if dl_url:
                        url_map[pdf_path.name] = dl_url
                    # Copy to pending if it's from CCAR-workflow downloads
                    if "podcast_inbox" not in str(pdf_path):
                        dest = pending_dir / pdf_path.name
                        if not dest.exists():
                            shutil.copy2(pdf_path, dest)
                            logger.info("Queued: %s", pdf_path.name)
                            new_count += 1
                    else:
                        new_count += 1
                else:
                    logger.debug("No PDF available for: %s", doc.get("title", ""))

            _save_url_map(url_map)
            logger.info("Found %d new PDFs from CCAR-workflow", new_count)
    else:
        logger.info("Step 1: Skipped (local-only mode)")

    # Step 2: Scan pending folder
    pending_pdfs = sorted(pending_dir.glob("*.pdf"))
    # Filter out already-processed files (by hash)
    pending_pdfs = [
        p for p in pending_pdfs
        if not _is_processed(processed, pdf_path=p)
    ]

    if not pending_pdfs:
        logger.info("No pending PDFs to process.")
        return []

    logger.info("Step 2: Found %d pending PDFs:", len(pending_pdfs))
    for p in pending_pdfs:
        logger.info("  - %s", p.name)

    if dry_run:
        logger.info("Dry run — skipping podcast generation.")
        return []

    # Step 3: Process each PDF
    results: list[Path] = []
    for i, pdf_path in enumerate(pending_pdfs, 1):
        logger.info("=" * 60)
        logger.info("Processing %d/%d: %s", i, len(pending_pdfs), pdf_path.name)
        logger.info("=" * 60)

        try:
            dl_url = url_map.get(pdf_path.name, "")
            mp3_path = podcast_run(day, pdf_path=str(pdf_path), download_url=dl_url)
            results.append(mp3_path)

            # Mark as processed (by hash + stem)
            _mark_processed(
                processed,
                pdf_path=pdf_path,
                title=pdf_path.stem,
                day=day,
                mp3_path=str(mp3_path),
            )
            _save_processed(processed)

            # Move PDF to done/
            done_path = done_dir / pdf_path.name
            shutil.move(str(pdf_path), str(done_path))
            logger.info("Done: %s → done/", pdf_path.name)

        except Exception as e:
            logger.error("Failed to process '%s': %s", pdf_path.name, e, exc_info=True)
            # Don't move — leave in pending for retry
            continue

    logger.info("=" * 60)
    logger.info("Inbox complete: %d/%d podcasts generated", len(results), len(pending_pdfs))
    return results

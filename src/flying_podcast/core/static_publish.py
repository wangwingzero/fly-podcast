"""Publish generated files to the self-hosted static web root."""
from __future__ import annotations

import hashlib
import shutil
from io import BytesIO
from pathlib import Path
from urllib.parse import quote, urlparse

import requests

from flying_podcast.core.config import settings
from flying_podcast.core.logging_utils import get_logger

logger = get_logger("static_publish")

_IMAGE_CONTENT_TYPE_TO_SUFFIX = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/gif": ".gif",
    "image/webp": ".webp",
    "image/avif": ".avif",
}

_KNOWN_SUFFIXES = {
    ".mp3",
    ".pdf",
    ".html",
    ".json",
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".webp",
    ".avif",
}


def static_configured() -> bool:
    return bool(settings.static_root and settings.static_public_base_url)


def _safe_static_destination(key: str) -> Path:
    root = Path(settings.static_root).expanduser().resolve()
    normalized = key.strip().replace("\\", "/").lstrip("/")
    if not normalized or ".." in Path(normalized).parts:
        raise ValueError(f"unsafe static key: {key!r}")
    dest = (root / normalized).resolve()
    if root != dest and root not in dest.parents:
        raise ValueError(f"static key escapes root: {key!r}")
    return dest


def public_url_for_key(key: str) -> str:
    base = settings.static_public_base_url.rstrip("/")
    if not base:
        digest_base = settings.web_digest_base_url.rstrip("/")
        base = digest_base[:-7] if digest_base.endswith("/digest") else digest_base
    if not base:
        return ""
    normalized = key.strip().replace("\\", "/").lstrip("/")
    quoted = "/".join(quote(part) for part in normalized.split("/"))
    return f"{base}/{quoted}"


def publish_file(local_path: Path, static_key: str) -> str:
    if not static_configured():
        raise RuntimeError("STATIC_ROOT / STATIC_PUBLIC_BASE_URL not configured")
    if not local_path.exists():
        raise FileNotFoundError(local_path)

    dest = _safe_static_destination(static_key)
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(local_path, dest)
    logger.info("Static publish OK: %s -> %s", local_path, dest)
    return public_url_for_key(static_key)


def publish_bytes(data: bytes, static_key: str) -> str:
    if not static_configured():
        raise RuntimeError("STATIC_ROOT / STATIC_PUBLIC_BASE_URL not configured")
    dest = _safe_static_destination(static_key)
    dest.parent.mkdir(parents=True, exist_ok=True)
    with BytesIO(data) as buf:
        dest.write_bytes(buf.getvalue())
    logger.info("Static publish OK: %s bytes -> %s", len(data), dest)
    return public_url_for_key(static_key)


def _guess_image_suffix(content_type: str, image_url: str) -> str:
    normalized = str(content_type or "").split(";", 1)[0].strip().lower()
    if normalized in _IMAGE_CONTENT_TYPE_TO_SUFFIX:
        return _IMAGE_CONTENT_TYPE_TO_SUFFIX[normalized]

    suffix = Path(urlparse(image_url).path or "").suffix.lower()
    if suffix in _KNOWN_SUFFIXES:
        return suffix
    return ".jpg"


def mirror_image_from_url(image_url: str, *, static_prefix: str = "digest/article-images") -> str:
    if not image_url.startswith(("http://", "https://")):
        return ""

    parsed = urlparse(image_url)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
    }
    if parsed.scheme and parsed.netloc:
        headers["Referer"] = f"{parsed.scheme}://{parsed.netloc}/"

    resp = requests.get(image_url, timeout=20, headers=headers)
    resp.raise_for_status()

    content_type = str(resp.headers.get("Content-Type", "image/jpeg")).split(";", 1)[0].strip().lower()
    if not content_type.startswith("image/"):
        logger.warning("Skip non-image content from %s: %s", image_url[:80], content_type)
        return ""

    data = resp.content
    if not data:
        return ""

    sha = hashlib.sha256(data).hexdigest()
    suffix = _guess_image_suffix(content_type, image_url)
    static_key = f"{static_prefix}/{sha[:2]}/{sha}{suffix}"
    dest = _safe_static_destination(static_key) if static_configured() else None
    if dest and dest.exists():
        cdn_url = public_url_for_key(static_key)
        logger.info("Static image cache hit: %s", cdn_url)
        return cdn_url

    return publish_bytes(data, static_key)

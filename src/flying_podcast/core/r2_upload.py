"""Upload files to Cloudflare R2 (S3-compatible)."""
from __future__ import annotations

import hashlib
from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError

from flying_podcast.core.config import settings
from flying_podcast.core.logging_utils import get_logger

logger = get_logger("r2")

_CONTENT_TYPES = {
    ".mp3": "audio/mpeg",
    ".pdf": "application/pdf",
    ".html": "text/html; charset=utf-8",
    ".json": "application/json",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".gif": "image/gif",
    ".webp": "image/webp",
    ".avif": "image/avif",
}

_IMAGE_CONTENT_TYPE_TO_SUFFIX = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/gif": ".gif",
    "image/webp": ".webp",
    "image/avif": ".avif",
}


def _get_client():
    if not settings.r2_access_key_id or not settings.r2_secret_access_key:
        raise RuntimeError("R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY not set")
    if not settings.r2_endpoint:
        raise RuntimeError("R2_ENDPOINT not set")

    return boto3.client(
        "s3",
        endpoint_url=settings.r2_endpoint,
        aws_access_key_id=settings.r2_access_key_id,
        aws_secret_access_key=settings.r2_secret_access_key,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def upload_file(local_path: Path, r2_key: str) -> str:
    """Upload a local file to R2.

    Args:
        local_path: Path to the local file.
        r2_key: Object key in R2 (e.g. "podcast/2026-02-25_xxx/file.mp3").

    Returns:
        Public CDN URL of the uploaded file.
    """
    client = _get_client()
    content_type = _CONTENT_TYPES.get(local_path.suffix.lower(), "application/octet-stream")

    size_mb = local_path.stat().st_size / (1024 * 1024)
    logger.info("Uploading %s (%.1f MB) → r2://%s/%s",
                local_path.name, size_mb, settings.r2_bucket, r2_key)

    client.upload_file(
        str(local_path),
        settings.r2_bucket,
        r2_key,
        ExtraArgs={"ContentType": content_type},
    )

    cdn_url = f"https://{settings.r2_domain}/{r2_key}"
    logger.info("Upload OK: %s", cdn_url)
    return cdn_url


def upload_bytes(data: bytes, r2_key: str, content_type: str) -> str:
    client = _get_client()
    client.put_object(
        Bucket=settings.r2_bucket,
        Key=r2_key,
        Body=BytesIO(data),
        ContentType=content_type,
        CacheControl="public, max-age=31536000, immutable",
    )
    cdn_url = f"https://{settings.r2_domain}/{r2_key}"
    logger.info("Upload OK: %s", cdn_url)
    return cdn_url


def _guess_image_suffix(content_type: str, image_url: str) -> str:
    normalized = str(content_type or "").split(";", 1)[0].strip().lower()
    if normalized in _IMAGE_CONTENT_TYPE_TO_SUFFIX:
        return _IMAGE_CONTENT_TYPE_TO_SUFFIX[normalized]

    suffix = Path(urlparse(image_url).path or "").suffix.lower()
    if suffix in _CONTENT_TYPES:
        return suffix
    return ".jpg"


def mirror_image_from_url(image_url: str, *, r2_prefix: str = "digest/article-images") -> str:
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
    r2_key = f"{r2_prefix}/{sha[:2]}/{sha}{suffix}"
    cdn_url = f"https://{settings.r2_domain}/{r2_key}"

    client = _get_client()
    try:
        client.head_object(Bucket=settings.r2_bucket, Key=r2_key)
        logger.info("R2 image cache hit: %s", cdn_url)
        return cdn_url
    except ClientError:
        pass

    return upload_bytes(data, r2_key, content_type)

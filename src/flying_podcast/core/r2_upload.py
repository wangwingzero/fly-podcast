"""Upload files to Cloudflare R2 (S3-compatible)."""
from __future__ import annotations

from pathlib import Path

import boto3
from botocore.config import Config

from flying_podcast.core.config import settings
from flying_podcast.core.logging_utils import get_logger

logger = get_logger("r2")

_CONTENT_TYPES = {
    ".mp3": "audio/mpeg",
    ".pdf": "application/pdf",
    ".html": "text/html; charset=utf-8",
    ".json": "application/json",
    ".jpg": "image/jpeg",
    ".png": "image/png",
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

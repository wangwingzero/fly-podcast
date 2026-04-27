from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env", override=False)


def _content_type(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".html":
        return "text/html; charset=utf-8"
    if suffix == ".json":
        return "application/json"
    if suffix == ".png":
        return "image/png"
    return "application/octet-stream"


def _upload_file(client, bucket: str, source: Path, key: str, *, cache_control: str = "") -> bool:
    if not source.exists():
        print(f"skip missing: {source}")
        return False
    extra = {"ContentType": _content_type(source)}
    if cache_control:
        extra["CacheControl"] = cache_control
    client.upload_file(str(source), bucket, key, ExtraArgs=extra)
    print(f"uploaded {source} -> s3://{bucket}/{key}")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload daily digest web outputs to R2.")
    parser.add_argument("--date", required=True, help="Target date, YYYY-MM-DD")
    args = parser.parse_args()

    access_key = os.getenv("R2_ACCESS_KEY_ID", "").strip()
    secret_key = os.getenv("R2_SECRET_ACCESS_KEY", "").strip()
    endpoint = os.getenv("R2_ENDPOINT", "").strip()
    bucket = os.getenv("R2_BUCKET", "ccar-pdfs").strip()

    if not (access_key and secret_key and endpoint and bucket):
        print("R2 not configured; skip upload")
        return 0

    try:
        import boto3
    except ImportError:
        print("boto3 is not installed", file=sys.stderr)
        return 2

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    uploaded = 0
    uploaded += _upload_file(
        client,
        bucket,
        ROOT / "data" / "output" / f"web_{args.date}.html",
        f"digest/web_{args.date}.html",
        cache_control="public, max-age=86400",
    )
    uploaded += _upload_file(
        client,
        bucket,
        ROOT / "static" / "copyright.html",
        "digest/copyright.html",
        cache_control="public, max-age=604800",
    )
    uploaded += _upload_file(
        client,
        bucket,
        ROOT / "static" / "beian_icon.png",
        "digest/beian_icon.png",
        cache_control="public, max-age=2592000",
    )
    uploaded += _upload_file(
        client,
        bucket,
        ROOT / "data" / "history" / "recent_published.json",
        "history/recent_published.json",
    )

    print(f"R2 upload complete: {uploaded} file(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

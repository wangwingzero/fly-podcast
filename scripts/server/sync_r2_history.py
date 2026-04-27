from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env", override=False)


def _is_missing_object_error(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return False
    error = response.get("Error", {})
    if not isinstance(error, dict):
        return False
    return str(error.get("Code", "")).lower() in {"404", "nosuchkey", "notfound"}


def _download_recent_published(client: Any, bucket: str, destination: Path, *, dry_run: bool = False) -> bool:
    key = "history/recent_published.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    if dry_run:
        print(f"dry-run: would download s3://{bucket}/{key} -> {destination}")
        return False
    try:
        client.download_file(bucket, key, str(destination))
    except Exception as exc:  # noqa: BLE001
        if _is_missing_object_error(exc):
            print(f"no R2 history file yet: s3://{bucket}/{key}")
            return False
        print(f"warning: failed to download R2 history: {exc}", file=sys.stderr)
        return False
    print(f"downloaded s3://{bucket}/{key} -> {destination}")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync daily digest history from R2.")
    parser.add_argument("--dry-run", action="store_true", help="Print the download action without contacting R2.")
    args = parser.parse_args()

    access_key = os.getenv("R2_ACCESS_KEY_ID", "").strip()
    secret_key = os.getenv("R2_SECRET_ACCESS_KEY", "").strip()
    endpoint = os.getenv("R2_ENDPOINT", "").strip()
    bucket = os.getenv("R2_BUCKET", "ccar-pdfs").strip()

    if not (access_key and secret_key and endpoint and bucket):
        print("R2 not configured; skip history sync")
        return 0

    try:
        import boto3
    except ImportError:
        print("boto3 is not installed; skip history sync", file=sys.stderr)
        return 0

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    _download_recent_published(
        client,
        bucket,
        ROOT / "data" / "history" / "recent_published.json",
        dry_run=args.dry_run,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

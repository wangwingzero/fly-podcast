from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env", override=False)


def _static_root() -> Path:
    configured = os.getenv("STATIC_ROOT", "").strip()
    return Path(configured or "/www/wwwroot/flighttoolbox-static/current")


def _copy_file(source: Path, static_root: Path, key: str) -> bool:
    if not source.exists():
        print(f"skip missing: {source}")
        return False
    destination = static_root / key
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)
    print(f"published {source} -> {destination}")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish daily digest outputs to the self-hosted static site.")
    parser.add_argument("--date", required=True, help="Target date, YYYY-MM-DD")
    args = parser.parse_args()

    static_root = _static_root()
    published = 0
    published += _copy_file(
        ROOT / "data" / "output" / f"web_{args.date}.html",
        static_root,
        f"digest/web_{args.date}.html",
    )
    published += _copy_file(
        ROOT / "static" / "copyright.html",
        static_root,
        "digest/copyright.html",
    )
    published += _copy_file(
        ROOT / "static" / "beian_icon.png",
        static_root,
        "digest/beian_icon.png",
    )
    published += _copy_file(
        ROOT / "data" / "history" / "recent_published.json",
        static_root,
        "history/recent_published.json",
    )

    print(f"static publish complete: {published} file(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

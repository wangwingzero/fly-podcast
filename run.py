from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env", override=False)

SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from flying_podcast.core.config import ensure_dirs
from flying_podcast.core.logging_utils import get_logger
from flying_podcast.stages.compose import run as compose
from flying_podcast.stages.ingest import run as ingest
from flying_podcast.stages.notify import run as notify
from flying_podcast.stages.publish import run as publish
from flying_podcast.stages.rank import run as rank
from flying_podcast.stages.verify import run as verify

logger = get_logger("run")


STAGES = {
    "ingest": ingest,
    "rank": rank,
    "compose": compose,
    "verify": verify,
    "publish": publish,
    "notify": notify,
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Flying Podcast Daily News Pipeline")
    parser.add_argument("stage", choices=[*STAGES.keys(), "all"])
    parser.add_argument("--date", dest="date", default=datetime.now().strftime("%Y-%m-%d"))
    args = parser.parse_args()

    ensure_dirs()

    if args.stage == "all":
        for name in ["ingest", "rank", "compose", "verify", "publish", "notify"]:
            logger.info("Running stage: %s", name)
            STAGES[name](args.date)
        return

    STAGES[args.stage](args.date)


if __name__ == "__main__":
    main()

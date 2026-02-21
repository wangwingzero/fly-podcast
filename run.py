from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env", override=False)

SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from flying_podcast.core.config import ensure_dirs
from flying_podcast.core.logging_utils import get_logger
from flying_podcast.core.time_utils import beijing_today_str
from flying_podcast.stages.compose import run as compose
from flying_podcast.stages.ingest import run as ingest
from flying_podcast.stages.notify import run as notify
from flying_podcast.stages.podcast import run as podcast
from flying_podcast.stages.podcast_inbox import run as podcast_inbox
from flying_podcast.stages.publish import run as publish
from flying_podcast.stages.publish_podcast import run as publish_podcast
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
    "podcast": podcast,
    "podcast-inbox": podcast_inbox,
    "publish-podcast": publish_podcast,
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Flying Podcast Daily News Pipeline")
    parser.add_argument("stage", choices=[*STAGES.keys(), "all"])
    parser.add_argument("--date", dest="date", default=beijing_today_str())
    parser.add_argument("--pdf", dest="pdf", default=None, help="PDF file path (for podcast stage)")
    parser.add_argument("--local-only", dest="local_only", action="store_true",
                        help="Only process PDFs in inbox/pending/ (for podcast-inbox)")
    parser.add_argument("--dry-run", dest="dry_run_flag", action="store_true",
                        help="Show what would be processed without generating (for podcast-inbox)")
    parser.add_argument("--podcast-dir", dest="podcast_dir", default=None,
                        help="Specific podcast output dir (for publish-podcast)")
    args = parser.parse_args()

    ensure_dirs()

    if args.stage == "all":
        for name in ["ingest", "rank", "compose", "verify", "publish", "notify"]:
            logger.info("Running stage: %s", name)
            STAGES[name](args.date)
        return

    if args.stage == "podcast":
        podcast(args.date, pdf_path=args.pdf)
        return

    if args.stage == "podcast-inbox":
        podcast_inbox(args.date, local_only=args.local_only, dry_run=args.dry_run_flag)
        return

    if args.stage == "publish-podcast":
        publish_podcast(args.date, podcast_dir=args.podcast_dir)
        return

    STAGES[args.stage](args.date)


if __name__ == "__main__":
    main()

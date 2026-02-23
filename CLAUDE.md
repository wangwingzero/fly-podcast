# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Automated daily international aviation news digest pipeline for airline staff. Runs on GitHub Actions, publishes 10 curated international news items daily to a WeChat Official Account. Also includes a podcast pipeline that converts CAAC regulatory PDFs into two-host dialogue audio. Written in Python 3.11+. Foreign airline names are kept in English (e.g., Delta, United, Lufthansa, Emirates).

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run full news pipeline (ingest → rank → compose → verify → publish → notify)
python run.py all

# Run individual news stages
python run.py ingest [--date YYYY-MM-DD]
python run.py rank [--date YYYY-MM-DD]
python run.py compose [--date YYYY-MM-DD]
python run.py verify [--date YYYY-MM-DD]
python run.py publish [--date YYYY-MM-DD]
python run.py notify [--date YYYY-MM-DD]

# Podcast pipeline
python run.py podcast --pdf path/to/file.pdf         # single PDF → dialogue → TTS → MP3
python run.py podcast-script --pdf path/to/file.pdf  # PDF → script.json + cover + HTML (stops before TTS)
python run.py podcast-audio --dir data/output/podcast/xxx/  # script.json → TTS → MP3
python run.py podcast-inbox                           # batch process CCAR docs
python run.py podcast-inbox --local-only              # only process data/podcast_inbox/pending/
python run.py podcast-inbox --dry-run                 # preview without generating
python run.py publish-podcast [--date YYYY-MM-DD]    # publish podcast to WeChat drafts

# Podcast Studio GUI (Rust)
cd podcast-studio && cargo run                        # dev mode
cd podcast-studio && cargo build --release            # build release exe (~5MB)

# Tests
pytest                                                        # all tests
pytest tests/test_rank_pilot_relevance.py                     # single file
pytest tests/test_rank_pilot_relevance.py::test_accepts_caac  # single test
pytest -v                                                     # verbose
```

No linter or formatter is configured.

## Architecture

### News Pipeline Stages

Six sequential stages communicate via JSON files in `data/`:

```
ingest → rank → compose → verify → publish → notify
```

- **ingest** (`stages/ingest.py`): Collects news from RSS feeds and website scrapers. Outputs `data/raw/YYYY-MM-DD.json`.
- **rank** (`stages/rank.py`): Filters (blocked domains, hard-reject keywords, relevance checks, pilot-relevance), scores, deduplicates, applies quotas (max 3 per source, tier A ratio). Outputs `data/processed/ranked_YYYY-MM-DD.json`.
- **compose** (`stages/compose.py`): LLM-powered summarization into structured entries (conclusion, facts, impact, citations). Uses `concurrent.futures` to parallelize LLM calls. Outputs `data/processed/composed_YYYY-MM-DD.json`.
- **verify** (`stages/verify.py`): Quality gates (citation validity, sensational titles, sensitive content, source conflicts, duplicates). Sets decision to `auto_publish` or `hold`. Outputs `data/processed/quality_YYYY-MM-DD.json`.
- **publish** (`stages/publish.py`): Renders HTML/Markdown, optionally publishes to WeChat. Outputs to `data/output/`.
- **notify** (`stages/notify.py`): Sends email report + webhook alert with results.

Each stage function signature: `run(target_date: str | None = None) -> Path`.

### Podcast Pipeline

Separate from the news pipeline; converts CAAC regulatory PDFs into two-host audio dialogues.

```
PDF → pdfplumber text extraction (max 30k chars)
    → LLM dialogue generation (1200-1500 words, 25-35 turns)
    → DashScope TTS (Cherry=千羽, Ethan=虎机长)
    → MP3 + JSON metadata + HTML preview
```

- **podcast** (`stages/podcast.py`): Single PDF → dialogue → TTS → MP3. Split into sub-stages:
  - `run_script()`: PDF → text extraction → LLM dialogue → script.json + dialogue.html + cover.jpg
  - `run_audio()`: script.json → TTS synthesis → MP3 concatenation
  - `run()`: Full pipeline (calls `run_script()` then `run_audio()`, used by GitHub Actions)
- **podcast_inbox** (`stages/podcast_inbox.py`): Batch processing via CCAR-workflow integration. Auto-fetches pilot-relevant docs (categories 13/14/15), filters by Part 121 relevance (rule-based + LLM two-layer filter in `core/pilot_filter.py`), deduplicates by URL + file hash. Inbox state tracked in `data/podcast_inbox/processed.json`.
- **publish_podcast** (`stages/publish_podcast.py`): Uploads finished podcast MP3 to R2 and publishes to WeChat drafts.

### Podcast Studio GUI (`podcast-studio/`)

Rust desktop application (egui/eframe) providing a 5-step timeline interface for interactive podcast production. Calls Python backend via `std::process::Command`. Steps: Select PDF → Generate Script → Edit Script → Generate Audio → Publish. Key modules:
- `app.rs`: Main UI state and step content rendering
- `pipeline.rs`: 5-step state machine (`Pending → Running → Done/Failed`)
- `runner.rs`: Subprocess management (spawn Python, stream stdout/stderr via channels)
- `widgets/timeline.rs`: Vertical timeline UI component with status indicators

### Core Modules (`core/`)

- **config.py**: `@dataclass(frozen=True)` Settings singleton populated from env vars. Access via `from flying_podcast.core.config import settings`.
- **models.py**: Dataclasses — `NewsItem`, `DigestEntry`, `DailyDigest`, `QualityReport`. All implement `to_dict()`.
- **llm_client.py**: OpenAI-compatible HTTP client (no SDK). Auto-detects Anthropic native API by key prefix (`sk-ant-`), URL pattern, or model name (`claude`). Retries with exponential backoff. Extracts JSON from markdown code blocks as fallback.
- **tts_client.py**: DashScope TTS client (`qwen3-tts-instruct-flash` model). 2000-char limit per request; auto-segments longer text.
- **pilot_filter.py**: Two-layer Part 121 relevance filter — rule-based (doc prefix + keyword) → LLM judgment for borderline cases.
- **scoring.py**: Weighted quality scoring — 30% factual + 35% relevance + 15% authority + 10% timeliness + 10% readability.
- **image_gen.py**: Cover image sourcing with fallback chain: Unsplash → Pixabay → Gemini → Grok.
- **wechat.py**: WeChat Official Account API integration.
- **time_utils.py**: Beijing timezone (`UTC+8`) helpers — `beijing_today_str()`, `beijing_now()`.
- **io_utils.py**: JSON/YAML/text file helpers.
- **logging_utils.py**: Structured logging setup; provides `get_logger()`.
- **email_notify.py**: Email notification sender.

### Configuration Files (`config/`)

- **sources.yaml**: News sources with `id`, `name`, `url`, `type` (rss/web), `source_tier` (A/B/C), `fetch_mode`, `link_patterns`.
- **keywords.yaml**: `relevance_keywords`, `pilot_signal_keywords`, `hard_reject_words`, `sensitive_keywords`, `sensational_words`, `blocked_domains`.

### Key Constraints

| Parameter | Default | Env Var |
|-----------|---------|---------|
| Article count | 10 | `TARGET_ARTICLE_COUNT` |
| Min Tier A ratio | 70% | `MIN_TIER_A_RATIO` |
| Quality threshold | 80 | `QUALITY_THRESHOLD` |
| Max per source | 3 | `MAX_ENTRIES_PER_SOURCE` |
| Dry run | true | `DRY_RUN` |

### Web Parser Registry (`stages/web_parser_registry.py`)

Site-specific HTML parsers for non-RSS sources (IATA, FAA, Airbus, Boeing, FlightGlobal, Reuters, NTSB, EASA). Each parser extracts `ParsedWebEntry` objects. Fetch modes: `requests` → `playwright` → `nodriver` with auto-fallback.

### Data Flow

All inter-stage communication is via dated JSON files under `data/`. No database. Deduplication is multi-layered: by ID (SHA256 of title + source), by normalized URL, by title similarity, and cross-day via `data/history/recent_published.json` (last 7 days). Seen IDs tracked in `data/history/seen_ids.txt`.

### Import Path

`run.py` and `tests/conftest.py` both add `src/` to `sys.path`. The package is `flying_podcast` under `src/flying_podcast/`. All modules use `from __future__ import annotations`.

### Conventions

- **Logging**: Use `logger = get_logger("module_name")` (not `__name__`).
- **Timestamps**: Store as ISO 8601 with UTC internally; convert to Beijing time for display. Use `time_utils` helpers for date strings.
- **New config values**: Add to `Settings` dataclass in `config.py` with env var helper (`_env_bool`, `_env_int`, `_env_float`), update `.env.example`.
- **New stages**: Follow `def run(target_date: str | None = None) -> Path` signature, register in `run.py` STAGES dict.
- **Domain exceptions**: Define as `class MyError(RuntimeError)` near the throwing code (e.g., `LLMError`, `WeChatPublishError`, `TTSError`).
- **Dataclasses**: Use `@dataclass` with `to_dict()` method for JSON serialization; `field(default_factory=list)` for mutable defaults.

### CI/CD

GitHub Actions workflows in `.github/workflows/` — one per stage, running on UTC cron schedule. All support `workflow_dispatch` for manual runs. Each uses `ubuntu-latest` with Python 3.11, uploads stage output as artifacts.

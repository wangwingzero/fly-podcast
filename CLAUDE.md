# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Automated daily aviation news digest pipeline for airline staff. Runs on GitHub Actions, publishes 10 curated news items daily to a WeChat Official Account. Written in Python 3.11+.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run full pipeline
python run.py all

# Run individual stages (in order)
python run.py ingest [--date YYYY-MM-DD]
python run.py rank [--date YYYY-MM-DD]
python run.py compose [--date YYYY-MM-DD]
python run.py verify [--date YYYY-MM-DD]
python run.py publish [--date YYYY-MM-DD]
python run.py notify [--date YYYY-MM-DD]

# Tests
pytest                                                        # all tests
pytest tests/test_rank_pilot_relevance.py                     # single file
pytest tests/test_rank_pilot_relevance.py::test_accepts_caac  # single test
pytest -v                                                     # verbose
```

No linter or formatter is configured.

## Architecture

### Pipeline Stages

Six sequential stages communicate via JSON files in `data/`:

```
ingest → rank → compose → verify → publish → notify
```

- **ingest** (`stages/ingest.py`): Collects news from RSS feeds and website scrapers. Outputs `data/raw/YYYY-MM-DD.json`.
- **rank** (`stages/rank.py`): Filters (blocked domains, hard-reject keywords, relevance checks, pilot-relevance), scores, deduplicates, applies quotas (60% domestic, max 3 per source, section diversity). Outputs `data/processed/ranked_YYYY-MM-DD.json`.
- **compose** (`stages/compose.py`): LLM-powered summarization into structured entries (conclusion, facts, impact, citations). Outputs `data/processed/composed_YYYY-MM-DD.json`.
- **verify** (`stages/verify.py`): 12 quality gates (citation validity, sensational titles, sensitive content, source conflicts, duplicates). Produces quality report and sets decision to `auto_publish` or `hold`. Outputs `data/processed/quality_YYYY-MM-DD.json`.
- **publish** (`stages/publish.py`): Renders HTML/Markdown, optionally publishes to WeChat. Outputs to `data/output/`.
- **notify** (`stages/notify.py`): Sends webhook alert with results.

Each stage function signature: `run(target_date: str | None = None) -> Path`.

### Core Modules (`core/`)

- **models.py**: Dataclasses — `NewsItem`, `DigestEntry`, `DailyDigest`, `QualityReport`
- **config.py**: `Settings` dataclass populated from environment variables. Singleton `settings` instance used throughout.
- **scoring.py**: Scoring functions — `tier_score`, `recency_score`, `relevance_score`, `readability_score`, `weighted_quality`
- **llm_client.py**: OpenAI-compatible HTTP client (no SDK dependency)
- **wechat.py**: WeChat Official Account API integration
- **io_utils.py**: JSON/YAML file helpers
- **logging_utils.py**: Logging setup

### Configuration Files (`config/`)

- **sources.yaml**: 30+ news sources with tier (A/B/C), region (domestic/international), type (rss/web), fetch_mode, link_patterns
- **keywords.yaml**: Section keywords, pilot-relevant terms, hard-reject words, sensitive keywords, blocked domains

### Key Constraints

| Parameter | Default | Env Var |
|-----------|---------|---------|
| Article count | 10 | `TARGET_ARTICLE_COUNT` |
| Domestic ratio | 60% | `DOMESTIC_RATIO` |
| Min Tier A ratio | 70% | `MIN_TIER_A_RATIO` |
| Quality threshold | 80 | `QUALITY_THRESHOLD` |
| Max per source | 3 | `MAX_ENTRIES_PER_SOURCE` |
| Dry run | true | `DRY_RUN` |

### Web Parser Registry (`stages/web_parser_registry.py`)

Site-specific HTML parsers for non-RSS sources (CAAC, CARNOC, IATA, FAA, Airbus, Boeing, FlightGlobal). Each parser extracts `ParsedWebEntry` objects. Fetch modes: `requests` → `playwright` → `nodriver` with auto-fallback.

### Data Flow

All inter-stage communication is via dated JSON files under `data/`. No database. Deduplication history tracked in `data/history/seen_ids.txt`.

### CI/CD

Six GitHub Actions workflows in `.github/workflows/` run on UTC schedule (Beijing time: ingest every 2h, rank 06:00, compose 06:30, verify 07:00, publish 08:00, notify on publish completion). All support `workflow_dispatch` for manual runs.

### Import Path

`run.py` and `tests/conftest.py` both add `src/` to `sys.path`. The package is `flying_podcast` under `src/flying_podcast/`.

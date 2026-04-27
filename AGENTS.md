# Repository Guidelines

## Project Structure & Module Organization
The main application is a Python pipeline under `src/flying_podcast/`. Core integrations and shared utilities live in `src/flying_podcast/core/`, while executable stages such as `ingest`, `rank`, `compose`, `verify`, `publish`, and podcast workflows live in `src/flying_podcast/stages/`. The CLI entrypoint is `run.py`. Tests are in `tests/`, with HTML fixtures in `tests/fixtures/web/`. Runtime configs live in `config/`, JSON schemas in `schemas/`, static assets in `assets/` and `static/`, and generated outputs in `data/`, `artifacts*/`, and `tmp/`. The optional desktop GUI is a separate Rust crate in `podcast-studio/`.

## Build, Test, and Development Commands
Create a virtual environment and install dependencies with `python -m venv .venv` and `pip install -r requirements.txt`. Run the full daily pipeline with `python run.py all`. Run a single stage with commands such as `python run.py ingest --date 2026-03-09` or `python run.py podcast --pdf podcast_pdfs/example.pdf`. Execute tests with `pytest` or narrow scope with `pytest tests/test_compose_llm.py`. For the desktop app, use `cd podcast-studio && cargo run` or `podcast-studio\dev.bat` on Windows.

## Coding Style & Naming Conventions
Follow the existing Python style: 4-space indentation, type hints where practical, `snake_case` for functions/modules, and small stage-focused modules. Keep new pipeline code under `src/flying_podcast/...` rather than top-level scripts. Prefer explicit, descriptive filenames like `test_publish_images.py` and config keys in uppercase environment-variable form. Preserve the current Rust style in `podcast-studio/src/`: `snake_case` modules and clear `struct`/`impl` boundaries.

## Testing Guidelines
Use `pytest` for all Python tests. Name new tests `test_<behavior>.py`, and keep fixtures close to the feature they support under `tests/fixtures/`. Add regression tests for ranking, composing, verification, and publishing changes, especially when output JSON shape or source parsing changes. Run targeted tests before a PR, then a full `pytest` pass for cross-stage changes.

## Commit & Pull Request Guidelines
Recent history uses short imperative summaries such as `Tighten ops story signal matching` and `Skip same-day recent dedup on rerun`. Keep commits focused and descriptive. PRs should explain the workflow impact, note any config or schema changes, link related issues, and include screenshots or sample output when UI, WeChat rendering, or generated dialogue/podcast artifacts change.

## Security & Configuration Tips
Load secrets from `.env`; do not commit real keys. Start from `.env.example`. The repo defaults to dry-run behavior unless publish credentials are configured, so keep that safety behavior intact when changing `src/flying_podcast/core/config.py`.

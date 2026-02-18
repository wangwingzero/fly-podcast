from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[3]


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return float(value) if value else default


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value else default


@dataclass(frozen=True)
class Settings:
    app_env: str = os.getenv("APP_ENV", "dev")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    dry_run: bool = _env_bool("DRY_RUN", True)

    target_article_count: int = _env_int("TARGET_ARTICLE_COUNT", 10)
    domestic_ratio: float = _env_float("DOMESTIC_RATIO", 0.6)
    min_tier_a_ratio: float = _env_float("MIN_TIER_A_RATIO", 0.7)
    quality_threshold: float = _env_float("QUALITY_THRESHOLD", 80.0)
    allow_google_redirect_citation: bool = _env_bool("ALLOW_GOOGLE_REDIRECT_CITATION", False)
    strict_web_published_at: bool = _env_bool("STRICT_WEB_PUBLISHED_AT", True)
    max_entries_per_source: int = _env_int("MAX_ENTRIES_PER_SOURCE", 3)
    max_article_age_hours: int = _env_int("MAX_ARTICLE_AGE_HOURS", 72)

    llm_api_key: str = os.getenv("LLM_API_KEY", "")
    llm_base_url: str = os.getenv("LLM_BASE_URL", "")
    llm_model: str = os.getenv("LLM_MODEL", "")
    llm_max_tokens: int = _env_int("LLM_MAX_TOKENS", 6000)
    llm_temperature: float = _env_float("LLM_TEMPERATURE", 0.1)

    wechat_enable_publish: bool = _env_bool("WECHAT_ENABLE_PUBLISH", False)
    wechat_app_id: str = os.getenv("WECHAT_APP_ID", "")
    wechat_app_secret: str = os.getenv("WECHAT_APP_SECRET", "")
    wechat_author: str = os.getenv("WECHAT_AUTHOR", "飞行播客")
    wechat_thumb_media_id: str = os.getenv("WECHAT_THUMB_MEDIA_ID", "")
    wechat_proxy: str = os.getenv("WECHAT_PROXY", "")
    web_digest_base_url: str = os.getenv("WEB_DIGEST_BASE_URL", "")
    copyright_notice_url: str = os.getenv("COPYRIGHT_NOTICE_URL", "")

    alert_webhook_url: str = os.getenv("ALERT_WEBHOOK_URL", "")

    unsplash_access_key: str = os.getenv("UNSPLASH_ACCESS_KEY", "")
    unsplash_access_key_2: str = os.getenv("UNSPLASH_ACCESS_KEY_2", "")
    pixabay_api_key: str = os.getenv("PIXABAY_API_KEY", "")

    image_gen_api_key: str = os.getenv("IMAGE_GEN_API_KEY", "")
    image_gen_base_url: str = os.getenv("IMAGE_GEN_BASE_URL", "")
    image_gen_model: str = os.getenv("IMAGE_GEN_MODEL", "")
    image_gen_backup_api_key: str = os.getenv("IMAGE_GEN_BACKUP_API_KEY", "")
    image_gen_backup_base_url: str = os.getenv("IMAGE_GEN_BACKUP_BASE_URL", "")
    image_gen_backup_model: str = os.getenv("IMAGE_GEN_BACKUP_MODEL", "")

    sources_config: Path = ROOT_DIR / "config" / "sources.yaml"
    keywords_config: Path = ROOT_DIR / "config" / "keywords.yaml"

    raw_dir: Path = ROOT_DIR / "data" / "raw"
    processed_dir: Path = ROOT_DIR / "data" / "processed"
    history_dir: Path = ROOT_DIR / "data" / "history"
    output_dir: Path = ROOT_DIR / "data" / "output"


settings = Settings()


def ensure_dirs() -> None:
    for path in [settings.raw_dir, settings.processed_dir, settings.history_dir, settings.output_dir]:
        path.mkdir(parents=True, exist_ok=True)

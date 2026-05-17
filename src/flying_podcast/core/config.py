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

    # 0 means unlimited: publish every article that clears the value gates.
    target_article_count: int = _env_int("TARGET_ARTICLE_COUNT", 0)
    domestic_ratio: float = _env_float("DOMESTIC_RATIO", 0.0)
    min_tier_a_ratio: float = _env_float("MIN_TIER_A_RATIO", 0.0)
    quality_threshold: float = _env_float("QUALITY_THRESHOLD", 80.0)
    require_llm_for_publish: bool = _env_bool("REQUIRE_LLM_FOR_PUBLISH", True)
    allow_google_redirect_citation: bool = _env_bool("ALLOW_GOOGLE_REDIRECT_CITATION", False)
    strict_web_published_at: bool = _env_bool("STRICT_WEB_PUBLISHED_AT", True)
    max_entries_per_source: int = _env_int("MAX_ENTRIES_PER_SOURCE", 0)
    min_rank_score_for_compose: float = _env_float("MIN_RANK_SCORE_FOR_COMPOSE", 80.0)
    source_health_gate_enabled: bool = _env_bool("SOURCE_HEALTH_GATE_ENABLED", True)
    min_primary_industry_sources_ok: int = _env_int("MIN_PRIMARY_INDUSTRY_SOURCES_OK", 2)
    min_primary_industry_items: int = _env_int("MIN_PRIMARY_INDUSTRY_ITEMS", 3)
    # 每期 ranked 池中至少保留的 industry_novelty（趣闻类）数量，用于让飞行员
    # 看到首飞、纪念飞行、特殊任务等"非事故"内容；候选池没有时不强行凑数。
    min_novelty_articles: int = _env_int("MIN_NOVELTY_ARTICLES", 1)
    max_article_age_hours: int = _env_int("MAX_ARTICLE_AGE_HOURS", 48)
    max_tier_a_article_age_hours: int = _env_int(
        "MAX_TIER_A_ARTICLE_AGE_HOURS",
        _env_int("MAX_ARTICLE_AGE_HOURS", 48),
    )
    recent_published_days: int = _env_int("RECENT_PUBLISHED_DAYS", 14)
    # Publish 阶段最低发文条数，少于此值则 hold 不发，避免出现只有 1~2 条的尴尬日报
    min_publish_count: int = _env_int("MIN_PUBLISH_COUNT", 3)

    llm_api_key: str = os.getenv("LLM_API_KEY", "")
    llm_base_url: str = os.getenv("LLM_BASE_URL", "")
    llm_model: str = os.getenv("LLM_MODEL", "")
    llm_max_tokens: int = _env_int("LLM_MAX_TOKENS", 6000)
    llm_temperature: float = _env_float("LLM_TEMPERATURE", 0.1)
    llm_backup_api_key: str = os.getenv("LLM_BACKUP_API_KEY", "")
    llm_backup_base_url: str = os.getenv("LLM_BACKUP_BASE_URL", "")
    llm_backup_model: str = os.getenv("LLM_BACKUP_MODEL", "")
    llm_secondary_backup_api_key: str = os.getenv("LLM_SECONDARY_BACKUP_API_KEY", "")
    llm_secondary_backup_base_url: str = os.getenv("LLM_SECONDARY_BACKUP_BASE_URL", "")
    llm_secondary_backup_model: str = os.getenv("LLM_SECONDARY_BACKUP_MODEL", "")
    llm_fallback_api_key: str = os.getenv("LLM_FALLBACK_API_KEY", "")
    llm_fallback_base_url: str = os.getenv("LLM_FALLBACK_BASE_URL", "")
    llm_fallback_model: str = os.getenv("LLM_FALLBACK_MODEL", "")

    wechat_enable_publish: bool = _env_bool("WECHAT_ENABLE_PUBLISH", False)
    wechat_auto_publish: bool = _env_bool("WECHAT_AUTO_PUBLISH", False)
    wechat_app_id: str = os.getenv("WECHAT_APP_ID", "")
    wechat_app_secret: str = os.getenv("WECHAT_APP_SECRET", "")
    wechat_use_stable_token: bool = _env_bool("WECHAT_USE_STABLE_TOKEN", True)
    wechat_author: str = os.getenv("WECHAT_AUTHOR", "Global Aviation Digest")
    wechat_thumb_media_id: str = os.getenv("WECHAT_THUMB_MEDIA_ID", "")
    wechat_proxy: str = os.getenv("WECHAT_PROXY", "")
    web_digest_base_url: str = os.getenv("WEB_DIGEST_BASE_URL", "")
    copyright_notice_url: str = os.getenv("COPYRIGHT_NOTICE_URL", "")

    alert_webhook_url: str = os.getenv("ALERT_WEBHOOK_URL", "")

    dashscope_api_key: str = os.getenv("DASHSCOPE_API_KEY", "")
    mineru_token: str = os.getenv("MINERU", "")

    # TTS backend controls
    tts_enable_dashscope: bool = _env_bool("TTS_ENABLE_DASHSCOPE", False)
    tts_enable_edge: bool = _env_bool("TTS_ENABLE_EDGE", False)

    # qwen-tts2api (self-hosted, OpenAI-compatible endpoint)
    qwen_tts_url: str = os.getenv("QWEN_TTS_URL", "http://72.249.203.10:8825")
    qwen_tts_api_key: str = os.getenv("QWEN_TTS_API_KEY", "")

    # Podcast extra prompt (e.g. holiday greetings)
    podcast_greeting: str = os.getenv("PODCAST_GREETING", "")

    # CCAR-workflow integration (podcast inbox)
    ccar_data_path: str = os.getenv("CCAR_DATA_PATH", "D:/CCAR-workflow/data/regulations.json")
    ccar_downloads_path: str = os.getenv("CCAR_DOWNLOADS_PATH", "D:/CCAR-workflow/downloads")
    static_root: str = os.getenv("STATIC_ROOT", "")
    static_public_base_url: str = os.getenv("STATIC_PUBLIC_BASE_URL", "")
    podcast_inbox_dir: Path = ROOT_DIR / "data" / "podcast_inbox"

    email_user: str = os.getenv("EMAIL_USER", "")
    email_pass: str = os.getenv("EMAIL_PASS", "")
    email_to: str = os.getenv("EMAIL_TO", "")
    email_sender: str = os.getenv("EMAIL_SENDER", "Global Aviation Digest")
    email_smtp_server: str = os.getenv("EMAIL_SMTP_SERVER", "")

    unsplash_access_key: str = os.getenv("UNSPLASH_ACCESS_KEY", "")
    unsplash_access_key_2: str = os.getenv("UNSPLASH_ACCESS_KEY_2", "")
    pixabay_api_key: str = os.getenv("PIXABAY_API_KEY", "")
    public_image_search_timeout_seconds: int = _env_int("PUBLIC_IMAGE_SEARCH_TIMEOUT_SECONDS", 5)
    web_image_search_budget_seconds: int = _env_int("WEB_IMAGE_SEARCH_BUDGET_SECONDS", 20)

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
    wechat_token_cache_path: Path = ROOT_DIR / "data" / "history" / "wechat_stable_token.json"


settings = Settings()


def ensure_dirs() -> None:
    for path in [settings.raw_dir, settings.processed_dir, settings.history_dir, settings.output_dir]:
        path.mkdir(parents=True, exist_ok=True)

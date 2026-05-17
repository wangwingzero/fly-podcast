from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "server" / "install_daily_cron.py"
    spec = importlib.util.spec_from_file_location("install_daily_cron", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_build_crontab_replaces_legacy_schedules():
    module = _load_module()
    existing = "\n".join(
        [
            "LANG=en_US.UTF-8",
            "# flying-podcast daily digest: Beijing 03:00 (server UTC 19:00), creates WeChat draft only",
            "0 19 * * *  flock -xn /www/server/cron/flying_podcast_daily.lock -c /www/server/cron/flying_podcast_daily >> /www/server/cron/flying_podcast_daily.log 2>&1",
            "",
        ]
    )

    updated = module.build_crontab(existing)

    assert "0 19 * * *" not in updated
    assert "server UTC 19:00" not in updated
    assert "# flying-podcast daily digest: Beijing 07:00, auto-publishes non-empty WeChat digest" in updated
    assert "0 7 * * *  flock -xn /www/server/cron/flying_podcast_daily.lock" in updated
    assert "0 3 * * *" not in updated
    assert "LANG=en_US.UTF-8" in updated


def test_build_crontab_is_idempotent():
    module = _load_module()
    first = module.build_crontab("LANG=en_US.UTF-8\n")
    second = module.build_crontab(first)

    assert second == first
    assert second.count("0 7 * * *") == 1


def test_build_wrapper_enables_wechat_auto_publish():
    module = _load_module()
    wrapper = module.build_wrapper("/www/wwwroot/flying-podcast")

    assert "export WECHAT_ENABLE_PUBLISH=true" in wrapper
    assert "export WECHAT_AUTO_PUBLISH=true" in wrapper
    assert "export WECHAT_PROXY=" in wrapper

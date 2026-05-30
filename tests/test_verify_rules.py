import json
import importlib
from types import SimpleNamespace

from flying_podcast.core.scoring import has_source_conflict
from flying_podcast.stages.verify import _llm_editor_review

verify_module = importlib.import_module("flying_podcast.stages.verify")


def test_source_conflict_detected():
    entry = {
        "title": "Airline profit expected to increase",
        "facts": ["report says revenue will decrease"],
    }
    assert has_source_conflict(entry) is True


def test_source_conflict_not_detected():
    entry = {
        "title": "Airline expands network",
        "facts": ["new routes launched"],
    }
    assert has_source_conflict(entry) is False


class _FakeClient:
    def __init__(self, payload=None):
        self.system_prompt = ""
        self.user_prompt = ""
        self.payload = payload or {"reviews": [{"id": "a1", "keep": True, "reason": "结构正常"}]}

    def complete_json(self, *, system_prompt: str, user_prompt: str, **kwargs):
        self.system_prompt = system_prompt
        self.user_prompt = user_prompt
        return type(
            "Resp",
            (),
            {"payload": self.payload},
        )()


def test_gossip_entry_skips_accident_without_major_impact_gate():
    entry = {
        "id": "gossip1",
        "section": "industry_gossip",
        "source_role": "accident_exception",
        "source_tier": "B",
        "title": "美调查人员恢复档案访问 此前因驾驶舱语音重构担忧暂停",
        "conclusion": "调查人员因担忧频谱分析可重建驾驶舱录音而临时限制档案访问。",
        "facts": ["美国调查人员已基本恢复档案系统访问权限。"],
        "body": "此次限制源于对频谱分析技术可重构机组语音的顾虑。\n划重点：老美这波操作，怕不是CSI看多了。",
        "citations": ["https://www.flightglobal.com/example"],
        "score_breakdown": {"factual": 90, "relevance": 90, "authority": 80, "timeliness": 90, "readability": 100},
    }
    role = verify_module._source_role_for_entry(entry)
    accident_terms = list(verify_module._DEFAULT_VERIFY_MAJOR_ACCIDENT_TERMS)
    visible = entry["title"] + entry["body"]
    assert role == "accident_exception"
    assert not verify_module._contains_any(visible, accident_terms)
    assert verify_module._is_gossip_entry(entry)
    should_block_accident = (
        role == "accident_exception"
        and not verify_module._contains_any(visible, accident_terms)
        and not verify_module._is_gossip_entry(entry)
    )
    assert should_block_accident is False


def test_llm_editor_overrides_rejection_for_gossip_entry():
    client = _FakeClient(
        payload={
            "reviews": [
                {
                    "id": "g1",
                    "keep": False,
                    "reason": "事故调查缺少停飞、监管或更大范围运行影响，不适合作为日报主体。",
                }
            ]
        }
    )
    entry = {
        "id": "g1",
        "section": "industry_gossip",
        "source_role": "accident_exception",
        "title": "美调查人员恢复档案访问",
        "conclusion": "调查人员因担忧可重建驾驶舱录音而临时限制档案访问。",
        "facts": ["41份档案仍在审查中。"],
        "body": "美国调查人员已基本恢复此前暂停的档案系统访问权限。",
    }
    blocked = _llm_editor_review([entry], client)
    assert blocked == []


def test_llm_editor_review_prompt_allows_humorous_highlight():
    client = _FakeClient()
    blocked = _llm_editor_review(
        [{
            "id": "a1",
            "title": "测试标题",
            "conclusion": "结论一句。",
            "facts": ["第一句事实。", "第二句事实。"],
            "body": "第一句事实。第二句事实。\n划重点：这波操作，机长群里肯定要聊两句。",
        }],
        client,
    )

    assert blocked == []
    assert "划重点" in client.system_prompt
    assert "严禁仅因为口语化" in client.system_prompt
    assert "技术增量" in client.system_prompt
    assert "运行增量" in client.system_prompt
    assert "第一句事实" in client.user_prompt
    assert "目标读者：一线民航飞行员" in client.system_prompt
    assert "签派" not in client.system_prompt


def test_llm_editor_review_keeps_high_value_ops_story_when_reason_is_only_too_thin():
    client = _FakeClient(
        payload={
            "reviews": [
                {
                    "id": "a1",
                    "keep": False,
                    "reason": "正文只有对标题的重复性概述，缺少时间、航班等基本新闻事实，内容过于空泛。",
                }
            ]
        }
    )
    blocked = _llm_editor_review(
        [{
            "id": "a1",
            "title": "American Airlines一架Airbus A321因Dark Cockpit放出RAT并备降巴尔的摩",
            "conclusion": "一架A321在飞行中出现Dark Cockpit后放出RAT并备降。",
            "facts": ["机组报告出现Dark Cockpit。", "飞机放出RAT并改降巴尔的摩。"],
            "body": "机组报告出现Dark Cockpit，随后放出RAT并备降巴尔的摩。",
        }],
        client,
    )

    assert blocked == []


def test_llm_editor_review_still_blocks_high_value_ops_story_for_hard_quality_failure():
    client = _FakeClient(
        payload={
            "reviews": [
                {
                    "id": "a1",
                    "keep": False,
                    "reason": "正文机翻严重且前后矛盾，不适合发布。",
                }
            ]
        }
    )
    blocked = _llm_editor_review(
        [{
            "id": "a1",
            "title": "American Airlines一架Airbus A321因Dark Cockpit放出RAT并备降巴尔的摩",
            "conclusion": "一架A321在飞行中出现Dark Cockpit后放出RAT并备降。",
            "facts": ["机组报告出现Dark Cockpit。", "飞机放出RAT并改降巴尔的摩。"],
            "body": "机组报告出现Dark Cockpit，随后放出RAT并备降巴尔的摩。",
        }],
        client,
    )

    assert blocked == ["a1"]


def test_llm_editor_review_overrides_accident_exception_rejection_when_high_value():
    """2026-05-20 update: accident_exception entries are no longer auto-blocked
    from override. As long as the entry hits a high-value ops keyword
    (go-around, divert, engine, pylon, airprox 等), the editor's vague
    rejection (e.g. "事故未触发更大范围影响") should be overridden.

    Reason: structural failures, midair separation losses, and airprox events
    are exactly the hard-core content the target audience expects, even when
    they don't trigger a fleet-wide grounding.
    """
    client = _FakeClient(
        payload={
            "reviews": [
                {
                    "id": "a1",
                    "keep": False,
                    "reason": "致命事故调查缺少停飞、监管或更大范围运行影响，不适合作为日报主体。",
                }
            ]
        }
    )
    blocked = _llm_editor_review(
        [{
            "id": "a1",
            "source_role": "accident_exception",
            "title": "Astra公务机不稳定进近后撞地",
            "conclusion": "机长未响应go-around呼叫，飞机撞地。",
            "facts": ["副驾驶呼叫go-around。", "飞机撞地并造成fatal事故。"],
            "body": "事故调查显示飞机处于不稳定进近，副驾驶呼叫go-around后飞机撞地。",
        }],
        client,
    )

    # go-around / 备降 / 发动机 等高价值运行关键词命中 → 改判保留
    assert blocked == []


def test_llm_editor_review_treats_known_accident_source_id_as_exception():
    """When the editor reason hits a HARD reject hint (机翻, 软文, 重复, etc.),
    even high-value ops entries get blocked. accident_exception sources are no
    longer auto-skipped from override — they go through the same gate.
    """
    client = _FakeClient(
        payload={
            "reviews": [
                {
                    "id": "a1",
                    "keep": False,
                    "reason": "正文机翻严重，无法读懂，前后矛盾。",
                }
            ]
        }
    )
    blocked = _llm_editor_review(
        [{
            "id": "a1",
            "source_id": "avherald_web",
            "title": "Astra公务机不稳定进近后撞地",
            "conclusion": "机长未响应go-around呼叫，飞机撞地。",
            "facts": ["副驾驶呼叫go-around。", "飞机撞地。"],
            "body": "事故调查显示飞机处于不稳定进近，副驾驶呼叫go-around后飞机撞地。",
        }],
        client,
    )

    # 机翻 / 读不通 命中 _HARD_REJECT_REASON_HINTS → 不改判，保持 blocked
    assert blocked == ["a1"]


def test_llm_editor_review_does_not_treat_emirates_as_rat_signal():
    client = _FakeClient(
        payload={
            "reviews": [
                {
                    "id": "a1",
                    "keep": False,
                    "reason": "只有区域局势与运行受扰的笼统表述，缺少具体航班、机场、空域限制或航司处置细节，信息增量不足。",
                }
            ]
        }
    )
    blocked = _llm_editor_review(
        [{
            "id": "a1",
            "title": "伊朗导弹回应导致中东航司运行受扰",
            "conclusion": "美国和以色列袭击伊朗后，伊朗发射导弹回应，已对中东航空公司运行造成严重干扰。",
            "facts": [
                "过去一周，美国和以色列袭击伊朗后，伊朗以发射导弹作出回应。",
                "受此影响，中东多家航空公司运行严重受扰，Emirates的航线网络运营也出现困难。",
            ],
            "body": "受此影响，中东多家航空公司运行严重受扰，Emirates的航线网络运营也出现困难。",
        }],
        client,
    )

    assert blocked == ["a1"]


def test_llm_editor_review_prompt_rejects_schedule_advisory_style_story():
    client = _FakeClient()
    _llm_editor_review(
        [{
            "id": "a1",
            "title": "American Airlines延长Philadelphia-Doha停飞并推迟JFK-Tel Aviv复航",
            "conclusion": "American Airlines将Philadelphia-Doha停飞延长至5月7日，并把JFK-Tel Aviv恢复时间推迟到4月23日。",
            "facts": [
                "调整与中东局势导致大部分空域仍基本无法使用有关。",
                "文中主要是航班暂停与恢复时间安排，以及旅客改签信息。",
            ],
            "body": "American Airlines更新了中东航线暂停与恢复时间安排，正文未提供NOTAM、程序限制或绕飞策略细节。",
        }],
        client,
    )

    assert "航司航线暂停/恢复" in client.system_prompt


def test_verify_skips_publish_when_llm_is_required_for_rules_content(monkeypatch, tmp_path):
    processed_dir = tmp_path / "processed"
    processed_dir.mkdir()
    keywords_path = tmp_path / "keywords.yaml"
    keywords_path.write_text(
        "sensitive_keywords: []\nsensational_words: []\n",
        encoding="utf-8",
    )
    (processed_dir / "composed_2026-04-26.json").write_text(
        json.dumps(
            {
                "date": "2026-04-26",
                "article_count": 1,
                "entries": [
                    {
                        "id": "a1",
                        "title": "FAA issues safety directive",
                        "conclusion": "FAA issues safety directive",
                        "facts": ["FAA issues safety directive", "Boeing 737 inspection required"],
                        "body": "FAA issues a safety directive for Boeing 737 inspection.",
                        "citations": ["https://www.faa.gov/newsroom/demo"],
                        "source_tier": "A",
                        "source_id": "faa_newsroom_web",
                        "score_breakdown": {
                            "factual": 90,
                            "relevance": 90,
                            "timeliness": 90,
                            "readability": 100,
                        },
                    }
                ],
                "meta": {"compose_mode": "rules"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    fake_settings = SimpleNamespace(
        processed_dir=processed_dir,
        keywords_config=keywords_path,
        target_article_count=1,
        min_tier_a_ratio=0.7,
        max_entries_per_source=3,
        allow_google_redirect_citation=False,
        quality_threshold=80,
        require_llm_for_publish=True,
    )
    monkeypatch.setattr(verify_module, "settings", fake_settings)
    monkeypatch.setattr(verify_module.OpenAICompatibleClient, "is_configured", staticmethod(lambda: False))

    out = verify_module.run("2026-04-26")
    report = json.loads(out.read_text(encoding="utf-8"))

    assert report["decision"] == "skip_publish"
    assert "llm_required_for_publish" in report["reasons"]
    assert "non_chinese_content" in report["reasons"]
    assert "all_entries_blocked" in report["reasons"]
    assert report["blocked_entry_ids"] == ["a1"]


def test_verify_target_zero_does_not_require_fixed_article_count(monkeypatch, tmp_path):
    processed_dir = tmp_path / "processed"
    processed_dir.mkdir()
    keywords_path = tmp_path / "keywords.yaml"
    keywords_path.write_text(
        "sensitive_keywords: []\nsensational_words: []\n",
        encoding="utf-8",
    )
    (processed_dir / "composed_2026-04-27.json").write_text(
        json.dumps(
            {
                "date": "2026-04-27",
                "article_count": 1,
                "entries": [
                    {
                        "id": "a1",
                        "title": "FAA发布跑道安全通报",
                        "conclusion": "FAA发布跑道安全通报。",
                        "facts": ["FAA发布跑道安全通报。", "通报要求运行单位复核风险控制。"],
                        "body": "FAA发布跑道安全通报，要求运行单位复核风险控制。划重点：少一条也不凑数。",
                        "citations": ["https://www.faa.gov/newsroom/demo"],
                        "source_tier": "A",
                        "source_id": "faa_newsroom_web",
                        "event_fingerprint": "fp-a1",
                        "score_breakdown": {
                            "factual": 90,
                            "relevance": 90,
                            "timeliness": 90,
                            "readability": 100,
                        },
                    }
                ],
                "meta": {"compose_mode": "llm_two_phase"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    fake_settings = SimpleNamespace(
        processed_dir=processed_dir,
        keywords_config=keywords_path,
        target_article_count=0,
        min_tier_a_ratio=0.0,
        max_entries_per_source=0,
        allow_google_redirect_citation=False,
        quality_threshold=80,
        require_llm_for_publish=False,
    )
    monkeypatch.setattr(verify_module, "settings", fake_settings)
    monkeypatch.setattr(verify_module.OpenAICompatibleClient, "is_configured", staticmethod(lambda: False))

    out = verify_module.run("2026-04-27")
    report = json.loads(out.read_text(encoding="utf-8"))

    assert report["decision"] == "auto_publish"
    assert "insufficient_articles" not in report["reasons"]
    assert "tier_a_ratio_too_low" not in report["reasons"]
    assert "source_concentration_exceeded" not in report["reasons"]


def test_verify_skips_publish_when_primary_sources_unhealthy_and_accident_only(monkeypatch, tmp_path):
    processed_dir = tmp_path / "processed"
    raw_dir = tmp_path / "raw"
    processed_dir.mkdir()
    raw_dir.mkdir()
    keywords_path = tmp_path / "keywords.yaml"
    keywords_path.write_text(
        "sensitive_keywords: []\nsensational_words: []\n",
        encoding="utf-8",
    )
    (raw_dir / "source_health_2026-05-16.json").write_text(
        json.dumps(
            [
                {
                    "source_id": "flightglobal_air_transport_cli",
                    "source_role": "primary_industry",
                    "status": "failed",
                    "item_count": 0,
                },
                {
                    "source_id": "aviation_week_air_transport_cli",
                    "source_role": "primary_industry",
                    "status": "empty",
                    "item_count": 0,
                },
                {
                    "source_id": "avherald_web",
                    "source_role": "accident_exception",
                    "status": "ok",
                    "item_count": 3,
                },
            ]
        ),
        encoding="utf-8",
    )
    (processed_dir / "composed_2026-05-16.json").write_text(
        json.dumps(
            {
                "date": "2026-05-16",
                "article_count": 1,
                "entries": [
                    {
                        "id": "a1",
                        "title": "Astra事故触发监管机队检查",
                        "conclusion": "事故后监管机构要求机队检查。",
                        "facts": ["监管机构要求fleet-wide inspection。", "FAA要求相关运营人复核。"],
                        "body": "事故后监管机构要求fleet-wide inspection，FAA要求相关运营人复核。",
                        "citations": ["https://avherald.com/h?article=demo"],
                        "source_tier": "B",
                        "source_id": "avherald_web",
                        "source_role": "accident_exception",
                        "event_fingerprint": "fp-a1",
                        "score_breakdown": {
                            "factual": 90,
                            "relevance": 90,
                            "timeliness": 90,
                            "readability": 100,
                        },
                    }
                ],
                "meta": {"compose_mode": "llm_two_phase"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    fake_settings = SimpleNamespace(
        processed_dir=processed_dir,
        raw_dir=raw_dir,
        keywords_config=keywords_path,
        target_article_count=0,
        min_tier_a_ratio=0.0,
        max_entries_per_source=0,
        allow_google_redirect_citation=False,
        quality_threshold=80,
        require_llm_for_publish=False,
        source_health_gate_enabled=True,
        min_primary_industry_sources_ok=2,
        min_primary_industry_items=3,
    )
    monkeypatch.setattr(verify_module, "settings", fake_settings)
    monkeypatch.setattr(verify_module.OpenAICompatibleClient, "is_configured", staticmethod(lambda: False))

    out = verify_module.run("2026-05-16")
    report = json.loads(out.read_text(encoding="utf-8"))

    assert report["decision"] == "skip_publish"
    assert "primary_source_health_below_threshold" in report["reasons"]
    assert "accident_only_fallback_digest" in report["reasons"]


def test_verify_blocks_mainland_subject_entry_with_distinct_reason(monkeypatch, tmp_path):
    processed_dir = tmp_path / "processed"
    processed_dir.mkdir()
    keywords_path = tmp_path / "keywords.yaml"
    keywords_path.write_text(
        "sensitive_keywords: []\nsensational_words: []\n",
        encoding="utf-8",
    )
    (processed_dir / "composed_2026-05-22.json").write_text(
        json.dumps(
            {
                "date": "2026-05-22",
                "article_count": 1,
                "entries": [
                    {
                        "id": "a1",
                        "title": "CAAC orders review of airline safety reporting",
                        "conclusion": "CAAC orders airlines to review safety reporting procedures.",
                        "facts": ["CAAC launched the review.", "Mainland airlines must respond this month."],
                        "body": "CAAC ordered mainland carriers to review safety reporting procedures.",
                        "citations": ["https://www.reuters.com/world/china/caac-review"],
                        "source_tier": "A",
                        "source_id": "reuters_aviation",
                        "event_fingerprint": "fp-a1",
                        "score_breakdown": {
                            "factual": 90,
                            "relevance": 90,
                            "timeliness": 90,
                            "readability": 100,
                        },
                    }
                ],
                "meta": {"compose_mode": "llm_two_phase"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    fake_settings = SimpleNamespace(
        processed_dir=processed_dir,
        keywords_config=keywords_path,
        target_article_count=0,
        min_tier_a_ratio=0.0,
        max_entries_per_source=0,
        allow_google_redirect_citation=False,
        quality_threshold=80,
        require_llm_for_publish=False,
    )
    monkeypatch.setattr(verify_module, "settings", fake_settings)
    monkeypatch.setattr(verify_module.OpenAICompatibleClient, "is_configured", staticmethod(lambda: False))

    out = verify_module.run("2026-05-22")
    report = json.loads(out.read_text(encoding="utf-8"))

    assert report["decision"] == "skip_publish"
    assert "mainland_china_subject" in report["reasons"]
    assert "all_entries_blocked" in report["reasons"]
    assert report["blocked_entry_ids"] == ["a1"]

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
from typing import Any
from urllib.parse import urlparse
from dateutil import parser as dt_parser

from flying_podcast.core.config import settings
from flying_podcast.core.io_utils import dump_json, load_json, load_yaml
from flying_podcast.core.logging_utils import get_logger
from flying_podcast.core.scoring import recency_score, relevance_score, tier_score
from flying_podcast.core.time_utils import beijing_today_str

logger = get_logger("rank")

_TITLE_TOKEN_RE = re.compile(r"[a-zA-Z][a-zA-Z0-9]+|[0-9]+|[\u4e00-\u9fff]")
_BAD_IMAGE_TOKEN_RE = re.compile(r"(^|[-_/])(?:logo|favicon|icon|placeholder|sprite|avatar|blank)(?:[-_.?/]|$)", re.IGNORECASE)
_TITLE_STOP_TOKENS = {
    "a", "an", "the", "and", "or", "of", "to", "in", "on", "at", "for",
    "with", "from", "after", "before", "into", "near",
    "about", "this", "that", "will", "has", "have", "had", "its", "their",
    "new", "first", "more", "says", "said", "seeks", "learn", "why",
}


def _is_usable_article_image_url(url: str) -> bool:
    clean = str(url or "").strip()
    if not clean.startswith(("http://", "https://")):
        return False
    lower = clean.lower()
    path = urlparse(lower).path
    if lower.startswith("data:image") or path.endswith(".svg"):
        return False
    if any("logo" in segment for segment in path.split("/")):
        return False
    return _BAD_IMAGE_TOKEN_RE.search(path) is None


def _merge_missing_image(primary: dict, duplicate: dict) -> None:
    if _is_usable_article_image_url(str(primary.get("image_url", ""))):
        return
    image_url = str(duplicate.get("image_url", "")).strip()
    if _is_usable_article_image_url(image_url):
        primary["image_url"] = image_url


def _merge_raw_images_by_url(rows: list[dict]) -> None:
    image_by_url: dict[str, str] = {}
    for row in rows:
        canonical_url = str(row.get("canonical_url") or row.get("url") or "").strip()
        image_url = str(row.get("image_url", "")).strip()
        if canonical_url and _is_usable_article_image_url(image_url):
            image_by_url.setdefault(canonical_url, image_url)
    if not image_by_url:
        return
    for row in rows:
        canonical_url = str(row.get("canonical_url") or row.get("url") or "").strip()
        if canonical_url in image_by_url and not _is_usable_article_image_url(str(row.get("image_url", ""))):
            row["image_url"] = image_by_url[canonical_url]
_TITLE_TOKEN_SYNONYMS = {
    "crash": "collision",
    "collided": "collision",
    "collides": "collision",
    "colliding": "collision",
}

_MAINLAND_CHINA_SUBJECT_KEYWORDS = (
    "caac",
    "civil aviation administration of china",
    "中国民航",
    "中国民用航空局",
    "民航局",
    "mainland china",
    "mainland chinese",
    "中国大陆",
    "内地",
    "china confirms order",
)

_MAINLAND_CHINA_CARRIER_KEYWORDS = (
    "air china",
    "china southern",
    "china eastern",
    "hainan airlines",
    "shenzhen airlines",
    "xiamen airlines",
    "sichuan airlines",
    "juneyao airlines",
    "spring airlines",
    "9 air",
    "9air",
    "okay air",
    "okay airways",
    "lucky air",
    "capital airlines",
    "tianjin airlines",
    "qingdao airlines",
    "chengdu airlines",
    "loong air",
    "suparna airlines",
    "grand china air",
    "中国国际航空",
    "国航",
    "中国南方航空",
    "南航",
    "中国东方航空",
    "东航",
    "海南航空",
    "海航",
    "深圳航空",
    "厦门航空",
    "四川航空",
    "吉祥航空",
    "春秋航空",
    "九元航空",
    "奥凯航空",
    "祥鹏航空",
    "首都航空",
    "天津航空",
    "青岛航空",
    "成都航空",
    "长龙航空",
    "金鹏航空",
)

_MAINLAND_CHINA_AIRPORT_KEYWORDS = (
    "beijing daxing",
    "beijing capital airport",
    "shanghai pudong",
    "shanghai hongqiao",
    "guangzhou baiyun",
    "shenzhen bao'an",
    "chengdu tianfu",
    "chengdu shuangliu",
    "xiamen gaoqi",
    "hangzhou xiaoshan",
    "中国大陆机场",
    "北京大兴",
    "北京首都机场",
    "上海浦东",
    "上海虹桥",
    "广州白云",
    "深圳宝安",
    "成都天府",
    "成都双流",
    "厦门高崎",
    "杭州萧山",
)

_MAINLAND_CHINA_SUBJECT_ACTION_KEYWORDS = (
    "orders review",
    "review of",
    "regulator",
    "regulatory",
    "safety reporting",
    "domestic network",
    "domestic route",
    "network",
    "capacity",
    "fleet plan",
    "fleet expansion",
    "delivery",
    "deliveries",
    "airport operations",
    "slot",
    "slots",
    "航线",
    "航班",
    "国内航线",
    "国内网络",
    "运力",
    "机队",
    "交付",
    "航权",
    "时刻",
    "机场运行",
    "监管",
    "安全通报",
)

_MAINLAND_CHINA_EXEMPT_TITLE_PATTERNS = (
    "china airlines",
    "taiwan",
    "eva air",
    "starlux",
    "mandarin airlines",
    "中华航空",
    "长荣航空",
    "星宇航空",
    "华信航空",
)

_SOFT_CONTENT_TITLE_PATTERNS = (
    "video interview",
    "press release",
)

_LOW_VALUE_INFRASTRUCTURE_TITLE_PATTERNS = (
    "airport construction plan",
    "breaks ground on",
    "engineering complex",
    "operations base",
)

_DEFAULT_PILOT_SIGNAL_KEYWORDS = [
    "aviation",
    "airline",
    "aircraft",
    "flight",
    "airport",
    "airspace",
    "runway",
    "notam",
    "atc",
    "faa",
    "easa",
    "iata",
    "icao",
    "ntsb",
    "airworthiness",
    "service bulletin",
    "ad ",
    "safety",
    "incident",
    "turbulence",
    "diversion",
    "go-around",
]

_DEFAULT_PILOT_ENTITY_KEYWORDS = [
    "faa",
    "easa",
    "icao",
    "iata",
    "ntsb",
    "boeing",
    "airbus",
    "delta",
    "united",
    "american airlines",
    "lufthansa",
    "emirates",
    "singapore airlines",
    "qatar airways",
    "british airways",
    "cathay pacific",
    "ryanair",
    "southwest",
]

_DEFAULT_HARD_REJECT_KEYWORDS = [
    "stock",
    "shares",
    "dividend",
    "market cap",
    "ipo",
    "earnings",
    "luxury",
    "lounge opening",
    "loyalty program",
    "frequent flyer",
    "meal service",
    "celebrity",
]

_DEFAULT_STRICT_HARD_REJECT_KEYWORDS = [
    "military aircraft",
    "fighter jet",
    "eurofighter",
    "navy",
    "army",
    "aircraft acquisition",
    "acquiring",
    "immigration enforcement",
    "ice vehicles",
    "press release announces partnership",
    "announces strategic partnership",
    "signs memorandum of understanding",
    "announces order for",
    "signs purchase agreement",
    "usaf",
    "raf spy plane",
    "trainer jet",
    "mrtt",
]

_DEFAULT_PILOT_DIRECT_OPERATION_KEYWORDS = [
    "incident",
    "accident",
    "emergency",
    "diversion",
    "go-around",
    "airworthiness",
    "directive",
    "service bulletin",
    "inspection",
    "fault",
    "failure",
    "defect",
    "crack",
    "engine issue",
    "engine fault",
    "smoke",
    "fire",
    "runway",
    "notam",
    "tfr",
    "atc",
    "airspace",
    "weather",
    "turbulence",
    "windshear",
    "icing",
    "volcanic ash",
    "closure",
    "closed",
    "restriction",
    "restricted",
    "grounding",
    "grounded",
    "training",
    "simulator",
    "fatigue",
    "procedure",
    "checklist",
    "mel",
    "etops",
    "cpdlc",
    "navigation",
    "gps interference",
    "spoofing",
    "jamming",
    "事故",
    "事件",
    "紧急",
    "备降",
    "复飞",
    "适航",
    "检查",
    "故障",
    "失效",
    "裂纹",
    "跑道",
    "航行通告",
    "空域",
    "天气",
    "颠簸",
    "风切变",
    "结冰",
    "火山灰",
    "关闭",
    "限制",
    "停飞",
    "训练",
    "疲劳",
    "程序",
    "检查单",
    "导航",
    "干扰",
]

_DEFAULT_PILOT_BACKGROUND_ONLY_KEYWORDS = [
    "new route",
    "new routes",
    "route launch",
    "network",
    "schedule",
    "timetable",
    "frequency",
    "frequencies",
    "additional flights",
    "extra flights",
    "more flights",
    "adds flights",
    "increase flights",
    "increases flights",
    "capacity",
    "expansion",
    "demand",
    "market",
    "fleet",
    "order",
    "orders",
    "delivery",
    "deliveries",
    "takes delivery",
    "receives",
    "received",
    "deploy",
    "deployment",
    "assigned to",
    "to serve",
    "service to",
    "nonstop service",
    "planned maintenance",
    "scheduled maintenance",
    "maintenance rotation",
    "aircraft assignment",
    "widebody assignment",
    "livery",
    "inaugural",
    "launch ceremony",
    "新航线",
    "增班",
    "加班",
    "航线安排",
    "航线调整",
    "时刻",
    "排班",
    "停航",
    "复航",
    "暂停运营",
    "恢复运营",
    "恢复时间",
    "恢复时间推迟",
    "延长停飞",
    "航线停飞",
    "机型安排",
    "执飞",
    "订单",
    "交付",
    "机队",
    "扩张",
    "计划维护",
    "定检",
    "停场",
    "首航",
]

_DEFAULT_PILOT_SCHEDULE_ADVISORY_KEYWORDS = [
    "flight suspension",
    "service suspension",
    "suspension of",
    "suspend flights",
    "suspends flights",
    "suspended flights",
    "return of flights",
    "return of service",
    "service return",
    "pause flights",
    "pauses flights",
    "paused flights",
    "resume flights",
    "resumes flights",
    "resumed flights",
    "resume service",
    "resumes service",
    "service resumption",
    "operations update",
    "travel waiver",
    "rebooking",
    "恢复运营",
    "恢复航班",
    "恢复时间",
    "恢复时间推迟",
    "暂停运营",
    "暂停航班",
    "停飞安排",
    "延长停飞",
    "航线停飞",
]

_DEFAULT_PILOT_SPECIFIC_OPS_KEYWORDS = [
    "notam",
    "tfr",
    "procedure",
    "procedures",
    "reroute",
    "rerouting",
    "alternate",
    "slot restriction",
    "airport closure",
    "runway closure",
    "gps interference",
    "spoofing",
    "jamming",
    "atc restriction",
    "航行通告",
    "程序限制",
    "航路变更",
    "绕飞",
    "备降机场",
    "跑道关闭",
    "机场关闭",
]

_DEFAULT_PILOT_PRIORITY_SOURCES = [
    "avherald_web",
    "asn_2026_web",
    "easa_ad_web",
    "faa_safo_web",
    "faa_info_web",
    "nasa_asrs_callback_web",
    "ntsb_press_web",
    "flightglobal_safety",
    "flightglobal_engines",
]

# 飞行员喜欢看的"新奇/趣闻"信号——新机型首飞、驾驶舱创新、人物故事、纪念飞行等。
# 命中这些词时允许放宽 background_only 的拒绝（首飞/退役飞行常带 "delivery"/"first flight" 字样）。
_DEFAULT_PILOT_NOVELTY_KEYWORDS = [
    # 新机型首飞 / 试飞 / 型号合格证里程碑
    "maiden flight",
    "first flight",
    "takes flight",
    "took flight",
    "takes to the sky",
    "took to the sky",
    "takes to the air",
    "took to the air",
    "test flight",
    "flight test",
    "flight testing",
    "prototype",
    "certification flight",
    "type certification",
    "amended type certificate",
    "type certificate",
    "proving flight",
    "first production",
    "first delivery",
    "delivery flight",
    "entered service",
    "enters service",
    "rollout",
    "rolls out",
    "rolled out",
    "first revenue flight",
    "inaugural flight",
    "首飞",
    "试飞",
    "原型机",
    "验证飞行",
    "取证试飞",
    "型号合格证",
    "首架交付",
    "投入运营",
    "首航",
    "总装下线",
    "下线",
    # 驾驶舱新技术
    "synthetic vision",
    "enhanced vision",
    "head-up display",
    "hud upgrade",
    "cockpit upgrade",
    "avionics upgrade",
    "new cockpit",
    "flight deck upgrade",
    "augmented reality cockpit",
    "ai copilot",
    "reduced crew",
    "single-pilot operations",
    "合成视景",
    "增强视景",
    "平视显示",
    "hud升级",
    "驾驶舱升级",
    "航电升级",
    "ai副驾",
    "单飞行员驾驶",
    # 飞行员/机组人物故事
    "first female captain",
    "first woman captain",
    "first black captain",
    "retiring captain",
    "captain retires",
    "veteran pilot",
    "record-setting pilot",
    "hero pilot",
    "heroic crew",
    "legendary captain",
    "退役机长",
    "首位女机长",
    "首位女飞行员",
    "首位华人机长",
    "传奇机长",
    "资深机长",
    "英雄机组",
    # 罕见飞行 / 世界纪录
    "world record",
    "record flight",
    "record-breaking flight",
    "longest flight",
    "polar flight",
    "rare diversion route",
    "special mission",
    "unique flight",
    "milestone flight",
    "世界纪录",
    "极地航班",
    "超长航班",
    "特殊任务",
    "里程碑航班",
    # 历史 / 纪念
    "farewell flight",
    "retirement flight",
    "final flight",
    "last flight",
    "decommissioning ceremony",
    "anniversary flight",
    "commemorative flight",
    "heritage flight",
    "retro livery",
    "special livery",
    "throwback livery",
    "museum aircraft",
    "告别飞行",
    "退役飞行",
    "最后一班",
    "纪念飞行",
    "周年飞行",
    "复古涂装",
    "怀旧涂装",
    "纪念涂装",
    "特别涂装",
]

# 吃瓜类：航司/机组/旅客争议、 viral 事件——读者最爱，排序时应给高分。
_DEFAULT_PILOT_GOSSIP_KEYWORDS = [
    "scandal", "controversy", "controversial", "viral", "went viral", "trending",
    "backlash", "outrage", "slammed", "criticised", "criticized",
    "fired", "sacked", "suspended", "resignation", "resign", "terminated",
    "disciplinary action", "under investigation",
    "unruly passenger", "disruptive passenger", "passenger altercation", "passenger fight",
    "kicked off", "removed from flight", "banned from", "deplaned",
    "intoxicated", "drunk pilot", "alcohol test", "caught on camera", "video shows",
    "social media", "tiktok", "instagram", "reddit",
    "drama", "feud", "embarrassing", "mishap", "blunder", "gaffe",
    "pilot error", "captain suspended", "first officer suspended",
    "inappropriate", "sexual harassment", "whistleblower",
    "public apology", "airline apologizes", "airline apologises",
    "争议", "丑闻", "辞退", "解聘", "停职", "网曝", "曝光", "热搜",
    "引热议", "引争议", "遭吐槽", "遭质疑", "翻车", "闹事", "冲突", "吵架", "殴打", "醉酒",
    "闹事旅客", "被赶下飞机", "公开道歉", "机组被停飞", "机长被停职",
]

# 即使在 novelty 路径下也要拒绝的话题（用户明确不感兴趣的前沿空中出行类）。
_DEFAULT_PILOT_NOVELTY_REJECT_KEYWORDS = [
    "evtol",
    "electric aircraft",
    "electric airliner",
    "hybrid-electric",
    "hydrogen aircraft",
    "hydrogen-powered",
    "supersonic airliner",
    "boom overture",
    "joby aviation",
    "archer aviation",
    "lilium",
    "vertiport",
    "air taxi",
    "urban air mobility",
    "电动飞机",
    "电动客机",
    "氢能飞机",
    "氢动力",
    "超音速客机",
    "城市空中出行",
    "空中出租车",
]

_DEFAULT_INDUSTRY_NEWS_KEYWORDS = [
    "air transport",
    "airline",
    "airlines",
    "airport",
    "airports",
    "airspace",
    "alliance",
    "joint venture",
    "fleet",
    "order",
    "orders",
    "delivery",
    "deliveries",
    "lessor",
    "leasing",
    "mro",
    "maintenance",
    "engine",
    "engines",
    "supply chain",
    "production",
    "oem",
    "regulator",
    "regulation",
    "certification",
    "slots",
    "saf",
    "sustainable aviation fuel",
    "labor",
    "strike",
    "union",
    "network",
]

_DEFAULT_MACRO_AVIATION_EFFECT_KEYWORDS = [
    "airline",
    "airlines",
    "airport",
    "airports",
    "aircraft",
    "airspace",
    "flight",
    "flights",
    "fleet",
    "faa",
    "easa",
    "iata",
    "icao",
    "regulator",
    "boeing",
    "airbus",
    "engine",
    "supply chain",
    "route",
    "routes",
    "capacity",
    "slots",
]

_DEFAULT_MAJOR_ACCIDENT_IMPACT_KEYWORDS = [
    "grounding",
    "grounded",
    "ground",
    "fleet-wide",
    "fleet wide",
    "inspection",
    "inspections",
    "airworthiness directive",
    "regulator",
    "regulators",
    "faa",
    "easa",
    "iata",
    "icao",
    "ntsb",
    "manufacturer",
    "boeing",
    "airbus",
    "engine",
    "engines",
    "airport closure",
    "runway closure",
    "airspace closure",
    "international airlines",
    "suspend operations",
    "suspended operations",
    "停飞",
    "适航指令",
    "监管",
    "检查",
    "空域关闭",
    "机场关闭",
]

# 用于 novelty 旁路的航空背景检测——确保人物故事/纪念飞行类
# 至少与航空场景挂钩（避免完全无关的"退役"故事被放行）。
_AVIATION_CONTEXT_HINTS = [
    # 通用航空场景词
    "aircraft",
    "airliner",
    "airline",
    "airlines",
    "airways",
    "pilot",
    "pilots",
    "captain",
    "first officer",
    "cockpit",
    "flight deck",
    "crew",
    "fleet",
    # 主要制造商
    "boeing",
    "airbus",
    "embraer",
    "atr",
    "bombardier",
    "comac",
    # 主要机型代号（避免纯数字 substring 误报，使用独特组合或字母前缀）
    "c919",
    "arj21",
    "777x",
    "a321xlr",
    "a350f",
    "a330",
    "a340",
    "a350",
    "a380",
    "boeing 737",
    "boeing 747",
    "boeing 757",
    "boeing 767",
    "boeing 777",
    "boeing 787",
    "747-8",
    "747-400",
    "737 max",
    "concorde",
    # 常见航司（entity_keywords 之外的补充；避免短缩写引发 substring 误报）
    "air france",
    "klm",
    "iberia",
    "swiss air",
    "qantas",
    "all nippon",
    "japan airlines",
    "korean air",
    "asiana",
    "china airlines",
    "eva air",
    "thai airways",
    "garuda",
    "vietnam airlines",
    "philippine airlines",
    "alitalia",
    "ita airways",
    "tap air",
    "finnair",
    "scandinavian airlines",
    "aer lingus",
    "icelandair",
    "air canada",
    "alaska airlines",
    "jetblue",
    "spirit airlines",
    "frontier airlines",
    "hawaiian airlines",
    # 中文场景词
    "飞行员",
    "机长",
    "副驾驶",
    "驾驶舱",
    "客机",
    "机组",
    "航空公司",
    "国航",
    "东航",
    "南航",
    "海航",
]

_PILOT_CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "safety_event": [
        "accident", "incident", "serious incident", "emergency", "rejected takeoff",
        "aborted takeoff", "go-around", "diversion", "turnback", "evacuation",
        "runway excursion", "runway incursion", "runway overrun", "loss of separation",
        "tcas", " ra ", "smoke", "fire", "engine failure", "hydraulic", "electrical",
        "pressurization", "unreliable air data", "incapacitated", "turbulence",
        "windshear", "icing", "fuel reserve", "tail strike", "malfunction",
        "事故", "事件", "严重征候", "紧急",
        "中断起飞", "复飞", "备降", "返航", "紧急撤离", "跑道侵入", "冲出跑道",
        "间隔丧失", "烟雾", "火警", "发动机", "液压", "增压", "颠簸", "风切变",
    ],
    "airworthiness_technical": [
        "airworthiness directive", "safety directive", "service bulletin", "inspection",
        "replacement", "defect", "crack", "fault", "failure", "rudder", "stabilizer",
        "flight controls", "fuel shut-off", "brake", "landing gear", "gear", "flap",
        "hud", "window malfunction", "engine smoke", "leap", "gtf", "适航指令", "安全通告", "检查", "更换",
        "故障", "失效", "裂纹", "飞控", "起落架",
    ],
    "ops_environment": [
        "notam", "tfr", "airspace", "airspace closure", "airspace restriction",
        "procedure", "procedures", "reroute", "alternate", "runway data", "overrun",
        "cpdlc", "route uplink", "gps interference", "spoofing", "jamming",
        "volcanic ash", "航行通告", "空域", "程序", "绕飞", "备降机场", "导航干扰",
    ],
    "human_factors_training": [
        "spatial disorientation", "fatigue", "crm", "training", "simulator",
        "pilot training", "crew resource", "incapacitated", "疲劳", "训练", "模拟机",
    ],
    "industry_novelty": [
        "maiden flight", "first flight", "takes flight", "took flight",
        "takes to the sky", "took to the sky", "takes to the air", "took to the air",
        "test flight", "flight test", "prototype",
        "type certification", "type certificate",
        "first production", "first delivery", "delivery flight",
        "entered service", "enters service", "rollout", "rolls out", "rolled out",
        "first revenue flight", "inaugural flight",
        "synthetic vision", "enhanced vision", "head-up display", "hud upgrade",
        "cockpit upgrade", "avionics upgrade", "ai copilot",
        "first female captain", "first woman captain", "retiring captain",
        "captain retires", "veteran pilot", "world record", "longest flight",
        "polar flight", "farewell flight", "retirement flight", "final flight",
        "anniversary flight", "commemorative flight", "retro livery",
        "首飞", "试飞", "原型机", "型号合格证", "合成视景", "增强视景",
        "驾驶舱升级", "航电升级", "退役机长", "首位女机长", "传奇机长",
        "世界纪录", "极地航班", "超长航班", "告别飞行", "纪念飞行",
        "复古涂装", "纪念涂装", "首架交付", "投入运营", "首航", "总装下线", "下线",
    ],
    "industry_gossip": [
        "scandal", "controversy", "viral", "went viral", "trending", "backlash", "outrage",
        "fired", "sacked", "suspended", "resignation", "terminated", "disciplinary",
        "unruly passenger", "disruptive passenger", "passenger fight", "altercation",
        "kicked off", "removed from flight", "deplaned", "intoxicated", "drunk pilot",
        "caught on camera", "video shows", "social media", "tiktok", "drama", "feud",
        "embarrassing", "mishap", "blunder", "pilot error", "captain suspended",
        "inappropriate", "whistleblower", "public apology", "airline apologizes",
        "争议", "丑闻", "辞退", "解聘", "停职", "网曝", "曝光", "热搜", "引热议",
        "引争议", "遭吐槽", "翻车", "闹事", "冲突", "醉酒", "被赶下飞机", "公开道歉",
        "机组被停飞", "机长被停职",
    ],
    "industry_news": _DEFAULT_INDUSTRY_NEWS_KEYWORDS,
}

_CATEGORY_BONUS = {
    "safety_event": 22.0,
    "airworthiness_technical": 18.0,
    "ops_environment": 14.0,
    "human_factors_training": 10.0,
    # 趣闻类——给到与人因训练相近的加分，让首飞/纪念飞行/特殊任务等能稳定
    # 进入 ranked 池，但仍低于事故和适航，避免新闻日替代严肃内容。
    "industry_novelty": 14.0,
    # 吃瓜类——读者最爱看，给到接近事故级的加分，确保争议/ viral 稿能进前排。
    "industry_gossip": 22.0,
    "industry_news": 16.0,
}


def _load_raw(day: str) -> list[dict]:
    path = settings.raw_dir / f"{day}.json"
    if not path.exists():
        return []
    return load_json(path)


def _load_source_health(day: str) -> list[dict]:
    path = settings.raw_dir / f"source_health_{day}.json"
    if not path.exists():
        return []
    try:
        data = load_json(path)
    except Exception:  # noqa: BLE001
        return []
    return data if isinstance(data, list) else []


def _keyword_hits(text: str, keywords: list[str]) -> int:
    text_l = text.lower()
    return sum(1 for word in keywords if word.lower() in text_l)


def _domain(url: str) -> str:
    try:
        return (urlparse(url).netloc or "").lower()
    except Exception:  # noqa: BLE001
        return ""


def _looks_like_google_redirect(url: str) -> bool:
    dm = _domain(url)
    path = (urlparse(url).path or "").lower() if dm else ""
    return dm.endswith("news.google.com") and path.startswith("/rss/articles/")


def _is_relevant(
    item: dict,
    keyword_hits: int,
    all_keywords: list[str],
    novelty_keywords: list[str] | None = None,
    gossip_keywords: list[str] | None = None,
) -> bool:
    if keyword_hits > 0:
        return True
    text = f"{item.get('title', '')} {item.get('raw_text', '')}".lower()
    hit = sum(1 for k in all_keywords if k.lower() in text)
    if hit >= 2:
        return True
    # Novelty 旁路：首飞/纪念飞行/驾驶舱创新/人物故事等内容通常不带事故词，
    # 但仍是飞行员关心的航空新闻——命中至少 1 个 novelty 词即视为相关，
    # 后续会由 _is_pilot_relevant 的 novelty_hits 路径再次校验。
    if novelty_keywords:
        if any(k.lower() in text for k in novelty_keywords):
            return True
    # 吃瓜旁路：争议/ viral / 机组丑闻类标题常不带标准运行 signal。
    if gossip_keywords:
        if any(k.lower() in text for k in gossip_keywords):
            return True
    return False


def _keyword_list(values: list[str] | None, defaults: list[str]) -> list[str]:
    if not values:
        return defaults
    return [str(x).strip().lower() for x in values if str(x).strip()]


def _count_hits(text: str, keywords: list[str]) -> int:
    text_l = text.lower()
    return sum(1 for k in keywords if k and k in text_l)


def _domain_allowed(domain: str, allowed_domains: set[str]) -> bool:
    if not domain or not allowed_domains:
        return False
    return any(domain.endswith(x) for x in allowed_domains)


def _source_role(item: dict) -> str:
    return str(item.get("source_role") or "").strip().lower()


def _has_major_accident_impact(text: str, kw_cfg: dict) -> bool:
    words = _keyword_list(
        kw_cfg.get("major_accident_impact_keywords"),
        _DEFAULT_MAJOR_ACCIDENT_IMPACT_KEYWORDS,
    )
    return _count_hits(text, words) > 0


def _has_macro_aviation_effect(text: str, kw_cfg: dict) -> bool:
    words = _keyword_list(
        kw_cfg.get("macro_aviation_effect_keywords"),
        _DEFAULT_MACRO_AVIATION_EFFECT_KEYWORDS,
    )
    return _count_hits(text, words) > 0


def _has_primary_industry_signal(text: str, kw_cfg: dict) -> bool:
    words = _keyword_list(
        kw_cfg.get("industry_news_keywords"),
        _DEFAULT_INDUSTRY_NEWS_KEYWORDS,
    )
    return _count_hits(text, words) >= 2


def _looks_like_mainland_china_aviation_subject(item: dict) -> bool:
    title = str(item.get("title") or "").strip().lower()
    if not title:
        return False
    if any(term in title for term in _MAINLAND_CHINA_EXEMPT_TITLE_PATTERNS):
        return False

    if any(term in title for term in _MAINLAND_CHINA_SUBJECT_KEYWORDS):
        return True
    if any(term in title for term in _MAINLAND_CHINA_AIRPORT_KEYWORDS):
        return True
    if any(term in title for term in _MAINLAND_CHINA_CARRIER_KEYWORDS):
        if any(term in title for term in _MAINLAND_CHINA_SUBJECT_ACTION_KEYWORDS):
            return True
        return "domestic" in title or "国内" in title
    if any(term in title for term in ("china airport", "chinese airport", "mainland airport")):
        return True
    return False


def _title_tokens_for_event(title: str) -> frozenset[str]:
    text = str(title or "").lower().replace("la guardia", "laguardia")
    tokens: set[str] = set()
    for raw in _TITLE_TOKEN_RE.findall(text):
        token = raw.lower().strip()
        if not token or token in _TITLE_STOP_TOKENS:
            continue
        if len(token) == 1 and token.isascii():
            continue
        token = _TITLE_TOKEN_SYNONYMS.get(token, token)
        tokens.add(token)
    return frozenset(tokens)


def _looks_like_same_event_title(a: frozenset[str], b: frozenset[str]) -> bool:
    if not a or not b:
        return False
    shared = len(a & b)
    if shared < 3:
        return False
    return shared / max(min(len(a), len(b)), 1) >= 0.33


def _is_pilot_relevant(item: dict, text: str, kw_cfg: dict) -> tuple[bool, str]:
    signal_words = _keyword_list(kw_cfg.get("pilot_signal_keywords"), _DEFAULT_PILOT_SIGNAL_KEYWORDS)
    entity_words = _keyword_list(kw_cfg.get("pilot_entity_keywords"), _DEFAULT_PILOT_ENTITY_KEYWORDS)
    reject_words = _keyword_list(kw_cfg.get("hard_reject_keywords"), _DEFAULT_HARD_REJECT_KEYWORDS)
    strict_reject_words = _keyword_list(
        kw_cfg.get("strict_hard_reject_keywords"), _DEFAULT_STRICT_HARD_REJECT_KEYWORDS,
    )
    direct_operation_words = _keyword_list(
        kw_cfg.get("pilot_direct_operation_keywords"),
        _DEFAULT_PILOT_DIRECT_OPERATION_KEYWORDS,
    )
    background_only_words = _keyword_list(
        kw_cfg.get("pilot_background_only_keywords"),
        _DEFAULT_PILOT_BACKGROUND_ONLY_KEYWORDS,
    )
    schedule_advisory_words = _keyword_list(
        kw_cfg.get("pilot_schedule_advisory_keywords"),
        _DEFAULT_PILOT_SCHEDULE_ADVISORY_KEYWORDS,
    )
    specific_ops_words = _keyword_list(
        kw_cfg.get("pilot_specific_ops_keywords"),
        _DEFAULT_PILOT_SPECIFIC_OPS_KEYWORDS,
    )
    novelty_words = _keyword_list(
        kw_cfg.get("pilot_novelty_keywords"),
        _DEFAULT_PILOT_NOVELTY_KEYWORDS,
    )
    gossip_words = _keyword_list(
        kw_cfg.get("pilot_gossip_keywords"),
        _DEFAULT_PILOT_GOSSIP_KEYWORDS,
    )
    novelty_reject_words = _keyword_list(
        kw_cfg.get("pilot_novelty_reject_keywords"),
        _DEFAULT_PILOT_NOVELTY_REJECT_KEYWORDS,
    )
    non_aviation_patterns = _keyword_list(
        kw_cfg.get("non_aviation_reject_patterns"), [],
    )

    allowed_source_ids = {str(x).strip() for x in kw_cfg.get("pilot_allowed_source_ids", []) if str(x).strip()}
    allowed_domains = {
        str(x).strip().lower()
        for x in kw_cfg.get("pilot_allowed_domains", [])
        if str(x).strip()
    }

    canonical_url = (item.get("canonical_url") or item.get("url") or "").strip()
    domain = _domain(canonical_url)
    source_id = str(item.get("source_id") or "").strip()
    role = _source_role(item)

    title_l = item.get("title", "").lower()
    text_l = text.lower()
    combined_l = f"{title_l} {text_l}"
    if role == "macro_supplement" and not _has_macro_aviation_effect(combined_l, kw_cfg):
        return False, "macro_without_explicit_aviation_effect"
    if role == "accident_exception" and not _has_major_accident_impact(combined_l, kw_cfg):
        return False, "accident_without_major_impact"
    if source_id.startswith("asn_") and _looks_like_non_transport_asn_record(text_l):
        return False, "non_transport_accident_record"
    if source_id == "easa_ad_web" and _looks_like_non_transport_easa_ad(text_l):
        return False, "non_transport_airworthiness_record"
    signal_hits = _count_hits(text_l, signal_words)
    entity_hits = _count_hits(text_l, entity_words)
    reject_hits = _count_hits(text_l, reject_words)
    trusted_source = source_id in allowed_source_ids or _domain_allowed(domain, allowed_domains)
    direct_operation_hits = _count_hits(title_l, direct_operation_words) + _count_hits(text_l, direct_operation_words)
    background_only_hits = _count_hits(title_l, background_only_words) + _count_hits(text_l, background_only_words)
    schedule_advisory_hits = _count_hits(title_l, schedule_advisory_words) + _count_hits(text_l, schedule_advisory_words)
    specific_ops_hits = _count_hits(title_l, specific_ops_words) + _count_hits(text_l, specific_ops_words)
    novelty_hits = _count_hits(title_l, novelty_words) + _count_hits(text_l, novelty_words)
    gossip_hits = _count_hits(title_l, gossip_words) + _count_hits(text_l, gossip_words)
    novelty_reject_hits = _count_hits(text_l, novelty_reject_words)

    # Hard-reject known non-aviation entities (e.g. "Minnesota United" soccer)
    if non_aviation_patterns and _count_hits(title_l, non_aviation_patterns) > 0:
        return False, "non_aviation_entity"
    if strict_reject_words and _count_hits(text_l, strict_reject_words) > 0:
        return False, "strict_hard_reject_keywords"
    if any(term in title_l for term in _SOFT_CONTENT_TITLE_PATTERNS):
        return False, "hard_reject_keywords"
    if any(term in title_l for term in _LOW_VALUE_INFRASTRUCTURE_TITLE_PATTERNS):
        return False, "hard_reject_keywords"

    # 用户明确不想要的前沿空中出行类（eVTOL、电动、氢能、超音速等）——
    # 即使在 novelty 路径下也直接拒绝。
    if novelty_reject_hits > 0:
        return False, "novelty_excluded_topic"

    # Reject obvious noise unless signal is very strong.
    if reject_hits > 0 and signal_hits < 2:
        return False, "hard_reject_keywords"

    if role == "primary_industry" and _has_primary_industry_signal(combined_l, kw_cfg):
        aviation_context_hits = _count_hits(combined_l, _AVIATION_CONTEXT_HINTS)
        if entity_hits >= 1 or aviation_context_hits >= 2:
            return True, "ok_industry_news"

    if signal_hits <= 0:
        # Novelty 旁路：人物故事/纪念飞行/驾驶舱创新通常不带运行类 signal，
        # 至少 1 条 novelty + 命名实体或 2+ 航空场景词才放行，避免噪声混入。
        if novelty_hits >= 1:
            aviation_context_hits = _count_hits(text_l, _AVIATION_CONTEXT_HINTS) + _count_hits(
                title_l, _AVIATION_CONTEXT_HINTS,
            )
            if entity_hits >= 1 or aviation_context_hits >= 2:
                return True, "ok_novelty"
        # 吃瓜旁路：争议/ viral / 机组丑闻——标题里常有 airline/pilot/passenger 等 aviation 词。
        if gossip_hits >= 1:
            aviation_context_hits = _count_hits(text_l, _AVIATION_CONTEXT_HINTS) + _count_hits(
                title_l, _AVIATION_CONTEXT_HINTS,
            )
            if entity_hits >= 1 or aviation_context_hits >= 1:
                return True, "ok_gossip"
        return False, "missing_pilot_signal"

    # Trusted sources still need either an aviation entity OR strong signal (2+)
    # to prevent travel/lifestyle content from slipping through.
    if entity_hits <= 0:
        if not trusted_source:
            # Novelty 兜底：人物故事/告别飞行常没有 entity_keywords 命中（如
            # "Air France retires last A380"），但只要 novelty 信号足够强且
            # 有航空场景词，就当作可信背景放行。
            if novelty_hits >= 2:
                aviation_context_hits = _count_hits(text_l, _AVIATION_CONTEXT_HINTS) + _count_hits(
                    title_l, _AVIATION_CONTEXT_HINTS,
                )
                if aviation_context_hits >= 1:
                    return True, "ok_novelty"
            return False, "missing_aviation_entity"
        if signal_hits < 2 and direct_operation_hits <= 0:
            return False, "trusted_source_weak_signal"

    # 首飞/退役飞行/纪念航班往往同时出现 "delivery"/"first flight" 等 background_only 词面。
    # 仅当 novelty 信号足够强（>=2 命中）时放行，避免"航司新航线首飞"混入趣闻位。
    if background_only_hits > 0 and direct_operation_hits <= 0 and novelty_hits < 2:
        return False, "background_only_story"
    if schedule_advisory_hits > 0 and specific_ops_hits <= 0:
        return False, "schedule_advisory_story"

    return True, "ok"


def _looks_like_non_transport_asn_record(text_l: str) -> bool:
    transport_markers = [
        "airbus", "boeing", "embraer emb-120", "embraer erj", "embraer e1",
        "embraer e2", "bombardier crj", "de havilland", "atr ", "airlines",
        "air lines", "airways", "cargo", "regional", "express",
    ]
    if any(marker in text_l for marker in transport_markers):
        return False
    non_transport_markers = [
        "private", "air force", "navy", "army", "idf/af", "drone", "uav",
        "mod hur", "fuerza aérea", "self-defense force", "defense force", "military",
        "cessna 172", "cessna 182", "piper pa-", "beechcraft", "bonanza",
        "citabria", "husky", "jonker", "glider", "super cub", "elbit hermes",
        "air tractor", "robinson r44", "robinson r22", "robinson", "bell 206",
        "skyranger", "agustawestland", " oh-1 ", "helicopter",
    ]
    return any(marker in text_l for marker in non_transport_markers)


def _looks_like_non_transport_easa_ad(text_l: str) -> bool:
    transport_markers = [
        "airbus s.a.s. a319", "airbus s.a.s. a320", "airbus s.a.s. a321",
        "airbus a330", "airbus a350", "boeing 737", "boeing 747", "boeing 757",
        "boeing 767", "boeing 777", "boeing 787", "embraer emb-120",
        "embraer erj", "atr ", "de havilland", "bombardier crj",
    ]
    if any(marker in text_l for marker in transport_markers):
        return False
    non_transport_markers = [
        "helicopter", "helicopters", "rotorcraft", "ec135", "ec145", "mbb-bk",
        "grob", "g 109", "continental aerospace", "tae125", "rotax",
        "agustawestland", "bell helicopter", "robinson", "sailplane", "glider",
    ]
    return any(marker in text_l for marker in non_transport_markers)


def _pilot_value_profile(item: dict, text: str, kw_cfg: dict) -> dict[str, Any]:
    """Classify how directly a story maps to cockpit/line operations."""
    text_l = text.lower()
    title_l = str(item.get("title", "")).lower()
    combined = f"{title_l} {text_l}"
    priority_sources = {
        str(x).strip()
        for x in kw_cfg.get("pilot_priority_sources", _DEFAULT_PILOT_PRIORITY_SOURCES)
        if str(x).strip()
    }
    source_id = str(item.get("source_id") or "").strip()
    role = _source_role(item)

    category_hits = {
        category: _count_hits(combined, words)
        for category, words in _PILOT_CATEGORY_KEYWORDS.items()
    }
    category = max(category_hits, key=lambda x: category_hits[x]) if category_hits else "other"
    category_hit_count = category_hits.get(category, 0)
    if category_hit_count <= 0:
        category = "other"
    if role == "primary_industry" and _has_primary_industry_signal(combined, kw_cfg):
        category = "industry_news"

    direct_words = _keyword_list(
        kw_cfg.get("pilot_direct_operation_keywords"),
        _DEFAULT_PILOT_DIRECT_OPERATION_KEYWORDS,
    )
    background_words = _keyword_list(
        kw_cfg.get("pilot_background_only_keywords"),
        _DEFAULT_PILOT_BACKGROUND_ONLY_KEYWORDS,
    )
    novelty_words = _keyword_list(
        kw_cfg.get("pilot_novelty_keywords"),
        _DEFAULT_PILOT_NOVELTY_KEYWORDS,
    )
    gossip_words = _keyword_list(
        kw_cfg.get("pilot_gossip_keywords"),
        _DEFAULT_PILOT_GOSSIP_KEYWORDS,
    )
    direct_hits = _count_hits(combined, direct_words)
    background_hits = _count_hits(combined, background_words)
    novelty_hits = _count_hits(combined, novelty_words)
    gossip_hits = _count_hits(combined, gossip_words)
    # 吃瓜信号强时优先归类为 industry_gossip，便于后续 quota / compose 加权。
    gossip_category_hits = category_hits.get("industry_gossip", 0)
    if gossip_hits >= 1 and gossip_category_hits >= 1:
        if gossip_category_hits >= category_hit_count or category == "other":
            category = "industry_gossip"
            category_hit_count = gossip_category_hits
    priority_source = source_id in priority_sources
    raw_len = len(str(item.get("raw_text", "") or ""))

    value = 0.0
    value += min(direct_hits * 8.0, 40.0)
    # 新奇/趣闻信号——温和加分让其能进 ranked 池，仍低于直接运行类的 40 上限。
    value += min(novelty_hits * 4.0, 24.0)
    # 吃瓜信号——读者最爱，单独加分且上限更高。
    value += min(gossip_hits * 6.0, 30.0)
    value += _CATEGORY_BONUS.get(category, 0.0)
    value += 12.0 if priority_source else 0.0
    value += 18.0 if role == "primary_industry" and category == "industry_news" else 0.0
    value += 8.0 if raw_len >= 300 else 0.0
    # background_only 罚分仅在没有 novelty/gossip 抵消时生效——避免误伤首飞/吃瓜稿。
    engagement_hits = novelty_hits + gossip_hits
    if role == "primary_industry":
        value -= min(max(background_hits - 2, 0) * 3.0, 12.0)
    elif engagement_hits <= 0:
        value -= min(background_hits * 8.0, 28.0)
    else:
        value -= min(max(background_hits - engagement_hits, 0) * 4.0, 16.0)
    value = max(0.0, min(100.0, 35.0 + value))
    return {
        "category": category,
        "category_hits": category_hits,
        "direct_hits": direct_hits,
        "background_hits": background_hits,
        "novelty_hits": novelty_hits,
        "gossip_hits": gossip_hits,
        "priority_source": priority_source,
        "pilot_value_score": round(value, 2),
    }


def _has_valid_published_at(value: str) -> bool:
    if not value:
        return False
    try:
        dt_parser.parse(str(value))
        return True
    except (ValueError, TypeError):
        return False


def _is_too_old(value: str, max_age_hours: int) -> bool:
    """Return True if published_at is older than *max_age_hours* from now."""
    if not value or max_age_hours <= 0:
        return False
    try:
        pub = dt_parser.parse(str(value))
        if pub.tzinfo is None:
            pub = pub.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return (now - pub) > timedelta(hours=max_age_hours)
    except (ValueError, TypeError):
        return False


def _max_age_for_item(item: dict) -> int:
    if str(item.get("source_tier", "")).upper() == "A":
        return max(settings.max_article_age_hours, settings.max_tier_a_article_age_hours)
    return settings.max_article_age_hours


def _pick_by_quota(candidates: list[dict], total: int, domestic_ratio: float) -> list[dict]:
    if domestic_ratio <= 0.0:
        candidates = [c for c in candidates if c.get("region") != "domestic"]
    if total <= 0:
        return candidates
    return candidates[:total]


def _ensure_novelty_quota(
    candidates: list[dict],
    backfill_pool: list[dict],
    min_novelty: int,
    max_per_source: int,
    novelty_min_score: float = 60.0,
    protected_top_count: int = 5,
) -> tuple[list[dict], bool]:
    """Make sure at least *min_novelty* industry_novelty / industry_gossip stories are kept.

    替换策略：从 backfill_pool 里挑分数最高的趣闻/吃瓜候选（通常用 deduped
    而非 compose_candidates，让 engagement 稿不被 min_rank_score 门槛卡住），
    替换 candidates 中分数最低的非 engagement 项；同时保留分数最高的
    *protected_top_count* 条不被替换（保证严肃骨架不丢）。

    若候选池中没有达到 novelty_min_score 的 engagement 类，原样返回，不强行凑数。
    """
    if min_novelty <= 0:
        return list(candidates), False

    out = list(candidates)
    used_ids = {x.get("id") for x in out}

    def _is_engagement(row: dict) -> bool:
        pv = row.get("pilot_value") or {}
        cat = str(pv.get("category", ""))
        return cat in {"industry_novelty", "industry_gossip"}

    def _source_key(row: dict) -> str:
        return str(row.get("source_id") or row.get("source_name") or "")

    novelty_count = sum(1 for r in out if _is_engagement(r))
    if novelty_count >= min_novelty:
        return out, False

    novelty_pool = [
        r for r in backfill_pool
        if _is_engagement(r)
        and r.get("id") not in used_ids
        and float(r.get("rank_score") or 0.0) >= novelty_min_score
    ]
    novelty_pool.sort(key=lambda r: float(r.get("rank_score") or 0.0), reverse=True)
    if not novelty_pool:
        return out, False

    # 计算受保护的索引集合（按 rank_score 降序的前 N 个）
    indexed = sorted(
        enumerate(out),
        key=lambda kv: float(kv[1].get("rank_score") or 0.0),
        reverse=True,
    )
    protected_idx = {idx for idx, _ in indexed[:max(0, protected_top_count)]}

    applied = False
    for replacement in novelty_pool:
        if novelty_count >= min_novelty:
            break

        # 找一个最低分的"可被替换"项：非 engagement、不在受保护 top-N 内。
        victim_idx = None
        for i in range(len(out) - 1, -1, -1):
            row = out[i]
            if _is_engagement(row):
                continue
            if i in protected_idx:
                continue
            victim_idx = i
            break

        if victim_idx is None:
            break

        # 检查 source cap，避免替换后某源超额。
        if max_per_source > 0:
            counts: dict[str, int] = {}
            for r in out:
                counts[_source_key(r)] = counts.get(_source_key(r), 0) + 1
            new_key = _source_key(replacement)
            old_key = _source_key(out[victim_idx])
            if new_key != old_key and counts.get(new_key, 0) >= max_per_source:
                continue  # 跳过这个 replacement，否则会破坏 source cap

        used_ids.discard(out[victim_idx].get("id"))
        out[victim_idx] = replacement
        used_ids.add(replacement.get("id"))
        novelty_count += 1
        applied = True
        # 重新计算受保护集合（替换后排名变了）
        indexed = sorted(
            enumerate(out),
            key=lambda kv: float(kv[1].get("rank_score") or 0.0),
            reverse=True,
        )
        protected_idx = {idx for idx, _ in indexed[:max(0, protected_top_count)]}

    return out, applied


def _enforce_source_cap(candidates: list[dict], ranked_pool: list[dict], max_per_source: int) -> tuple[list[dict], bool]:
    if max_per_source <= 0:
        return list(candidates), False

    out = list(candidates)
    used_ids = {x.get("id") for x in out}
    applied = False

    def _counts(rows: list[dict]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for r in rows:
            key = str(r.get("source_id") or r.get("source_name") or "")
            counts[key] = counts.get(key, 0) + 1
        return counts

    guard = 0
    while guard < 100:
        guard += 1
        counts = _counts(out)
        over_key = next((k for k, v in counts.items() if v > max_per_source), "")
        if not over_key:
            break

        victim_idx = None
        for i in range(len(out) - 1, -1, -1):
            key = str(out[i].get("source_id") or out[i].get("source_name") or "")
            if key == over_key:
                victim_idx = i
                break
        if victim_idx is None:
            break
        victim = out[victim_idx]

        replacement = next(
            (
                x
                for x in ranked_pool
                if x.get("id") not in used_ids
                and str(x.get("source_id") or x.get("source_name") or "") != over_key
                and counts.get(str(x.get("source_id") or x.get("source_name") or ""), 0) < max_per_source
            ),
            None,
        )
        if replacement is None:
            used_ids.discard(out[victim_idx].get("id"))
            del out[victim_idx]
            applied = True
            continue

        used_ids.discard(out[victim_idx].get("id"))
        out[victim_idx] = replacement
        used_ids.add(replacement.get("id"))
        applied = True

    return out, applied


def _dedupe_ranked_events(ranked: list[dict]) -> list[dict]:
    deduped: list[dict] = []
    seen_fp: set[str] = set()
    seen_url: set[str] = set()
    by_fp: dict[str, dict] = {}
    by_url: dict[str, dict] = {}
    seen_titles: list[frozenset[str]] = []
    for row in ranked:
        fp = row.get("event_fingerprint", "")
        u = row.get("canonical_url", "")
        if fp in seen_fp:
            _merge_missing_image(by_fp.get(fp, {}), row)
            continue
        if u in seen_url:
            _merge_missing_image(by_url.get(u, {}), row)
            continue
        title_tokens = _title_tokens_for_event(str(row.get("title", "")))
        if any(_looks_like_same_event_title(title_tokens, old) for old in seen_titles):
            continue
        seen_fp.add(fp)
        seen_url.add(u)
        if fp:
            by_fp[fp] = row
        if u:
            by_url[u] = row
        if title_tokens:
            seen_titles.append(title_tokens)
        deduped.append(row)
    return deduped


def run(target_date: str | None = None) -> Path:
    day = target_date or beijing_today_str()
    rows = _load_raw(day)
    _merge_raw_images_by_url(rows)
    source_health = _load_source_health(day)
    kw = load_yaml(settings.keywords_config)
    section_map = kw.get("sections", {})
    relevance_kw = kw.get("relevance_keywords", [])
    # Support both old sections format and new flat relevance_keywords
    if relevance_kw:
        all_keywords = [str(x).strip() for x in relevance_kw if str(x).strip()]
    else:
        all_keywords = [x for words in section_map.values() for x in words]
    blocked_domains = [x.lower() for x in kw.get("blocked_domains", [])]
    novelty_keywords_for_relevance = _keyword_list(
        kw.get("pilot_novelty_keywords"), _DEFAULT_PILOT_NOVELTY_KEYWORDS,
    )
    gossip_keywords_for_relevance = _keyword_list(
        kw.get("pilot_gossip_keywords"), _DEFAULT_PILOT_GOSSIP_KEYWORDS,
    )

    ranked: list[dict] = []
    dropped_non_relevant = 0
    dropped_non_pilot_relevant = 0
    dropped_hard_reject = 0
    dropped_blocked_domain = 0
    dropped_no_original_link = 0
    dropped_no_published_at = 0
    dropped_too_old = 0
    dropped_mainland_china_subject = 0
    for item in rows:
        canonical_url = (item.get("canonical_url") or item.get("url") or "").strip()
        if not canonical_url.startswith(("http://", "https://")):
            dropped_no_original_link += 1
            continue
        dm = _domain(canonical_url)
        if dm in blocked_domains:
            dropped_blocked_domain += 1
            continue
        # Google redirect URLs are kept but penalised in scoring below.
        # Dropping them would eliminate nearly all domestic news from Google News RSS.
        if not _has_valid_published_at(item.get("published_at", "")):
            dropped_no_published_at += 1
            continue
        max_age = _max_age_for_item(item)
        if _is_too_old(item.get("published_at", ""), max_age):
            dropped_too_old += 1
            continue

        text = f"{item['title']} {item['raw_text']}"
        hits = _keyword_hits(text, all_keywords)
        if not _is_relevant(
            item, hits, all_keywords,
            novelty_keywords_for_relevance,
            gossip_keywords_for_relevance,
        ):
            dropped_non_relevant += 1
            continue
        if _looks_like_mainland_china_aviation_subject(item):
            dropped_mainland_china_subject += 1
            continue
        pilot_ok, pilot_reason = _is_pilot_relevant(item, text, kw)
        if not pilot_ok:
            dropped_non_pilot_relevant += 1
            if pilot_reason == "hard_reject_keywords":
                dropped_hard_reject += 1
            continue

        pilot_signal_words = _keyword_list(kw.get("pilot_signal_keywords"), _DEFAULT_PILOT_SIGNAL_KEYWORDS)
        pilot_hits = _count_hits(text, pilot_signal_words)
        pilot_profile = _pilot_value_profile(item, text, kw)
        if pilot_profile["category"] == "other" and float(pilot_profile["pilot_value_score"]) < 70.0:
            dropped_non_pilot_relevant += 1
            continue
        rel = max(
            relevance_score(text, hits + pilot_hits),
            float(pilot_profile["pilot_value_score"]),
        )
        auth = tier_score(item.get("source_tier", "C"))
        if str(item.get("source_id", "")).startswith("google_"):
            auth = min(auth, 80.0)
        time_score = recency_score(item.get("published_at", ""))
        google_penalty = 15.0 if _looks_like_google_redirect(canonical_url) else 0.0
        priority_bonus = 8.0 if pilot_profile["priority_source"] else 0.0
        category_bonus = {
            "safety_event": 8.0,
            "airworthiness_technical": 6.0,
            "ops_environment": 4.0,
            "human_factors_training": 2.0,
            # 趣闻类：与 ops_environment 持平，确保有趣的稿件能与严肃稿同场竞争。
            "industry_novelty": 4.0,
            # 吃瓜类：读者最爱，给最高 category 加分。
            "industry_gossip": 10.0,
            "industry_news": 6.0,
        }.get(str(pilot_profile["category"]), 0.0)
        rank_score = round(
            rel * 0.70 + auth * 0.10 + time_score * 0.12 + priority_bonus + category_bonus - google_penalty,
            2,
        )

        enriched = dict(item)
        enriched.update(
            {
                "canonical_url": canonical_url,
                "publisher_domain": item.get("publisher_domain", dm),
                "event_fingerprint": item.get("event_fingerprint") or item.get("id"),
                "is_google_redirect": item.get("is_google_redirect", _looks_like_google_redirect(canonical_url)),
                "keyword_hits": hits,
                "pilot_value": pilot_profile,
                "rank_score": rank_score,
                "score_breakdown": {
                    "relevance": rel,
                    "authority": auth,
                    "timeliness": time_score,
                },
            }
        )
        ranked.append(enriched)

    ranked.sort(key=lambda x: x["rank_score"], reverse=True)
    deduped = _dedupe_ranked_events(ranked)

    min_rank_score = float(getattr(settings, "min_rank_score_for_compose", 80.0) or 0.0)
    compose_candidates = [
        row for row in deduped
        if min_rank_score <= 0.0 or float(row.get("rank_score") or 0.0) >= min_rank_score
    ]
    article_limit = max(0, int(getattr(settings, "target_article_count", 0) or 0))
    candidate_total = max(article_limit * 6, 50) if article_limit > 0 else 0
    top_candidates = _pick_by_quota(compose_candidates, total=candidate_total, domestic_ratio=settings.domestic_ratio)
    top_candidates, source_cap_applied = _enforce_source_cap(
        top_candidates,
        compose_candidates,
        max_per_source=getattr(settings, "max_entries_per_source", 0),
    )

    # Ensure A-tier ratio >= configured threshold when possible.
    min_tier_a_ratio = float(getattr(settings, "min_tier_a_ratio", 0.0) or 0.0)
    min_a = int(len(top_candidates) * min_tier_a_ratio)
    current_a = sum(1 for x in top_candidates if x.get("source_tier") == "A")
    if min_tier_a_ratio > 0.0 and current_a < min_a:
        alt_a = [x for x in compose_candidates if x.get("source_tier") == "A" and x not in top_candidates]
        for replacement in alt_a:
            replace_idx = next(
                (i for i, v in enumerate(top_candidates[::-1]) if v.get("source_tier") != "A"),
                None,
            )
            if replace_idx is None:
                break
            real_idx = len(top_candidates) - 1 - replace_idx
            top_candidates[real_idx] = replacement
            current_a += 1
            if current_a >= min_a:
                break
        top_candidates, a_tier_source_cap_applied = _enforce_source_cap(
            top_candidates,
            compose_candidates,
            max_per_source=getattr(settings, "max_entries_per_source", 0),
        )
        source_cap_applied = source_cap_applied or a_tier_source_cap_applied

    # 趣闻配额：保证 ranked 池里至少 min_novelty 篇 industry_novelty。
    # backfill 使用 deduped（未受 min_rank_score 门槛限制），让 novelty 在
    # 严肃稿件评分普遍偏高时仍能补足；同时 novelty_min_score=60 兜底，
    # 避免把质量太差的 novelty 强塞进来。
    min_novelty_articles = max(0, int(getattr(settings, "min_novelty_articles", 1) or 0))
    novelty_quota_applied = False
    if min_novelty_articles > 0:
        top_candidates, novelty_quota_applied = _ensure_novelty_quota(
            top_candidates,
            deduped,
            min_novelty=min_novelty_articles,
            max_per_source=getattr(settings, "max_entries_per_source", 0),
            novelty_min_score=60.0,
        )
    top_candidates.sort(key=lambda x: x["rank_score"], reverse=True)

    source_distribution = Counter(x.get("source_id", "") for x in top_candidates if x.get("source_id"))
    source_health_summary = Counter(str(x.get("status", "unknown")) for x in source_health)
    source_failures = [
        {
            "source_id": str(x.get("source_id", "")),
            "source_name": str(x.get("source_name", "")),
            "status": str(x.get("status", "")),
            "item_count": int(x.get("item_count", 0) or 0),
            "error": str(x.get("error", ""))[:240],
        }
        for x in source_health
        if str(x.get("status", "")) in {"failed", "empty"}
    ][:12]

    payload = {
        "date": day,
        "meta": {
            "total_candidates": len(rows),
            "dropped_non_relevant": dropped_non_relevant,
            "dropped_non_pilot_relevant": dropped_non_pilot_relevant,
            "dropped_hard_reject": dropped_hard_reject,
            "dropped_blocked_domain": dropped_blocked_domain,
            "dropped_no_original_link": dropped_no_original_link,
            "dropped_no_published_at": dropped_no_published_at,
            "dropped_too_old": dropped_too_old,
            "dropped_mainland_china_subject": dropped_mainland_china_subject,
            "max_article_age_hours": settings.max_article_age_hours,
            "max_tier_a_article_age_hours": settings.max_tier_a_article_age_hours,
            "min_rank_score_for_compose": min_rank_score,
            "ranked_after_value_gate": len(compose_candidates),
            "article_count_limit": article_limit,
            "selected_for_compose": len(top_candidates),
            "source_cap_applied": source_cap_applied,
            "min_novelty_articles": min_novelty_articles,
            "novelty_quota_applied": novelty_quota_applied,
            "novelty_in_top": sum(
                1 for x in top_candidates
                if str((x.get("pilot_value") or {}).get("category", ""))
                in {"industry_novelty", "industry_gossip"}
            ),
            "source_distribution": dict(source_distribution.most_common(10)),
            "source_health_summary": dict(source_health_summary),
            "source_failures": source_failures,
        },
        "articles": top_candidates,
    }
    out = settings.processed_dir / f"ranked_{day}.json"
    dump_json(out, payload)
    logger.info("Rank done. candidates=%s selected=%s", len(rows), len(top_candidates))
    return out


if __name__ == "__main__":
    run()

"""Filter CAAC documents for Part 121 transport pilot relevance.

Two-layer filter:
1. Rule-based hard reject / hard accept by title + doc_number patterns
2. LLM quick judgment for borderline cases
"""

from __future__ import annotations

import re
from typing import Any

from flying_podcast.core.logging_utils import get_logger

logger = get_logger("pilot_filter")

# ── Layer 1: Rule-based patterns ──────────────────────────────

# Hard REJECT — title or doc_number contains any of these → skip immediately
REJECT_KEYWORDS = [
    # Aircraft types we don't cover
    "旋翼", "直升机", "无人", "气球", "滑翔", "运动类", "小型航空器",
    # General aviation
    "通用航空", "通航", "农林", "空中游览", "跳伞",
    # Airport / ground equipment (not pilot ops)
    "机场设备", "专用设备", "地面服务设备", "加油设备", "燃油设备",
    "机场助航灯光", "施工工期", "登机梯",
    "运输机场运行安全", "运输机场使用许可", "机场运行安全管理",
    # Maintenance / airworthiness (not pilot ops)
    "维修单位", "维修人员执照", "适航审定", "适航规定", "适航合格审定",
    "产品和零部件", "产品认可", "噪声规定", "排放审定",
    "型号合格", "补充型号",
    # Air traffic / telecom (not pilot-facing)
    "航行通告系列", "电信人员", "VoIP", "语音通信技术",
    # Other non-pilot
    "计量管理", "行政处罚", "国籍登记", "企业改制",
    "安全文化建设", "安全工作作风", "事件信息填报",
    "卫生培训", "公共卫生事件", "应急演练",
    "质量一致性审核", "运营管理办法",
    "灭火机项目",
]

# Hard REJECT — doc_number prefix patterns
REJECT_DOC_PREFIXES = [
    "AC-27-",  "AC-29-",   # Rotorcraft
    "AC-92-",               # Unmanned
    "AC-137-", "AC-139-",  # Airport
    "AC-145-", "AC-147-",  # Maintenance
    "AP-21-",  "AP-137-",  # Certification procedures
    "AP-156-",              # Fuel supply
    "AC-34-",               # Engine emissions
    "AC-175-",              # NOTAM management
    "AC-398-",              # Safety culture
    "AC-396-",              # Incident reporting
    "AP-65",                # Telecom personnel
    "MH/T",                 # Technical standards (almost never pilot-relevant)
    "JJF",                  # Calibration standards
    "SC-25-",               # Supplemental type certs
]

# Hard ACCEPT — doc_number prefix patterns → definitely pilot-relevant
ACCEPT_DOC_PREFIXES = [
    "AC-121-",  "IB-121-",     # Part 121 transport ops
    "AC-91-",   "IB-91-",      # General operating rules
    "AC-61-",   "IB-61-",      # Pilot licensing
    "CCAR-121", "CCAR-91",     # Regulations
    "CCAR-61",                  # Pilot licensing regulation
]

# Hard ACCEPT — title keywords → very likely pilot-relevant
ACCEPT_KEYWORDS = [
    "飞行技术", "飞行员", "驾驶员", "机长", "副驾驶",
    "运行规定", "运行合格审定",
    "飞行签派", "机组资源管理",
    "训练规定", "EBT", "资格规范",
]

# Keywords that suggest possible relevance (send to LLM for judgment)
MAYBE_KEYWORDS = [
    "机组", "空勤", "膳食", "疗养", "体检",
    "安全管理", "安全隐患", "危险品",
    "签派员", "航空器事件",
]


def _match_any(text: str, patterns: list[str]) -> str | None:
    """Return the first matching pattern found in text, or None."""
    text_lower = text.lower()
    for p in patterns:
        if p.lower() in text_lower:
            return p
    return None


def _match_prefix(doc_number: str, prefixes: list[str]) -> str | None:
    """Return the first matching prefix of doc_number, or None."""
    upper = doc_number.upper().strip()
    for p in prefixes:
        if upper.startswith(p.upper()):
            return p
    return None


def rule_filter(doc: dict[str, Any]) -> str:
    """Apply rule-based filter. Returns 'accept', 'reject', or 'maybe'."""
    title = doc.get("title", "")
    doc_number = doc.get("doc_number", "")
    combined = f"{title} {doc_number}"

    # Check hard reject first
    hit = _match_any(combined, REJECT_KEYWORDS)
    if hit:
        return "reject"

    hit = _match_prefix(doc_number, REJECT_DOC_PREFIXES)
    if hit:
        return "reject"

    # Check hard accept
    hit = _match_prefix(doc_number, ACCEPT_DOC_PREFIXES)
    if hit:
        return "accept"

    hit = _match_any(combined, ACCEPT_KEYWORDS)
    if hit:
        return "accept"

    # Check maybe
    hit = _match_any(combined, MAYBE_KEYWORDS)
    if hit:
        return "maybe"

    # Default: reject (most CAAC docs are not pilot-relevant)
    return "reject"


# ── Layer 2: LLM quick judgment ───────────────────────────────

LLM_FILTER_SYSTEM = """\
你是民航飞行员内容筛选器。判断文件是否与121部运输类飞机飞行员直接相关。
输出JSON：{"relevant": true} 或 {"relevant": false}"""

LLM_FILTER_USER = """\
文件信息：
- 标题：{title}
- 文号：{doc_number}
- 分类：{category}
- 发文单位：{office_unit}

判断标准：
- 与飞行运行、飞行技术、飞行训练、机组管理、飞行员健康/体检/疗养/膳食直接相关 → true
- 仅与机场、维修、适航审定、通用航空、无人机、行政管理相关 → false"""


def llm_filter(doc: dict[str, Any], client: Any) -> bool:
    """Use LLM to judge if a borderline doc is pilot-relevant."""
    user_prompt = LLM_FILTER_USER.format(
        title=doc.get("title", ""),
        doc_number=doc.get("doc_number", ""),
        category=doc.get("category", ""),
        office_unit=doc.get("office_unit", ""),
    )
    try:
        resp = client.complete_json(
            system_prompt=LLM_FILTER_SYSTEM,
            user_prompt=user_prompt,
            max_tokens=20,
            temperature=0,
            retries=1,
            timeout=15,
        )
        return resp.payload.get("relevant", False) is True
    except Exception as e:
        logger.warning("LLM filter failed for '%s': %s — defaulting to reject",
                       doc.get("title", ""), e)
        return False


# ── Public API ────────────────────────────────────────────────

def filter_documents(
    docs: list[dict[str, Any]],
    llm_client: Any | None = None,
) -> list[dict[str, Any]]:
    """Filter a list of CAAC documents for Part 121 pilot relevance.

    Args:
        docs: List of document dicts from CCAR-workflow regulations.json
        llm_client: Optional LLM client for borderline cases

    Returns:
        List of accepted documents
    """
    accepted = []
    stats = {"accept": 0, "reject": 0, "maybe": 0, "llm_accept": 0, "llm_reject": 0}

    for doc in docs:
        title = doc.get("title", "")
        result = rule_filter(doc)
        stats[result] += 1

        if result == "accept":
            logger.debug("ACCEPT (rule): %s", title)
            accepted.append(doc)
        elif result == "reject":
            logger.debug("REJECT (rule): %s", title)
        elif result == "maybe":
            if llm_client:
                is_relevant = llm_filter(doc, llm_client)
                if is_relevant:
                    stats["llm_accept"] += 1
                    logger.info("ACCEPT (LLM): %s", title)
                    accepted.append(doc)
                else:
                    stats["llm_reject"] += 1
                    logger.debug("REJECT (LLM): %s", title)
            else:
                # No LLM available — accept borderline docs to be safe
                logger.debug("ACCEPT (maybe, no LLM): %s", title)
                accepted.append(doc)

    logger.info(
        "Filter results: %d/%d accepted (rule_accept=%d, rule_reject=%d, "
        "maybe=%d → llm_accept=%d, llm_reject=%d)",
        len(accepted), len(docs),
        stats["accept"], stats["reject"],
        stats["maybe"], stats["llm_accept"], stats["llm_reject"],
    )
    return accepted

from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pdfplumber

from flying_podcast.core.config import settings
from flying_podcast.core.io_utils import dump_json
from html import escape
from flying_podcast.core.llm_client import OpenAICompatibleClient
from flying_podcast.core.logging_utils import get_logger
from flying_podcast.core.r2_upload import upload_file as r2_upload_file
from flying_podcast.core.time_utils import beijing_today_str
from flying_podcast.core.tts_client import (
    concatenate_audio,
    synthesize_dialogue,
)

logger = get_logger("podcast")

# ── PDF extraction ─────────────────────────────────────────────

MAX_PDF_CHARS = 30000  # Truncate very long PDFs


def extract_pdf_text(pdf_path: Path) -> str:
    """Extract all text from a PDF file."""
    logger.info("Extracting text from: %s", pdf_path.name)
    pages_text: list[str] = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            if text.strip():
                pages_text.append(text.strip())
            logger.debug("Page %d: %d chars", i + 1, len(text))

    full_text = "\n\n".join(pages_text)
    logger.info("Extracted %d chars from %d pages", len(full_text), len(pages_text))

    return full_text


# ── Long text condensation ────────────────────────────────────

CONDENSE_SYSTEM_PROMPT = """\
你是一位专业的民航法规文档分析师。请提取以下文档片段的核心要点。

要求：
- 保留所有关键数据、数字、术语、条款编号
- 保留具体的标准、限制、要求（如高度、速度、距离等）
- 去除重复内容、套话、格式性文字
- 用简洁的条目式中文输出
- 输出JSON格式：{"key_points": "提取的核心要点文本"}"""

CONDENSE_USER_TEMPLATE = """\
请提取以下文档片段的核心要点：

{chunk_text}"""


def _split_into_chunks(text: str, max_chars: int) -> list[str]:
    """Split text into chunks at paragraph boundaries, each ≤ max_chars."""
    paragraphs = text.split("\n\n")
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0

    for para in paragraphs:
        para_len = len(para) + 2  # account for \n\n separator
        if current and current_len + para_len > max_chars:
            chunks.append("\n\n".join(current))
            current = []
            current_len = 0
        # Single paragraph exceeds max_chars — split by chars as fallback
        if para_len > max_chars and not current:
            for i in range(0, len(para), max_chars):
                chunks.append(para[i:i + max_chars])
            continue
        current.append(para)
        current_len += para_len

    if current:
        chunks.append("\n\n".join(current))
    return chunks


def _condense_chunk(client: OpenAICompatibleClient, chunk: str, idx: int) -> str:
    """Condense a single chunk via LLM. Falls back to truncated original on error."""
    try:
        resp = client.complete_json(
            system_prompt=CONDENSE_SYSTEM_PROMPT,
            user_prompt=CONDENSE_USER_TEMPLATE.format(chunk_text=chunk),
            max_tokens=2000,
            temperature=0.1,
            retries=3,
            timeout=120,
        )
        key_points = resp.payload.get("key_points", "")
        if key_points:
            logger.info("Chunk %d condensed: %d → %d chars", idx, len(chunk), len(key_points))
            return key_points
    except Exception as exc:
        logger.warning("Chunk %d condensation failed: %s, using truncated original", idx, exc)
    # Fallback: return first portion of original chunk
    return chunk[:5000]


def condense_long_text(pdf_text: str, max_chars: int = MAX_PDF_CHARS) -> str:
    """Condense long text via chunked LLM extraction. Short text passes through unchanged."""
    if len(pdf_text) <= max_chars:
        return pdf_text

    logger.info("Text exceeds %d chars (%d chars), starting condensation...", max_chars, len(pdf_text))

    if not OpenAICompatibleClient.is_configured():
        logger.warning("LLM not configured, falling back to hard truncation")
        return pdf_text[:max_chars]

    client = OpenAICompatibleClient(
        settings.llm_api_key,
        settings.llm_base_url,
        settings.llm_model,
    )

    chunks = _split_into_chunks(pdf_text, max_chars)
    logger.info("Split into %d chunks for condensation", len(chunks))

    results: dict[int, str] = {}
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_idx = {
            executor.submit(_condense_chunk, client, chunk, i): i
            for i, chunk in enumerate(chunks)
        }
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            results[idx] = future.result()

    # Merge in original order
    condensed = "\n\n".join(results[i] for i in range(len(chunks)))
    logger.info("Condensation complete: %d → %d chars", len(pdf_text), len(condensed))
    return condensed


# ── LLM dialogue generation ───────────────────────────────────

SYSTEM_PROMPT = """\
你是一位顶级播客脚本编剧，为航空播客《飞行播客》创作幽默风趣的双人对话脚本。

## 角色设定
- **虎机长**：资深机长，飞行经验丰富，性格沉稳但有冷幽默，喜欢用飞行中的真实经历举例子，偶尔自嘲，说话有老飞行员的范儿。
- **千羽**：年轻副驾驶，聪明好学，性格活泼，喜欢提问和吐槽，经常用年轻人的视角看问题，和虎机长形成反差萌。

你的听众全部是中国民航飞行员，拥有专业航空知识，不需要科普基础概念。

## 输出要求
输出严格的JSON对象，格式如下：
{
  "title": "本期播客标题（简短有吸引力，可以带点幽默感）",
  "chapters": [
    {
      "title": "开场",
      "dialogue": [
        {"role": "千羽", "text": "台词内容", "emotion": "warm"},
        {"role": "虎机长", "text": "台词内容", "emotion": "neutral"}
      ]
    },
    {
      "title": "第二章标题",
      "dialogue": [
        {"role": "千羽", "text": "台词内容", "emotion": "curious"},
        {"role": "虎机长", "text": "台词内容", "emotion": "serious"}
      ]
    }
  ]
}

## 情绪标注
每句台词必须标注 emotion 字段，可选值：
- neutral — 平静叙述、过渡衔接（默认）
- curious — 提问、好奇、追问（千羽常用）
- serious — 重要知识点、安全底线（虎机长讲关键内容时）
- warm — 友好互动、鼓励、感谢、共鸣
- humorous — 调侃、比喻、段子、轻松化解复杂概念

整体风格是轻松幽默的氛围下，化繁为简、深入浅出地讲解专业内容。多用比喻和生活化的例子把复杂知识讲透，让听众一听就懂。

## 章节划分
将对话分为 3-6 个 chapters，每章有简短标题：
- 第一章固定为"开场"（包含自我介绍、节日问候、话题引入）
- 最后一章固定为"总结"（虎机长总结 + 千羽结束语）
- 中间按文档主要话题分段，每段一个简短标题（如"起飞最低标准"、"进近着陆要求"）

## 风格要求——轻松幽默、化繁为简
- 像两个飞行员在驾驶舱里闲聊，轻松自然
- 虎机长偶尔抖包袱、讲飞行段子，千羽负责接梗和调侃
- 可以互相调侃，但要有分寸、不低俗
- **化繁为简**：用比喻和类比把复杂概念讲透（如"这就好比开车时……"），让听众一听就懂
- **深入浅出**：严肃知识点用轻松的方式讲出来，让人听着不累
- 千羽可以问"傻问题"，虎机长用幽默又通俗的方式解答
- 适当加入飞行圈内的梗和共鸣点

## 对话结构规则
1. **开头**（固定格式，必须严格遵守）：
   - 第一句（千羽）："欢迎来到飞行播客！我是千羽。"
   - 第二句（虎机长）："我是虎机长。"
   - 如果有节日问候指令，在自我介绍后、进入主题前，两人自然地互动式送上节日祝福（不要生硬地念祝福语，要像聊天一样自然带出）
   - 然后千羽用一个有趣的引子引出今天的主题
2. **主体**：逐一讨论文档核心要点
   - 每段台词控制在1-3句话，节奏要快
   - 互动要密集——提问、反驳、补充、调侃、举例
   - 不允许任何一方连续独白超过3句
   - 每个要点：千羽抛出问题或现象 → 虎机长分析+举例 → 千羽追问或吐槽 → 进入下一个要点
   - 用飞行中真实会遇到的场景来说明
   - **称呼规则**：对话中必须频繁用名字称呼对方！千羽叫"虎哥"或"虎机长"，虎机长叫"千羽"或"小千"。每隔3-5轮至少出现一次称呼。
   - **衔接过渡**：每个话题之间要有自然过渡，比如"说到这个我想起来……"、"虎哥你刚才说的让我想到……"、"千羽你知道吗……"、"对了还有一个事儿……"等
3. **结尾**：虎机长做一句话总结，千羽以固定结束语收尾："感谢收听本期飞行播客。祝大家起降安妥。我们下次云端再会。"

## 语言规范
- 使用中国大陆习惯用语（空客不是空巴、乘务员不是空服员、空管不是航管）
- 口语化，像真的在聊天不是在念稿
- 大量使用语气词（嗯、对、哈哈、得了吧、你别说、还真是、我跟你说）
- 外国航空公司名保留英文（如Delta、United、Lufthansa）
- **数字必须写成中文**：所有数字一律用中文书写，不能用阿拉伯数字。例如：200米→二百米，75米→七十五米，150→一百五十，400→四百，20000→两万，3.5→三点五，15米→十五米，720米→七百二十米。这是为了语音合成自然朗读。

## 篇幅
- 整个对话大约1200-1500字
- 大约25-35个对话轮次（互动要密集）"""

USER_PROMPT_TEMPLATE = """\
请根据以下文档内容，创作一期《飞行播客》的男女双人对话脚本。

## 文档内容
{pdf_text}"""

GREETING_ADDENDUM = """

## 特别指令
{greeting}"""


def generate_dialogue(pdf_text: str) -> dict[str, Any]:
    """Use LLM to generate podcast dialogue from PDF text."""
    if not OpenAICompatibleClient.is_configured():
        raise RuntimeError("LLM is not configured (check LLM_API_KEY, LLM_BASE_URL, LLM_MODEL)")

    client = OpenAICompatibleClient(
        settings.llm_api_key,
        settings.llm_base_url,
        settings.llm_model,
    )

    user_prompt = USER_PROMPT_TEMPLATE.format(pdf_text=pdf_text)
    if settings.podcast_greeting:
        user_prompt += GREETING_ADDENDUM.format(greeting=settings.podcast_greeting)
        logger.info("Added greeting: %s", settings.podcast_greeting[:50])

    logger.info("Generating dialogue via LLM (%s)...", settings.llm_model)
    logger.info("  Prompt length: system=%d chars, user=%d chars",
                len(SYSTEM_PROMPT), len(user_prompt))

    # Heartbeat thread — prints periodic "waiting" logs so the GUI stays alive
    heartbeat_stop = threading.Event()

    def _heartbeat():
        import sys
        start = time.monotonic()
        while not heartbeat_stop.wait(5):
            elapsed = int(time.monotonic() - start)
            print(f"LLM 生成中... 已等待 {elapsed} 秒", flush=True)

    heartbeat = threading.Thread(target=_heartbeat, daemon=True)
    heartbeat.start()

    try:
        resp = client.complete_json(
            system_prompt=SYSTEM_PROMPT,
            user_prompt=user_prompt,
            max_tokens=settings.llm_max_tokens,
            temperature=0.7,  # Higher for creative dialogue
            retries=5,
            timeout=180,
        )
    finally:
        heartbeat_stop.set()
        heartbeat.join(timeout=2)

    dialogue = resp.payload
    title = dialogue.get("title", "飞行播客")
    # Count lines from either format
    if "chapters" in dialogue:
        n_lines = sum(len(ch.get("dialogue", [])) for ch in dialogue["chapters"])
        n_chapters = len(dialogue["chapters"])
        # Per-role stats
        role_counts: dict[str, int] = {}
        for ch in dialogue["chapters"]:
            for line in ch.get("dialogue", []):
                role = line.get("role", "?")
                role_counts[role] = role_counts.get(role, 0) + 1
        role_summary = ", ".join(f"{r}={c}" for r, c in role_counts.items())
        logger.info("Generated dialogue: title='%s', chapters=%d, lines=%d (%s)",
                     title, n_chapters, n_lines, role_summary)
        # Preview first few lines
        preview_lines = dialogue["chapters"][0].get("dialogue", [])[:3]
        for i, line in enumerate(preview_lines):
            text_preview = line.get("text", "")[:30]
            logger.info("  [preview %d] %s: %s...", i + 1, line.get("role", "?"), text_preview)
    else:
        n_lines = len(dialogue.get("dialogue", []))
        logger.info("Generated dialogue: title='%s', lines=%d", title, n_lines)

    return dialogue


# ── Dialogue normalization (new chapters ↔ old flat format) ───

def normalize_dialogue(data: dict) -> tuple[list[dict], list[dict]]:
    """Normalize LLM output to (flat_lines, chapters_info).

    Supports two formats:
    - New: {"chapters": [{"title", "dialogue": [{"role","text","emotion"}]}]}
    - Old: {"dialogue": [{"role","text"}]}

    Returns:
        flat_lines: [{"role", "text", "emotion"}, ...]
        chapters_info: [{"title", "start_line", "end_line"}, ...]
    """
    if "chapters" in data and data["chapters"]:
        flat_lines: list[dict] = []
        chapters_info: list[dict] = []
        for ch in data["chapters"]:
            start = len(flat_lines)
            for line in ch.get("dialogue", []):
                flat_lines.append({
                    "role": line["role"],
                    "text": line["text"],
                    "emotion": line.get("emotion", "neutral"),
                })
            chapters_info.append({
                "title": ch.get("title", ""),
                "start_line": start,
                "end_line": len(flat_lines),
            })
        return flat_lines, chapters_info

    # Old flat format — wrap as single chapter
    lines = data.get("dialogue", [])
    flat_lines = [
        {"role": l["role"], "text": l["text"], "emotion": l.get("emotion", "neutral")}
        for l in lines
    ]
    chapters_info = [{"title": data.get("title", ""), "start_line": 0,
                      "end_line": len(flat_lines)}]
    return flat_lines, chapters_info


# ── Cover image generation ────────────────────────────────────

WECHAT_COVER_RATIO = 2.35  # width / height for large cover
COVER_WIDTH = 900
COVER_HEIGHT = int(COVER_WIDTH / WECHAT_COVER_RATIO)  # ~383


def generate_cover_image(pdf_path: Path, title: str, output_path: Path) -> Path:
    """Generate WeChat cover image from PDF first page, cropped to 2.35:1.

    Output: 900 x ~383 px, suitable for WeChat large cover.
    """
    import fitz
    from PIL import Image

    # Render PDF first page to image
    doc = fitz.open(pdf_path)
    page = doc[0]
    mat = fitz.Matrix(2.0, 2.0)  # 2x for quality
    pix = page.get_pixmap(matrix=mat)
    img_data = pix.tobytes("png")
    doc.close()

    from io import BytesIO
    bg = Image.open(BytesIO(img_data)).convert("RGB")

    # Crop to 2.35:1 ratio
    w, h = bg.size
    target_ratio = WECHAT_COVER_RATIO
    current_ratio = w / h

    if current_ratio < target_ratio:
        # Too tall — crop top/bottom, bias toward top
        new_h = int(w / target_ratio)
        top = (h - new_h) // 4
        bg = bg.crop((0, top, w, top + new_h))
    else:
        # Too wide — crop left/right
        new_w = int(h * target_ratio)
        left = (w - new_w) // 2
        bg = bg.crop((left, 0, left + new_w, h))

    bg = bg.resize((COVER_WIDTH, COVER_HEIGHT), Image.LANCZOS)

    # Save
    output_path.parent.mkdir(parents=True, exist_ok=True)
    bg.save(output_path, quality=92)
    logger.info("Cover image saved: %s (%dx%d)", output_path.name, COVER_WIDTH, COVER_HEIGHT)
    return output_path


# ── Scrollable dialogue HTML ──────────────────────────────────

def render_dialogue_html(title: str, dialogue: list[dict[str, str]],
                         download_url: str = "") -> str:
    """Render podcast dialogue as WeChat-chat-style bubbles for WeChat articles.

    Mimics the native WeChat conversation UI: avatars, white/green bubbles,
    triangle pointers, dark top bar. All inline styles, no SVG/JS.
    """
    # Avatar URLs on WeChat CDN
    QIANYU_AVATAR = ("https://mmbiz.qpic.cn/sz_mmbiz_jpg/"
                     "F9Rrg4S5wWGCugYMMib4DQWwUbia6OoiacJycHicW1lZejia3y9Creiau"
                     "PpibZJ122quuppkicr32YpoTibJXqHFMgY6hx39molicsJxYdm9EZzUqTEmg/0?from=appmsg")
    HU_AVATAR = ("https://mmbiz.qpic.cn/sz_mmbiz_jpg/"
                 "F9Rrg4S5wWGPyAMuCK0DkkDZs4vlIrtEPibDR9icxFiaDSnzAC0IAlfBtauicm9QSB"
                 "RJOYwQulHoXCGC4lllFPAInXVicaVpbPHAicVKiaf2kpiaKXQ/0?from=appmsg")

    bubbles: list[str] = []
    for line in dialogue:
        role = line["role"]
        text = escape(line["text"])
        is_qianyu = role in ("千羽", "女")

        if is_qianyu:
            # Left side — white bubble
            bubble = (
                '<section style="display:flex;align-items:flex-start;margin:10px 10px;'
                'padding-right:100px;">'
                # Avatar
                '<section style="width:36px;height:36px;border-radius:4px;overflow:hidden;'
                'margin-right:8px;flex-shrink:0;">'
                f'<img src="{QIANYU_AVATAR}" style="width:100%;height:100%;display:block;"/>'
                '</section>'
                # Name + bubble column
                '<section style="display:flex;flex-direction:column;">'
                '<span style="font-size:11px;color:#999;margin-bottom:2px;margin-left:2px;">千羽</span>'
                '<section style="background:#fff;color:#000;padding:8px 10px;'
                'border-radius:0 8px 8px 8px;font-size:14px;line-height:1.6;'
                f'word-wrap:break-word;">{text}</section>'
                '</section>'
                '</section>'
            )
        else:
            # Right side — green bubble
            bubble = (
                '<section style="display:flex;flex-direction:row-reverse;align-items:flex-start;'
                'margin:10px 10px;padding-left:100px;">'
                # Avatar
                '<section style="width:36px;height:36px;border-radius:4px;overflow:hidden;'
                'margin-left:8px;flex-shrink:0;">'
                f'<img src="{HU_AVATAR}" style="width:100%;height:100%;display:block;"/>'
                '</section>'
                # Name + bubble column
                '<section style="display:flex;flex-direction:column;align-items:flex-end;">'
                '<span style="font-size:11px;color:#999;margin-bottom:2px;margin-right:2px;">虎机长</span>'
                '<section style="background:#95ec69;color:#000;padding:8px 10px;'
                'border-radius:8px 0 8px 8px;font-size:14px;line-height:1.6;'
                f'word-wrap:break-word;">{text}</section>'
                '</section>'
                '</section>'
            )
        bubbles.append(bubble)

    dialogue_html = "\n".join(bubbles)
    safe_title = escape(title)

    # ── Listening guide ──
    guide_html = (
        '<section style="margin:20px auto 0;max-width:420px;'
        'padding:14px 20px;'
        'background:linear-gradient(135deg,#fffbeb,#fef3c7);'
        'border-radius:12px 12px 0 0;'
        'border:1px solid #fde68a;border-bottom:none;'
        'box-shadow:0 2px 8px rgba(0,0,0,0.04);">'

        '<section style="display:flex;align-items:center;justify-content:center;gap:10px;">'
        '<section style="width:30px;height:30px;border-radius:8px;'
        'background:linear-gradient(135deg,#fbbf24,#f59e0b);'
        'display:flex;align-items:center;justify-content:center;'
        'font-size:14px;flex-shrink:0;color:#fff;'
        'box-shadow:0 2px 6px rgba(245,158,11,0.25);">♪</section>'
        '<section style="color:#92400e;font-size:15px;font-weight:600;'
        'letter-spacing:0.3px;">点击上方音频，边听边看</section>'
        '</section>'

        '</section>'
    )

    # ── WeChat-style top bar ──
    topbar_html = (
        '<section style="margin:0 auto;max-width:420px;'
        'background:#ededed;padding:10px 16px;'
        'border-left:1px solid #e5e7eb;border-right:1px solid #e5e7eb;'
        'display:flex;align-items:center;justify-content:center;">'
        f'<span style="font-size:15px;font-weight:600;color:#333;">{safe_title}</span>'
        '</section>'
    )

    # ── Dialogue body with scroll ──
    body_html = (
        '<section style="margin:0 auto;max-width:420px;height:520px;'
        'overflow:hidden;background:#ededed;'
        'border-left:1px solid #e5e7eb;border-right:1px solid #e5e7eb;'
        'position:relative;">'

        '<section style="text-align:center;font-size:11px;color:#999;'
        'padding:8px 0 4px 0;">上下滑动查看完整对话 ↕</section>'

        '<section style="height:470px;overflow-y:scroll;'
        '-webkit-overflow-scrolling:touch;padding:4px 4px 24px 4px;">'

        f'{dialogue_html}'

        '<section style="text-align:center;color:#bbb;font-size:11px;'
        'padding:20px 0 10px 0;letter-spacing:2px;">— 对话结束 —</section>'

        '</section>'

        '<section style="position:absolute;bottom:0;left:0;right:0;height:30px;'
        'background:linear-gradient(transparent,#ededed);pointer-events:none;"></section>'

        '</section>'
    )

    # ── Bottom border ──
    bottom_html = (
        '<section style="margin:0 auto;max-width:420px;height:8px;'
        'background:#ededed;border-left:1px solid #e5e7eb;'
        'border-right:1px solid #e5e7eb;border-bottom:1px solid #e5e7eb;'
        'border-radius:0 0 12px 12px;"></section>'
    )

    html = f'{guide_html}{topbar_html}{body_html}{bottom_html}'

    # Download link below
    if download_url:
        safe_url = escape(download_url)
        html += (
            '<section style="text-align:center;margin:10px auto;max-width:420px;'
            'padding:8px 16px;">'
            f'<a href="{safe_url}" style="color:#7c3aed;font-size:13px;'
            'text-decoration:none;">点击查看原文件</a>'
            '</section>'
        )

    return html


# ── Stage entry points ────────────────────────────────────────


def _resolve_pdf(pdf_path: str | None) -> Path:
    """Resolve and validate PDF path from argument or env var."""
    import os

    pdf_path = pdf_path or os.getenv("PODCAST_PDF_PATH", "")
    if not pdf_path:
        raise RuntimeError(
            "No PDF path provided. Use --pdf <path> or set PODCAST_PDF_PATH env var."
        )
    pdf_file = Path(pdf_path)
    if not pdf_file.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_file}")
    return pdf_file


def run_script(target_date: str | None = None, *, pdf_path: str | None = None,
               download_url: str = "", output_dir: str | None = None) -> Path:
    """Generate podcast script from PDF (steps 1-3).

    PDF → text extraction → LLM dialogue → script.json + dialogue.html + cover.jpg

    Args:
        target_date: Date string (YYYY-MM-DD) for output naming.
        pdf_path: Path to the input PDF file. Falls back to PODCAST_PDF_PATH env var.
        download_url: Optional URL to the original document for the dialogue HTML footer.
        output_dir: Custom output base directory. Defaults to settings.output_dir.

    Returns:
        Path to the work directory containing script.json, dialogue.html, cover.jpg.
    """
    day = target_date or beijing_today_str()
    pdf_file = _resolve_pdf(pdf_path)

    pdf_name = pdf_file.stem
    base_dir = Path(output_dir) if output_dir else settings.output_dir / "podcast"
    work_dir = base_dir / f"{day}_{pdf_name}"
    work_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("Podcast script: %s", pdf_file.name)
    logger.info("Output dir: %s", work_dir)
    logger.info("=" * 60)

    # Step 1: Extract PDF text
    logger.info("Step 1/3: Extracting PDF text...")
    pdf_text = extract_pdf_text(pdf_file)
    if not pdf_text.strip():
        raise RuntimeError(f"No text extracted from PDF: {pdf_file}")
    logger.info("PDF text ready: %d chars", len(pdf_text))

    # Step 1.5: Condense if text exceeds limit
    pdf_text = condense_long_text(pdf_text)

    # Step 2: Generate dialogue via LLM
    logger.info("Step 2/3: Generating dialogue script...")
    dialogue_data = generate_dialogue(pdf_text)

    # Save dialogue script for reference
    script_path = work_dir / "script.json"
    dump_json(script_path, dialogue_data)
    logger.info("Script saved: %s", script_path)

    # Normalize to flat lines + chapter info (supports old and new format)
    flat_lines, chapters_info = normalize_dialogue(dialogue_data)
    if not flat_lines:
        raise RuntimeError("LLM returned empty dialogue")
    logger.info("Dialogue: %d lines, %d chapters", len(flat_lines), len(chapters_info))

    # Generate scrollable dialogue HTML for WeChat
    title = dialogue_data.get("title", pdf_name)
    dialogue_html = render_dialogue_html(title, flat_lines, download_url=download_url)
    html_path = work_dir / "dialogue.html"
    html_path.write_text(dialogue_html, encoding="utf-8")
    logger.info("Dialogue HTML saved: %s (%d bytes)", html_path.name, len(dialogue_html.encode("utf-8")))

    # Step 3: Generate cover image
    logger.info("Step 3/3: Generating cover image...")
    cover_path = work_dir / "cover.jpg"
    generate_cover_image(pdf_file, title, cover_path)

    # Save partial metadata (no audio yet)
    meta = {
        "date": day,
        "pdf_source": str(pdf_file),
        "title": title,
        "download_url": download_url,
        "cover_path": str(cover_path),
        "dialogue_html_path": str(html_path),
        "dialogue_lines": len(flat_lines),
        "total_chars": sum(len(l["text"]) for l in flat_lines),
    }
    dump_json(work_dir / "metadata.json", meta)

    logger.info("Script generation complete: %s", work_dir)
    return work_dir


def run_audio(*, work_dir: str | Path) -> Path:
    """Generate podcast audio from an existing script (steps 4-5).

    Reads script.json from work_dir → TTS synthesis → MP3 concatenation.

    Args:
        work_dir: Path to the podcast work directory containing script.json.

    Returns:
        Path to the generated MP3 file.
    """
    from urllib.parse import quote

    work_dir = Path(work_dir)
    if not work_dir.exists():
        raise FileNotFoundError(f"Work directory not found: {work_dir}")

    script_path = work_dir / "script.json"
    if not script_path.exists():
        raise FileNotFoundError(f"script.json not found in: {work_dir}")

    logger.info("=" * 60)
    logger.info("Podcast audio: %s", work_dir.name)
    logger.info("=" * 60)

    # Load script
    dialogue_data = json.loads(script_path.read_text(encoding="utf-8"))
    flat_lines, chapters_info = normalize_dialogue(dialogue_data)
    if not flat_lines:
        raise RuntimeError("script.json contains empty dialogue")
    logger.info("Loaded script: %d lines, %d chapters", len(flat_lines), len(chapters_info))

    title = dialogue_data.get("title", work_dir.name)

    # Step 1: TTS synthesis
    logger.info("Step 1/2: Synthesizing %d dialogue segments...", len(flat_lines))
    segments_dir = work_dir / "segments"
    segment_files = synthesize_dialogue(flat_lines, segments_dir)

    # Step 2: Concatenate (with music + chapters if assets available)
    logger.info("Step 2/2: Concatenating audio...")
    mp3_path = work_dir / f"{title}.mp3"
    chapter_timestamps = concatenate_audio(
        segment_files, mp3_path,
        chapters=chapters_info,
        num_lines=len(flat_lines),
    )

    # Update metadata with audio info
    meta_path = work_dir / "metadata.json"
    meta: dict = {}
    if meta_path.exists():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))

    dir_name = work_dir.name
    mp3_filename = mp3_path.name
    r2_key = f"podcast/{dir_name}/{mp3_filename}"

    # Upload MP3 to R2
    try:
        mp3_cdn_url = r2_upload_file(mp3_path, r2_key)
    except Exception as e:
        logger.error("R2 upload failed, using constructed URL: %s", e)
        mp3_cdn_url = (
            f"https://{settings.r2_domain}/podcast/"
            f"{quote(dir_name)}/{quote(mp3_filename)}"
        )
    meta.update({
        "title": title,
        "mp3_path": str(mp3_path),
        "mp3_cdn_url": mp3_cdn_url,
        "dialogue_lines": len(flat_lines),
        "total_chars": sum(len(l["text"]) for l in flat_lines),
        "chapters": chapter_timestamps,
    })
    dump_json(meta_path, meta)

    logger.info("Audio generation complete: %s", mp3_path)
    return mp3_path


def run(target_date: str | None = None, *, pdf_path: str | None = None,
        download_url: str = "") -> Path:
    """Full podcast generation pipeline (script + audio).

    Used by GitHub Actions and CLI ``python run.py podcast``.

    Args:
        target_date: Date string (YYYY-MM-DD) for output naming.
        pdf_path: Path to the input PDF file. Falls back to PODCAST_PDF_PATH env var.
        download_url: Optional URL to the original document for the dialogue HTML footer.

    Returns:
        Path to the generated MP3 file.
    """
    work_dir = run_script(target_date, pdf_path=pdf_path, download_url=download_url)
    return run_audio(work_dir=work_dir)

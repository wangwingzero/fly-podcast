from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pdfplumber

from flying_podcast.core.config import settings
from flying_podcast.core.io_utils import dump_json
from html import escape
from flying_podcast.core.llm_client import OpenAICompatibleClient
from flying_podcast.core.logging_utils import get_logger
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

    if len(full_text) > MAX_PDF_CHARS:
        logger.warning("PDF text truncated from %d to %d chars", len(full_text), MAX_PDF_CHARS)
        full_text = full_text[:MAX_PDF_CHARS]

    return full_text


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
- **英文缩写用小写连读形式**：英文缩写必须写成小写字母以便语音合成时连读而非逐字母拼读。例如：CAT→cat，RVR→rvr，ILS→ils，HUD→hud，DH→dh，NPA→npa，CDFA→cdfa，A-SMGCS→a-smgcs。只有专有名词（如航空公司名Delta）保持正常大小写。

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
    resp = client.complete_json(
        system_prompt=SYSTEM_PROMPT,
        user_prompt=user_prompt,
        max_tokens=settings.llm_max_tokens,
        temperature=0.7,  # Higher for creative dialogue
        retries=5,
        timeout=120,
    )

    dialogue = resp.payload
    title = dialogue.get("title", "飞行播客")
    # Count lines from either format
    if "chapters" in dialogue:
        n_lines = sum(len(ch.get("dialogue", [])) for ch in dialogue["chapters"])
        n_chapters = len(dialogue["chapters"])
        logger.info("Generated dialogue: title='%s', lines=%d, chapters=%d",
                     title, n_lines, n_chapters)
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
    """Render podcast dialogue as scrollable chat-bubble HTML for WeChat articles.

    Uses section nesting with overflow-y:scroll, compatible with WeChat MP editor.
    If download_url is provided, a link to the original document is shown below the dialogue.
    """
    # Bubble HTML for each line
    bubbles: list[str] = []
    for line in dialogue:
        role = line["role"]
        text = escape(line["text"])
        is_qianyu = role in ("千羽", "女")

        if is_qianyu:
            bubble = (
                '<section style="margin-bottom:14px;display:flex;flex-direction:column;align-items:flex-start;">'
                '<span style="background:#1e6fff;color:#fff;padding:2px 8px;border-radius:4px;'
                'font-size:12px;font-weight:bold;margin-bottom:4px;">千羽</span>'
                '<section style="background:#fff;border:1px solid #e8e8e8;padding:10px 12px;'
                'border-radius:0 12px 12px 12px;font-size:15px;line-height:1.7;color:#333;'
                f'max-width:85%;">{text}</section>'
                '</section>'
            )
        else:
            bubble = (
                '<section style="margin-bottom:14px;display:flex;flex-direction:column;align-items:flex-end;">'
                '<span style="background:#07c160;color:#fff;padding:2px 8px;border-radius:4px;'
                'font-size:12px;font-weight:bold;margin-bottom:4px;">虎机长</span>'
                '<section style="background:#e7f8ee;border:1px solid #d0f0d8;padding:10px 12px;'
                'border-radius:12px 0 12px 12px;font-size:15px;line-height:1.7;color:#333;'
                f'max-width:85%;">{text}</section>'
                '</section>'
            )
        bubbles.append(bubble)

    dialogue_html = "\n".join(bubbles)
    safe_title = escape(title)

    html = (
        # Outer: fixed height, hidden overflow, rounded card
        '<section style="margin:15px auto;width:100%;max-width:420px;height:520px;'
        'overflow:hidden;border-radius:12px;border:1px solid #e0e0e0;'
        'box-shadow:0 2px 12px rgba(0,0,0,0.06);background:#f7f8fa;'
        'position:relative;">'

        # Title bar
        '<section style="background:linear-gradient(135deg,#1e6fff,#4e95ff);'
        'padding:12px 16px;color:#fff;font-size:15px;font-weight:bold;'
        f'text-align:center;letter-spacing:1px;">{safe_title}</section>'

        # Scroll hint (top fade)
        '<section style="text-align:center;font-size:11px;color:#aaa;'
        'padding:8px 0 2px 0;">上下滑动查看完整对话 ↕</section>'

        # Inner: scrollable area
        '<section style="height:430px;overflow-y:scroll;'
        '-webkit-overflow-scrolling:touch;padding:6px 12px 20px 12px;">'

        f'{dialogue_html}'

        # End marker
        '<section style="text-align:center;color:#ccc;font-size:11px;'
        'padding:16px 0 8px 0;">— 对话结束 —</section>'

        '</section>'

        # Bottom fade overlay for visual hint
        '<section style="position:absolute;bottom:0;left:0;right:0;height:30px;'
        'background:linear-gradient(transparent,#f7f8fa);pointer-events:none;'
        'border-radius:0 0 12px 12px;"></section>'

        '</section>'
    )

    # Download link below the dialogue card
    if download_url:
        safe_url = escape(download_url)
        html += (
            '<section style="text-align:center;margin:10px auto;max-width:420px;'
            'padding:8px 16px;">'
            f'<a href="{safe_url}" style="color:#1e6fff;font-size:13px;'
            'text-decoration:none;">点击查看原文件</a>'
            '</section>'
        )

    return html


# ── Main stage ─────────────────────────────────────────────────

def run(target_date: str | None = None, *, pdf_path: str | None = None,
        download_url: str = "") -> Path:
    """
    Podcast generation stage.

    Args:
        target_date: Date string (YYYY-MM-DD) for output naming.
        pdf_path: Path to the input PDF file. Falls back to PODCAST_PDF_PATH env var.
        download_url: Optional URL to the original document for the dialogue HTML footer.

    Returns:
        Path to the generated MP3 file.
    """
    import os

    day = target_date or beijing_today_str()
    pdf_path = pdf_path or os.getenv("PODCAST_PDF_PATH", "")

    if not pdf_path:
        raise RuntimeError(
            "No PDF path provided. Use --pdf <path> or set PODCAST_PDF_PATH env var."
        )

    pdf_file = Path(pdf_path)
    if not pdf_file.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_file}")

    pdf_name = pdf_file.stem
    work_dir = settings.output_dir / "podcast" / f"{day}_{pdf_name}"
    work_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("Podcast stage: %s", pdf_file.name)
    logger.info("Output dir: %s", work_dir)
    logger.info("=" * 60)

    # Step 1: Extract PDF text
    logger.info("Step 1/5: Extracting PDF text...")
    pdf_text = extract_pdf_text(pdf_file)
    if not pdf_text.strip():
        raise RuntimeError(f"No text extracted from PDF: {pdf_file}")

    # Step 2: Generate dialogue via LLM
    logger.info("Step 2/5: Generating dialogue script...")
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
    logger.info("Dialogue HTML saved: %s", html_path)

    # Step 3: Generate cover image
    logger.info("Step 3/5: Generating cover image...")
    cover_path = work_dir / "cover.jpg"
    generate_cover_image(pdf_file, title, cover_path)

    # Step 4: TTS synthesis
    logger.info("Step 4/5: Synthesizing %d dialogue segments...", len(flat_lines))
    segments_dir = work_dir / "segments"
    segment_files = synthesize_dialogue(flat_lines, segments_dir)

    # Step 5: Concatenate (with music + chapters if assets available)
    logger.info("Step 5/5: Concatenating audio...")
    title = dialogue_data.get("title", pdf_name)
    mp3_path = work_dir / f"{title}.mp3"
    chapter_timestamps = concatenate_audio(
        segment_files, mp3_path,
        chapters=chapters_info,
        num_lines=len(flat_lines),
    )

    # Save metadata
    dir_name = work_dir.name  # e.g. "2026-02-21_全天候运行规定"
    mp3_filename = mp3_path.name
    from urllib.parse import quote
    mp3_cdn_url = (
        f"https://{settings.r2_domain}/podcast/"
        f"{quote(dir_name)}/{quote(mp3_filename)}"
    )
    meta = {
        "date": day,
        "pdf_source": str(pdf_file),
        "title": title,
        "mp3_path": str(mp3_path),
        "mp3_cdn_url": mp3_cdn_url,
        "cover_path": str(cover_path),
        "dialogue_html_path": str(html_path),
        "dialogue_lines": len(flat_lines),
        "total_chars": sum(len(l["text"]) for l in flat_lines),
        "chapters": chapter_timestamps,
    }
    dump_json(work_dir / "metadata.json", meta)

    logger.info("Podcast complete: %s", mp3_path)
    return mp3_path

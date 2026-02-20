"""Image sourcing for article illustrations.

Priority: Unsplash -> Pixabay -> Gemini AI (primary) -> Grok AI (backup).
"""
from __future__ import annotations

import base64
import logging
import re

import requests

from .config import settings

logger = logging.getLogger(__name__)

# Chinese-to-English keyword mapping for common aviation terms
_AVIATION_KEYWORDS: dict[str, str] = {
    "航班": "flight",
    "飞机": "airplane aircraft",
    "客机": "passenger aircraft",
    "机场": "airport",
    "航线": "airline route",
    "起飞": "takeoff",
    "降落": "landing",
    "机舱": "cabin",
    "机长": "pilot cockpit",
    "空客": "airbus",
    "波音": "boeing",
    "发动机": "jet engine",
    "适航": "airworthiness",
    "安全": "aviation safety",
    "维修": "aircraft maintenance",
    "退租": "aircraft",
    "紧急": "emergency landing",
    "航司": "airline",
    "民航": "civil aviation",
    "春运": "airport passengers",
    "直航": "direct flight",
    "机型": "aircraft",
    "罚款": "aviation regulation",
    "出售": "commercial aircraft",
    "返航": "divert emergency",
    "颠簸": "turbulence",
    "模拟机": "flight simulator",
    "订单": "aircraft order delivery",
    "交付": "aircraft delivery",
    "停飞": "grounded aircraft",
}

# Airline name mapping - searched FIRST, highest priority
_AIRLINE_NAMES: dict[str, str] = {
    "国航": "Air China",
    "中国国际航空": "Air China",
    "南航": "China Southern Airlines",
    "中国南方航空": "China Southern Airlines",
    "东航": "China Eastern Airlines",
    "中国东方航空": "China Eastern Airlines",
    "海航": "Hainan Airlines",
    "海南航空": "Hainan Airlines",
    "厦航": "Xiamen Airlines",
    "厦门航空": "Xiamen Airlines",
    "深航": "Shenzhen Airlines",
    "深圳航空": "Shenzhen Airlines",
    "川航": "Sichuan Airlines",
    "四川航空": "Sichuan Airlines",
    "春秋航空": "Spring Airlines",
    "吉祥航空": "Juneyao Airlines",
    "山东航空": "Shandong Airlines",
    "西部航空": "West Air",
    "联合航空": "United Airlines",
    "美国航空": "American Airlines",
    "达美航空": "Delta Airlines",
    "英国航空": "British Airways",
    "法国航空": "Air France",
    "汉莎航空": "Lufthansa",
    "阿联酋航空": "Emirates",
    "卡塔尔航空": "Qatar Airways",
    "新加坡航空": "Singapore Airlines",
    "国泰航空": "Cathay Pacific",
    "全日空": "ANA All Nippon Airways",
    "日本航空": "Japan Airlines",
    "大韩航空": "Korean Air",
    "印度航空": "Air India",
    "澳洲航空": "Qantas",
    "芬兰航空": "Finnair",
    "北欧航空": "SAS Scandinavian Airlines",
    "土耳其航空": "Turkish Airlines",
    "LATAM": "LATAM Airlines",
    "瑞安航空": "Ryanair",
    "易捷航空": "easyJet",
}


def _extract_search_query(title: str) -> str:
    """Extract English search keywords from an article title.

    Combines airline name (if found) with aircraft models and aviation terms
    for more targeted stock photo results.
    """
    terms: list[str] = []

    # 1. Match airline names (longest match wins)
    for cn, en in sorted(_AIRLINE_NAMES.items(), key=lambda x: len(x[0]), reverse=True):
        if cn in title:
            terms.append(en)
            break

    # Also match English airline names directly in title
    for en_name in ["JetBlue", "Delta", "United", "Southwest", "American Airlines",
                     "Ryanair", "easyJet", "Emirates", "Qatar", "Lufthansa",
                     "Air France", "British Airways", "Boeing", "Airbus",
                     "Porter", "Spirit", "Frontier", "Alaska Airlines",
                     "TAP", "Wizz Air", "Volaris", "Avianca"]:
        if en_name in title and en_name not in " ".join(terms):
            terms.append(en_name)
            break

    # 2. Extract aircraft models
    models = re.findall(r"(?:A\d{3}|[Bb]-?\d{3,4}|737|747|777|787|320|321|330|350|380)", title)
    for m in models[:1]:
        terms.append(m)

    # 3. Fill with aviation terms for context
    for cn, en in _AVIATION_KEYWORDS.items():
        if cn in title:
            terms.append(en.split()[0])  # take first word only
            if len(terms) >= 3:
                break

    if not terms:
        terms = ["aviation", "airplane"]

    return " ".join(terms[:4])


# ── Unsplash ──────────────────────────────────────────────


def _search_unsplash(query: str) -> bytes | None:
    """Search Unsplash for a photo, return image bytes or None."""
    keys = [k for k in [settings.unsplash_access_key, settings.unsplash_access_key_2] if k]
    if not keys:
        return None

    for key in keys:
        try:
            resp = requests.get(
                "https://api.unsplash.com/search/photos",
                params={
                    "query": query,
                    "per_page": 1,
                    "orientation": "landscape",
                    "content_filter": "high",
                },
                headers={"Authorization": f"Client-ID {key}"},
                timeout=15,
            )
            if resp.status_code == 403:
                logger.info("Unsplash key rate-limited, trying next")
                continue
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", [])
            if not results:
                logger.info("Unsplash: no results for '%s'", query)
                return None

            img_url = results[0]["urls"].get("regular", "")
            if not img_url:
                return None

            img_resp = requests.get(img_url, timeout=20)
            img_resp.raise_for_status()
            logger.info("Unsplash: found image for '%s'", query)
            return img_resp.content
        except Exception as exc:
            logger.warning("Unsplash search failed: %s", exc)
            continue

    return None


# ── Pixabay ───────────────────────────────────────────────


def _search_pixabay(query: str) -> bytes | None:
    """Search Pixabay for a photo, return image bytes or None."""
    if not settings.pixabay_api_key:
        return None

    try:
        resp = requests.get(
            "https://pixabay.com/api/",
            params={
                "key": settings.pixabay_api_key,
                "q": query,
                "image_type": "photo",
                "orientation": "horizontal",
                "per_page": 3,
                "safesearch": "true",
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        hits = data.get("hits", [])
        if not hits:
            logger.info("Pixabay: no results for '%s'", query)
            return None

        img_url = hits[0].get("largeImageURL", "")
        if not img_url:
            return None

        img_resp = requests.get(img_url, timeout=20)
        img_resp.raise_for_status()
        logger.info("Pixabay: found image for '%s'", query)
        return img_resp.content
    except Exception as exc:
        logger.warning("Pixabay search failed: %s", exc)
        return None


# ── Gemini AI (primary image generator) ──────────────────


def _call_gemini_api(base_url: str, api_key: str, model: str, prompt: str,
                     size: str = "1024x1024") -> bytes | None:
    """Call Gemini image generation via chat completions endpoint.

    Gemini returns images in message.images[].image_url.url as base64 data URIs.
    """
    try:
        resp = requests.post(
            f"{base_url.rstrip('/')}/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": model,
                "messages": [{"role": "user", "content": f"Generate an image: {prompt}"}],
                "max_tokens": 4096,
            },
            timeout=120,
        )
        resp.raise_for_status()
        data = resp.json()
        msg = data.get("choices", [{}])[0].get("message", {})

        # Extract from images array (Gemini format)
        images = msg.get("images", [])
        if images:
            url = images[0].get("image_url", {}).get("url", "")
            if url.startswith("data:image"):
                b64_data = url.split(",", 1)[1]
                return base64.b64decode(b64_data)

        # Fallback: check if content itself is base64
        content = msg.get("content", "") or ""
        if content.startswith("data:image"):
            b64_data = content.split(",", 1)[1]
            return base64.b64decode(b64_data)

        return None
    except Exception as exc:
        logger.warning("Gemini API failed (%s): %s", base_url[:40], exc)
        return None


# ── Grok AI (backup image generator) ─────────────────────


_STATIC_PROMPT_TEMPLATE = (
    "Create a photorealistic, editorial-quality news illustration for this aviation article. "
    "Title: {title}. Context: {context}. "
    "Style: wide-angle cinematic shot, professional photography, no text or watermarks, "
    "aviation/aerospace theme, suitable as a news article header image."
)


def _build_prompt(title: str, body: str) -> str:
    """Build a static fallback prompt (used when LLM prompt design fails)."""
    context = body[:150].rstrip() if body else title
    return _STATIC_PROMPT_TEMPLATE.format(title=title, context=context)


def _call_grok_api(base_url: str, api_key: str, model: str, prompt: str,
                    size: str = "1024x1024") -> bytes | None:
    try:
        resp = requests.post(
            f"{base_url.rstrip('/')}/v1/images/generations",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"model": model, "prompt": prompt, "n": 1, "size": size},
            timeout=90,
        )
        resp.raise_for_status()
        data = resp.json()
        items = data.get("data", [])
        if not items:
            return None

        item = items[0]
        if "url" in item and item["url"]:
            img_resp = requests.get(item["url"], timeout=30)
            img_resp.raise_for_status()
            return img_resp.content
        if "b64_json" in item and item["b64_json"]:
            return base64.b64decode(item["b64_json"])
        return None
    except Exception as exc:
        logger.warning("Grok API failed (%s): %s", base_url[:40], exc)
        return None


# ── LLM-designed image prompts ────────────────────────────


_LLM_IMAGE_PROMPT_TEMPLATE = (
    "你是航空新闻视觉编辑。根据以下航空新闻，写一段英文画图AI提示词，用于生成文章配图。\n"
    "要求：\n"
    "1. 摄影级画质，电影感光影，专业航空杂志风格\n"
    "2. 不要文字、水印、UI元素\n"
    "3. 根据新闻内容选择最合适的具体场景\n"
    "只输出英文提示词，不要任何其他内容。\n\n"
    "标题：{title}\n内容：{body}"
)


def _build_llm_image_prompt(title: str, body: str) -> str:
    """Use the main LLM to design a tailored image generation prompt.

    Falls back to the static template if LLM is not configured or fails.
    """
    if not settings.llm_api_key:
        return _build_prompt(title, body)

    from .llm_client import OpenAICompatibleClient

    try:
        client = OpenAICompatibleClient(
            api_key=settings.llm_api_key,
            base_url=settings.llm_base_url,
            model=settings.llm_model,
        )
        user_content = _LLM_IMAGE_PROMPT_TEMPLATE.format(
            title=title, body=body[:200],
        )
        result = client.complete_json(
            system_prompt="You are a visual editor. Output only the English prompt text.",
            user_prompt=user_content,
            max_tokens=300,
            temperature=0.7,
            retries=1,
            timeout=30,
        )
        # complete_json returns a JSONResponse; extract raw text
        text = result.raw_text.strip() if result and result.raw_text else ""
        if text and len(text) > 20:
            logger.info("LLM image prompt: %s", text[:80])
            return text
    except Exception as exc:
        logger.debug("LLM image prompt failed: %s", exc)

    return _build_prompt(title, body)


# ── AI image generation (Gemini primary, Grok backup) ────


def _generate_with_ai(title: str, body: str) -> bytes | None:
    """Generate an article image using AI.

    Chain: Gemini (primary) -> Grok (backup).
    Uses LLM-designed prompt when available, static fallback otherwise.
    """
    if not settings.image_gen_api_key:
        return None

    prompt = _build_llm_image_prompt(title, body)
    logger.info("AI image: generating for '%s'", title[:40])

    # Primary: Gemini (chat completions format)
    result = _call_gemini_api(
        settings.image_gen_base_url,
        settings.image_gen_api_key,
        settings.image_gen_model,
        prompt,
    )
    if result:
        logger.info("Gemini: image generated for '%s'", title[:40])
        return result

    # Backup: Grok (images/generations format)
    if settings.image_gen_backup_api_key:
        logger.info("Gemini failed, trying Grok backup")
        result = _call_grok_api(
            settings.image_gen_backup_base_url,
            settings.image_gen_backup_api_key,
            settings.image_gen_backup_model,
            prompt,
        )
        if result:
            return result

    return None


# ── Public API ────────────────────────────────────────────


def generate_article_image(title: str, body: str = "") -> bytes | None:
    """Find or generate an illustration for an article.

    Chain: Unsplash -> Pixabay -> Gemini AI (primary) -> Grok AI (backup).
    Returns image bytes or None.
    """
    query = _extract_search_query(title)
    logger.info("Image search query: '%s' (from: %s)", query, title[:40])

    # 1. Unsplash
    data = _search_unsplash(query)
    if data:
        return data

    # 2. Pixabay
    data = _search_pixabay(query)
    if data:
        return data

    # 3. AI generation (Gemini primary, Grok backup)
    return _generate_with_ai(title, body)


def generate_cover_image(prompt: str, size: str = "1792x1024") -> bytes | None:
    """Generate a cover image from a pre-built prompt.

    Chain: Gemini (primary) -> Grok (backup).
    Called from publish.py with an LLM-designed prompt.
    """
    if not settings.image_gen_api_key:
        return None

    # Primary: Gemini
    result = _call_gemini_api(
        settings.image_gen_base_url,
        settings.image_gen_api_key,
        settings.image_gen_model,
        prompt,
        size=size,
    )
    if result:
        return result

    # Backup: Grok
    if settings.image_gen_backup_api_key:
        logger.info("Cover: Gemini failed, trying Grok backup")
        result = _call_grok_api(
            settings.image_gen_backup_base_url,
            settings.image_gen_backup_api_key,
            settings.image_gen_backup_model,
            prompt,
            size=size,
        )
        if result:
            return result

    return None


def search_public_image_url(title: str) -> str:
    """Search for a publicly accessible image URL for an article.

    Chain: Unsplash -> Pixabay.  Returns a direct URL string or "".
    Unlike generate_article_image(), does NOT download image bytes —
    just returns the public URL for embedding in external web pages.
    """
    query = _extract_search_query(title)

    # 1. Unsplash
    keys = [k for k in [settings.unsplash_access_key, settings.unsplash_access_key_2] if k]
    for key in keys:
        try:
            resp = requests.get(
                "https://api.unsplash.com/search/photos",
                params={
                    "query": query,
                    "per_page": 1,
                    "orientation": "landscape",
                    "content_filter": "high",
                },
                headers={"Authorization": f"Client-ID {key}"},
                timeout=15,
            )
            if resp.status_code == 403:
                continue
            resp.raise_for_status()
            results = resp.json().get("results", [])
            if results:
                url = results[0]["urls"].get("regular", "")
                if url:
                    logger.info("Unsplash URL for '%s': %s", query, url[:60])
                    return url
        except Exception:
            continue

    # 2. Pixabay
    if settings.pixabay_api_key:
        try:
            resp = requests.get(
                "https://pixabay.com/api/",
                params={
                    "key": settings.pixabay_api_key,
                    "q": query,
                    "image_type": "photo",
                    "orientation": "horizontal",
                    "per_page": 3,
                    "safesearch": "true",
                },
                timeout=15,
            )
            resp.raise_for_status()
            hits = resp.json().get("hits", [])
            if hits:
                url = hits[0].get("webformatURL", "")
                if url:
                    logger.info("Pixabay URL for '%s': %s", query, url[:60])
                    return url
        except Exception:
            pass

    return ""

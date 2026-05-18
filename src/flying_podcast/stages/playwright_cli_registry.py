from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


_DEFAULT_ARTICLE_IMAGE_EVAL = (
    "/* articleImage */ (() => {"
    " const bad = /logo|favicon|icon|placeholder|sprite|avatar|blank/i;"
    " const pick = img => { if (!img) return '';"
    " const srcset = img.getAttribute('srcset') || img.getAttribute('data-srcset') || '';"
    " const fromSet = srcset ? srcset.split(',').map(x => x.trim().split(/\\s+/)[0]).filter(Boolean).pop() : '';"
    " const raw = fromSet || img.getAttribute('data-src') || img.getAttribute('data-lazy-src') ||"
    " img.getAttribute('data-original') || img.getAttribute('src') || '';"
    " if (!raw || raw.startsWith('data:')) return '';"
    " try { const u = new URL(raw, location.href).href; return bad.test(new URL(u).pathname) ? '' : u; } catch (e) { return ''; } };"
    " const scope = document.querySelector('article') || document.querySelector('main') || document.body;"
    " for (const img of Array.from(scope.querySelectorAll('figure img, picture img, img'))) { const u = pick(img); if (u) return u; }"
    " for (const selector of ['meta[property=\"og:image\"]','meta[name=\"twitter:image\"]']) {"
    " const node = document.querySelector(selector); const raw = node && node.getAttribute('content');"
    " if (raw) { try { const u = new URL(raw, location.href).href; if (!bad.test(new URL(u).pathname)) return u; } catch (e) {} }"
    " }"
    " return ''; })()"
)


_DEFAULT_ARTICLE_EVAL = (
    "/* articleText */ Array.from((document.querySelector('article') || document.querySelector('main') || document.body)"
    ".querySelectorAll('p, h1, h2, h3, li'))"
    ".map(el => (el.textContent || '').replace(/\\s+/g, ' ').trim())"
    ".filter(text => text.length > 30).join('\\n')"
)


_DEFAULT_ARTICLE_DATE_EVAL = (
    "/* articleDate */ (() => {"
    " const selectors = ['meta[property=\"article:published_time\"]','meta[name=\"article:published_time\"]','meta[property=\"og:published_time\"]','meta[name=\"parsely-pub-date\"]','time[datetime]','[datetime]','[class*=\"date\"]','[class*=\"time\"]','[class*=\"published\"]'];"
    " for (const selector of selectors) {"
    "  const node = document.querySelector(selector);"
    "  if (!node) continue;"
    "  const value = (node.getAttribute && (node.getAttribute('datetime') || node.getAttribute('content'))) || node.textContent || '';"
    "  const clean = (value || '').replace(/\\s+/g, ' ').trim();"
    "  if (clean) return clean;"
    " }"
    " return ''; })()"
)


_DEFAULT_ROUTE_BLOCK_CODE = (
    "async page => {"
    " const blockedTypes = new Set(['image','media','font']);"
    " const blockedHosts = /doubleclick|googletagmanager|google-analytics|googleadservices|"
    "fundingchoicesmessages|facebook\\.net|adservice|adsystem|chartbeat|outbrain|taboola|"
    "scorecardresearch|hotjar|optimizely/i;"
    " await page.route('**/*', async route => {"
    "   const request = route.request();"
    "   const url = request.url();"
    "   if (blockedTypes.has(request.resourceType()) || blockedHosts.test(url)) {"
    "     await route.abort().catch(() => {});"
    "     return;"
    "   }"
    "   await route.continue().catch(() => {});"
    " });"
    "}"
)


def _build_list_eval(
    anchor_selectors: list[str],
    *,
    container_selector: str = "article, li, div",
    heading_selector: str = "h1,h2,h3,[class*='title'],[class*='headline']",
    summary_selector: str = "p,[class*='summary'],[class*='dek'],[class*='excerpt'],[class*='description']",
    time_selector: str = "time,[datetime],[class*='date'],[class*='time'],[class*='published']",
    image_selector: str = "picture img, figure img, img",
) -> str:
    return (
        "JSON.stringify((() => {"
        f" const selectors = {json.dumps(anchor_selectors)};"
        f" const containerSelector = {json.dumps(container_selector)};"
        f" const headingSelector = {json.dumps(heading_selector)};"
        f" const summarySelector = {json.dumps(summary_selector)};"
        f" const timeSelector = {json.dumps(time_selector)};"
        f" const imageSelector = {json.dumps(image_selector)};"
        " const clean = value => (value || '').replace(/\\s+/g, ' ').trim();"
        " const imageFrom = img => { if (!img) return '';"
        " const srcset = img.getAttribute('srcset') || img.getAttribute('data-srcset') || '';"
        " const fromSet = srcset ? srcset.split(',').map(x => x.trim().split(/\\s+/)[0]).filter(Boolean).pop() : '';"
        " const raw = fromSet || img.getAttribute('data-src') || img.getAttribute('data-lazy-src') || img.getAttribute('data-original') || img.getAttribute('src') || '';"
        " if (!raw || raw.startsWith('data:')) return '';"
        " try { return new URL(raw, location.href).href; } catch (e) { return ''; } };"
        " const anchors = selectors.flatMap(sel => Array.from(document.querySelectorAll(sel)));"
        " const items = anchors.map(anchor => {"
        "  const href = anchor.getAttribute('href') || ''; let url = '';"
        "  try { url = new URL(href, location.href).href; } catch (e) { return null; }"
        "  const inner = anchor.querySelector(containerSelector);"
        "  const container = (inner && inner !== anchor) ? inner : (anchor.closest(containerSelector) || anchor.parentElement || anchor);"
        "  const heading = container.querySelector(headingSelector);"
        "  const lines = (container.innerText || anchor.innerText || '').split(/\\n+/).map(clean).filter(Boolean);"
        "  const firstLine = lines[0] || '';"
        "  const lineTitle = (/^[A-Z &-]{3,}$/.test(firstLine) && lines[1]) ? lines[1] : firstLine;"
        "  const title = clean((heading && heading.textContent) || anchor.getAttribute('aria-label') || lineTitle || anchor.textContent);"
        "  const summaryNode = container.querySelector(summarySelector);"
        "  const summary = clean((summaryNode && summaryNode.textContent) || container.textContent || '');"
        "  const timeNode = container.querySelector(timeSelector) || anchor.closest('time');"
        "  const published_at = clean((timeNode && (timeNode.getAttribute('datetime') || timeNode.textContent)) || '');"
        "  const image_url = imageFrom(container.querySelector(imageSelector));"
        "  return {title, url, summary, published_at, image_url};"
        " }).filter(item => item && item.url.startsWith('http') && (item.title || '').length >= 8);"
        " const score = it => {"
        "  const t = (it.title || '');"
        "  let s = t.length;"
        "  if (/^[A-Z0-9 &/\\-]{3,40}$/.test(t)) s -= 100;"
        "  if (/^MORE\\b/i.test(t)) s -= 100;"
        "  if (/^SUBSCRIBE\\b/i.test(t)) s -= 100;"
        "  if (it.image_url) s += 20;"
        "  if (it.published_at) s += 30;"
        "  return s;"
        " };"
        " const byUrl = new Map();"
        " for (const it of items) {"
        "  const prev = byUrl.get(it.url);"
        "  if (!prev || score(it) > score(prev)) byUrl.set(it.url, it);"
        " }"
        " return Array.from(byUrl.values()).slice(0, __MAX_ITEMS__);"
        "})())"
    )


@dataclass(frozen=True)
class PlaywrightCliStrategy:
    name: str
    list_eval: str
    article_eval: str = _DEFAULT_ARTICLE_EVAL
    article_image_eval: str = _DEFAULT_ARTICLE_IMAGE_EVAL
    article_date_eval: str = _DEFAULT_ARTICLE_DATE_EVAL
    route_block_code: str = _DEFAULT_ROUTE_BLOCK_CODE
    list_wait_until: str = "domcontentloaded"
    article_wait_until: str = "domcontentloaded"
    post_list_wait_ms: int = 0
    post_article_wait_ms: int = 0
    list_prep_code: tuple[str, ...] = ()
    article_prep_code: tuple[str, ...] = ()
    fetch_published_at_when_missing: bool = False


_GENERIC_PLAYWRIGHT_CLI_STRATEGY = PlaywrightCliStrategy(
    name="generic",
    list_eval=_build_list_eval(["article a[href]", "main a[href]", "a[href]"]),
)


_FLIGHTGLOBAL_PLAYWRIGHT_CLI_STRATEGY = PlaywrightCliStrategy(
    name="flightglobal_air_transport_cli",
    list_eval=_build_list_eval(
        ["a:has(article)", "main article a[href]", "article a[href]", "main [class*='story'] a[href]"],
        container_selector="article, [class*='summary'], [class*='post'], [class*='story'], [class*='card'], [class*='article'], li, div",
        heading_selector="[class*='post-title'],[class*='title'],h1,h2,h3,[class*='headline']",
    ),
)


_AVIATION_WEEK_PLAYWRIGHT_CLI_STRATEGY = PlaywrightCliStrategy(
    name="aviation_week_air_transport_cli",
    list_eval=_build_list_eval(
        ["main [class*='title'] a[href]", "main article a[href]", "article a[href]"],
        container_selector="article, [class*='title'], [class*='headline'], [class*='card'], [class*='tile'], [class*='story'], [class*='item'], li, div",
    ),
    post_list_wait_ms=1500,
    fetch_published_at_when_missing=True,
)


_SIMPLE_FLYING_PLAYWRIGHT_CLI_STRATEGY = PlaywrightCliStrategy(
    name="simple_flying_cli",
    list_eval=_build_list_eval(
        ["main h3 a[href]", "main [class*='card'] a[href]", "main [class*='title'] a[href]"],
        container_selector="article, [class*='card'], [class*='title'], [class*='post'], [class*='news'], li, div",
    ),
    post_list_wait_ms=1500,
)


_PLAYWRIGHT_CLI_REGISTRY = {
    "flightglobal_air_transport_cli": _FLIGHTGLOBAL_PLAYWRIGHT_CLI_STRATEGY,
    "aviation_week_air_transport_cli": _AVIATION_WEEK_PLAYWRIGHT_CLI_STRATEGY,
    "simple_flying_cli": _SIMPLE_FLYING_PLAYWRIGHT_CLI_STRATEGY,
}


def get_playwright_cli_strategy(source: dict[str, Any]) -> PlaywrightCliStrategy:
    key = str(source.get("cli_strategy") or source.get("id") or "").strip()
    return _PLAYWRIGHT_CLI_REGISTRY.get(key, _GENERIC_PLAYWRIGHT_CLI_STRATEGY)

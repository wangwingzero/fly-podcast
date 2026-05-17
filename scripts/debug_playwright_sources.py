"""Debug helper: run playwright-cli against each problem source, dump
list-eval output + anchor/article-link statistics so we can fix selectors
or link_patterns. Run on the server (where playwright-cli is installed):

  cd /www/wwwroot/flying-podcast
  PLAYWRIGHT_BROWSERS_PATH=$PWD/.playwright \
    .venv/bin/python scripts/debug_playwright_sources.py [source_id ...]

If no source_id given, runs all 5 problem sources.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from flying_podcast.stages.ingest import (  # noqa: E402
    _PLAYWRIGHT_LIST_EVAL,
    _PLAYWRIGHT_ROUTE_BLOCK_CODE,
    _playwright_goto_args,
)

PROBLEM_SOURCES = {
    "aviation_week_air_transport_cli": "https://aviationweek.com/air-transport",
    "simple_flying_cli": "https://simpleflying.com/category/aviation-news/",
    "ch_aviation_cli": "https://www.ch-aviation.com/news",
    "reuters_aerospace_cli": "https://www.reuters.com/business/aerospace-defense/",
    "bloomberg_airlines_cli": "https://www.bloomberg.com/industries/airlines",
}

LINK_PATTERNS = {
    "aviation_week_air_transport_cli": r"aviationweek\.com/air-transport/.+",
    "simple_flying_cli": r"simpleflying\.com/.+",
    "ch_aviation_cli": r"ch-aviation\.com/news/.+",
    "reuters_aerospace_cli": r"reuters\.com/.+",
    "bloomberg_airlines_cli": r"bloomberg\.com/.+",
}


def run_cli(args: list[str], session: str, timeout: int = 60) -> str:
    exe = shutil.which("playwright-cli")
    if not exe:
        raise RuntimeError("playwright-cli not in PATH")
    cmd = [exe, f"-s={session}", *args]
    if sys.platform.startswith("linux") and not os.getenv("DISPLAY") and shutil.which("xvfb-run"):
        cmd = ["xvfb-run", "-a", *cmd]
    env = dict(os.environ)
    env["PLAYWRIGHT_CLI_SESSION"] = session
    proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                          errors="replace", timeout=timeout, env=env, check=False)
    if proc.returncode != 0:
        return f"__ERR__:{proc.stderr.strip() or proc.stdout.strip() or 'cli_failed'}"
    return proc.stdout.strip()


STATS_EVAL = (
    "JSON.stringify({"
    " title: document.title,"
    " url: location.href,"
    " html_len: document.documentElement.outerHTML.length,"
    " a_total: document.querySelectorAll('a[href]').length,"
    " a_with_href_http: Array.from(document.querySelectorAll('a[href]')).filter(a => (a.href||'').startsWith('http')).length,"
    " article_count: document.querySelectorAll('article').length,"
    " h1_count: document.querySelectorAll('h1').length,"
    " h2_count: document.querySelectorAll('h2').length,"
    " h3_count: document.querySelectorAll('h3').length,"
    " body_text_len: (document.body.innerText||'').length,"
    " has_main: !!document.querySelector('main'),"
    " sample_h2_links: Array.from(document.querySelectorAll('h2 a[href], h3 a[href]')).slice(0,8).map(a => ({title: (a.textContent||'').trim().slice(0,80), href: a.href})),"
    " sample_article_links: Array.from(document.querySelectorAll('article a[href]')).slice(0,5).map(a => ({title: (a.textContent||'').trim().slice(0,80), href: a.href})),"
    " sample_first_anchors: Array.from(document.querySelectorAll('a[href]')).slice(0,15).map(a => ({title: (a.textContent||'').trim().slice(0,80), href: a.href}))"
    "})"
)


def explore_source(sid: str, url: str, link_re: str) -> dict:
    print(f"\n========== {sid}  {url} ==========", flush=True)
    out: dict = {"source_id": sid, "url": url}
    session = re.sub(r"[^a-zA-Z0-9_.-]+", "-", sid).strip("-")
    try:
        r = run_cli(["open"], session, 30)
        if r.startswith("__ERR__"):
            print("open FAILED:", r)
            return {**out, "phase": "open", "error": r}
        try:
            run_cli(["run-code", _PLAYWRIGHT_ROUTE_BLOCK_CODE], session, 15)
        except Exception as e:
            print("route-block warn:", e)
        r = run_cli(_playwright_goto_args(url, 60), session, 80)
        if r.startswith("__ERR__"):
            print("goto FAILED:", r)
            run_cli(["close"], session, 10)
            return {**out, "phase": "goto", "error": r}
        # Wait a bit for SPA rendering
        try:
            run_cli(["run-code", "async page => { await page.waitForTimeout(3000); }"], session, 10)
        except Exception:
            pass
        # Stats
        stats_raw = run_cli(["--raw", "eval", STATS_EVAL], session, 30)
        stats: dict
        try:
            stats = json.loads(stats_raw)
        except Exception:
            try:
                stats = json.loads(stats_raw[stats_raw.find("{"): stats_raw.rfind("}")+1])
            except Exception:
                stats = {"raw_stats": stats_raw[:400]}
        out["stats"] = stats
        # Current ingest list-eval output
        list_raw = run_cli(["--raw", "eval", _PLAYWRIGHT_LIST_EVAL.replace("__MAX_ITEMS__", "120")], session, 30)
        try:
            list_items = json.loads(list_raw)
        except Exception:
            try:
                list_items = json.loads(list_raw[list_raw.find("["): list_raw.rfind("]")+1])
            except Exception:
                list_items = []
        out["current_list_count"] = len(list_items) if isinstance(list_items, list) else 0
        out["current_list_sample"] = (list_items[:5] if isinstance(list_items, list) else [])
        # Filter via link_pattern to see how many match the regex
        link_re_compiled = re.compile(link_re, re.I)
        if isinstance(list_items, list):
            matched = [x for x in list_items if isinstance(x, dict) and link_re_compiled.search(x.get("url", ""))]
            out["match_link_pattern_count"] = len(matched)
            out["match_link_pattern_sample"] = matched[:5]
        # Print summary
        s = out.get("stats", {})
        print(f"  title: {s.get('title','')[:80]}")
        print(f"  url: {s.get('url','')}")
        print(f"  html_len={s.get('html_len')} body_text={s.get('body_text_len')} a_total={s.get('a_total')} a_http={s.get('a_with_href_http')} article={s.get('article_count')} h1/h2/h3={s.get('h1_count')}/{s.get('h2_count')}/{s.get('h3_count')}")
        print(f"  current_list_count={out['current_list_count']}, match_link_pattern={out.get('match_link_pattern_count')}")
        print("  sample h2/h3 links:")
        for it in (s.get("sample_h2_links") or [])[:5]:
            print(f"    - [{it.get('title','')}] {it.get('href','')}")
        print("  sample article-tag links:")
        for it in (s.get("sample_article_links") or [])[:5]:
            print(f"    - [{it.get('title','')}] {it.get('href','')}")
        print("  current_list_sample:")
        for it in out["current_list_sample"][:5]:
            print(f"    - [{(it or {}).get('title','')[:80]}] {(it or {}).get('url','')}")
    except subprocess.TimeoutExpired as e:
        out["error"] = f"timeout {e}"
        print("  TIMEOUT:", e)
    except Exception as e:
        out["error"] = repr(e)
        print("  ERROR:", e)
    finally:
        try:
            run_cli(["close"], session, 10)
        except Exception:
            pass
    return out


def main():
    sources = sys.argv[1:] or list(PROBLEM_SOURCES.keys())
    results = []
    for sid in sources:
        if sid not in PROBLEM_SOURCES:
            print("skip unknown:", sid)
            continue
        url = PROBLEM_SOURCES[sid]
        link_re = LINK_PATTERNS[sid]
        results.append(explore_source(sid, url, link_re))
    out_path = ROOT / "data" / "history" / "playwright_debug.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nSaved {out_path}")


if __name__ == "__main__":
    main()

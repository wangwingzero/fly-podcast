"""Verify the new _PLAYWRIGHT_LIST_EVAL against aviation_week + simple_flying"""
from __future__ import annotations
import json, os, re, shutil, subprocess, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from flying_podcast.stages.ingest import _PLAYWRIGHT_LIST_EVAL  # noqa

TARGETS = {
    "aviation_week_air_transport_cli": ("https://aviationweek.com/air-transport", r"aviationweek\.com/air-transport/.+"),
    "simple_flying_cli": ("https://simpleflying.com/category/aviation-news/", r"simpleflying\.com/.+"),
    "flightglobal_air_transport_cli": ("https://www.flightglobal.com/air-transport", r"flightglobal\.com/.+"),
}

def run_cli(args, session, timeout=60):
    exe = shutil.which("playwright-cli")
    cmd = [exe, f"-s={session}", *args]
    if sys.platform.startswith("linux") and not os.getenv("DISPLAY") and shutil.which("xvfb-run"):
        cmd = ["xvfb-run", "-a", *cmd]
    env = dict(os.environ); env["PLAYWRIGHT_CLI_SESSION"] = session
    p = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                       errors="replace", timeout=timeout, env=env, check=False)
    if p.returncode != 0: return f"__ERR__:{p.stderr.strip()[:200] or p.stdout.strip()[:200]}"
    return p.stdout.strip()

def parse_nested(raw):
    if not raw or raw.startswith("__ERR__"): return None
    text = raw.strip()
    for _ in range(3):
        try: v = json.loads(text)
        except Exception: break
        if isinstance(v, str): text = v; continue
        return v
    return None

def goto(url, t=60):
    return ["run-code", f"async page => {{ await page.goto({json.dumps(url)}, {{ waitUntil: 'domcontentloaded', timeout: {t*1000} }}); }}"]

for sid, (url, link_re) in TARGETS.items():
    print(f"\n========== {sid} ==========")
    sess = sid
    try:
        if (r := run_cli(["open"], sess, 30)).startswith("__ERR__"): print("open err:", r); continue
        if (r := run_cli(goto(url), sess, 80)).startswith("__ERR__"): print("goto err:", r); continue
        run_cli(["run-code", "async page => { await page.waitForTimeout(3000); }"], sess, 8)
        raw = run_cli(["--raw", "eval", _PLAYWRIGHT_LIST_EVAL.replace("__MAX_ITEMS__", "120")], sess, 30)
        items = parse_nested(raw)
        if not isinstance(items, list):
            print("  parse failed; raw[:300]=", (raw or "")[:300]); continue
        print(f"  total list items: {len(items)}")
        rgx = re.compile(link_re, re.I)
        matched = [it for it in items if rgx.search((it or {}).get("url", ""))]
        print(f"  matched link_pattern: {len(matched)}")
        for it in matched[:6]:
            print(f"    [{(it.get('title') or '')[:70]}]  {it.get('url')[:90]}")
    finally:
        try: run_cli(["close"], sess, 10)
        except Exception: pass

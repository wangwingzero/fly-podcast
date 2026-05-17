"""Round 2: deep dive into aviation_week / simple_flying / ch_aviation
to find the actual list-item DOM patterns."""
from __future__ import annotations
import json, os, re, shutil, subprocess, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

TARGETS = {
    "aviation_week_air_transport_cli": "https://aviationweek.com/air-transport",
    "simple_flying_cli": "https://simpleflying.com/category/aviation-news/",
    "ch_aviation_cli": "https://www.ch-aviation.com/news",
}


def run_cli(args, session, timeout=60):
    exe = shutil.which("playwright-cli")
    cmd = [exe, f"-s={session}", *args]
    if sys.platform.startswith("linux") and not os.getenv("DISPLAY") and shutil.which("xvfb-run"):
        cmd = ["xvfb-run", "-a", *cmd]
    env = dict(os.environ); env["PLAYWRIGHT_CLI_SESSION"] = session
    p = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                       errors="replace", timeout=timeout, env=env, check=False)
    if p.returncode != 0:
        return f"__ERR__:{p.stderr.strip() or p.stdout.strip()}"
    return p.stdout.strip()


def parse_json_maybe_nested(raw):
    if not raw or raw.startswith("__ERR__"):
        return None
    text = raw.strip()
    for _ in range(3):
        try:
            v = json.loads(text)
        except Exception:
            break
        if isinstance(v, str):
            text = v
            continue
        return v
    # try to extract object
    try:
        s = text.find("{"); e = text.rfind("}")
        if s >= 0 and e > s:
            return json.loads(text[s:e+1])
    except Exception:
        pass
    return None


# Probe several candidate selectors and report counts + first 5 samples
PROBE_EVAL = r"""
JSON.stringify((() => {
  const selectors = [
    'article h2 a[href]', 'article h3 a[href]',
    'article a[href]',
    '.card a[href]', '.card-title a[href]', '.tile a[href]', '.tile-title a[href]',
    'h2 a[href]', 'h3 a[href]',
    'a.title', 'a.headline', 'a.card-title',
    '[class*="title"] a[href]', '[class*="headline"] a[href]',
    '[class*="card"] a[href]', '[class*="tile"] a[href]',
    '[class*="entry"] a[href]', '[class*="post"] a[href]',
    '[class*="news"] a[href]',
    'main a[href]'
  ];
  const result = {};
  for (const sel of selectors) {
    let nodes;
    try { nodes = Array.from(document.querySelectorAll(sel)); } catch (e) { continue; }
    const items = nodes.map(a => ({
      title: (a.textContent || a.getAttribute('aria-label') || '').replace(/\s+/g,' ').trim().slice(0,100),
      href: a.href
    })).filter(x => x.title.length >= 8 && /^https?:\/\//.test(x.href));
    // dedup by href
    const seen = new Set(); const uniq = [];
    for (const it of items) { if (seen.has(it.href)) continue; seen.add(it.href); uniq.push(it); }
    result[sel] = { count: uniq.length, sample: uniq.slice(0, 4) };
  }
  return result;
})())
"""


def goto_args(url, timeout=60):
    ms = timeout * 1000
    code = f"async page => {{ await page.goto({json.dumps(url)}, {{ waitUntil: 'domcontentloaded', timeout: {ms} }}); }}"
    return ["run-code", code]


def explore(sid, url):
    print(f"\n========== {sid} ==========\n  url={url}", flush=True)
    sess = sid
    try:
        r = run_cli(["open"], sess, 30)
        if r.startswith("__ERR__"): print("  open err:", r); return
        r = run_cli(goto_args(url, 60), sess, 80)
        if r.startswith("__ERR__"): print("  goto err:", r); return
        # wait for SPA
        run_cli(["run-code", "async page => { await page.waitForTimeout(5000); }"], sess, 10)
        # full URL after redirects
        cur = run_cli(["--raw", "eval", "JSON.stringify({url:location.href, title:document.title, html_len:document.documentElement.outerHTML.length})"], sess, 15)
        print("  current:", parse_json_maybe_nested(cur))
        probe = run_cli(["--raw", "eval", PROBE_EVAL], sess, 30)
        data = parse_json_maybe_nested(probe)
        if not isinstance(data, dict):
            print("  probe parse failed; raw[:300] =", (probe or "")[:300])
            return
        # sort selectors by count desc
        ranked = sorted(data.items(), key=lambda kv: -kv[1].get("count", 0))
        for sel, info in ranked[:8]:
            print(f"  [{info.get('count'):3}]  {sel}")
            for it in (info.get("sample") or [])[:3]:
                print(f"        - {it.get('title','')[:70]}  ::  {it.get('href','')}")
    finally:
        try: run_cli(["close"], sess, 10)
        except Exception: pass


def main():
    targets = sys.argv[1:] or list(TARGETS.keys())
    for sid in targets:
        if sid in TARGETS:
            explore(sid, TARGETS[sid])

if __name__ == "__main__":
    main()

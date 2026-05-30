from __future__ import annotations

import functools
import hmac
import json
import os
import re
import secrets
import shutil
import subprocess
import sys
import threading
import time
import zipfile
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import quote

from dotenv import load_dotenv
from flask import (
    Flask,
    abort,
    jsonify,
    redirect,
    render_template_string,
    request,
    send_file,
    session,
)

ROOT = Path(__file__).resolve().parents[3]
load_dotenv(ROOT / ".env", override=False)

from flying_podcast.core.config import settings  # noqa: E402
from flying_podcast.core.time_utils import beijing_today_str  # noqa: E402


WEB_DIR = ROOT / "data" / "podcast_web"
UPLOAD_DIR = WEB_DIR / "uploads"
UPLOAD_SESSION_DIR = WEB_DIR / "upload_sessions"
JOB_DIR = WEB_DIR / "jobs"
MAX_UPLOAD_MB = int(os.getenv("PODCAST_WEB_MAX_UPLOAD_MB", "1024"))
MAX_LLM_BRIEFING_CHARS = 4000
URL_PREFIX = os.getenv("PODCAST_WEB_PREFIX", "").rstrip("/")

app = Flask(__name__)
app.secret_key = os.getenv("PODCAST_WEB_SECRET") or secrets.token_hex(32)
app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_MB * 1024 * 1024

_LOCK = threading.RLock()
_PROCESSES: dict[str, subprocess.Popen[str]] = {}


@app.after_request
def add_no_store_headers(response):
    if request.path.startswith(("/api/", "/login", "/logout")) or request.path == "/":
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response


def _ensure_dirs() -> None:
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    UPLOAD_SESSION_DIR.mkdir(parents=True, exist_ok=True)
    JOB_DIR.mkdir(parents=True, exist_ok=True)
    (settings.output_dir / "podcast").mkdir(parents=True, exist_ok=True)


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _web_path(path: str) -> str:
    if not path.startswith("/"):
        path = "/" + path
    return f"{URL_PREFIX}{path}"


def _console_public_url() -> str:
    configured = os.getenv("PODCAST_WEB_PUBLIC_URL", "").rstrip("/")
    if configured:
        return configured
    return request.host_url.rstrip("/")


def _safe_name(name: str) -> str:
    stem = Path(name).stem or "document"
    stem = re.sub(r"[\\/:*?\"<>|\x00-\x1f]", "_", stem)
    stem = re.sub(r"\s+", "_", stem).strip("._-")
    return (stem or "document")[:96]


def _job_path(job_id: str) -> Path:
    return JOB_DIR / job_id / "job.json"


def _log_path(job_id: str) -> Path:
    return JOB_DIR / job_id / "run.log"


def _upload_session_path(upload_id: str) -> Path:
    return UPLOAD_SESSION_DIR / upload_id


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_job(job_id: str) -> dict[str, Any]:
    data = _read_json(_job_path(job_id))
    if not data:
        abort(404)
    return _refresh_job(data)


def _save_job(job: dict[str, Any]) -> None:
    job["updated_at"] = _now()
    _write_json(_job_path(job["id"]), job)


def _append_log(job_id: str, line: str) -> None:
    path = _log_path(job_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", errors="replace") as fp:
        fp.write(line)
        if not line.endswith("\n"):
            fp.write("\n")


def _tail_log(job_id: str, max_chars: int = 40000) -> str:
    path = _log_path(job_id)
    if not path.exists():
        return ""
    data = path.read_text(encoding="utf-8", errors="replace")
    return data[-max_chars:]


def _refresh_job(job: dict[str, Any]) -> dict[str, Any]:
    work_dir = Path(job.get("work_dir", ""))
    meta_path = work_dir / "metadata.json"
    publish_path = work_dir / "publish_result.json"

    if meta_path.exists():
        meta = _read_json(meta_path)
        job["title"] = meta.get("title", job.get("title", ""))
        job["mp3_path"] = meta.get("mp3_path", job.get("mp3_path", ""))
        job["mp3_cdn_url"] = meta.get("mp3_cdn_url", job.get("mp3_cdn_url", ""))
        job["dialogue_lines"] = meta.get("dialogue_lines", job.get("dialogue_lines", 0))
        job["chapters"] = meta.get("chapters", job.get("chapters", []))

    if publish_path.exists():
        result = _read_json(publish_path)
        job["publish_result"] = result
        job["media_id"] = result.get("media_id", job.get("media_id", ""))

    files: list[dict[str, str]] = []
    if work_dir.exists():
        for path in sorted(work_dir.iterdir()):
            if path.is_file() and path.suffix.lower() in {".mp3", ".json", ".html", ".jpg", ".jpeg", ".png"}:
                files.append({
                    "name": path.name,
                    "kind": path.suffix.lower().lstrip("."),
                    "url": _web_path(f"/download/{quote(job['id'])}/{quote(path.name)}"),
                })
    job["files"] = files
    return job


def _list_jobs() -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    for path in JOB_DIR.glob("*/job.json"):
        try:
            jobs.append(_refresh_job(_read_json(path)))
        except Exception:
            continue
    jobs.sort(key=lambda item: item.get("created_at", ""), reverse=True)
    return jobs


def _normalize_llm_briefing(text: str) -> str:
    return (text or "").strip()[:MAX_LLM_BRIEFING_CHARS]


def _persist_job_briefing(job_id: str, briefing: str) -> str:
    briefing = _normalize_llm_briefing(briefing)
    if not briefing:
        return ""
    path = JOB_DIR / job_id / "llm_briefing.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(briefing, encoding="utf-8")
    return str(path)


def _create_job_from_pdf(
    upload_path: Path,
    original_name: str,
    day: str,
    publish_requested: bool,
    *,
    llm_briefing: str = "",
) -> dict[str, Any]:
    job_id = datetime.now().strftime("%Y%m%d%H%M%S") + "-" + secrets.token_hex(3)
    upload_size = upload_path.stat().st_size
    work_dir = settings.output_dir / "podcast" / f"{day}_{upload_path.stem}"
    briefing_clean = _normalize_llm_briefing(llm_briefing)
    briefing_path = _persist_job_briefing(job_id, briefing_clean)
    job = {
        "id": job_id,
        "status": "queued",
        "stage": "queued",
        "created_at": _now(),
        "updated_at": _now(),
        "date": day,
        "original_filename": original_name,
        "upload_size": upload_size,
        "upload_path": str(upload_path),
        "work_dir": str(work_dir),
        "publish_requested": publish_requested,
        "llm_briefing": briefing_clean,
        "llm_briefing_path": briefing_path,
        "title": "",
        "mp3_path": "",
        "mp3_cdn_url": "",
        "error": "",
    }
    _save_job(job)
    _append_log(job_id, f"Job created at {_now()}")
    _append_log(job_id, f"PDF: {original_name} ({upload_size} bytes)")
    if briefing_clean:
        _append_log(job_id, f"LLM briefing: {len(briefing_clean)} chars")

    thread = threading.Thread(target=_run_generation, args=(job_id,), daemon=True)
    thread.start()
    return job


def _admin_password() -> str:
    return os.getenv("PODCAST_WEB_PASSWORD", "")


def _is_logged_in() -> bool:
    return session.get("authenticated") is True


def login_required(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        if not _is_logged_in():
            return redirect(_web_path("/login"))
        return fn(*args, **kwargs)

    return wrapper


def _run_command(job: dict[str, Any], cmd: list[str], label: str) -> int:
    _append_log(job["id"], f"\n== {label} ==")
    _append_log(job["id"], " ".join(cmd))
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONPATH"] = str(ROOT / "src")

    proc = subprocess.Popen(
        cmd,
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )
    _PROCESSES[job["id"]] = proc
    try:
        assert proc.stdout is not None
        for line in proc.stdout:
            _append_log(job["id"], line)
        return proc.wait()
    finally:
        _PROCESSES.pop(job["id"], None)


def _run_generation(job_id: str) -> None:
    with _LOCK:
        job = _load_job(job_id)
        job["status"] = "running"
        job["stage"] = "generate"
        _save_job(job)

    try:
        cmd = [
            sys.executable,
            str(ROOT / "run.py"),
            "podcast",
            "--date",
            job["date"],
            "--pdf",
            job["upload_path"],
        ]
        briefing_path = job.get("llm_briefing_path") or ""
        if briefing_path and Path(briefing_path).exists():
            cmd.extend(["--briefing-file", briefing_path])
        code = _run_command(job, cmd, "generate podcast")
        if code != 0:
            raise RuntimeError(f"podcast generation failed with exit code {code}")

        with _LOCK:
            job = _refresh_job(_load_job(job_id))
            job["status"] = "completed"
            job["stage"] = "done"
            job["returncode"] = code
            _save_job(job)

        if job.get("publish_requested"):
            _run_publish(job_id)
    except Exception as exc:  # noqa: BLE001
        with _LOCK:
            job = _load_job(job_id)
            job["status"] = "failed"
            job["stage"] = "failed"
            job["error"] = str(exc)
            _append_log(job_id, f"\nERROR: {exc}")
            _save_job(job)


def _run_publish(job_id: str) -> None:
    with _LOCK:
        job = _load_job(job_id)
        if job.get("status") in {"running", "publishing"}:
            return
        job["status"] = "publishing"
        job["stage"] = "publish"
        _save_job(job)

    try:
        cmd = [
            sys.executable,
            str(ROOT / "run.py"),
            "publish-podcast",
            "--podcast-dir",
            job["work_dir"],
        ]
        code = _run_command(job, cmd, "publish WeChat draft")
        if code != 0:
            raise RuntimeError(f"publish failed with exit code {code}")
        with _LOCK:
            job = _refresh_job(_load_job(job_id))
            job["status"] = "completed"
            job["stage"] = "done"
            _save_job(job)
    except Exception as exc:  # noqa: BLE001
        with _LOCK:
            job = _load_job(job_id)
            job["status"] = "failed"
            job["stage"] = "failed"
            job["error"] = str(exc)
            _append_log(job_id, f"\nERROR: {exc}")
            _save_job(job)


@app.get("/login")
def login():
    if _is_logged_in():
        return redirect(_web_path("/"))
    return render_template_string(LOGIN_TEMPLATE, error="", base=URL_PREFIX)


@app.get("/favicon.ico")
def favicon():
    return "", 204


@app.post("/login")
def login_post():
    expected = _admin_password()
    password = request.form.get("password", "")
    if expected and hmac.compare_digest(password, expected):
        session.clear()
        session["authenticated"] = True
        return redirect(_web_path("/"))
    time.sleep(0.4)
    return render_template_string(LOGIN_TEMPLATE, error="密码不对，再试一次。", base=URL_PREFIX), 401


@app.post("/logout")
@login_required
def logout():
    session.clear()
    return redirect(_web_path("/login"))


@app.get("/")
@login_required
def dashboard():
    return render_template_string(
        DASHBOARD_TEMPLATE,
        today=beijing_today_str(),
        max_upload_mb=MAX_UPLOAD_MB,
        console_url=_console_public_url(),
        base=URL_PREFIX,
    )


@app.post("/api/jobs")
@login_required
def create_job():
    _ensure_dirs()
    uploaded = request.files.get("pdf")
    if not uploaded or not uploaded.filename:
        return jsonify({"error": "请选择 PDF 文件"}), 400
    if not uploaded.filename.lower().endswith(".pdf"):
        return jsonify({"error": "只接受 PDF 文件"}), 400

    job_id = datetime.now().strftime("%Y%m%d%H%M%S") + "-" + secrets.token_hex(3)
    day = request.form.get("date") or beijing_today_str()
    original_name = uploaded.filename
    safe_stem = _safe_name(original_name)
    upload_path = UPLOAD_DIR / f"{job_id}_{safe_stem}.pdf"
    uploaded.save(upload_path)
    job = _create_job_from_pdf(
        upload_path=upload_path,
        original_name=original_name,
        day=day,
        publish_requested=request.form.get("publish") == "1",
        llm_briefing=request.form.get("briefing", ""),
    )
    return jsonify({"job": _refresh_job(job)})


@app.post("/api/uploads/start")
@login_required
def start_chunked_upload():
    _ensure_dirs()
    payload = request.get_json(silent=True) or {}
    filename = str(payload.get("filename", ""))
    if not filename.lower().endswith(".pdf"):
        return jsonify({"error": "只接受 PDF 文件"}), 400

    size = int(payload.get("size") or 0)
    max_size = MAX_UPLOAD_MB * 1024 * 1024
    if size <= 0:
        return jsonify({"error": "文件大小无效"}), 400
    if size > max_size:
        return jsonify({"error": f"文件超过 {MAX_UPLOAD_MB} MB 上限"}), 413

    upload_id = datetime.now().strftime("%Y%m%d%H%M%S") + "-" + secrets.token_hex(4)
    session_dir = _upload_session_path(upload_id)
    parts_dir = session_dir / "parts"
    parts_dir.mkdir(parents=True, exist_ok=True)
    meta = {
        "id": upload_id,
        "filename": filename,
        "size": size,
        "date": str(payload.get("date") or beijing_today_str()),
        "publish_requested": bool(payload.get("publish")),
        "llm_briefing": _normalize_llm_briefing(str(payload.get("briefing") or "")),
        "created_at": _now(),
        "updated_at": _now(),
        "received": [],
    }
    _write_json(session_dir / "meta.json", meta)
    return jsonify({"upload_id": upload_id, "chunk_size": 64 * 1024})


@app.post("/api/uploads/<upload_id>/chunk")
@login_required
def upload_chunk(upload_id: str):
    session_dir = _upload_session_path(upload_id)
    meta_path = session_dir / "meta.json"
    if not meta_path.exists():
        return jsonify({"error": "上传会话不存在，请重新开始"}), 404

    chunk = request.files.get("chunk")
    if chunk is None:
        return jsonify({"error": "缺少分片"}), 400
    try:
        index = int(request.form.get("index", "-1"))
    except ValueError:
        index = -1
    if index < 0:
        return jsonify({"error": "分片序号无效"}), 400

    parts_dir = session_dir / "parts"
    parts_dir.mkdir(parents=True, exist_ok=True)
    part_path = parts_dir / f"{index:06d}.part"
    chunk.save(part_path)

    meta = _read_json(meta_path)
    received = set(int(item) for item in meta.get("received", []))
    received.add(index)
    meta["received"] = sorted(received)
    meta["updated_at"] = _now()
    _write_json(meta_path, meta)
    return jsonify({"ok": True, "received": len(received)})


@app.post("/api/uploads/<upload_id>/finish")
@login_required
def finish_chunked_upload(upload_id: str):
    session_dir = _upload_session_path(upload_id)
    meta_path = session_dir / "meta.json"
    if not meta_path.exists():
        return jsonify({"error": "上传会话不存在，请重新开始"}), 404

    meta = _read_json(meta_path)
    payload = request.get_json(silent=True) or {}
    total_chunks = int(payload.get("total_chunks") or 0)
    if total_chunks <= 0:
        return jsonify({"error": "缺少分片总数"}), 400

    parts_dir = session_dir / "parts"
    missing = [index for index in range(total_chunks) if not (parts_dir / f"{index:06d}.part").exists()]
    if missing:
        return jsonify({"error": f"缺少分片：{missing[:5]}"}), 400

    original_name = str(meta.get("filename") or "document.pdf")
    safe_stem = _safe_name(original_name)
    upload_path = UPLOAD_DIR / f"{upload_id}_{safe_stem}.pdf"
    with upload_path.open("wb") as out:
        for index in range(total_chunks):
            with (parts_dir / f"{index:06d}.part").open("rb") as part:
                shutil.copyfileobj(part, out)

    expected_size = int(meta.get("size") or 0)
    actual_size = upload_path.stat().st_size
    if expected_size and actual_size != expected_size:
        upload_path.unlink(missing_ok=True)
        return jsonify({"error": f"文件大小不一致：{actual_size}/{expected_size}"}), 400

    job = _create_job_from_pdf(
        upload_path=upload_path,
        original_name=original_name,
        day=str(meta.get("date") or beijing_today_str()),
        publish_requested=bool(meta.get("publish_requested")),
        llm_briefing=str(meta.get("llm_briefing") or ""),
    )
    shutil.rmtree(session_dir, ignore_errors=True)
    return jsonify({"job": _refresh_job(job)})


@app.get("/api/jobs")
@login_required
def jobs_api():
    _ensure_dirs()
    return jsonify({"jobs": _list_jobs()})


@app.get("/api/jobs/<job_id>")
@login_required
def job_api(job_id: str):
    job = _load_job(job_id)
    job["log"] = _tail_log(job_id)
    return jsonify({"job": job})


@app.post("/api/jobs/<job_id>/publish")
@login_required
def publish_job(job_id: str):
    job = _load_job(job_id)
    if job.get("status") in {"running", "publishing", "queued"}:
        return jsonify({"error": "任务还在运行"}), 409
    thread = threading.Thread(target=_run_publish, args=(job_id,), daemon=True)
    thread.start()
    return jsonify({"ok": True})


@app.post("/api/jobs/<job_id>/cancel")
@login_required
def cancel_job(job_id: str):
    proc = _PROCESSES.get(job_id)
    if proc and proc.poll() is None:
        proc.terminate()
        _append_log(job_id, "\nCancel requested.")
        job = _load_job(job_id)
        job["status"] = "failed"
        job["stage"] = "cancelled"
        job["error"] = "cancelled by user"
        _save_job(job)
        return jsonify({"ok": True})
    return jsonify({"error": "没有正在运行的进程"}), 409


@app.get("/download/<job_id>/<path:filename>")
@login_required
def download_file(job_id: str, filename: str):
    job = _load_job(job_id)
    work_dir = Path(job["work_dir"]).resolve()
    target = (work_dir / filename).resolve()
    if work_dir != target.parent:
        abort(403)
    if not target.exists() or not target.is_file():
        abort(404)
    return send_file(target, as_attachment=True)


@app.get("/download/<job_id>.zip")
@login_required
def download_zip(job_id: str):
    job = _load_job(job_id)
    work_dir = Path(job["work_dir"]).resolve()
    if not work_dir.exists():
        abort(404)
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in work_dir.rglob("*"):
            if path.is_file():
                zf.write(path, path.relative_to(work_dir))
    buf.seek(0)
    return send_file(buf, as_attachment=True, download_name=f"{work_dir.name}.zip")


LOGIN_TEMPLATE = """
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>飞行播客控制台</title>
  <style>
    :root { color-scheme: light; --ink:#16211d; --muted:#66736d; --line:#d7ded8; --paper:#f6f3eb; --panel:#fffdfa; --green:#126b4d; --blue:#1d5f92; --amber:#b7791f; --red:#b42318; }
    * { box-sizing: border-box; }
    body { margin:0; min-height:100vh; display:grid; place-items:center; background:linear-gradient(135deg,#f6f3eb,#e7efe8 48%,#dfeaf1); color:var(--ink); font-family:"Microsoft YaHei","Segoe UI",sans-serif; }
    .shell { width:min(420px, calc(100vw - 32px)); border:1px solid rgba(22,33,29,.18); background:rgba(255,253,250,.88); box-shadow:0 24px 70px rgba(25,51,43,.18); padding:34px; }
    h1 { margin:0 0 8px; font-size:28px; letter-spacing:0; font-weight:800; }
    p { margin:0 0 24px; color:var(--muted); line-height:1.6; }
    label { display:block; font-size:13px; color:var(--muted); margin-bottom:8px; }
    input { width:100%; height:46px; padding:0 14px; border:1px solid var(--line); background:#fff; color:var(--ink); font-size:16px; outline:none; }
    input:focus { border-color:var(--green); box-shadow:0 0 0 3px rgba(18,107,77,.12); }
    button { width:100%; height:46px; margin-top:14px; border:0; background:var(--green); color:#fff; font-weight:700; cursor:pointer; }
    .error { min-height:22px; margin-top:14px; color:var(--red); font-size:14px; }
  </style>
</head>
<body>
  <form class="shell" method="post" action="{{ base }}/login">
    <h1>飞行播客控制台</h1>
    <p>服务器制作间</p>
    <label for="password">登录密码</label>
    <input id="password" name="password" type="password" autocomplete="current-password" autofocus>
    <button type="submit">进入</button>
    <div class="error">{{ error }}</div>
  </form>
</body>
</html>
"""


DASHBOARD_TEMPLATE = """
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>飞行播客控制台</title>
  <style>
    :root { --ink:#17211d; --muted:#68736e; --line:#d9dfd8; --paper:#f6f3eb; --panel:#fffefa; --green:#126b4d; --green2:#0f533d; --blue:#1d5f92; --amber:#aa6b16; --red:#b42318; --shadow:0 18px 50px rgba(28,44,36,.12); }
    * { box-sizing:border-box; }
    body { margin:0; min-height:100vh; background:linear-gradient(180deg,#f7f4eb 0,#edf2ed 46%,#e8eef3 100%); color:var(--ink); font-family:"Microsoft YaHei","Segoe UI",sans-serif; }
    button,input { font:inherit; }
    .top { position:sticky; top:0; z-index:5; border-bottom:1px solid rgba(23,33,29,.12); background:rgba(247,244,235,.9); backdrop-filter:blur(16px); }
    .bar { max-width:1280px; margin:auto; padding:18px 24px; display:flex; align-items:center; justify-content:space-between; gap:16px; }
    .brand { display:flex; align-items:baseline; gap:14px; min-width:0; }
    .brand h1 { margin:0; font-size:24px; line-height:1; font-weight:900; letter-spacing:0; }
    .brand span { color:var(--muted); font-size:13px; white-space:nowrap; }
    .logout { border:1px solid var(--line); background:#fff; color:var(--ink); height:34px; padding:0 14px; cursor:pointer; }
    main { max-width:1280px; margin:0 auto; padding:24px; display:grid; grid-template-columns:360px 1fr; gap:20px; }
    section { background:rgba(255,254,250,.9); border:1px solid rgba(23,33,29,.12); box-shadow:var(--shadow); }
    .upload { padding:22px; }
    .upload h2,.jobs h2,.detail h2 { margin:0 0 16px; font-size:16px; letter-spacing:0; }
    .field { margin-bottom:14px; }
    .field textarea { width:100%; min-height:108px; resize:vertical; border:1px solid var(--line); background:#fff; padding:10px 12px; font:inherit; line-height:1.5; }
    .field .hint { margin-top:6px; color:var(--muted); font-size:12px; line-height:1.45; }
    label { display:block; color:var(--muted); font-size:12px; margin-bottom:7px; }
    input[type="date"], input[type="file"] { width:100%; min-height:42px; border:1px solid var(--line); background:#fff; padding:9px 10px; color:var(--ink); }
    .file-summary { display:none; margin-top:8px; border:1px solid rgba(23,33,29,.12); background:#f8f5ed; padding:10px; }
    .file-summary.active { display:block; }
    .file-summary strong { display:block; font-size:13px; line-height:1.45; overflow-wrap:anywhere; }
    .file-summary span { display:block; margin-top:4px; color:var(--muted); font-size:12px; }
    .check { display:flex; gap:10px; align-items:center; padding:10px 0 4px; color:var(--ink); font-size:14px; }
    .primary { width:100%; height:44px; border:0; background:var(--green); color:white; font-weight:800; cursor:pointer; }
    .primary:disabled { cursor:progress; opacity:.9; }
    .primary:hover { background:var(--green2); }
    .upload-progress { display:none; margin:12px 0 14px; border:1px solid rgba(23,33,29,.12); background:#fff; padding:12px; }
    .upload-progress.active { display:block; }
    .progress-top { display:flex; justify-content:space-between; gap:12px; color:var(--muted); font-size:12px; line-height:1.4; }
    .progress-top strong { color:var(--ink); }
    .progress-track { height:10px; margin:10px 0 8px; border:1px solid rgba(23,33,29,.1); background:#e9e4d8; overflow:hidden; border-radius:4px; }
    .progress-fill { width:0%; height:100%; background:linear-gradient(90deg,var(--green),var(--blue)); transition:width .16s ease; border-radius:3px; }
    .upload-readout { min-height:17px; color:var(--muted); font-size:12px; line-height:1.4; }
    .upload-readout strong { color:var(--green); }
    .ghost { height:34px; border:1px solid var(--line); background:#fff; color:var(--ink); padding:0 12px; cursor:pointer; }
    .danger { color:var(--red); border-color:#e4b5ae; }
    .jobs { margin-top:20px; overflow:hidden; }
    .jobs h2 { padding:18px 18px 0; }
    .job-list { display:flex; flex-direction:column; max-height:52vh; overflow:auto; }
    .job { border-top:1px solid rgba(23,33,29,.1); padding:14px 18px; cursor:pointer; background:transparent; text-align:left; }
    .job:hover,.job.active { background:#f3f0e6; }
    .job-title { display:flex; justify-content:space-between; gap:10px; align-items:center; font-weight:800; line-height:1.3; }
    .job small { display:block; color:var(--muted); margin-top:7px; line-height:1.4; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .pill { display:inline-flex; align-items:center; min-width:72px; justify-content:center; height:24px; padding:0 9px; border-radius:99px; font-size:12px; font-weight:800; border:1px solid var(--line); background:#fff; }
    .pill.running,.pill.queued,.pill.publishing { color:var(--blue); border-color:#b7cfe1; background:#eef7ff; }
    .pill.completed { color:var(--green); border-color:#b6d7c8; background:#edf8f2; }
    .pill.failed { color:var(--red); border-color:#e6b8b2; background:#fff0ee; }
    .detail { min-height:calc(100vh - 120px); display:flex; flex-direction:column; }
    .detail-head { padding:22px; border-bottom:1px solid rgba(23,33,29,.1); display:flex; justify-content:space-between; gap:16px; align-items:flex-start; }
    .detail-title { min-width:0; }
    .detail-title h2 { margin:0 0 8px; font-size:22px; overflow-wrap:anywhere; }
    .meta { color:var(--muted); font-size:13px; line-height:1.7; }
    .actions { display:flex; gap:8px; flex-wrap:wrap; justify-content:flex-end; }
    .grid { padding:18px 22px; display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:12px; border-bottom:1px solid rgba(23,33,29,.1); }
    .metric { background:#f7f4eb; border:1px solid rgba(23,33,29,.1); padding:13px; min-height:74px; }
    .metric b { display:block; font-size:20px; margin-bottom:6px; overflow-wrap:anywhere; }
    .metric span { color:var(--muted); font-size:12px; }
    .files { padding:0 22px 18px; display:flex; flex-wrap:wrap; gap:8px; }
    .files a { border:1px solid var(--line); background:#fff; color:var(--ink); text-decoration:none; padding:8px 10px; font-size:13px; }
    .log { margin:0 22px 22px; flex:1; min-height:360px; background:#101815; color:#dce8df; border:1px solid #27342f; padding:16px; overflow:auto; white-space:pre-wrap; font-family:"Cascadia Mono","Consolas",monospace; font-size:12px; line-height:1.55; }
    .empty { padding:40px; color:var(--muted); }
    .toast { position:fixed; right:18px; bottom:18px; max-width:360px; padding:12px 14px; background:#17211d; color:white; box-shadow:var(--shadow); opacity:0; transform:translateY(10px); transition:.2s; pointer-events:none; }
    .toast.show { opacity:1; transform:translateY(0); }
    @media (max-width: 880px) { main { grid-template-columns:1fr; padding:14px; } .grid { grid-template-columns:1fr; } .bar { padding:16px 14px; } .brand { display:block; } .brand span { display:block; margin-top:7px; } }
  </style>
</head>
<body>
  <header class="top">
    <div class="bar">
      <div class="brand"><h1>飞行播客控制台</h1><span>服务器制作间 · {{ console_url }}</span></div>
      <form method="post" action="{{ base }}/logout"><button class="logout">退出</button></form>
    </div>
  </header>
  <main>
    <aside>
      <section class="upload">
        <h2>新建节目</h2>
        <form id="uploadForm">
          <div class="field">
            <label>PDF 文件</label>
            <input name="pdf" type="file" accept="application/pdf,.pdf" required>
            <div class="file-summary" id="fileSummary">
              <strong id="fileName"></strong>
              <span id="fileSize"></span>
            </div>
          </div>
          <div class="field">
            <label>日期</label>
            <input name="date" type="date" value="{{ today }}">
          </div>
          <div class="field">
            <label>制作说明（给 LLM，可选）</label>
            <textarea name="briefing" maxlength="4000" placeholder="例如：本期重点讲 spoofing 与 jamming 的区别；多举中东/东欧干扰案例；少讲法律条文，多讲机组处置……"></textarea>
            <div class="hint">上传前填写。会单独交给 LLM 理解，用于强调本期要讲的侧重点（不写入 PDF）。最多 4000 字。</div>
          </div>
          <label class="check"><input name="publish" type="checkbox" value="1"> 完成后创建公众号草稿</label>
          <div class="upload-progress" id="uploadProgress">
            <div class="progress-top">
              <strong id="uploadState">准备上传</strong>
              <span id="uploadPercent">0%</span>
            </div>
            <div class="progress-track" aria-hidden="true"><div class="progress-fill" id="progressFill"></div></div>
            <div class="upload-readout" id="uploadReadout">等待选择文件</div>
          </div>
          <button class="primary" type="submit">上传并开始</button>
        </form>
      </section>
      <section class="jobs">
        <h2>任务</h2>
        <div id="jobList" class="job-list"></div>
      </section>
    </aside>
    <section class="detail" id="detail">
      <div class="empty">等待任务。</div>
    </section>
  </main>
  <div class="toast" id="toast"></div>
  <script>
    const BASE = {{ base|tojson }};
    let selectedId = null;
    let lastLog = "";
    let uploadInFlight = false;
    const statusLabel = {queued:"排队", running:"制作中", publishing:"发布中", completed:"完成", failed:"失败"};

    function toast(msg) {
      const el = document.getElementById("toast");
      el.textContent = msg;
      el.classList.add("show");
      setTimeout(() => el.classList.remove("show"), 2600);
    }

    async function api(url, options = {}) {
      const res = await fetch(url, options);
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        const err = new Error(data.error || "请求失败");
        err.status = res.status;
        throw err;
      }
      return data;
    }

    function renderJobs(jobs) {
      const list = document.getElementById("jobList");
      if (!jobs.length) {
        list.innerHTML = '<div class="job"><small>暂无任务</small></div>';
        return;
      }
      list.innerHTML = jobs.map(job => {
        const title = job.title || job.original_filename || job.id;
        const active = selectedId === job.id ? " active" : "";
        const pill = statusLabel[job.status] || job.status;
        return `<button class="job${active}" data-id="${job.id}">
          <div class="job-title"><span>${escapeHtml(title)}</span><span class="pill ${job.status}">${pill}</span></div>
          <small>${escapeHtml(job.date || "")} · ${escapeHtml(job.original_filename || "")}</small>
        </button>`;
      }).join("");
      list.querySelectorAll(".job").forEach(btn => {
        btn.addEventListener("click", () => {
          selectedId = btn.dataset.id;
          loadDetail();
          renderJobs(jobs);
        });
      });
      if (!selectedId && jobs[0]) {
        selectedId = jobs[0].id;
        loadDetail();
      }
    }

    function renderDetail(job) {
      const detail = document.getElementById("detail");
      const title = job.title || job.original_filename || job.id;
      const files = (job.files || []).map(file => `<a href="${file.url}">${escapeHtml(file.name)}</a>`).join("");
      const canPublish = job.status === "completed" && !job.media_id;
      const canCancel = job.status === "running" || job.status === "publishing";
      detail.innerHTML = `
        <div class="detail-head">
          <div class="detail-title">
            <h2>${escapeHtml(title)}</h2>
            <div class="meta">${escapeHtml(job.id)} · ${escapeHtml(job.date || "")}<br>${escapeHtml(job.original_filename || "")}${job.upload_size ? " · " + formatBytes(job.upload_size) : ""}${job.llm_briefing ? `<br><strong>制作说明</strong>：${escapeHtml(job.llm_briefing.slice(0, 240))}${job.llm_briefing.length > 240 ? "…" : ""}` : ""}</div>
          </div>
          <div class="actions">
            <span class="pill ${job.status}">${statusLabel[job.status] || job.status}</span>
            ${canPublish ? `<button class="ghost" id="publishBtn">创建草稿</button>` : ""}
            ${canCancel ? `<button class="ghost danger" id="cancelBtn">停止</button>` : ""}
            <a class="ghost" href="${BASE}/download/${job.id}.zip" style="display:inline-flex;align-items:center;text-decoration:none;">下载全部</a>
          </div>
        </div>
        <div class="grid">
          <div class="metric"><b>${escapeHtml(String(job.dialogue_lines || 0))}</b><span>对话行</span></div>
          <div class="metric"><b>${job.mp3_cdn_url ? "已上传" : "未生成"}</b><span>音频 URL</span></div>
          <div class="metric"><b>${job.media_id ? "已创建" : "未创建"}</b><span>公众号草稿</span></div>
        </div>
        <div class="files">${files || '<span class="meta">暂无文件</span>'}</div>
        <pre class="log" id="log">${escapeHtml(job.log || "")}</pre>
      `;
      const log = document.getElementById("log");
      if (job.log !== lastLog) {
        log.scrollTop = log.scrollHeight;
        lastLog = job.log || "";
      }
      const publishBtn = document.getElementById("publishBtn");
      if (publishBtn) publishBtn.addEventListener("click", publishSelected);
      const cancelBtn = document.getElementById("cancelBtn");
      if (cancelBtn) cancelBtn.addEventListener("click", cancelSelected);
    }

    async function loadJobs() {
      try {
        const data = await api(`${BASE}/api/jobs`);
        renderJobs(data.jobs || []);
      } catch (err) {
        toast(err.message);
      }
    }

    async function loadDetail() {
      if (!selectedId) return;
      try {
        const data = await api(`${BASE}/api/jobs/${selectedId}`);
        renderDetail(data.job);
      } catch (err) {
        if (err.status === 404) {
          selectedId = null;
          lastLog = "";
          document.getElementById("detail").innerHTML = '<div class="empty">等待任务。</div>';
          return;
        }
        toast(err.message);
      }
    }

    async function publishSelected() {
      if (!selectedId) return;
      try {
        await api(`${BASE}/api/jobs/${selectedId}/publish`, {method:"POST"});
        toast("已开始创建草稿");
        loadDetail();
      } catch (err) {
        toast(err.message);
      }
    }

    async function cancelSelected() {
      if (!selectedId) return;
      try {
        await api(`${BASE}/api/jobs/${selectedId}/cancel`, {method:"POST"});
        toast("已发送停止请求");
        loadDetail();
      } catch (err) {
        toast(err.message);
      }
    }

    const uploadForm = document.getElementById("uploadForm");
    const fileInput = uploadForm.querySelector('input[name="pdf"]');
    const fileSummary = document.getElementById("fileSummary");
    const fileName = document.getElementById("fileName");
    const fileSize = document.getElementById("fileSize");
    const uploadProgress = document.getElementById("uploadProgress");
    const uploadState = document.getElementById("uploadState");
    const uploadPercent = document.getElementById("uploadPercent");
    const progressFill = document.getElementById("progressFill");
    const uploadReadout = document.getElementById("uploadReadout");

    fileInput.addEventListener("change", () => {
      const file = fileInput.files && fileInput.files[0];
      if (!file) {
        fileSummary.classList.remove("active");
        resetProgress();
        return;
      }
      fileName.textContent = file.name;
      fileSize.textContent = `${formatBytes(file.size)} · 最大 {{ max_upload_mb }} MB`;
      fileSummary.classList.add("active");
      setProgress(0, 0, file.size, "准备上传");
    });

    uploadForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      const form = event.currentTarget;
      const button = form.querySelector("button");
      const file = fileInput.files && fileInput.files[0];
      button.disabled = true;
      button.textContent = "上传中 0%";
      uploadInFlight = true;
      setProgress(0, 0, file ? file.size : 0, "正在上传");
      try {
        const data = await uploadInChunks(file, form, button);
        selectedId = data.job.id;
        form.reset();
        form.querySelector('input[type="date"]').value = "{{ today }}";
        fileSummary.classList.remove("active");
        setProgress(100, file ? file.size : 0, file ? file.size : 0, "任务已创建");
        toast("任务已开始");
        await loadJobs();
        await loadDetail();
      } catch (err) {
        uploadState.textContent = "上传失败";
        toast(err.message);
      } finally {
        uploadInFlight = false;
        button.disabled = false;
        button.textContent = "上传并开始";
      }
    });

    async function uploadInChunks(file, form, button) {
      if (!file) throw new Error("请选择 PDF 文件");
      const start = await api(`${BASE}/api/uploads/start`, {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({
          filename: file.name,
          size: file.size,
          date: form.querySelector('input[name="date"]').value || "{{ today }}",
          publish: form.querySelector('input[name="publish"]').checked,
          briefing: (form.querySelector('textarea[name="briefing"]')?.value || "").trim(),
        }),
      });

      const uploadId = start.upload_id;
      const chunkSize = Number(start.chunk_size) || 65536;
      const totalChunks = Math.ceil(file.size / chunkSize);
      for (let index = 0; index < totalChunks; index += 1) {
        const begin = index * chunkSize;
        const end = Math.min(file.size, begin + chunkSize);
        const label = `上传分片 ${index + 1}/${totalChunks}`;
        await uploadChunkWithRetry(uploadId, index, file.slice(begin, end), file.name, begin, file.size, label, button);
        const percent = Math.min(100, Math.round((end / file.size) * 100));
        setProgress(percent, end, file.size, label);
        button.textContent = `上传中 ${percent}%`;
      }

      setProgress(100, file.size, file.size, "上传完成，正在创建任务");
      button.textContent = "正在创建任务";
      return api(`${BASE}/api/uploads/${uploadId}/finish`, {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({total_chunks: totalChunks}),
      });
    }

    async function uploadChunkWithRetry(uploadId, index, blob, filename, loadedBefore, totalBytes, label, button) {
      let lastError = null;
      for (let attempt = 1; attempt <= 3; attempt += 1) {
        try {
          return await uploadChunkRequest(uploadId, index, blob, filename, loadedBefore, totalBytes, label, attempt, button);
        } catch (err) {
          lastError = err;
          uploadState.textContent = `${label} 重试 ${attempt}/3`;
          await new Promise(resolve => setTimeout(resolve, 600 * attempt));
        }
      }
      throw lastError || new Error("分片上传失败");
    }

    function uploadChunkRequest(uploadId, index, blob, filename, loadedBefore, totalBytes, label, attempt, button) {
      return new Promise((resolve, reject) => {
        const xhr = new XMLHttpRequest();
      const formData = new FormData();
      formData.append("index", String(index));
      formData.append("chunk", blob, `${filename}.part${index}`);
        xhr.open("POST", `${BASE}/api/uploads/${uploadId}/chunk`, true);
        xhr.timeout = 0;
        xhr.upload.onprogress = (event) => {
          const chunkLoaded = event.lengthComputable ? event.loaded : 0;
          const loaded = Math.min(totalBytes, loadedBefore + chunkLoaded);
          const percent = Math.min(100, Math.round((loaded / totalBytes) * 100));
          setProgress(percent, loaded, totalBytes, attempt > 1 ? `${label} 重试 ${attempt}/3` : label);
          button.textContent = `上传中 ${percent}%`;
        };
        xhr.onload = () => {
          const data = tryParseJson(xhr.responseText);
          if (xhr.status >= 200 && xhr.status < 300) {
            resolve(data);
            return;
          }
          reject(new Error((data && data.error) || `请求失败 (${xhr.status})`));
        };
        xhr.onerror = () => reject(new Error("网络连接中断，分片未完成"));
        xhr.onabort = () => reject(new Error("上传已取消"));
        xhr.send(formData);
      });
    }

    function setProgress(percent, loaded, total, state) {
      uploadProgress.classList.add("active");
      const safePercent = Math.max(0, Math.min(100, percent));
      progressFill.style.width = `${safePercent}%`;
      uploadPercent.textContent = `${safePercent}%`;
      uploadState.textContent = state;
      uploadReadout.innerHTML = total
        ? `<strong>${formatBytes(loaded)}</strong> / ${formatBytes(total)}`
        : "正在准备上传";
    }

    function resetProgress() {
      progressFill.style.width = "0%";
      uploadPercent.textContent = "0%";
      uploadState.textContent = "准备上传";
      uploadReadout.textContent = "等待选择文件";
      uploadProgress.classList.remove("active");
    }

    function formatBytes(bytes) {
      const value = Number(bytes) || 0;
      if (value < 1024) return `${value} B`;
      const units = ["KB", "MB", "GB"];
      let size = value / 1024;
      let unit = units.shift();
      while (size >= 1024 && units.length) {
        size /= 1024;
        unit = units.shift();
      }
      return `${size >= 10 ? size.toFixed(1) : size.toFixed(2)} ${unit}`;
    }

    function tryParseJson(text) {
      try { return JSON.parse(text || "{}"); } catch { return {}; }
    }

    function escapeHtml(value) {
      return String(value).replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[ch]));
    }

    loadJobs();
    setInterval(() => {
      if (uploadInFlight) return;
      loadJobs();
      loadDetail();
    }, 10000);
  </script>
</body>
</html>
"""


def main() -> None:
    _ensure_dirs()
    host = os.getenv("PODCAST_WEB_HOST", "0.0.0.0")
    port = int(os.getenv("PODCAST_WEB_PORT", "8091"))
    app.run(host=host, port=port, threaded=True)


if __name__ == "__main__":
    main()

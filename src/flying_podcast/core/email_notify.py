"""Email notification — pipeline process summary report."""

from __future__ import annotations

import smtplib
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr

from flying_podcast.core.config import settings
from flying_podcast.core.logging_utils import get_logger

logger = get_logger("email_notify")

# ---------------------------------------------------------------------------
# HTML builder
# ---------------------------------------------------------------------------

_STYLE = """\
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; color: #333; }
  .wrap { max-width: 600px; margin: 0 auto; padding: 20px; }
  h2 { color: #1a73e8; margin-bottom: 4px; }
  .sub { color: #888; font-size: 13px; margin-bottom: 18px; }
  .stage { margin-bottom: 14px; }
  .stage-title { font-weight: 700; font-size: 14px; color: #1a73e8; margin-bottom: 4px; }
  .metric { display: inline-block; background: #f0f4ff; border-radius: 6px; padding: 3px 10px; margin: 2px 4px 2px 0; font-size: 13px; }
  .metric b { color: #1a73e8; }
  .drop { color: #c0392b; }
  .ok { color: #27ae60; }
  table.scores { border-collapse: collapse; font-size: 13px; margin-top: 4px; }
  table.scores td { padding: 2px 10px 2px 0; }
  .reasons { font-size: 13px; color: #e67e22; margin-top: 4px; }
  .footer { margin-top: 24px; font-size: 11px; color: #aaa; text-align: center; }
</style>"""


def _build_report_html(
    day: str,
    ingest_count: int,
    rank_meta: dict,
    compose_meta: dict,
    quality: dict,
    publish: dict,
) -> str:
    # --- Rank stage ---
    total_cand = rank_meta.get("total_candidates", ingest_count)
    selected = rank_meta.get("selected_for_compose", "?")
    drops = []
    drop_keys = [
        ("dropped_hard_reject", "硬拒绝"),
        ("dropped_blocked_domain", "黑名单域名"),
        ("dropped_non_relevant", "无关内容"),
        ("dropped_non_pilot_relevant", "非飞行相关"),
        ("dropped_no_original_link", "无原链接"),
        ("dropped_no_published_at", "无发布时间"),
        ("dropped_too_old", "过期"),
    ]
    for key, label in drop_keys:
        v = rank_meta.get(key, 0)
        if v:
            drops.append(f'<span class="metric drop">{label} <b>-{v}</b></span>')
    drops_html = " ".join(drops) if drops else '<span class="metric ok">无过滤</span>'

    # source distribution top 5
    src_dist = rank_meta.get("source_distribution", {})
    top_sources = sorted(src_dist.items(), key=lambda x: x[1], reverse=True)[:5]
    src_html = ", ".join(f"{s}({n})" for s, n in top_sources) if top_sources else "-"

    # --- Compose stage ---
    entry_count = compose_meta.get("entry_count", "?")

    # --- Quality stage ---
    total_score = quality.get("total_score", "-")
    factual = quality.get("factual_score", "-")
    relevance = quality.get("relevance_score", "-")
    citation = quality.get("citation_score", "-")
    timeliness = quality.get("timeliness_score", "-")
    readability = quality.get("readability_score", "-")
    decision = quality.get("decision", "-")
    reasons = quality.get("reasons", [])
    blocked_ids = quality.get("blocked_entry_ids", [])

    reasons_html = ""
    if reasons:
        reasons_html = '<div class="reasons">⚠ ' + "、".join(reasons) + "</div>"

    # --- Publish stage ---
    pub_status = publish.get("status", "-")
    pub_url = publish.get("url", "")
    compose_mode = publish.get("compose_mode", "-")

    status_color = "#27ae60" if pub_status in ("draft_created", "published") else "#c0392b"

    return f"""\
<!DOCTYPE html><html><head><meta charset="utf-8">{_STYLE}</head><body>
<div class="wrap">
  <h2>Global Aviation Digest Pipeline Report</h2>
  <div class="sub">{day}</div>

  <div class="stage">
    <div class="stage-title">① 采集 Ingest</div>
    <span class="metric">采集文章 <b>{ingest_count}</b></span>
  </div>

  <div class="stage">
    <div class="stage-title">② 筛选 Rank</div>
    <span class="metric">候选 <b>{total_cand}</b></span>
    <span class="metric ok">入选 <b>{selected}</b></span><br>
    {drops_html}
    <div style="font-size:12px;color:#888;margin-top:4px;">来源 TOP5: {src_html}</div>
  </div>

  <div class="stage">
    <div class="stage-title">③ 成稿 Compose</div>
    <span class="metric">成稿 <b>{entry_count}</b> 篇</span>
    <span class="metric">模式 <b>{compose_mode}</b></span>
  </div>

  <div class="stage">
    <div class="stage-title">④ 质检 Verify</div>
    <table class="scores">
      <tr><td>综合分</td><td><b>{total_score}</b></td>
          <td>事实性</td><td>{factual}</td></tr>
      <tr><td>相关性</td><td>{relevance}</td>
          <td>引用</td><td>{citation}</td></tr>
      <tr><td>时效性</td><td>{timeliness}</td>
          <td>可读性</td><td>{readability}</td></tr>
    </table>
    <div style="margin-top:4px;font-size:13px;">决策: <b>{decision}</b>
    {"　拦截 " + str(len(blocked_ids)) + " 条" if blocked_ids else ""}</div>
    {reasons_html}
  </div>

  <div class="stage">
    <div class="stage-title">⑤ 发布 Publish</div>
    <span class="metric">状态 <b style="color:{status_color}">{pub_status}</b></span>
    {"<br><a href='" + pub_url + "' style='font-size:13px;'>" + pub_url + "</a>" if pub_url and pub_url != "-" else ""}
  </div>

  <div class="footer">Auto-generated by Global Aviation Digest</div>
</div>
</body></html>"""


# ---------------------------------------------------------------------------
# Send
# ---------------------------------------------------------------------------

def send_pipeline_report(
    day: str,
    ingest_count: int,
    rank_meta: dict,
    compose_meta: dict,
    quality: dict,
    publish: dict,
) -> bool:
    """Send a pipeline process summary email.

    Returns True if sent, False if skipped or failed.
    """
    if not settings.email_user or not settings.email_pass:
        logger.debug("Email not configured, skipping")
        return False

    email_to = settings.email_to or settings.email_user
    sender_name = settings.email_sender or "Global Aviation Digest"
    smtp_server = settings.email_smtp_server
    if not smtp_server:
        domain = settings.email_user.split("@")[1]
        smtp_server = f"smtp.{domain}"

    pub_status = publish.get("status", "unknown")
    subject = f"Global Aviation Digest {day} — {pub_status}"
    html_content = _build_report_html(
        day, ingest_count, rank_meta, compose_meta, quality, publish,
    )

    msg = MIMEMultipart("alternative")
    msg.attach(MIMEText("请使用支持 HTML 的邮件客户端查看此邮件。", "plain", "utf-8"))
    msg.attach(MIMEText(html_content, "html", "utf-8"))
    msg["From"] = formataddr(
        (Header(sender_name, "utf-8").encode(), settings.email_user),
    )
    msg["To"] = email_to
    msg["Subject"] = Header(subject, "utf-8")

    try:
        with smtplib.SMTP_SSL(smtp_server, 465, timeout=30) as server:
            server.login(settings.email_user, settings.email_pass)
            server.sendmail(settings.email_user, [email_to], msg.as_string())
        logger.info("Pipeline report email sent to %s", email_to)
        return True
    except Exception:
        logger.exception("Failed to send email")
        return False

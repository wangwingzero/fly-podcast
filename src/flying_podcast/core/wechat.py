from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import tempfile
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse, urlencode

import requests

from .config import settings

logger = logging.getLogger(__name__)


class WeChatPublishError(RuntimeError):
    pass


@dataclass
class WeChatPublishResult:
    publish_id: str
    message: str
    detail: dict[str, Any]


def _clean_proxy_env() -> dict[str, str]:
    """Return a copy of os.environ with system proxy vars removed."""
    env = os.environ.copy()
    for k in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
              "http_proxy", "https_proxy", "all_proxy",
              "NO_PROXY", "no_proxy"):
        env.pop(k, None)
    return env


def _curl_get(url: str, params: dict | None = None, *,
              proxy: str = "", timeout: int = 30) -> dict:
    """HTTP GET via curl subprocess (reliable proxy CONNECT tunneling)."""
    if params:
        url = f"{url}?{urlencode(params)}"
    cmd = ["curl", "-sS", "-m", str(timeout)]
    if proxy:
        cmd += ["-x", proxy]
    cmd.append(url)
    r = subprocess.run(cmd, capture_output=True, text=True,
                       timeout=timeout + 10, env=_clean_proxy_env())
    if r.returncode != 0:
        raise WeChatPublishError(f"curl GET failed (rc={r.returncode}): {r.stderr[:200]}")
    return json.loads(r.stdout)


def _curl_post_json(url: str, params: dict | None = None,
                    body: dict | None = None, *,
                    proxy: str = "", timeout: int = 60) -> dict:
    """HTTP POST JSON via curl subprocess."""
    if params:
        url = f"{url}?{urlencode(params)}"
    cmd = ["curl", "-sS", "-m", str(timeout), "-X", "POST",
           "-H", "Content-Type: application/json; charset=utf-8"]
    if proxy:
        cmd += ["-x", proxy]
    if body:
        payload = json.dumps(body, ensure_ascii=False)
        cmd += ["-d", payload]
    cmd.append(url)
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 10,
                       encoding="utf-8", env=_clean_proxy_env())
    if r.returncode != 0:
        raise WeChatPublishError(f"curl POST failed (rc={r.returncode}): {r.stderr[:200]}")
    return json.loads(r.stdout)


def _curl_post_file(url: str, params: dict | None = None,
                    file_field: str = "media",
                    file_path: str = "", file_name: str = "",
                    content_type: str = "image/jpeg", *,
                    proxy: str = "", timeout: int = 60) -> dict:
    """HTTP POST multipart file upload via curl subprocess."""
    if params:
        url = f"{url}?{urlencode(params)}"
    cmd = ["curl", "-sS", "-m", str(timeout)]
    if proxy:
        cmd += ["-x", proxy]
    cmd += ["-F", f"{file_field}=@{file_path};filename={file_name};type={content_type}"]
    cmd.append(url)
    r = subprocess.run(cmd, capture_output=True, text=True,
                       timeout=timeout + 10, env=_clean_proxy_env())
    if r.returncode != 0:
        raise WeChatPublishError(f"curl upload failed (rc={r.returncode}): {r.stderr[:200]}")
    return json.loads(r.stdout)


class WeChatClient:
    def __init__(self) -> None:
        self.base = "https://api.weixin.qq.com/cgi-bin"
        self._proxy = settings.wechat_proxy or ""
        self._cached_token: str = ""
        self._token_expires: float = 0

    def _access_token(self) -> str:
        if self._cached_token and time.time() < self._token_expires:
            return self._cached_token
        if not settings.wechat_app_id or not settings.wechat_app_secret:
            raise WeChatPublishError("Missing WECHAT_APP_ID/WECHAT_APP_SECRET")
        data = _curl_get(
            f"{self.base}/token",
            params={
                "grant_type": "client_credential",
                "appid": settings.wechat_app_id,
                "secret": settings.wechat_app_secret,
            },
            proxy=self._proxy, timeout=30,
        )
        token = data.get("access_token")
        if not token:
            raise WeChatPublishError(f"Get token failed: {data}")
        self._cached_token = token
        self._token_expires = time.time() + data.get("expires_in", 7200) - 60
        return token

    def _upload_image(self, endpoint: str, image_data: bytes,
                      file_name: str, token: str) -> dict:
        """Upload image bytes via curl using a temp file."""
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as f:
            f.write(image_data)
            tmp_path = f.name
        try:
            return _curl_post_file(
                f"{self.base}/{endpoint}",
                params={"access_token": token},
                file_field="media", file_path=tmp_path,
                file_name=file_name, content_type="image/jpeg",
                proxy=self._proxy, timeout=60,
            )
        finally:
            os.unlink(tmp_path)

    def upload_content_image_bytes(self, image_data: bytes, token: str | None = None) -> str:
        """Upload raw image bytes to WeChat CDN for article content."""
        if not token:
            token = self._access_token()
        try:
            data = self._upload_image("media/uploadimg", image_data,
                                      "generated.jpg", token)
            wx_url = data.get("url", "")
            if wx_url:
                return wx_url
            logger.warning("Upload image bytes failed: %s", data)
        except Exception:
            logger.warning("Upload image bytes exception")
        return ""

    def upload_thumb_image_bytes(self, image_data: bytes, token: str | None = None) -> str:
        """Upload image bytes as permanent material for use as article thumbnail."""
        if not token:
            token = self._access_token()
        try:
            with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as f:
                f.write(image_data)
                tmp_path = f.name
            try:
                url = f"{self.base}/material/add_material"
                data = _curl_post_file(
                    url,
                    params={"access_token": token, "type": "image"},
                    file_field="media", file_path=tmp_path,
                    file_name="cover.jpg", content_type="image/jpeg",
                    proxy=self._proxy, timeout=60,
                )
            finally:
                os.unlink(tmp_path)
            media_id = data.get("media_id", "")
            if media_id:
                logger.info("Uploaded thumb material: %s", media_id[:40])
                return media_id
            logger.warning("Upload thumb material failed: %s", data)
        except Exception:
            logger.warning("Upload thumb material exception")
        return ""

    def upload_content_image(self, image_url: str, token: str | None = None) -> str:
        """Download an external image and upload it to WeChat's CDN."""
        if not token:
            token = self._access_token()
        try:
            resp = requests.get(image_url, timeout=15,
                                headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
        except Exception:
            logger.warning("Failed to download image: %s", image_url)
            return ""

        content_type = resp.headers.get("Content-Type", "image/jpeg")
        ext = "jpg"
        if "png" in content_type:
            ext = "png"
        elif "gif" in content_type:
            ext = "gif"

        try:
            with tempfile.NamedTemporaryFile(suffix=f".{ext}", delete=False) as f:
                f.write(resp.content)
                tmp_path = f.name
            try:
                data = _curl_post_file(
                    f"{self.base}/media/uploadimg",
                    params={"access_token": token},
                    file_field="media", file_path=tmp_path,
                    file_name=f"image.{ext}", content_type=content_type,
                    proxy=self._proxy, timeout=60,
                )
            finally:
                os.unlink(tmp_path)
            wx_url = data.get("url", "")
            if wx_url:
                logger.info("Uploaded image to WeChat CDN: %s -> %s",
                            image_url[:60], wx_url[:60])
                return wx_url
            logger.warning("Upload image failed: %s", data)
        except Exception:
            logger.warning("Upload image exception for: %s", image_url)
        return ""

    def replace_external_images(self, html: str) -> str:
        """Find all external <img src="..."> in HTML and replace with WeChat CDN URLs."""
        html = html.replace("http://mmbiz.qpic.cn", "https://mmbiz.qpic.cn")

        img_pattern = re.compile(r'(<img\s[^>]*?src=")([^"]+)(")')
        urls_to_replace: dict[str, str] = {}

        for match in img_pattern.finditer(html):
            src = match.group(2)
            parsed = urlparse(src)
            if parsed.scheme in ("http", "https") and "qpic.cn" not in parsed.netloc:
                if src not in urls_to_replace:
                    urls_to_replace[src] = ""

        if not urls_to_replace:
            return html

        token = self._access_token()
        for ext_url in urls_to_replace:
            wx_url = self.upload_content_image(ext_url, token=token)
            if wx_url:
                urls_to_replace[ext_url] = wx_url

        for ext_url, wx_url in urls_to_replace.items():
            if wx_url:
                html = html.replace(ext_url, wx_url)
            else:
                html = re.sub(
                    rf'<img\s[^>]*?src="{re.escape(ext_url)}"[^>]*/?>',
                    "",
                    html,
                )

        return html

    def create_draft(
        self, title: str, author: str, content_html: str, digest: str,
        source_url: str = "", thumb_media_id: str = "",
    ) -> str:
        token = self._access_token()
        article = {
            "title": title,
            "author": author,
            "digest": digest[:120],
            "content": content_html,
            "content_source_url": source_url,
            "thumb_media_id": thumb_media_id or settings.wechat_thumb_media_id,
            "need_open_comment": 0,
            "only_fans_can_comment": 0,
        }
        data = _curl_post_json(
            f"{self.base}/draft/add",
            params={"access_token": token},
            body={"articles": [article]},
            proxy=self._proxy, timeout=60,
        )
        media_id = data.get("media_id")
        if not media_id:
            raise WeChatPublishError(f"Create draft failed: {data}")
        return media_id

    def publish_draft(self, media_id: str) -> WeChatPublishResult:
        token = self._access_token()
        submit_data = _curl_post_json(
            f"{self.base}/freepublish/submit",
            params={"access_token": token},
            body={"media_id": media_id},
            proxy=self._proxy, timeout=60,
        )
        publish_id = submit_data.get("publish_id")
        if not publish_id:
            raise WeChatPublishError(f"Publish submit failed: {submit_data}")
        return WeChatPublishResult(
            publish_id=publish_id,
            message="submitted",
            detail=submit_data,
        )

    def get_publish_status(self, publish_id: str) -> dict[str, Any]:
        token = self._access_token()
        data = _curl_post_json(
            f"{self.base}/freepublish/get",
            params={"access_token": token},
            body={"publish_id": publish_id},
            proxy=self._proxy, timeout=60,
        )
        if data.get("errcode", 0) not in (0, None):
            raise WeChatPublishError(f"Get publish status failed: {data}")
        return data

    def list_drafts(self, count: int = 20) -> list[dict[str, Any]]:
        """List drafts in the WeChat backend."""
        token = self._access_token()
        data = _curl_post_json(
            f"{self.base}/draft/batchget",
            params={"access_token": token},
            body={"offset": 0, "count": min(count, 20), "no_content": 1},
            proxy=self._proxy, timeout=60,
        )
        if data.get("errcode", 0) not in (0, None):
            logger.warning("List drafts failed: %s", data)
            return []
        return data.get("item", [])

    def delete_draft(self, media_id: str) -> bool:
        """Delete a draft by media_id."""
        token = self._access_token()
        data = _curl_post_json(
            f"{self.base}/draft/delete",
            params={"access_token": token},
            body={"media_id": media_id},
            proxy=self._proxy, timeout=60,
        )
        if data.get("errcode", 0) in (0, None):
            logger.info("Deleted draft: %s", media_id[:40])
            return True
        logger.warning("Delete draft failed: %s %s", media_id[:40], data)
        return False

    def get_article_detail(self, article_id: str) -> dict[str, Any]:
        token = self._access_token()
        data = _curl_post_json(
            f"{self.base}/freepublish/getarticle",
            params={"access_token": token},
            body={"article_id": article_id},
            proxy=self._proxy, timeout=60,
        )
        if data.get("errcode", 0) not in (0, None):
            raise WeChatPublishError(f"Get article detail failed: {data}")
        return data

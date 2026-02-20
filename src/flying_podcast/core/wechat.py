from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

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


class WeChatClient:
    def __init__(self) -> None:
        self.base = "https://api.weixin.qq.com/cgi-bin"
        self.proxies = (
            {"http": settings.wechat_proxy, "https": settings.wechat_proxy}
            if settings.wechat_proxy
            else None
        )

    def _access_token(self) -> str:
        if not settings.wechat_app_id or not settings.wechat_app_secret:
            raise WeChatPublishError("Missing WECHAT_APP_ID/WECHAT_APP_SECRET")
        resp = requests.get(
            f"{self.base}/token",
            params={
                "grant_type": "client_credential",
                "appid": settings.wechat_app_id,
                "secret": settings.wechat_app_secret,
            },
            timeout=20,
            proxies=self.proxies,
        )
        data = resp.json()
        token = data.get("access_token")
        if not token:
            raise WeChatPublishError(f"Get token failed: {data}")
        return token

    def upload_content_image_bytes(self, image_data: bytes, token: str | None = None) -> str:
        """Upload raw image bytes to WeChat CDN for article content.

        Returns the WeChat CDN URL, or empty string on failure.
        """
        if not token:
            token = self._access_token()
        try:
            resp = requests.post(
                f"{self.base}/media/uploadimg",
                params={"access_token": token},
                files={"media": ("generated.jpg", image_data, "image/jpeg")},
                timeout=30,
                proxies=self.proxies,
            )
            data = resp.json()
            wx_url = data.get("url", "")
            if wx_url:
                return wx_url
            logger.warning("Upload image bytes failed: %s", data)
        except Exception:
            logger.warning("Upload image bytes exception")
        return ""

    def upload_thumb_image_bytes(self, image_data: bytes, token: str | None = None) -> str:
        """Upload image bytes as permanent material for use as article thumbnail.

        Returns the media_id (thumb_media_id), or empty string on failure.
        """
        if not token:
            token = self._access_token()
        try:
            resp = requests.post(
                f"{self.base}/material/add_material",
                params={"access_token": token, "type": "image"},
                files={"media": ("cover.jpg", image_data, "image/jpeg")},
                timeout=30,
                proxies=self.proxies,
            )
            data = resp.json()
            media_id = data.get("media_id", "")
            if media_id:
                logger.info("Uploaded thumb material: %s", media_id[:40])
                return media_id
            logger.warning("Upload thumb material failed: %s", data)
        except Exception:
            logger.warning("Upload thumb material exception")
        return ""

    def upload_content_image(self, image_url: str, token: str | None = None) -> str:
        """Download an external image and upload it to WeChat's CDN for article content.

        Returns the WeChat CDN URL, or empty string on failure.
        """
        if not token:
            token = self._access_token()
        try:
            resp = requests.get(image_url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
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
            upload_resp = requests.post(
                f"{self.base}/media/uploadimg",
                params={"access_token": token},
                files={"media": (f"image.{ext}", resp.content, content_type)},
                timeout=30,
                proxies=self.proxies,
            )
            data = upload_resp.json()
            wx_url = data.get("url", "")
            if wx_url:
                logger.info("Uploaded image to WeChat CDN: %s -> %s", image_url[:60], wx_url[:60])
                return wx_url
            logger.warning("Upload image failed: %s", data)
        except Exception:
            logger.warning("Upload image exception for: %s", image_url)
        return ""

    def replace_external_images(self, html: str) -> str:
        """Find all external <img src="..."> in HTML and replace with WeChat CDN URLs.

        Also upgrades http:// to https:// for WeChat CDN images.
        """
        # Fix protocol for existing WeChat CDN images
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
                # Remove broken img tags
                html = re.sub(
                    rf'<img\s[^>]*?src="{re.escape(ext_url)}"[^>]*/?>',
                    "",
                    html,
                )

        return html

    def create_draft(
        self, title: str, author: str, content_html: str, digest: str,
        source_url: str, thumb_media_id: str = "",
    ) -> str:
        token = self._access_token()
        url = f"{self.base}/draft/add"
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
        resp = requests.post(
            url,
            params={"access_token": token},
            data=json.dumps({"articles": [article]}, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json; charset=utf-8"},
            timeout=30,
            proxies=self.proxies,
        )
        data = resp.json()
        media_id = data.get("media_id")
        if not media_id:
            raise WeChatPublishError(f"Create draft failed: {data}")
        return media_id

    def publish_draft(self, media_id: str) -> WeChatPublishResult:
        token = self._access_token()
        submit_url = f"{self.base}/freepublish/submit"
        submit_resp = requests.post(
            submit_url,
            params={"access_token": token},
            json={"media_id": media_id},
            timeout=30,
            proxies=self.proxies,
        )
        submit_data = submit_resp.json()
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
        resp = requests.post(
            f"{self.base}/freepublish/get",
            params={"access_token": token},
            json={"publish_id": publish_id},
            timeout=30,
            proxies=self.proxies,
        )
        data = resp.json()
        if data.get("errcode", 0) not in (0, None):
            raise WeChatPublishError(f"Get publish status failed: {data}")
        return data

    def list_drafts(self, count: int = 20) -> list[dict[str, Any]]:
        """List drafts in the WeChat backend. Returns list of draft items."""
        token = self._access_token()
        resp = requests.post(
            f"{self.base}/draft/batchget",
            params={"access_token": token},
            json={"offset": 0, "count": min(count, 20), "no_content": 1},
            timeout=30,
            proxies=self.proxies,
        )
        data = resp.json()
        if data.get("errcode", 0) not in (0, None):
            logger.warning("List drafts failed: %s", data)
            return []
        return data.get("item", [])

    def delete_draft(self, media_id: str) -> bool:
        """Delete a draft by media_id. Returns True on success."""
        token = self._access_token()
        resp = requests.post(
            f"{self.base}/draft/delete",
            params={"access_token": token},
            json={"media_id": media_id},
            timeout=30,
            proxies=self.proxies,
        )
        data = resp.json()
        if data.get("errcode", 0) in (0, None):
            logger.info("Deleted draft: %s", media_id[:40])
            return True
        logger.warning("Delete draft failed: %s %s", media_id[:40], data)
        return False

    def get_article_detail(self, article_id: str) -> dict[str, Any]:
        token = self._access_token()
        resp = requests.post(
            f"{self.base}/freepublish/getarticle",
            params={"access_token": token},
            json={"article_id": article_id},
            timeout=30,
            proxies=self.proxies,
        )
        data = resp.json()
        if data.get("errcode", 0) not in (0, None):
            raise WeChatPublishError(f"Get article detail failed: {data}")
        return data

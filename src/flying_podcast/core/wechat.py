from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests

from .config import settings


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
        )
        data = resp.json()
        token = data.get("access_token")
        if not token:
            raise WeChatPublishError(f"Get token failed: {data}")
        return token

    def create_draft(self, title: str, author: str, content_html: str, digest: str, source_url: str) -> str:
        token = self._access_token()
        url = f"{self.base}/draft/add"
        article = {
            "title": title,
            "author": author,
            "digest": digest[:120],
            "content": content_html,
            "content_source_url": source_url,
            "thumb_media_id": settings.wechat_thumb_media_id,
            "need_open_comment": 0,
            "only_fans_can_comment": 0,
        }
        resp = requests.post(
            url,
            params={"access_token": token},
            json={"articles": [article]},
            timeout=30,
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

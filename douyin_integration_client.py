"""抖音扫码登录集成客户端 — 供外部项目调用 Barra 的登录服务。

基于 MediaCrawler Server Integration Guide §6 实现。
提供完整的异步登录流程：发起 QR 登录 → 展示二维码 → 轮询扫描结果 → 获取 Cookie。

用法:
    client = DouyinIntegrationClient("http://localhost:8000")
    auth = await client.login_interactive()
    # auth.cookies 包含完整的 Douyin Cookie 字典
    # auth.cookie_str 可直接用作 HTTP Cookie 头
"""

import asyncio
import base64
import time
from dataclasses import dataclass, field
from typing import Optional, Dict, Callable, Awaitable

import httpx


@dataclass
class DouyinAuth:
    """持有身份认证状态，可直接用于 API 调用。

    Attributes:
        session_id:    登录会话 ID。
        cookies:       {name: value} Cookie 字典。
        cookie_str:    "name1=value1; name2=value2" 格式的 Cookie 字符串。
        authenticated: 是否成功完成登录。
        nickname:      登录用户的抖音昵称（如有）。
    """
    session_id: str = ""
    cookies: Dict[str, str] = field(default_factory=dict)
    cookie_str: str = ""
    authenticated: bool = False
    nickname: str = ""

    @property
    def is_valid(self) -> bool:
        """Cookie 是否有效（同时持有 sessionid 且已认证）。"""
        return self.authenticated and bool(self.cookies.get("sessionid") or
                                           self.cookies.get("sessionid_ss"))


class DouyinIntegrationClient:
    """抖音登录服务集成客户端。

    封装与 Barra / douyin_login_server 的 HTTP 通信，提供
    高层的登录流程方法。

    Args:
        server_url: 登录服务器地址（默认 http://localhost:8000）。
        timeout:    HTTP 请求超时秒数。
    """

    def __init__(self, server_url: str = "http://localhost:8000", timeout: int = 30):
        self.server_url = server_url.rstrip("/")
        self.auth = DouyinAuth()
        self._http = httpx.AsyncClient(timeout=timeout)

    # ── 登录流程 ────────────────────────────────────

    async def login_interactive(
        self,
        qrcode_callback: Optional[Callable[[str], Awaitable[None]]] = None,
        poll_interval: float = 2.0,
        timeout: int = 600,
    ) -> DouyinAuth:
        """完整的交互式扫码登录流程。

        流程:
            1. POST /api/douyin/login/start → 获取二维码 base64
            2. 通过回调展示二维码
            3. 轮询 /api/douyin/login/status 直至扫码成功或超时
            4. 返回完整的 DouyinAuth 对象

        Args:
            qrcode_callback: 异步回调，接收二维码 base64 字符串。
                             默认打印摘要信息到控制台。
            poll_interval:   轮询间隔秒数（默认 2s）。
            timeout:         总超时秒数（默认 600s = 10 分钟）。

        Returns:
            包含登录态 Cookie 的 DouyinAuth 对象。

        Raises:
            RuntimeError: 服务器返回失败时。
            TimeoutError: 超过 timeout 仍未扫码成功时。
        """
        # Step 1: 启动登录会话
        resp = await self._http.post(f"{self.server_url}/api/douyin/login/start")
        resp.raise_for_status()
        data = resp.json()
        session_id = data["session_id"]
        qrcode_base64 = data["qrcode_base64"]
        self.auth.session_id = session_id

        # Step 2: 展示二维码
        if qrcode_callback:
            await qrcode_callback(qrcode_base64)
        else:
            print(f"\n{'=' * 60}")
            print("请使用抖音 APP 扫描二维码登录")
            print(f"{'=' * 60}")
            print(f"Session: {session_id}")
            print(f"二维码 base64: {qrcode_base64[:80]}...")
            print(f"或在浏览器打开: {self.server_url}/qr/{session_id}")
            print(f"{'=' * 60}\n")

        # Step 3: 轮询登录状态
        deadline = time.time() + timeout
        last_log = 0
        while time.time() < deadline:
            await asyncio.sleep(poll_interval)

            status_resp = await self._http.post(
                f"{self.server_url}/api/douyin/login/status",
                params={"session_id": session_id},
            )
            status_resp.raise_for_status()
            status_data = status_resp.json()
            state = status_data.get("state", "")

            if state == "success":
                self.auth.cookies = status_data.get("cookies", {})
                self.auth.cookie_str = "; ".join(
                    f"{k}={v}" for k, v in self.auth.cookies.items()
                )
                self.auth.authenticated = True
                self.auth.nickname = status_data.get("nickname", "")
                print("✅ 扫码登录成功！")
                return self.auth

            elif state == "failed":
                raise RuntimeError(
                    f"登录失败: {status_data.get('message', '服务器端错误')}"
                )

            # 每 30s 打印一次等待日志
            elapsed = time.time() - (deadline - timeout)
            if elapsed - last_log >= 30:
                last_log = elapsed
                print(f"⏳ 等待扫码... ({elapsed:.0f}s / {timeout}s)")

        raise TimeoutError(f"扫码登录超时（{timeout}s）")

    # ── 会话管理 ────────────────────────────────────

    async def load_saved_session(self, session_id: str) -> bool:
        """尝试恢复一个之前完成的登录会话。

        从服务器加载已保存的会话，如果有效则填充 auth。

        Returns:
            True 表示成功加载有效会话。
        """
        resp = await self._http.post(
            f"{self.server_url}/api/douyin/login/status",
            params={"session_id": session_id},
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("state") == "success" and data.get("cookies"):
            self.auth.session_id = session_id
            self.auth.cookies = data["cookies"]
            self.auth.cookie_str = "; ".join(
                f"{k}={v}" for k, v in data["cookies"].items()
            )
            self.auth.authenticated = True
            self.auth.nickname = data.get("nickname", "")
            return True
        return False

    async def check_auth_valid(self) -> bool:
        """验证当前会话的身份是否仍然有效。"""
        if not self.auth.session_id:
            return False
        try:
            resp = await self._http.post(
                f"{self.server_url}/api/douyin/check-auth",
                params={"session_id": self.auth.session_id},
            )
            resp.raise_for_status()
            data = resp.json()
            self.auth.authenticated = data.get("authenticated", False)
            return self.auth.authenticated
        except httpx.HTTPStatusError:
            self.auth.authenticated = False
            return False

    async def refresh_qrcode(self) -> str:
        """刷新二维码（当前二维码过期时调用）。

        Returns:
            新的二维码 base64 字符串。
        """
        resp = await self._http.post(
            f"{self.server_url}/api/douyin/login/refresh",
            params={"session_id": self.auth.session_id},
        )
        resp.raise_for_status()
        data = resp.json()
        self.auth.session_id = data.get("session_id", self.auth.session_id)
        return data.get("qrcode_base64", "")

    # ── 资源管理 ────────────────────────────────────

    async def close(self):
        """关闭底层 HTTP 会话。"""
        await self._http.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()


# ── 使用示例 ────────────────────────────────────────

async def demo_interactive_login():
    """演示：如何使用集成客户端完成扫码登录并保存二维码到文件。"""
    async def save_qr_to_file(base64_qr: str):
        if "," in base64_qr:
            base64_qr = base64_qr.split(",")[1]
        raw = base64.b64decode(base64_qr)
        with open("douyin_login_qr.png", "wb") as f:
            f.write(raw)
        print("📷 二维码已保存到 douyin_login_qr.png，请使用抖音 APP 扫描")

    async with DouyinIntegrationClient() as client:
        try:
            auth = await client.login_interactive(qrcode_callback=save_qr_to_file)
            print(f"✅ 登录成功！Session: {auth.session_id}")
            print(f"   Cookie 项数: {len(auth.cookies)}")
            print(f"   用户: {auth.nickname or '未知'}")
            # 现在可以直接使用 auth.cookie_str 作为请求头
            # headers = {"Cookie": auth.cookie_str}
        except (RuntimeError, TimeoutError) as e:
            print(f"❌ 登录失败: {e}")


if __name__ == "__main__":
    asyncio.run(demo_interactive_login())

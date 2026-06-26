"""抖音 QR 扫码登录服务 — 独立 FastAPI 服务器。

基于 MediaCrawler Server Integration Guide §5 实现。
为外部项目（如 Barra、MediaCrawler 等）提供远程 QR 登录能力。

架构:
    POST /api/douyin/login/start    → {"session_id", "qrcode_base64", "state"}
    POST /api/douyin/login/status   → {"state", "cookies", "nickname"}
    POST /api/douyin/check-auth     → {"authenticated"}
    POST /api/douyin/login/refresh  → 刷新二维码
    GET  /qr/{session_id}           → HTML 页面展示二维码

运行:
    python douyin_login_server.py           # 默认 0.0.0.0:8000
    python douyin_login_server.py --port 9000 --host 127.0.0.1
"""

import asyncio
import base64
import json
import logging
import os
import random
import time
import uuid
from enum import Enum
from io import BytesIO
from typing import Optional, Dict

from PIL import Image, ImageDraw
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from playwright.async_api import async_playwright, BrowserContext, Page

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("douyin-login-server")

# ── 配置 ─────────────────────────────────────────────

USER_DATA_BASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "browser_data")
SESSION_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sessions")
QR_TIMEOUT = 60           # 二维码出现超时（秒）
POLL_TIMEOUT = 600        # 等待扫码超时（秒）
SESSION_TTL = 7200        # 会话过期时间（秒）

# ── Session 状态机 ─────────────────────────────────

class LoginState(str, Enum):
    INITIATED = "initiated"
    QR_READY = "qr_ready"
    SCANNING = "scanning"
    SUCCESS = "success"
    FAILED = "failed"


class DouyinSession:
    """一个 QR 登录会话的生命周期状态。"""

    def __init__(self):
        self.session_id: str = uuid.uuid4().hex[:12]
        self.state: LoginState = LoginState.INITIATED
        self.qrcode_base64: Optional[str] = None
        self.cookie_dict: Dict[str, str] = {}
        self.cookie_str: str = ""
        self.nickname: str = ""
        self.browser_context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.playwright = None
        self.created_at: float = time.time()
        self.user_data_dir: str = ""
        self.error: str = ""

    @property
    def is_expired(self) -> bool:
        return time.time() - self.created_at > SESSION_TTL


# ── 全局状态 ────────────────────────────────────────

sessions: Dict[str, DouyinSession] = {}

# QR 元素选择器（抖音可能更新，按优先级排列）
QR_SELECTORS = [
    "xpath=//div[@id='animate_qrcode_container']//img",
    "xpath=//div[@class='qrcode-img']//img",
    "img[src*='qrcode']",
    "img[src*='qr_code']",
    "#qrcode_img",
    ".qrcode-img img",
    ".login-qrcode img",
    "canvas[class*='qr']",
]

# 登录按钮选择器
LOGIN_BUTTON_SELECTORS = [
    "xpath=//p[text()='登录']",
    "text=登录",
    "text=扫码登录",
    ".login-btn",
    '[data-e2e="login"]',
    '#login-btn',
    "span:has-text('登录')",
]

app = FastAPI(
    title="Douyin QR Login Server",
    version="1.1.0",
    description="远程抖音扫码登录服务 — 基于 Playwright 的 QR 认证",
)


# ── 二维码处理 ──────────────────────────────────────

def process_qrcode_image(base64_qr: str) -> str:
    """给二维码添加白色边框和黑色轮廓线，提升手机扫码成功率。

    Args:
        base64_qr: 原始二维码图像数据（data URI 或裸 base64）。

    Returns:
        处理后的 base64 PNG 字符串（不含 data URI 前缀）。
    """
    if "," in base64_qr:
        base64_qr = base64_qr.split(",")[1]

    raw = base64.b64decode(base64_qr)
    image = Image.open(BytesIO(raw))
    width, height = image.size

    # 添加 10px 白色边框
    new_image = Image.new("RGB", (width + 20, height + 20), (255, 255, 255))
    new_image.paste(image, (10, 10))

    # 添加 1px 黑色轮廓线
    draw = ImageDraw.Draw(new_image)
    draw.rectangle((0, 0, width + 19, height + 19), outline=(0, 0, 0), width=1)

    buffered = BytesIO()
    new_image.save(buffered, format="PNG")
    return base64.b64encode(buffered.getvalue()).decode()


async def find_login_qrcode(page: Page) -> str:
    """从抖音登录页面提取二维码图片。

    依次尝试各个选择器定位二维码元素，支持 data URI 和 HTTP URL 两种格式。

    Returns:
        base64 编码的二维码图像（不含 data URI 前缀）。

    Raises:
        RuntimeError: 所有选择器均无法定位二维码时。
    """
    for selector in QR_SELECTORS:
        try:
            elements = await page.wait_for_selector(selector=selector, timeout=3000)
            if elements:
                src = str(await elements.get_property("src"))
                if not src or src == "null":
                    continue

                if src.startswith("http"):
                    # HTTP URL → 下载并编码
                    import httpx
                    async with httpx.AsyncClient(follow_redirects=True, timeout=10) as client:
                        resp = await client.get(
                            src,
                            headers={
                                "User-Agent": (
                                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                                    "Chrome/120.0.0.0 Safari/537.36"
                                ),
                            },
                        )
                        resp.raise_for_status()
                        return base64.b64encode(resp.content).decode()
                else:
                    # data URI → 提取 base64 数据
                    if "," in src:
                        return src.split(",")[1]
                    return src
        except Exception:
            continue

    raise RuntimeError("无法在页面中找到二维码元素（抖音可能更新了登录面板）")


# ── 浏览器管理 ──────────────────────────────────────

ANTI_DETECTION_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-web-security",
    "--disable-features=IsolateOrigins,site-per-process",
    "--window-size=1920,1080",
    "--disable-gpu",
]

STEALTH_JS = """
// Playwright anti-detection: override navigator.webdriver
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
// Override chrome.runtime
window.chrome = { runtime: {} };
// Override permissions
const originalQuery = window.navigator.permissions.query;
window.navigator.permissions.query = (params) => (
    params.name === 'notifications' ? Promise.resolve({ state: 'denied' }) : originalQuery(params)
);
// Override plugins array
Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
Object.defineProperty(navigator, 'languages', { get: () => ['zh-CN', 'zh'] });
"""


async def launch_douyin_browser(session: DouyinSession):
    """启动 Playwright 浏览器，打开抖音登录页面并提取二维码。

    流程:
        1. 启动 headless Chromium（带反检测参数）
        2. 创建新页面，设置中文 locale
        3. 导航到 douyin.com
        4. 弹出登录对话框（如有必要手动点击"登录"）
        5. 提取 QR 码图像
        6. 启动后台轮询任务等待扫码完成
    """
    playwright = await async_playwright().start()
    session.playwright = playwright

    # 启动浏览器
    browser = await playwright.chromium.launch(
        headless=True,
        args=ANTI_DETECTION_ARGS,
    )

    # 创建上下文 — 使用中文 locale
    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1920, "height": 1080},
        locale="zh-CN",
        timezone_id="Asia/Shanghai",
    )

    page = await context.new_page()

    # 注入反检测脚本
    await page.add_init_script(STEALTH_JS)

    # 导航到抖音首页
    logger.info(f"[{session.session_id}] 正在打开抖音首页...")
    await page.goto("https://www.douyin.com/", wait_until="domcontentloaded", timeout=30000)
    await asyncio.sleep(3)

    # 弹出登录对话框
    try:
        await page.wait_for_selector("div#login-panel-new", timeout=8000)
        logger.info(f"[{session.session_id}] 登录面板已自动弹出")
    except Exception:
        logger.info(f"[{session.session_id}] 登录面板未自动弹出，尝试点击登录按钮...")
        for selector in LOGIN_BUTTON_SELECTORS:
            try:
                btn = page.locator(selector).first
                if await btn.is_visible(timeout=1000):
                    await btn.click()
                    await asyncio.sleep(2)
                    logger.info(f"[{session.session_id}] 已点击登录按钮: {selector}")
                    break
            except Exception:
                continue

    # 提取二维码
    await asyncio.sleep(1)
    try:
        qr_base64 = await find_login_qrcode(page)
    except RuntimeError as e:
        await context.close()
        await browser.close()
        raise

    # 处理二维码图像（添加边框/轮廓）
    session.qrcode_base64 = process_qrcode_image(qr_base64)
    session.state = LoginState.QR_READY
    session.browser_context = context
    session.page = page

    logger.info(f"[{session.session_id}] 二维码已就绪")

    # 启动后台轮询
    asyncio.create_task(_poll_login_state(session, context, page))

    return session


async def _poll_login_state(session: DouyinSession, context: BrowserContext, page: Page):
    """后台轮询：每秒检查一次是否扫码成功。

    通过两种方式检测登录状态:
        A. localStorage 中的 HasUserLogin 标志（前端 JS 设置）
        B. LOGIN_STATUS Cookie（服务端设置）
    任一成立即认为登录成功。
    """
    session.state = LoginState.SCANNING
    logger.info(f"[{session.session_id}] 开始轮询登录状态...")

    for attempt in range(POLL_TIMEOUT):
        await asyncio.sleep(1)

        # 检查页面是否已关闭
        if session.state in (LoginState.SUCCESS, LoginState.FAILED):
            return

        try:
            # 方法 A: localStorage 标志
            local_storage = await page.evaluate("() => window.localStorage")
            if local_storage.get("HasUserLogin") == "1":
                logger.info(f"[{session.session_id}] localStorage HasUserLogin=1 → 登录成功")
                await _finalize_login(session, context)
                return
        except Exception:
            pass  # 页面可能正在导航

        try:
            # 方法 B: Cookie 检测
            cookies = await context.cookies()
            cookie_dict = {c["name"]: c["value"] for c in cookies}
            if cookie_dict.get("LOGIN_STATUS") == "1":
                logger.info(f"[{session.session_id}] LOGIN_STATUS cookie=1 → 登录成功")
                await _finalize_login(session, context)
                return
        except Exception:
            pass

        # 每 30s 输出一次心跳日志
        if attempt > 0 and attempt % 30 == 0:
            logger.info(f"[{session.session_id}] 等待扫码中... ({attempt}s)")

    # 超时
    logger.warning(f"[{session.session_id}] 轮询超时（{POLL_TIMEOUT}s）")
    session.state = LoginState.FAILED
    session.error = f"扫码超时（{POLL_TIMEOUT}s），请重新发起登录"


async def _finalize_login(session: DouyinSession, context: BrowserContext):
    """登录成功后的收尾工作：
    1. 等待 5s 让页面重定向完成
    2. 捕获所有 Cookie
    3. 尝试从页面提取用户昵称
    4. 将 Cookie 保存到磁盘
    """
    await asyncio.sleep(5)

    # 捕获 Cookie
    raw_cookies = await context.cookies()
    session.cookie_dict = {c["name"]: c["value"] for c in raw_cookies}
    session.cookie_str = "; ".join(
        f"{c['name']}={c['value']}" for c in raw_cookies
    )
    session.state = LoginState.SUCCESS

    # 尝试提取用户昵称
    try:
        local_storage = await session.page.evaluate("() => window.localStorage")
        nickname = local_storage.get("nickname", "") or local_storage.get("user_name", "")
        if nickname:
            session.nickname = nickname
    except Exception:
        pass

    # 尝试从页面 HTML 提取昵称
    if not session.nickname:
        try:
            content = await session.page.content()
            import re
            m = re.search(
                r'defaultHeaderUserInfo.*?isLogin.*?true.*?nickname\\?"[,:]\\?"([^"\\]+)',
                content, re.DOTALL,
            )
            if m:
                session.nickname = m.group(1)
        except Exception:
            pass

    logger.info(
        f"[{session.session_id}] 登录完成！"
        f"用户={session.nickname or '未知'}, "
        f"Cookie={len(session.cookie_dict)} 项"
    )

    # 持久化 Cookie
    os.makedirs(SESSION_DIR, exist_ok=True)
    try:
        with open(os.path.join(SESSION_DIR, f"{session.session_id}_cookies.json"), "w") as f:
            json.dump({
                "session_id": session.session_id,
                "nickname": session.nickname,
                "cookies": session.cookie_dict,
                "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            }, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"[{session.session_id}] Cookie 持久化失败: {e}")


# ── API 模型 ───────────────────────────────────────

class LoginStartResponse(BaseModel):
    session_id: str
    qrcode_base64: str
    state: str
    nickname: str = ""
    message: str = "请使用抖音 APP 扫描二维码登录"


class LoginStatusResponse(BaseModel):
    session_id: str
    state: str
    message: str = ""
    cookies: Optional[Dict[str, str]] = None
    nickname: str = ""


class CheckAuthResponse(BaseModel):
    authenticated: bool
    has_cookies: bool = False
    session_id: str = ""


# ── API 端点 ───────────────────────────────────────

@app.post("/api/douyin/login/start", response_model=LoginStartResponse)
async def start_login():
    """开始一个新的 QR 登录会话。

    返回 session_id 和 base64 编码的二维码图像。
    客户端应展示二维码并开始轮询 /api/douyin/login/status。
    """
    session = DouyinSession()
    sessions[session.session_id] = session

    try:
        await launch_douyin_browser(session)
        return LoginStartResponse(
            session_id=session.session_id,
            qrcode_base64=session.qrcode_base64,
            state=session.state.value,
        )
    except Exception as e:
        session.state = LoginState.FAILED
        session.error = str(e)
        logger.error(f"[{session.session_id}] 启动失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/douyin/login/status", response_model=LoginStatusResponse)
async def check_login_status(session_id: str):
    """轮询此端点以检查扫码是否成功。

    当 state 为 "success" 时，cookies 字段将包含完整的登录态 Cookie。
    """
    session = sessions.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="会话不存在")

    return LoginStatusResponse(
        session_id=session.session_id,
        state=session.state.value,
        message=session.error or "",
        cookies=session.cookie_dict if session.state == LoginState.SUCCESS else None,
        nickname=session.nickname,
    )


@app.post("/api/douyin/check-auth", response_model=CheckAuthResponse)
async def check_auth(session_id: str):
    """检查指定会话是否仍然有效。"""
    session = sessions.get(session_id)
    if not session:
        return CheckAuthResponse(
            authenticated=False,
            has_cookies=False,
            session_id=session_id,
        )

    return CheckAuthResponse(
        authenticated=session.state == LoginState.SUCCESS,
        has_cookies=bool(session.cookie_dict),
        session_id=session.session_id,
    )


@app.post("/api/douyin/login/refresh")
async def refresh_login(session_id: str):
    """刷新二维码（当前二维码过期或用户主动要求刷新时使用）。

    清理旧会话资源并创建一个全新的登录会话。
    """
    session = sessions.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="会话不存在")

    # 清理旧的浏览器资源
    try:
        if session.browser_context:
            await session.browser_context.close()
        if session.playwright:
            await session.playwright.stop()
    except Exception:
        pass

    # 删除旧会话
    del sessions[session_id]

    # 启动新会话
    return await start_login()


@app.get("/qr/{session_id}", response_class=HTMLResponse)
async def show_qr_page(session_id: str):
    """在浏览器中显示二维码的 HTML 页面。"""
    session = sessions.get(session_id)
    if not session or not session.qrcode_base64:
        return HTMLResponse("<h2>会话不存在或二维码尚未就绪</h2>", status_code=404)

    return HTMLResponse(f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>抖音扫码登录</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            display: flex; justify-content: center; align-items: center;
            min-height: 100vh; background: #f5f5f5;
            font-family: -apple-system, "Helvetica Neue", sans-serif;
        }}
        .card {{
            background: #fff; border-radius: 12px; padding: 32px;
            box-shadow: 0 2px 12px rgba(0,0,0,0.08); text-align: center;
            max-width: 380px; width: 90%;
        }}
        h2 {{ font-size: 18px; color: #333; margin-bottom: 8px; }}
        .sub {{ font-size: 13px; color: #999; margin-bottom: 20px; }}
        .qr-wrapper {{
            background: #fff; border-radius: 8px; padding: 12px;
            display: inline-block; border: 1px solid #eee;
        }}
        .qr-wrapper img {{ display: block; width: 260px; height: auto; }}
        .status {{ margin-top: 16px; font-size: 14px; color: #666; }}
        .session-id {{ margin-top: 12px; font-size: 11px; color: #bbb; }}
        .refresh-btn {{
            margin-top: 16px; padding: 8px 20px; border: none;
            background: #333; color: #fff; border-radius: 6px;
            cursor: pointer; font-size: 13px;
        }}
        .refresh-btn:hover {{ background: #555; }}
    </style>
</head>
<body>
    <div class="card">
        <h2>抖音扫码登录</h2>
        <p class="sub">打开抖音 APP 扫描下方二维码</p>
        <div class="qr-wrapper">
            <img src="data:image/png;base64,{session.qrcode_base64}" alt="QR Code">
        </div>
        <p class="status" id="status">等待扫码...</p>
        <p class="session-id">Session: {session_id}</p>
    </div>
    <script>
        (function() {{
            var sid = "{session_id}";
            var statusEl = document.getElementById('status');
            function poll() {{
                fetch('/api/douyin/login/status', {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/x-www-form-urlencoded' }},
                    body: 'session_id=' + sid,
                }})
                .then(function(r) {{ return r.json(); }})
                .then(function(d) {{
                    if (d.state === 'success') {{
                        statusEl.textContent = '✅ 登录成功！可以关闭此页面';
                        statusEl.style.color = '#10b981';
                    }} else if (d.state === 'failed') {{
                        statusEl.textContent = '❌ ' + (d.message || '登录失败');
                        statusEl.style.color = '#ef4444';
                    }} else if (d.state === 'scanning') {{
                        statusEl.textContent = '📱 已扫码，请在手机上确认...';
                        statusEl.style.color = '#f59e0b';
                    }} else {{
                        statusEl.textContent = '⏳ 等待扫码...';
                    }}
                }})
                .catch(function() {{}});
            }}
            setInterval(poll, 2000);
            poll();
        }})();
    </script>
</body>
</html>""")


@app.on_event("startup")
async def startup():
    """启动时创建必要目录。"""
    os.makedirs(USER_DATA_BASE, exist_ok=True)
    os.makedirs(SESSION_DIR, exist_ok=True)
    logger.info(f"用户数据目录: {USER_DATA_BASE}")
    logger.info(f"会话保存目录: {SESSION_DIR}")


@app.on_event("shutdown")
async def shutdown():
    """关闭时清理所有浏览器实例。"""
    logger.info("正在关闭所有浏览器实例...")
    for sid, session in list(sessions.items()):
        try:
            if session.browser_context:
                await session.browser_context.close()
            if session.playwright:
                await session.playwright.stop()
        except Exception:
            pass
    sessions.clear()
    logger.info("已清理所有会话资源")


# ── 主入口 ──────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="抖音 QR 登录服务器")
    parser.add_argument("--host", default="0.0.0.0", help="监听地址")
    parser.add_argument("--port", default=8000, type=int, help="监听端口")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING"])
    args = parser.parse_args()

    logging.getLogger("douyin-login-server").setLevel(getattr(logging, args.log_level))

    print(f"""
╔══════════════════════════════════════════════════╗
║        抖音 QR 登录服务器                    ║
║                                                ║
║  {f"http://{args.host}:{args.port}":<44}║
║  API: /api/douyin/login/start                 ║
║       /api/douyin/login/status                ║
║       /api/douyin/check-auth                  ║
║       /api/douyin/login/refresh               ║
║  QR:  /qr/{{session_id}}                       ║
║                                                ║
║  请确保已安装 Playwright:                       ║
║    playwright install chromium                 ║
╚══════════════════════════════════════════════════╝
""")

    uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level.lower())

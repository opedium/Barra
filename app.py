#!/usr/bin/python
# coding:utf-8
"""Flask Web 服务器 — 弹幕后台管理面板

用法:
  python app.py [--port=8080] [--host=0.0.0.0]
"""

import argparse
import atexit
import base64
import collections
import csv
import json
import functools
import io
import os
import random
import re
import secrets
import sys
import threading
import time
from datetime import datetime

import yaml
from flask import Flask, jsonify, redirect, render_template, request, session, url_for

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from base.parser import (
    query_leaderboard, query_user, query_user_detail, query_user_timeline,
    query_chat, query_anonymous, query_million,
    query_sessions, query_session_detail, query_search, _get_conn, DB_PATH,
    end_session as db_end_session, delete_session as db_delete_session,
    set_sse_callback, init_db,
)
from service.fetcher import DouyinBarrage
from service.network import fetch_user_info_by_sec_uid, fetch_user_info_by_user_id

app = Flask(__name__)

# ── Web 配置加载 ──────────────────────────────────

_web_config = {
    'host': '0.0.0.0',
    'port': 8080,
    'password': '',
    'auto_refresh': 30,
}

def _load_web_config():
    """从 config.yaml 加载 web 配置节，缺失时使用默认值。"""
    global _web_config
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.yaml')
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f) or {}
        web_cfg = cfg.get('web', {})
        if isinstance(web_cfg, dict):
            _web_config['host'] = web_cfg.get('host', _web_config['host'])
            _web_config['port'] = web_cfg.get('port', _web_config['port'])
            _web_config['password'] = web_cfg.get('password', _web_config['password'])
            _web_config['auto_refresh'] = web_cfg.get('auto_refresh', _web_config['auto_refresh'])
            _web_config['rate_limit'] = {'enabled': True, 'max_per_minute': 30}
            rl_cfg = web_cfg.get('rate_limit', {})
            if isinstance(rl_cfg, dict):
                _web_config['rate_limit']['enabled'] = rl_cfg.get('enabled', True)
                _web_config['rate_limit']['max_per_minute'] = rl_cfg.get('max_per_minute', 30)
    except Exception:
        pass  # 配置文件缺失或损坏时使用默认值

_load_web_config()

# ── API 请求限流 ──────────────────────────────────

class RateLimiter:
    """Sliding-window rate limiter for Douyin API calls."""
    def __init__(self):
        self._window = collections.deque()
        self._lock = threading.Lock()

    def acquire(self):
        cfg = _web_config.get('rate_limit', {})
        if not cfg.get('enabled', True):
            return True
        max_per_minute = cfg.get('max_per_minute', 30)
        now = time.time()
        with self._lock:
            while self._window and now - self._window[0] > 60:
                self._window.popleft()
            if len(self._window) >= max_per_minute:
                return False
            self._window.append(now)
            return True

_rate_limiter = RateLimiter()

# ── SSE Event Buffer ──────────────────────────────

_event_buffer = collections.deque(maxlen=200)
_event_lock = threading.Lock()

def push_event(event_type, data):
    with _event_lock:
        _event_buffer.append({
            'type': event_type,
            'data': data,
            'time': time.strftime('%H:%M:%S'),
        })

# ── Register SSE callback after push_event is defined ──
set_sse_callback(push_event)

# ── Session 密钥 ──────────────────────────────────

_secret_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', '.flask_secret')
try:
    with open(_secret_path, 'rb') as _sf:
        _key = _sf.read()
        if _key:
            app.secret_key = _key
        else:
            raise ValueError('empty secret')
except Exception:
    app.secret_key = secrets.token_hex(32)
    os.makedirs(os.path.dirname(_secret_path), exist_ok=True)
    with open(_secret_path, 'w') as _sf:
        _sf.write(app.secret_key)

# ── 认证装饰器 ────────────────────────────────────

def require_auth(f):
    """装饰器：如果配置了密码则要求登录。"""
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if not _web_config['password']:
            return f(*args, **kwargs)
        if session.get('authenticated'):
            return f(*args, **kwargs)
        # API 路由返回 401 JSON，页面路由重定向到登录页
        if request.path.startswith('/api/'):
            return jsonify({'error': 'unauthorized', 'login_required': True}), 401
        next_url = request.full_path.rstrip('?') or '/'
        return redirect(url_for('login', next=next_url))
    return wrapper


# ── API 路由统一认证检查 ──────────────────────────

@app.before_request
def _check_api_auth():
    if not _web_config['password']:
        return None
    if session.get('authenticated'):
        return None
    if request.path in ('/login', '/logout') or request.path.startswith('/static/'):
        return None
    if request.path.startswith('/api/'):
        return jsonify({'error': 'unauthorized', 'login_required': True}), 401
    return None


# ═══════════════════════════════════════════════════════════════
#  Streamer Manager — manages DouyinBarrage instances in-process
# ═══════════════════════════════════════════════════════════════

class StreamerManager:
    """Manages DouyinBarrage instances for multiple streamers.

    Each streamer runs in its own daemon thread. Toggle state is persisted
    in the ``streamer_config`` SQLite table so it survives restarts.
    """

    def __init__(self):
        self._instances = {}   # live_id → DouyinBarrage
        self._lock = threading.Lock()

    # ── helpers ──────────────────────────────────────────────

    def _ensure_table(self):
        conn = _get_conn()
        conn.execute('''CREATE TABLE IF NOT EXISTS streamer_config (
            live_id TEXT PRIMARY KEY,
            anchor_name TEXT DEFAULT '',
            enabled INTEGER DEFAULT 0,
            added_at DATETIME DEFAULT (datetime('now', '+8 hours'))
        )''')
        conn.commit()

    # ── seed from rooms.txt ──────────────────────────────────

    def seed_from_rooms_txt(self):
        """Read ``rooms.txt`` and insert any new streamers.

        Lines starting with ``#`` are treated as removed/deleted and are
        skipped entirely — they will NOT be inserted into the database.
        """
        self._ensure_table()
        rooms_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'rooms.txt')
        if not os.path.exists(rooms_file):
            return
        conn = _get_conn()
        existing = set(r[0] for r in conn.execute(
            'SELECT live_id FROM streamer_config').fetchall())
        try:
            with open(rooms_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    if line.startswith('#'):
                        continue  # commented lines are deleted — skip entirely
                    parts = line.split(',', 1)
                    live_id = parts[0].strip()
                    name = parts[1].strip() if len(parts) > 1 else ''
                    if live_id and live_id not in existing:
                        conn.execute(
                            'INSERT OR IGNORE INTO streamer_config '
                            '(live_id, anchor_name, enabled) VALUES (?, ?, 0)',
                            (live_id, name))
            conn.commit()
        except Exception as e:
            print(f'[StreamerManager] seed failed: {e}')

    # ── start / stop ─────────────────────────────────────────

    def start_streamer(self, live_id):
        """Spawn a ``DouyinBarrage`` thread for *live_id*.

        Returns ``(success: bool, message: str)``.
        """
        with self._lock:
            if live_id in self._instances:
                return False, 'Already running'
            # Reserve the slot to prevent concurrent starts
            self._instances[live_id] = None

        conn = _get_conn()
        row = conn.execute(
            'SELECT anchor_name FROM streamer_config WHERE live_id = ?',
            (live_id,)).fetchone()
        anchor_name = row['anchor_name'] if row else ''

        def _on_room_info(rid, name):
            if name:
                c = _get_conn()
                c.execute('UPDATE streamer_config SET anchor_name = ? WHERE live_id = ?',
                          (name, rid))
                c.commit()

        try:
            instance = DouyinBarrage(
                live_id, multi_room=True, on_room_info=_on_room_info)
            instance.config['live_stop'] = False   # wait for re-broadcast

            thread = threading.Thread(
                target=instance.start,
                name=f'room-{live_id}',
                daemon=True,
            )

            with self._lock:
                self._instances[live_id] = instance

            thread.start()

            conn = _get_conn()
            conn.execute('UPDATE streamer_config SET enabled = 1 WHERE live_id = ?',
                         (live_id,))
            conn.commit()

            return True, f'Started {anchor_name or live_id}'
        except Exception as e:
            with self._lock:
                self._instances.pop(live_id, None)
            return False, str(e)

    def stop_streamer(self, live_id):
        """Stop and remove a running instance."""
        with self._lock:
            instance = self._instances.pop(live_id, None)

        if instance is not None:
            try:
                instance.stop()
            except Exception:
                pass

        conn = _get_conn()
        conn.execute('UPDATE streamer_config SET enabled = 0 WHERE live_id = ?',
                     (live_id,))
        conn.commit()

        return True, 'Stopped'

    # ── status query ─────────────────────────────────────────

    def get_all_status(self):
        """Return a list of dicts for every configured streamer."""
        self._ensure_table()
        conn = _get_conn()
        rows = conn.execute(
            'SELECT live_id, anchor_name, enabled FROM streamer_config '
            'ORDER BY added_at').fetchall()

        result = []
        for row in rows:
            live_id = row['live_id']
            with self._lock:
                instance = self._instances.get(live_id)

            if instance is not None and instance._started:
                info = instance.get_status()
                entry = {
                    'live_id': live_id,
                    'anchor_name': info.get('anchor_name') or row['anchor_name'] or live_id,
                    'enabled': bool(row['enabled']),
                    'running': info['status'] == 'collecting',
                    'status': info['status'],
                    'message_count': info.get('message_count', 0),
                    'live_status': info.get('live_status', 0),
                    'room_id': info.get('room_id', ''),
                    'uptime_seconds': info.get('uptime_seconds', 0),
                    'last_error': info.get('last_error', ''),
                }
            else:
                entry = {
                    'live_id': live_id,
                    'anchor_name': row['anchor_name'] or live_id,
                    'enabled': bool(row['enabled']),
                    'running': False,
                    'status': 'stopped',
                    'message_count': 0,
                    'live_status': 0,
                    'room_id': '',
                    'uptime_seconds': 0,
                    'last_error': '',
                }

            result.append(entry)
        return result

    # ── config CRUD ──────────────────────────────────────────

    def add_streamer(self, live_id, anchor_name=''):
        self._ensure_table()
        conn = _get_conn()
        try:
            conn.execute(
                'INSERT INTO streamer_config (live_id, anchor_name) VALUES (?, ?)',
                (live_id, anchor_name))
            conn.commit()
            return True, 'Added'
        except Exception as e:
            return False, str(e)

    def remove_streamer(self, live_id):
        self.stop_streamer(live_id)
        conn = _get_conn()
        conn.execute('DELETE FROM streamer_config WHERE live_id = ?', (live_id,))
        conn.commit()

        # Also comment out the line in rooms.txt so it doesn't come back on restart
        rooms_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'rooms.txt')
        if os.path.exists(rooms_file):
            try:
                with open(rooms_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                with open(rooms_file, 'w', encoding='utf-8') as f:
                    for line in lines:
                        stripped = line.lstrip()
                        if stripped.startswith('#'):
                            inner = stripped[1:].strip()
                        else:
                            inner = stripped
                        parts = inner.split(',', 1)
                        if parts and parts[0].strip() == live_id:
                            if not stripped.startswith('#'):
                                f.write('#' + line)
                            else:
                                f.write(line)
                        else:
                            f.write(line)
            except Exception as e:
                print(f'[StreamerManager] failed to update rooms.txt: {e}')

        return True, 'Removed'

    def shutdown_all(self):
        print('[StreamerManager] shutting down all …')
        with self._lock:
            ids = list(self._instances.keys())
        for lid in ids:
            self.stop_streamer(lid)


# ═══════════════════════════════════════════════════════════════
#  Cookie Manager — QR login + manual cookie management
# ═══════════════════════════════════════════════════════════════

class CookieLoginManager:
    """Manages Playwright-based QR login sessions and manual cookie input.

    Only one QR login session is allowed at a time.  Each session
    spawns a background thread that holds a headless Chromium
    instance until login succeeds or the 180 s timeout expires.
    """

    # QR 元素选择器（按优先级排列，抖音可能更新 DOM）
    _QR_SELECTORS = [
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
    _LOGIN_BUTTON_SELECTORS = [
        "xpath=//p[text()='登录']",
        "text=登录",
        "text=扫码登录",
        ".login-btn",
        '[data-e2e="login"]',
        '#login-btn',
        "span:has-text('登录')",
    ]

    # 反检测脚本
    _STEALTH_JS = """
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    window.chrome = { runtime: {} };
    window.navigator.permissions.query = (params) => (
        params.name === 'notifications' ? Promise.resolve({ state: 'denied' })
        : window.navigator.permissions.constructor.prototype.query.call(window.navigator.permissions, params)
    );
    Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
    Object.defineProperty(navigator, 'languages', { get: () => ['zh-CN', 'zh'] });
    """

    # 反检测浏览器启动参数
    _ANTI_DETECTION_ARGS = [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-gpu',
        '--disable-blink-features=AutomationControlled',
        '--disable-web-security',
        '--disable-features=IsolateOrigins,site-per-process',
        '--window-size=1280,800',
    ]

    def __init__(self):
        self._session = None          # single active session dict or None
        self._lock = threading.Lock()

    # ── QR image processing (from MediaCrawler guide §3.1) ──

    @staticmethod
    def _process_qrcode_image(base64_qr: str) -> str:
        """处理二维码图像：添加白色边框和黑色轮廓线，提高扫码成功率。

        Args:
            base64_qr: 原始二维码 base64 字符串（可含 data:image 前缀）。

        Returns:
            处理后的 base64 PNG 字符串（不含 data URI 前缀）。
        """
        from io import BytesIO
        from PIL import Image, ImageDraw

        if "," in base64_qr:
            base64_qr = base64_qr.split(",")[1]

        raw = base64.b64decode(base64_qr)
        image = Image.open(BytesIO(raw))
        width, height = image.size

        # 添加 10px 白色边框
        new_image = Image.new('RGB', (width + 20, height + 20), (255, 255, 255))
        new_image.paste(image, (10, 10))

        # 添加 1px 黑色轮廓线
        draw = ImageDraw.Draw(new_image)
        draw.rectangle((0, 0, width + 19, height + 19), outline=(0, 0, 0), width=1)

        buffered = BytesIO()
        new_image.save(buffered, format="PNG")
        return base64.b64encode(buffered.getvalue()).decode()

    @staticmethod
    def _extract_qr_from_page(page) -> str:
        """从页面中精确定位二维码元素并提取图像。

        依次尝试各种选择器，优先提取二维码元素本身而非整页截图。
        Returns base64 string (不含 data URI 前缀)，失败返回空字符串。
        """
        for selector in CookieLoginManager._QR_SELECTORS:
            try:
                el = page.locator(selector).first
                if el.is_visible(timeout=1000):
                    src = el.get_attribute('src')
                    if not src:
                        continue
                    if src.startswith('http'):
                        import requests as _req
                        resp = _req.get(src, timeout=10)
                        if resp.status_code == 200:
                            return base64.b64encode(resp.content).decode()
                    else:
                        if ',' in src:
                            return src.split(',')[1]
                        return src
            except Exception:
                continue
        return ''

    # ── QR login ──────────────────────────────────────────

    def start_qr_login(self):
        """Launch a Playwright QR-login session.

        Returns ``(token: str, qr_base64: str, timeout_s: int)``
        or raises ``RuntimeError`` when one is already running.
        """
        with self._lock:
            # Force-clear any previous session to avoid 409 staleness
            if self._session is not None:
                old = self._session
                # If there's a running thread and it's still active, cancel it
                if old.get('thread') and old.get('status') not in ('success', 'failed', 'timeout', 'cancelled'):
                    old['cancelled'] = True
                self._session = None
            token = os.urandom(12).hex()
            self._session = {
                'token': token,
                'status': 'starting',       # starting | waiting | success | timeout | failed | cancelled
                'message': '正在启动浏览器…',
                'qr_base64': '',
                'cookie_info': None,
                'started_at': time.time(),
                'thread': None,
            }

        thread = threading.Thread(
            target=self._do_qr_login, args=(token,),
            daemon=True, name=f'qr-login-{token[:6]}',
        )
        with self._lock:
            if self._session and self._session['token'] == token:
                self._session['thread'] = thread
        thread.start()

        # Wait for the QR to be captured
        deadline = time.time() + 90
        while time.time() < deadline:
            with self._lock:
                s = self._session
                if s is None or s['token'] != token:
                    raise RuntimeError('会话已被覆盖')
                if s['status'] in ('waiting', 'scanning', 'success', 'timeout', 'failed'):
                    print(f"[QR:{token[:6]}] wait loop sees status={s['status']}")
                    break
            time.sleep(0.3)
        else:
            print(f"[QR:{token[:6]}] wait loop TIMEOUT after 90s, session={self._session}")
            self._session = None
            raise RuntimeError('浏览器启动超时，请重试')

        with self._lock:
            s = self._session
            if s is None or s['token'] != token:
                raise RuntimeError('会话已被覆盖')
            if s['status'] == 'failed':
                msg = s['message']
                self._session = None
                raise RuntimeError(msg)
            return (token, s['qr_base64'], 180)

    def _do_qr_login(self, token):
        """Background thread: open Douyin login page, capture QR, wait for login."""
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            self._fail_session(token, 'Playwright 未安装，请使用手动输入方式')
            return

        try:
            from base.utils import USER_AGENTS as _UAS
            ua = random.choice(_UAS)
        except Exception:
            ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'

        try:
            with sync_playwright() as p:
                import time as _time
                _t0 = _time.time()
                def _elapsed():
                    return f"{_time.time()-_t0:.1f}s"

                # Use persistent context — creates a real Chrome profile
                # This is more resistant to headless detection (§2.2, §2.7)
                import tempfile
                _profile_dir = os.path.join(tempfile.gettempdir(), f"dy_qr_{token[:6]}")
                context = p.chromium.launch_persistent_context(
                    user_data_dir=_profile_dir,
                    headless=False,  # Visible browser → bypasses Douyin headless detection
                    args=self._ANTI_DETECTION_ARGS,
                    user_agent=ua,
                    viewport={'width': 1280, 'height': 800},
                    locale='zh-CN',
                    timezone_id='Asia/Shanghai',
                )
                page = context.pages[0] if context.pages else context.new_page()
                print(f"[QR:{token[:6]}] browser ready ({_elapsed()})")

                # Inject stealth anti-detection script
                page.add_init_script(self._STEALTH_JS)

                # ── 1. Navigate to Douyin and open login page ──
                try:
                    page.goto('https://www.douyin.com/', wait_until='domcontentloaded',
                              timeout=30000)
                except Exception as e:
                    print(f"[QR:{token[:6]}] goto timeout: {e}")
                print(f"[QR:{token[:6]}] page loaded ({_elapsed()})")

                page.wait_for_timeout(2000)

                # ── 2. Click login button to open login modal ──
                for sel in self._LOGIN_BUTTON_SELECTORS:
                    try:
                        el = page.locator(sel).first
                        if el.is_visible(timeout=500):
                            el.click()
                            print(f"[QR:{token[:6]}] clicked login: {sel} ({_elapsed()})")
                            break
                    except Exception:
                        continue

                # Wait for login modal to render
                page.wait_for_timeout(3000)

                # Try clicking "扫码登录" tab if visible
                for sel in ['text=扫码登录', '.qrcode-login-tab', '[data-login-type="qrcode"]',
                            '.scan-login', 'span:has-text("扫码")']:
                    try:
                        el = page.locator(sel).first
                        if el.is_visible(timeout=300):
                            el.click()
                            print(f"[QR:{token[:6]}] clicked QR tab: {sel} ({_elapsed()})")
                            page.wait_for_timeout(1500)
                            break
                    except Exception:
                        continue

                # ── 3. Extract QR code ──
                def _capture_qr():
                    """Try to capture QR from the page. Returns base64 or ''."""
                    # First try: element extraction
                    for selector in self._QR_SELECTORS:
                        try:
                            el = page.locator(selector).first
                            if el.is_visible(timeout=300):
                                src = el.get_attribute('src')
                                if src:
                                    if src.startswith('http'):
                                        import requests as _req
                                        resp = _req.get(src, timeout=5)
                                        if resp.status_code == 200:
                                            return base64.b64encode(resp.content).decode()
                                    else:
                                        return src.split(',')[1] if ',' in src else src
                        except Exception:
                            continue
                    # Fallback: viewport screenshot
                    try:
                        ss = page.screenshot(type='png', full_page=False)
                        return base64.b64encode(ss).decode()
                    except Exception:
                        return ''

                qr_base64 = _capture_qr()
                if not qr_base64:
                    print(f"[QR:{token[:6]}] QR capture failed!")
                    self._fail_session(token, '未能获取二维码，抖音页面可能已更新')
                    context.close()
                    return

                qr_processed = self._process_qrcode_image(qr_base64)
                print(f"[QR:{token[:6]}] QR captured ({_elapsed()})")

                # ── 4. Transition to waiting state ──
                with self._lock:
                    s = self._session
                    if s is None or s['token'] != token:
                        context.close()
                        return
                    s['qr_base64'] = qr_processed
                    s['status'] = 'waiting'
                    s['message'] = '请使用抖音 APP 扫描二维码'
                print(f"[QR:{token[:6]}] status=waiting set ({_elapsed()})")

                # ── 6. Wait for login (180s timeout) ──
                deadline = time.time() + 180
                _poll_start = time.time()
                last_status = 'waiting'
                while time.time() < deadline:
                    # ── Check for 2FA challenge page ──
                    _detected_2fa = False
                    try:
                        _url_2fa = page.url
                    except Exception:
                        _url_2fa = ''
                    # Log URL every 10s for debugging
                    _2fa_elapsed = time.time() - _poll_start
                    if int(_2fa_elapsed) % 10 == 0 and _2fa_elapsed > 0 and _url_2fa:
                        print(f"[QR:{token[:6]}] URL check ({_2fa_elapsed:.0f}s): {_url_2fa[:100]}")
                    # Only check for 2FA after 15s (gives time for QR scan + callback)
                    if _2fa_elapsed > 15:
                        if not _detected_2fa and _url_2fa:
                            if any(kw in _url_2fa.lower() for kw in
                                    ['sms', 'verify', 'security', '2fa', 'auth',
                                     '两步验证', '安全验证', '身份验证', '手机验证',
                                     'passport', 'code', 'token', 'phone', '手机', '验证码']):
                                _detected_2fa = True
                                print(f"[QR:{token[:6]}] 2FA via URL: {_url_2fa[:60]}")
                        if not _detected_2fa:
                            try:
                                _title = page.title()
                                if any(kw in _title for kw in ['身份验证', '短信验证', '安全验证']):
                                    _detected_2fa = True
                                    print(f"[QR:{token[:6]}] 2FA via title: {_title[:40]}")
                            except Exception:
                                pass
                        if not _detected_2fa:
                            try:
                                # 2FA method selection page or code input page
                                for _2fa_text in ['接收短信验证码', '身份验证', '请输入验证码', '短信已发送']:
                                    _el = page.locator(f'text={_2fa_text}').first
                                    if _el.is_visible(timeout=200):
                                        _detected_2fa = True
                                        print(f"[QR:{token[:6]}] 2FA via text: {_2fa_text}")
                                        break
                            except Exception:
                                pass

                    # Also check for manual 2FA trigger from user button
                    if not _detected_2fa:
                        with self._lock:
                            if self._session and self._session.get('requires_2fa'):
                                _detected_2fa = True
                                print(f"[QR:{token[:6]}] 2FA via manual trigger")

                    if _detected_2fa:
                        print(f"[QR:{token[:6]}] 2FA challenge detected!")
                        with self._lock:
                            if self._session and self._session['token'] == token:
                                self._session['requires_2fa'] = True
                                self._session['status'] = 'needs_2fa'
                                self._session['message'] = '需要两步验证，请在手机上查看验证码并在下方输入'
                        # Auto-click "接收短信验证码" to proceed to SMS code input
                        try:
                            for _sel in ['text=接收短信验证码', 'text=短信验证', 'text=手机验证',
                                         'button:has-text("短信")', '.sms-verify-btn', 'text=验证码']:
                                try:
                                    _el = page.locator(_sel).first
                                    if _el.is_visible(timeout=300):
                                        _el.click(force=True)
                                        print(f"[QR:{token[:6]}] clicked SMS option: {_sel}")
                                        page.wait_for_timeout(2000)
                                        break
                                except Exception:
                                    continue
                        except Exception as _2e:
                            print(f"[QR:{token[:6]}] SMS click error: {_2e}")
                        # Handle 2FA code submission
                        _code = ''
                        with self._lock:
                            if self._session and self._session['token'] == token:
                                _code = self._session.get('_2fa_code', '') or ''
                        if _code:
                            print(f"[QR:{token[:6]}] submitting 2FA code: {_code}")
                            try:
                                _filled = False
                                # Method 1: Find input and fill
                                for _sel in ['input[autocomplete="one-time-code"]', 'input[placeholder*="验证"]',
                                             'input[type="tel"]', 'input[type="text"]',
                                             'input[data-e2e*="code"]', 'input[data-e2e*="sms"]']:
                                    try:
                                        _inp = page.locator(_sel).first
                                        if _inp.is_visible(timeout=300):
                                            _inp.click(force=True)
                                            page.wait_for_timeout(200)
                                            # Type each character separately for digit-box inputs
                                            for _ch in _code:
                                                page.keyboard.type(_ch, delay=100)
                                            _filled = True
                                            print(f"[QR:{token[:6]}] filled via: {_sel}")
                                            break
                                    except Exception:
                                        continue
                                # Method 2: Keyboard at focused element
                                if not _filled:
                                    try:
                                        page.keyboard.press('Tab')
                                        page.wait_for_timeout(200)
                                        for _ch in _code:
                                            page.keyboard.type(_ch, delay=100)
                                        _filled = True
                                        print(f"[QR:{token[:6]}] filled via keyboard")
                                    except Exception:
                                        pass
                                # Method 3: JS fallback — fill all visible inputs
                                if not _filled:
                                    try:
                                        page.evaluate(f"""
                                            document.querySelectorAll('input').forEach(el => {{
                                                if (el.offsetParent !== null) {{
                                                    el.value = '{_code}';
                                                    el.dispatchEvent(new Event('input', {{ bubbles: true }}));
                                                    el.dispatchEvent(new Event('change', {{ bubbles: true }}));
                                                }}
                                            }});
                                        """)
                                        _filled = True
                                        print(f"[QR:{token[:6]}] filled via JS fallback")
                                    except Exception:
                                        pass
                                page.wait_for_timeout(500)
                                if _filled:
                                    for _btn in ['button[type="submit"]', 'text=验证', 'text=确认', 'text=下一步', '.verify-btn']:
                                        try:
                                            _b = page.locator(_btn).first
                                            if _b.is_visible(timeout=300):
                                                _b.click(force=True)
                                                print(f"[QR:{token[:6]}] 2FA submitted: {_btn}")
                                                break
                                        except Exception:
                                            continue
                                with self._lock:
                                    if self._session and self._session['token'] == token:
                                        self._session['_2fa_code'] = ''
                            except Exception as _2e:
                                print(f"[QR:{token[:6]}] 2FA submit error: {_2e}")
                            page.wait_for_timeout(3000)

                    # ── Fast cookie check ──
                    try:
                        all_cookies = context.cookies()
                    except Exception:
                        all_cookies = []

                    douyin_cookies = {}
                    for c in all_cookies:
                        domain = c.get('domain', '')
                        if 'douyin.com' in domain or 'snssdk.com' in domain:
                            douyin_cookies[c['name']] = c['value']

                    has_session = bool(
                        douyin_cookies.get('sessionid') or
                        douyin_cookies.get('sessionid_ss') or
                        douyin_cookies.get('LOGIN_STATUS') == '1'
                    )

                    if has_session:
                        print(f"[QR:{token[:6]}] COOKIE FOUND!")
                        self._save_cookies_file(douyin_cookies)
                        expire = self._extract_expiry(douyin_cookies) if douyin_cookies else ''
                        with self._lock:
                            if self._session and self._session['token'] == token:
                                self._session['status'] = 'success'
                                self._session['message'] = '登录成功，Cookie 已保存'
                                self._session['cookie_info'] = {'cookie_count': len(douyin_cookies), 'expire_date': expire}
                        context.close()
                        return

                    # ── Check cancellation & force-check ──
                    _force_triggered = False
                    with self._lock:
                        s = self._session
                        if s is None or s['token'] != token:
                            context.close()
                            return
                        if s.get('cancelled'):
                            s['status'] = 'cancelled'
                            s['message'] = '已取消'
                            context.close()
                            return
                        if s.get('force_check'):
                            s['force_check'] = False
                            _force_triggered = True

                    elapsed_sec = time.time() - _poll_start
                    if _force_triggered:
                        print(f"[QR:{token[:6]}] force-check triggered")
                        try:
                            page.goto('https://www.douyin.com/', timeout=15000)
                            page.wait_for_timeout(3000)
                            all_cookies = context.cookies()
                            _dy = {}
                            for c in all_cookies:
                                d = c.get('domain', '')
                                if 'douyin.com' in d or 'snssdk.com' in d:
                                    _dy[c['name']] = c['value']
                            _has = bool(_dy.get('sessionid') or _dy.get('sessionid_ss') or _dy.get('LOGIN_STATUS') == '1')
                            if _has:
                                self._save_cookies_file(_dy)
                                _exp = self._extract_expiry(_dy) if _dy else ''
                                with self._lock:
                                    if self._session and self._session['token'] == token:
                                        self._session['status'] = 'success'
                                        self._session['force_check_result'] = {'success': True}
                            else:
                                with self._lock:
                                    if self._session and self._session['token'] == token:
                                        self._session['force_check_result'] = {'success': False}
                            print(f"[QR:{token[:6]}] force-check done, ok={_has}")
                        except Exception as _fe:
                            print(f"[QR:{token[:6]}] force-check error: {_fe}")
                            with self._lock:
                                if self._session and self._session['token'] == token:
                                    self._session['force_check_result'] = {'success': False, 'message': str(_fe)[:100]}
                    elif int(elapsed_sec) % 30 == 0 and elapsed_sec > 0:
                        print(f"[QR:{token[:6]}] cookie check ({elapsed_sec:.0f}s): has_session={has_session}, cookies={len(douyin_cookies)}, url=({page.url[:60]})")

                    # Update status (skip if 2FA already detected — UI shows code input)
                    if not _detected_2fa:
                        try:
                            current_url = page.url
                            if 'passport' in current_url or 'login' in current_url:
                                new_status = 'waiting'
                            else:
                                new_status = 'scanning'
                        except Exception:
                            new_status = 'scanning'
                        if new_status != last_status:
                            last_status = new_status
                            with self._lock:
                                if self._session and self._session['token'] == token:
                                    self._session['status'] = new_status

                    time.sleep(2)

                # ── Timeout ──
                with self._lock:
                    if self._session and self._session['token'] == token:
                        self._session['status'] = 'timeout'
                        self._session['message'] = '扫码超时，请重新发起登录'
                        self._session = None
                context.close()

        except Exception as e:
            import traceback
            print(f"[QR:{token[:6]}] BACKGROUND THREAD CRASHED: {e}")
            traceback.print_exc()
            self._fail_session(token, f'登录失败: {e}')

    def _fail_session(self, token, message):
        with self._lock:
            s = self._session
            if s and s['token'] == token:
                s['status'] = 'failed'
                s['message'] = message

    # ── Cookie file I/O ──────────────────────────────────

    def _cookie_file_path(self):
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cookie.txt')

    def _save_cookies_file(self, cookies_dict):
        """Write cookies to cookie.txt in Netscape format (same as Playwright refresh)."""
        path = self._cookie_file_path()
        expiry = int(time.time()) + 365 * 24 * 3600
        lines = ['# Netscape HTTP Cookie File',
                 '# https://curl.haxx.se/rfc/cookie_spec.html',
                 '# This is a generated file! Do not edit.', '']
        for name, value in sorted(cookies_dict.items()):
            lines.append(f'.douyin.com\tTRUE\t/\tTRUE\t{expiry}\t{name}\t{value}')
        tmp = path + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines) + '\n')
        os.replace(tmp, path)

    def _extract_expiry(self, cookies_dict):
        """Parse expire date from sid_guard cookie value."""
        sid_guard = cookies_dict.get('sid_guard', '')
        if not sid_guard:
            return ''
        try:
            import urllib.parse as _up
            decoded = _up.unquote(sid_guard)
            parts = decoded.split('|')
            if len(parts) >= 4:
                date_str = parts[3].replace('+', ' ').strip()
                m = re.search(r'(\d+)-(\w+)-(\d+)', date_str)
                if m:
                    day, mon_str, year = m.group(1), m.group(2), m.group(3)
                    months = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
                              'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
                              'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}
                    mon = months.get(mon_str[:3], '00')
                    return f'{year}-{mon}-{day}'
        except Exception:
            pass
        return ''

    # ── Session management ───────────────────────────────

    def get_status(self, token):
        """Return the current state dict for *token*, or a summary for no token."""
        with self._lock:
            s = self._session
        if s is None or s['token'] != token:
            return {'status': 'not_found', 'message': '会话不存在或已过期'}
        return {
            'status': s['status'],
            'message': s['message'],
            'cookie_info': s.get('cookie_info'),
            'qr_base64': s.get('qr_base64', '') if s['status'] == 'waiting' else '',
        }

    def cancel(self, token):
        with self._lock:
            s = self._session
            if s is None or s['token'] != token:
                return False
            s['cancelled'] = True
            self._session = None
        return True

    def force_check_login(self, token):
        """Request the background thread to do an aggressive re-check.
        Sets a flag — the polling loop picks it up and re-navigates to trigger login."""
        with self._lock:
            s = self._session
            if s is None or s['token'] != token:
                return {'success': False, 'message': '会话不存在或已过期'}
            if s.get('status') == 'success':
                return {'success': True, 'message': '已登录', 'cookie_count': len(s.get('cookie_info', {}))}
            s['force_check'] = True
            s['force_check_result'] = None
        return {'success': False, 'message': '已通知浏览器检查登录态，请稍候…'}

    def get_force_check_result(self, token):
        with self._lock:
            s = self._session
            if s is None or s['token'] != token:
                return {'done': True, 'success': False, 'message': '会话不存在'}
            if s.get('status') == 'success':
                return {'done': True, 'success': True, 'message': '登录成功', 'cookie_count': len(s.get('cookie_info', {}))}
            result = s.get('force_check_result')
            if result:
                s['force_check'] = False
                s['force_check_result'] = None
                return {'done': True, **result}
            return {'done': False, 'success': False, 'message': '检查中…'}

    def submit_2fa_code(self, token, code):
        """Submit 2FA verification code to the background thread."""
        if not code or not code.strip():
            return {'success': False, 'message': '验证码不能为空'}
        code = code.strip()
        with self._lock:
            s = self._session
            if s is None or s['token'] != token:
                return {'success': False, 'message': '会话不存在'}
            if s.get('status') == 'success':
                return {'success': True, 'message': '已登录'}
            s['_2fa_code'] = code
            s['status'] = 'verifying_2fa'
            s['message'] = f'验证码已提交，验证中…'
        return {'success': True, 'message': '验证码已提交，请稍候…'}

    def trigger_2fa_mode(self, token):
        """Manually switch session to 2FA mode. Background thread will detect and act."""
        with self._lock:
            s = self._session
            if s is None or s['token'] != token:
                return {'success': False, 'message': '会话不存在'}
            if s.get('status') == 'success':
                return {'success': True, 'message': '已登录'}
            s['requires_2fa'] = True
            s['status'] = 'needs_2fa'
            s['message'] = '需要两步验证，请在手机上查看验证码并输入'
        print(f"[QR:{token[:6]}] 2FA mode manually triggered by user")
        return {'success': True, 'message': '已切换至两步验证模式'}

    def get_2fa_status(self, token):
        """Check 2FA status."""
        with self._lock:
            s = self._session
            if s is None or s['token'] != token:
                return {'needs_2fa': False, 'status': 'not_found'}
            return {
                'needs_2fa': s.get('requires_2fa', False),
                'status': s.get('status', ''),
                'message': s.get('message', ''),
            }

    # ── Manual cookie save ──────────────────────────────

    def manual_save(self, cookie_string):
        """Parse, validate, and save a manually-pasted cookie string.

        Returns ``(success: bool, message: str, details: dict | None)``.
        """
        if not cookie_string or not cookie_string.strip():
            return False, 'Cookie 内容为空', None

        # Reuse existing cookie parser
        from base.utils import load_cookies, USER_AGENTS
        import tempfile
        tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False,
                                          encoding='utf-8')
        try:
            tmp.write(cookie_string)
            tmp.close()
            cookies = load_cookies(tmp.name)
        finally:
            try:
                os.unlink(tmp.name)
            except Exception:
                pass

        if not cookies:
            return False, '无法解析 Cookie，请检查格式（支持浏览器导出格式、每行 key=value、Netscape 格式）', None

        has_session = bool(cookies.get('sessionid') or cookies.get('sessionid_ss'))
        if not has_session:
            return False, 'Cookie 中未包含 sessionid，登录态无效，请重新从浏览器导出', {
                'cookie_count': len(cookies),
            }

        # Quick validation: make a test request
        try:
            import requests as _r
            test_s = _r.Session()
            test_s.trust_env = False
            for name, value in cookies.items():
                test_s.cookies.set(name, value, domain='.douyin.com')
            test_s.headers.update({
                'User-Agent': random.choice(USER_AGENTS),
            })
            resp = test_s.get('https://www.douyin.com/', timeout=15, allow_redirects=True)
            if 'passport' in resp.url:
                return False, 'Cookie 验证失败：服务端返回未登录状态，cookie 可能已过期', {
                    'cookie_count': len(cookies),
                }
        except Exception as e:
            # Don't block on network error — save anyway, the user can test live
            pass

        # Save to cookie.txt
        self._save_cookies_file(cookies)
        expire = self._extract_expiry(cookies)

        return True, f'Cookie 已保存（{len(cookies)} 项）', {
            'cookie_count': len(cookies),
            'expire_date': expire,
        }

    def _fetch_nickname(self, cookies):
        """Extract logged-in nickname from douyin.com SSR page."""
        try:
            import requests as _r
            from base.utils import USER_AGENTS as _UAS
            s = _r.Session()
            s.trust_env = False
            for name, value in cookies.items():
                s.cookies.set(name, value, domain='.douyin.com')
            s.headers.update({
                'User-Agent': random.choice(_UAS),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'zh-CN,zh;q=0.9',
            })
            resp = s.get('https://live.douyin.com/', timeout=10, allow_redirects=True)
            body = resp.text
            m = re.search(
                r'defaultHeaderUserInfo.*?isLogin.*?(true|false).*?nickname\\?"[,:]\\?"([^"\\]+)',
                body, re.DOTALL
            )
            if m and m.group(1) == 'true':
                return m.group(2)
        except Exception:
            pass
        return ''

    def get_cookie_overview(self):
        """Return a quick summary of the cookie.txt on disk."""
        path = self._cookie_file_path()
        from base.utils import load_cookies
        cookies = load_cookies(path)

        if not cookies:
            return {
                'has_cookie': False,
                'is_login': False,
                'status': 'no_cookie',
                'cookie_count': 0,
                'expire_date': '',
                'days_remaining': -1,
                'nickname': '',
            }

        has_session = bool(cookies.get('sessionid') or cookies.get('sessionid_ss'))
        expire = self._extract_expiry(cookies)
        days = -1
        status = 'active'
        if expire:
            try:
                expire_dt = datetime.strptime(expire, '%Y-%m-%d')
                days = (expire_dt - datetime.now()).days
                if days <= 0:
                    status = 'expired'
                elif days <= 3:
                    status = 'expiring_soon'
                else:
                    status = 'active'
            except Exception:
                pass

        nickname = ''
        if has_session:
            nickname = self._fetch_nickname(cookies)

        return {
            'has_cookie': True,
            'is_login': has_session,
            'status': status if has_session else 'no_cookie',
            'cookie_count': len(cookies),
            'expire_date': expire,
            'days_remaining': days,
            'nickname': nickname,
        }


# ── singleton ──────────────────────────────────────────────────
_manager = StreamerManager()
_cookie_manager = CookieLoginManager()
atexit.register(_manager.shutdown_all)


@app.route('/')
@require_auth
def index():
    conn = _get_conn()
    sid = request.args.get('session_id', None, type=int)
    if sid:
        live = conn.execute("""
            SELECT s.*,
                (SELECT COUNT(*) FROM gift_logs WHERE session_id=s.id) as total_gifts,
                (SELECT COUNT(*) FROM chat_logs WHERE session_id=s.id) as total_chats,
                (SELECT COUNT(*) FROM contributions WHERE session_id=s.id AND qualified_1000=1) as user_count
            FROM sessions s WHERE s.id=?
        """, (sid,)).fetchone()
    else:
        live = conn.execute("""
            SELECT s.*,
                (SELECT COUNT(*) FROM gift_logs WHERE session_id=s.id) as total_gifts,
                (SELECT COUNT(*) FROM chat_logs WHERE session_id=s.id) as total_chats,
                (SELECT COUNT(*) FROM contributions WHERE session_id=s.id AND qualified_1000=1) as user_count
            FROM sessions s WHERE status='live' ORDER BY s.id DESC LIMIT 1
        """).fetchone()
    live_sessions = conn.execute("SELECT id, anchor_name, room_id, start_time FROM sessions WHERE status='live' ORDER BY id DESC").fetchall()
    total_gifts = conn.execute('SELECT COUNT(*) FROM gift_logs').fetchone()[0]
    total_chats = conn.execute('SELECT COUNT(*) FROM chat_logs').fetchone()[0]
    total_sessions = conn.execute('SELECT COUNT(*) FROM sessions').fetchone()[0]
    today = conn.execute("SELECT COUNT(*) FROM gift_logs WHERE date(created_at)=date('now')").fetchone()[0]
    today_chats = conn.execute("SELECT COUNT(*) FROM chat_logs WHERE date(created_at)=date('now')").fetchone()[0]
    today_users = conn.execute("SELECT COUNT(DISTINCT user_id) FROM gift_logs WHERE date(created_at)=date('now')").fetchone()[0]
    recent = conn.execute('SELECT s.*, (SELECT COUNT(*) FROM gift_logs WHERE session_id=s.id) as total_gifts, (SELECT COUNT(*) FROM chat_logs WHERE session_id=s.id) as total_chats FROM sessions s ORDER BY s.id DESC LIMIT 10').fetchall()
    recent_chats = conn.execute('''
        SELECT cl.user_name, cl.user_id, cl.content, cl.created_at as time,
               u.avatar_url, u.sec_uid
        FROM chat_logs cl
        LEFT JOIN users u ON u.user_id = cl.user_id
        ORDER BY cl.id DESC LIMIT 10
    ''').fetchall()
    top_users = []
    if live:
        top = conn.execute('''
            SELECT c.user_id, c.user_name, c.consume,
                   COALESCE(
                       NULLIF(u.fans_club, ''),
                       NULLIF(c.fans_club, ''),
                       (SELECT fans_club FROM chat_logs WHERE user_id=c.user_id AND fans_club!='' ORDER BY id DESC LIMIT 1),
                       ''
                   ) as fans_club,
                   COALESCE(u.grade, (SELECT grade FROM chat_logs WHERE user_id=c.user_id AND grade!='' ORDER BY id DESC LIMIT 1), '') as grade,
                   u.sec_uid, u.avatar_url,
                   (SELECT COUNT(*) FROM gift_logs WHERE session_id=c.session_id AND user_id=c.user_id) as gift_count,
                   (SELECT COUNT(*) FROM chat_logs WHERE session_id=c.session_id AND user_id=c.user_id) as chat_count
            FROM contributions c
            LEFT JOIN users u ON u.user_id = c.user_id
            WHERE c.session_id=? ORDER BY c.consume DESC LIMIT 10
        ''', (live['id'],)).fetchall()
        top_users = [dict(r) for r in top]

    # 查找主播头像（从 users 表中匹配主播昵称 + sec_uid）
    anchor_avatar = ''
    if live:
        anchor_row = conn.execute(
            'SELECT avatar_url FROM users WHERE sec_uid != "" AND user_name = ? LIMIT 1',
            (live['anchor_name'],)
        ).fetchone()
        if anchor_row and anchor_row['avatar_url']:
            anchor_avatar = anchor_row['avatar_url']

    return render_template('index.html',
        session=dict(live) if live else None,
        anchor_avatar=anchor_avatar,
        live_sessions=[dict(r) for r in live_sessions],
        stats={'total_gifts': total_gifts, 'total_chats': total_chats, 'total_sessions': total_sessions, 'today_gifts': today, 'today_chats': today_chats, 'today_users': today_users},
        sessions=[dict(r) for r in recent],
        recent_chats=[dict(r) for r in recent_chats],
        top_users=top_users,
        db_path=DB_PATH,
        auto_refresh=_web_config['auto_refresh'],
        auth_enabled=bool(_web_config['password']))


@app.route('/leaderboard')
@require_auth
def leaderboard():
    return render_template('leaderboard.html', auth_enabled=bool(_web_config['password']))


@app.route('/session/<int:session_id>')
@require_auth
def session_detail(session_id):
    return render_template('session.html', session_id=session_id, auth_enabled=bool(_web_config['password']))


@app.route('/user')
@require_auth
def user_detail():
    return render_template('user.html', uid=request.args.get('uid', ''), auth_enabled=bool(_web_config['password']))


@app.route('/chat')
@require_auth
def chat():
    return render_template('chat.html', auth_enabled=bool(_web_config['password']))


@app.route('/anonymous')
@require_auth
def anonymous():
    return render_template('anonymous.html', auth_enabled=bool(_web_config['password']))


@app.route('/million')
@require_auth
def million():
    return redirect('/leaderboard?period=million')


@app.route('/sessions')
@require_auth
def sessions():
    return render_template('sessions.html', auth_enabled=bool(_web_config['password']))


@app.route('/compare')
@require_auth
def compare():
    conn = _get_conn()
    all_sessions = conn.execute(
        'SELECT id, anchor_name, room_id, start_time, status '
        'FROM sessions ORDER BY id DESC LIMIT 200'
    ).fetchall()
    return render_template('compare.html',
        sessions=[dict(r) for r in all_sessions],
        auth_enabled=bool(_web_config['password']))


@app.route('/api/compare')
def api_compare():
    session_ids_str = request.args.get('session_ids', '')
    if not session_ids_str:
        return jsonify({'error': 'session_ids required'}), 400
    try:
        ids = [int(x.strip()) for x in session_ids_str.split(',') if x.strip()]
    except ValueError:
        return jsonify({'error': 'invalid session_ids'}), 400
    if not ids or len(ids) > 6:
        return jsonify({'error': 'session_ids must be 1-6'}), 400

    conn = _get_conn()
    result = {'sessions': []}
    for sid in ids:
        s = conn.execute('SELECT * FROM sessions WHERE id = ?', (sid,)).fetchone()
        if not s:
            continue
        session_data = dict(s)
        gifts = conn.execute(
            'SELECT COUNT(*) as total_gifts, COALESCE(SUM(diamond_total), 0) as total_diamonds '
            'FROM gift_logs WHERE session_id = ?', (sid,)).fetchone()
        chats = conn.execute(
            'SELECT COUNT(*) as total_chats FROM chat_logs WHERE session_id = ?',
            (sid,)).fetchone()
        users = conn.execute(
            'SELECT COUNT(*) as total_users FROM contributions '
            'WHERE session_id = ? AND qualified_1000 = 1', (sid,)).fetchone()
        top_gift = conn.execute(
            'SELECT gift_name, SUM(diamond_total) as total FROM gift_logs '
            'WHERE session_id = ? GROUP BY gift_name ORDER BY total DESC LIMIT 1',
            (sid,)).fetchone()
        session_data['total_gifts'] = gifts['total_gifts'] if gifts else 0
        session_data['total_diamonds'] = gifts['total_diamonds'] if gifts else 0
        session_data['total_chats'] = chats['total_chats'] if chats else 0
        session_data['total_users'] = users['total_users'] if users else 0
        session_data['top_gift'] = top_gift['gift_name'] if top_gift else ''
        result['sessions'].append(session_data)
    return jsonify(result)


@app.route('/settings')
@require_auth
def settings():
    return render_template('settings.html', auth_enabled=bool(_web_config['password']))


# ── 登录 / 登出 ──

@app.route('/login', methods=['GET', 'POST'])
def login():
    error = ''
    if request.method == 'POST':
        if request.form.get('password', '') == _web_config['password']:
            session['authenticated'] = True
            next_url = request.args.get('next', '/')
            # 防止 open-redirect：只允许相对路径
            if next_url.startswith(('http:', 'https:', '//')):
                next_url = '/'
            return redirect(next_url)
        error = '密码错误'
    return render_template('login.html', error=error,
                          auth_enabled=bool(_web_config['password']))


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


# ── API ──

@app.route('/api/leaderboard')
def api_leaderboard():
    threshold = request.args.get('threshold', 1000, type=int)
    period = request.args.get('period', 'session')
    page = request.args.get('page', 1, type=int)
    size = request.args.get('size', 50, type=int)
    sort_by = request.args.get('sort_by', 'consume')
    year_month = request.args.get('year_month', '')
    min_consume = request.args.get('min_consume', 0, type=int)

    # If session_id passed explicitly, use it; otherwise resolve from live session
    session_id = request.args.get('session_id', None, type=int)
    if period == 'session' and session_id is None:
        conn = _get_conn()
        s = conn.execute("SELECT id FROM sessions WHERE status='live' ORDER BY id DESC LIMIT 1").fetchone()
        if s:
            session_id = s[0]
        else:
            # No live session and no explicit session_id — return empty
            return jsonify({'users': [], 'total': 0, 'page': 1})

    data = query_leaderboard(threshold, period, page, size, session_id, year_month, min_consume)
    if sort_by == 'sessions' and data.get('users'):
        data['users'].sort(key=lambda u: (-u.get('sessions_count', 0), -u.get('consume', 0)))
        for i, u in enumerate(data['users']):
            u['rank'] = i + 1
    return jsonify(data)


@app.route('/api/user')
def api_user():
    uid = request.args.get('uid', '')
    if not uid:
        return jsonify({'error': 'uid required'}), 400
    if request.args.get('detail'):
        data = query_user_detail(uid)
    else:
        data = query_user(uid)
    if not data:
        return jsonify({'error': 'user not found'}), 404

    # 头像/ sec_uid 缺失时尝试从抖音 API 补全（受速率限制保护）
    need_avatar = not data.get('avatar_url')
    need_sec_uid = not data.get('sec_uid')
    if need_avatar or need_sec_uid:
        sec_uid = data.get('sec_uid', '')
        fetched = None
        if _rate_limiter.acquire():
            if sec_uid:
                fetched = fetch_user_info_by_sec_uid(sec_uid)
            if not fetched:
                fetched = fetch_user_info_by_user_id(uid)
        if fetched:
            updates = []
            params = []
            if need_avatar and fetched.get('avatar_url'):
                data['avatar_url'] = fetched['avatar_url']
                updates.append('avatar_url = ?')
                params.append(fetched['avatar_url'])
            if need_sec_uid and fetched.get('sec_uid'):
                data['sec_uid'] = fetched['sec_uid']
                updates.append('sec_uid = ?')
                params.append(fetched['sec_uid'])
            if updates:
                params.append(uid)
                conn = _get_conn()
                conn.execute(f'UPDATE users SET {", ".join(updates)} WHERE user_id = ?', params)
                conn.commit()

    return jsonify(data)


@app.route('/api/user/<user_id>/timeline')
def api_user_timeline(user_id):
    return jsonify(query_user_timeline(user_id,
        request.args.get('type', 'all'),
        request.args.get('keyword', ''),
        request.args.get('page', 1, type=int),
        request.args.get('size', 50, type=int)))


@app.route('/api/user/<user_id>/gifts')
def api_user_gifts(user_id):
    conn = _get_conn()
    session_id = request.args.get('session_id', None, type=int)
    if session_id:
        rows = conn.execute(
            '''SELECT g.gift_name, g.gift_count, g.diamond_total, g.created_at,
                      COALESCE(s.anchor_name, '') as anchor_name
               FROM gift_logs g
               LEFT JOIN sessions s ON s.id = g.session_id
               WHERE g.user_id=? AND g.session_id=?
               ORDER BY g.created_at DESC LIMIT 5000''',
            (user_id, session_id)).fetchall()
    else:
        rows = conn.execute(
            '''SELECT g.gift_name, g.gift_count, g.diamond_total, g.created_at,
                      COALESCE(s.anchor_name, '') as anchor_name
               FROM gift_logs g
               LEFT JOIN sessions s ON s.id = g.session_id
               WHERE g.user_id=?
               ORDER BY g.created_at DESC LIMIT 5000''',
            (user_id,)).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route('/api/user/<user_id>/notes', methods=['POST'])
def api_user_notes(user_id):
    """保存用户备注和标签。"""
    data = request.get_json(force=True, silent=True) or {}
    notes = data.get('notes', '')
    tags = data.get('tags', '')
    conn = _get_conn()
    conn.execute('''
        INSERT INTO users (user_id, user_name, notes, tags)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            notes = excluded.notes,
            tags = excluded.tags
    ''', (user_id, user_id, notes, tags))
    conn.commit()
    return jsonify({'success': True, 'notes': notes, 'tags': tags})


@app.route('/api/user/avatar-lookup')
def api_user_avatar_lookup():
    """通过 sec_uid 或 user_id 查询用户头像和信息。

    查询参数:
        sec_uid: 抖音用户 sec_uid（优先）
        user_id: 抖音用户数字 ID（备选，从本地 DB 查 sec_uid 后会调用抖音 API）

    返回:
        JSON 包含 avatar_url、nickname、sec_uid 等字段。

    流程:
        1. 如果传了 sec_uid → 直接调用抖音 API
        2. 如果传了 user_id → 先从 DB 查 sec_uid，有则调 API；无则返回 DB 中已有数据
        3. 都失败返回 404
    """
    sec_uid = request.args.get('sec_uid', '').strip()
    user_id = request.args.get('user_id', '').strip()

    if not sec_uid and not user_id:
        return jsonify({'error': 'sec_uid or user_id required'}), 400

    # 如果传了 sec_uid，直接调用抖音 API
    if sec_uid:
        if not _rate_limiter.acquire():
            return jsonify({'error': 'rate_limited', 'message': '请求频率过高，请稍后重试'}), 429
        info = fetch_user_info_by_sec_uid(sec_uid)
        if info:
            return jsonify(info)
        return jsonify({'error': 'failed to fetch user info from Douyin API'}), 502

    # 传了 user_id：先从本地 DB 查
    conn = _get_conn()
    user = conn.execute(
        'SELECT user_id, user_name, sec_uid, avatar_url, grade, fans_club FROM users WHERE user_id = ?',
        (user_id,)
    ).fetchone()

    if not user:
        return jsonify({'error': 'user not found in local database'}), 404

    # 本地已有 sec_uid 和 avatar_url → 直接返回
    if user['sec_uid'] and user['avatar_url']:
        return jsonify({
            'user_id': user['user_id'],
            'nickname': user['user_name'],
            'sec_uid': user['sec_uid'],
            'avatar_url': user['avatar_url'],
            'grade': user['grade'] or '',
            'fans_club': user['fans_club'] or '',
            'source': 'local',
        })

    # 有 sec_uid 但无 avatar_url → 调用抖音 API 补全
    if user['sec_uid']:
        if not _rate_limiter.acquire():
            return jsonify({'error': 'rate_limited', 'message': '请求频率过高，请稍后重试'}), 429
        info = fetch_user_info_by_sec_uid(user['sec_uid'])
        if info and info.get('avatar_url'):
            # 回写 avatar_url 到本地 DB
            conn.execute(
                'UPDATE users SET avatar_url = ? WHERE user_id = ?',
                (info['avatar_url'], user_id)
            )
            conn.commit()
            return jsonify({
                'user_id': user['user_id'],
                'nickname': user['user_name'],
                'sec_uid': user['sec_uid'],
                'avatar_url': info['avatar_url'],
                'grade': user['grade'] or '',
                'fans_club': user['fans_club'] or '',
                'source': 'api',
            })
        # API 调用失败，返回本地已有数据（可能无头像）
        return jsonify({
            'user_id': user['user_id'],
            'nickname': user['user_name'],
            'sec_uid': user['sec_uid'],
            'avatar_url': user['avatar_url'] or '',
            'grade': user['grade'] or '',
            'fans_club': user['fans_club'] or '',
            'source': 'local',
        })

    # 无 sec_uid：尝试通过数字 user_id 直接调用抖音 API
    if not _rate_limiter.acquire():
        return jsonify({'error': 'rate_limited', 'message': '请求频率过高，请稍后重试'}), 429
    info = fetch_user_info_by_user_id(user_id)
    if info and info.get('avatar_url'):
        # 回写 avatar_url 到本地 DB
        conn.execute(
            'UPDATE users SET avatar_url = ? WHERE user_id = ?',
            (info['avatar_url'], user_id)
        )
        conn.commit()
        return jsonify({
            'user_id': user_id,
            'nickname': info.get('nickname', user['user_name']),
            'sec_uid': '',
            'display_id': info.get('display_id', ''),
            'avatar_url': info['avatar_url'],
            'grade': user['grade'] or '',
            'fans_club': user['fans_club'] or '',
            'source': 'api_uid',
        })

    # 所有方式都失败：返回本地已有数据
    return jsonify({
        'user_id': user['user_id'],
        'nickname': user['user_name'],
        'sec_uid': '',
        'avatar_url': user['avatar_url'] or '',
        'grade': user['grade'] or '',
        'fans_club': user['fans_club'] or '',
        'source': 'local',
    })


@app.route('/api/chat')
def api_chat():
    return jsonify(query_chat(
        request.args.get('user_id', ''),
        request.args.get('keyword', ''),
        request.args.get('page', 1, type=int),
        request.args.get('size', 50, type=int)))


@app.route('/api/anonymous')
def api_anonymous():
    return jsonify(query_anonymous(
        request.args.get('page', 1, type=int),
        request.args.get('size', 50, type=int),
        request.args.get('search', '')))


@app.route('/api/million')
def api_million():
    return jsonify(query_million(
        request.args.get('year_month', ''),
        request.args.get('page', 1, type=int),
        request.args.get('size', 100, type=int)))


@app.route('/api/million/csv')
def api_million_csv():
    year_month = request.args.get('year_month', '')
    data = query_million(year_month, 1, 999999)
    users = data.get('users', [])
    _add_link_column(users, '{}/user?uid={}', 'user_id')
    return _make_csv_response(users,
        ['rank', 'user_id', 'user_name', 'grade', 'fans_club', 'total_consume', 'days_active',
         'sessions_1000', 'sessions_3000', 'sessions_10000', 'sessions_100000', '链接'],
        f'million_{year_month or "all"}.csv')


@app.route('/api/sessions')
def api_sessions():
    anchor = request.args.get('anchor', '')
    limit = request.args.get('limit', 20, type=int)
    return jsonify(query_sessions(limit=limit, anchor=anchor))

@app.route('/api/sessions/<int:session_id>')
def api_session_detail(session_id):
    data = query_session_detail(session_id)
    if not data:
        return jsonify({'error': 'session not found'}), 404
    return jsonify(data)


@app.route('/api/sessions/<int:session_id>/hourly')
def api_session_hourly(session_id):
    """返回该场次按小时的礼物活跃度分布。"""
    conn = _get_conn()
    rows = conn.execute('''
        SELECT CAST(strftime('%H', created_at) AS INTEGER) as hour,
               COUNT(*) as count
        FROM gift_logs
        WHERE session_id = ?
        GROUP BY hour ORDER BY hour
    ''', (session_id,)).fetchall()
    return jsonify({'hours': [dict(r) for r in rows]})


@app.route('/api/sessions/<int:session_id>/end', methods=['POST'])
def api_session_end(session_id):
    """手动结束一场直播（标记为 ended，设置结束时间）。"""
    try:
        db_end_session(session_id)
        return jsonify({'success': True, 'message': f'场次 #{session_id} 已结束'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/sessions/<int:session_id>', methods=['DELETE'])
def api_session_delete(session_id):
    """删除场次及其所有关联数据（礼物、弹幕、贡献记录）。"""
    try:
        result = db_delete_session(session_id)
        return jsonify({'success': True, 'message': f'场次 #{session_id} 已删除', **result})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/events')
def api_events():
    def event_stream():
        last_idx = len(_event_buffer)
        yield ':ok\n\n'
        while True:
            with _event_lock:
                current_len = len(_event_buffer)
                if current_len > last_idx:
                    for i in range(last_idx, current_len):
                        evt = _event_buffer[i]
                        yield 'event: {}\ndata: {}\n\n'.format(
                            evt['type'],
                            json.dumps(evt['data'], ensure_ascii=False))
                    last_idx = current_len
            time.sleep(1)
    response = app.response_class(event_stream(), mimetype='text/event-stream')
    response.headers['Cache-Control'] = 'no-cache'
    response.headers['X-Accel-Buffering'] = 'no'
    response.headers['Connection'] = 'keep-alive'
    return response


@app.route('/api/search')
def api_search():
    q = request.args.get('q', '')
    if not q:
        return jsonify({'users': [], 'total': 0, 'page': 1})
    return jsonify(query_search(q,
        request.args.get('page', 1, type=int),
        request.args.get('size', 20, type=int)))


@app.route('/api/stats')
def api_stats():
    conn = _get_conn()
    return jsonify({
        'total_users': conn.execute('SELECT COUNT(DISTINCT user_id) FROM contributions').fetchone()[0],
        'total_gifts': conn.execute('SELECT COUNT(*) FROM gift_logs').fetchone()[0],
        'total_chats': conn.execute('SELECT COUNT(*) FROM chat_logs').fetchone()[0],
        'total_sessions': conn.execute('SELECT COUNT(*) FROM sessions').fetchone()[0],
        'active_session': conn.execute("SELECT COUNT(*) FROM sessions WHERE status='live'").fetchone()[0] > 0,
    })


# ── Streamer Management API ──

@app.route('/api/streamers')
def api_streamers():
    """List all configured streamers with monitoring status."""
    return jsonify(_manager.get_all_status())


@app.route('/api/streamers/<live_id>/start', methods=['POST'])
def api_streamer_start(live_id):
    ok, msg = _manager.start_streamer(live_id)
    return jsonify({'success': ok, 'message': msg})


@app.route('/api/streamers/<live_id>/stop', methods=['POST'])
def api_streamer_stop(live_id):
    ok, msg = _manager.stop_streamer(live_id)
    return jsonify({'success': ok, 'message': msg})


@app.route('/api/streamers/<live_id>/toggle', methods=['POST'])
def api_streamer_toggle(live_id):
    with _manager._lock:
        is_running = live_id in _manager._instances
    if is_running:
        ok, msg = _manager.stop_streamer(live_id)
        action = 'stop'
    else:
        ok, msg = _manager.start_streamer(live_id)
        action = 'start'
    return jsonify({'success': ok, 'message': msg, 'action': action})


@app.route('/api/streamers', methods=['POST'])
def api_streamer_add():
    data = request.get_json() or {}
    live_id = data.get('live_id', '').strip()
    if not live_id or not re.match(r'^\d{6,18}$', live_id):
        return jsonify({'success': False, 'message': 'Invalid live_id (6-18 digits)'}), 400
    anchor_name = data.get('anchor_name', '').strip()
    ok, msg = _manager.add_streamer(live_id, anchor_name)
    return jsonify({'success': ok, 'message': msg})


@app.route('/api/streamers/<live_id>', methods=['DELETE'])
def api_streamer_delete(live_id):
    ok, msg = _manager.remove_streamer(live_id)
    return jsonify({'success': ok, 'message': msg})


# ── Cookie Management API ──

@app.route('/api/cookie-status')
def api_cookie_status():
    """Return current cookie.txt overview."""
    return jsonify(_cookie_manager.get_cookie_overview())


@app.route('/api/cookie-login/qr', methods=['POST'])
def api_cookie_login_qr():
    """Start a Playwright QR-login session."""
    import logging as _lg
    _lg.getLogger(__name__).info(f"[QR] start_qr_login called, cookie_manager._session = {_cookie_manager._session}")
    try:
        token, qr_b64, timeout_s = _cookie_manager.start_qr_login()
        _lg.getLogger(__name__).info(f"[QR] start_qr_login OK: token={token}")
        return jsonify({
            'success': True,
            'token': token,
            'qr_base64': qr_b64,
            'timeout_seconds': timeout_s,
        })
    except RuntimeError as e:
        _lg.getLogger(__name__).error(f"[QR] start_qr_login failed: {e}")
        return jsonify({'success': False, 'message': str(e)}), 409


@app.route('/api/cookie-login/<token>/status')
def api_cookie_login_status(token):
    """Poll the status of a QR-login session."""
    return jsonify(_cookie_manager.get_status(token))


@app.route('/api/cookie-login/<token>/cancel', methods=['POST'])
def api_cookie_login_cancel(token):
    """Cancel an ongoing QR-login session."""
    ok = _cookie_manager.cancel(token)
    return jsonify({'success': ok})


@app.route('/api/cookie-login/<token>/force-check', methods=['POST'])
def api_cookie_login_force_check(token):
    """After user scanned QR, request an aggressive cookie re-check.
    Returns immediately — the result becomes available at the status endpoint
    when the background thread processes it.
    """
    result = _cookie_manager.force_check_login(token)
    return jsonify(result)


@app.route('/api/cookie-login/<token>/force-check-result')
def api_cookie_login_force_check_result(token):
    """Poll for force-check result after triggering force-check."""
    result = _cookie_manager.get_force_check_result(token)
    return jsonify(result)


@app.route('/api/cookie-login/<token>/2fa-submit', methods=['POST'])
def api_cookie_login_2fa_submit(token):
    """Submit 2FA verification code."""
    data = request.get_json(force=True, silent=True) or {}
    code = data.get('code', '').strip()
    result = _cookie_manager.submit_2fa_code(token, code)
    return jsonify(result)


@app.route('/api/cookie-login/<token>/2fa-trigger', methods=['POST'])
def api_cookie_login_2fa_trigger(token):
    """Manually activate 2FA mode."""
    result = _cookie_manager.trigger_2fa_mode(token)
    return jsonify(result)


@app.route('/api/cookie-login/<token>/2fa-status')
def api_cookie_login_2fa_status(token):
    """Check if 2FA verification is needed."""
    result = _cookie_manager.get_2fa_status(token)
    return jsonify(result)


@app.route('/api/cookie-login/manual', methods=['POST'])
def api_cookie_login_manual():
    """Save manually-pasted cookie string."""
    data = request.get_json(force=True, silent=True) or {}
    cookie_string = data.get('cookie_string', '').strip()
    ok, message, details = _cookie_manager.manual_save(cookie_string)
    return jsonify({'success': ok, 'message': message, 'details': details})


# ── CSV Export ──

def _add_link_column(rows, url_template, key, col_name='链接'):
    """给每行数据添加页面链接列，点击可跳转到对应页面。

    Args:
        rows: 数据行列表（会被原地修改）。
        url_template: 链接模板，如 '{}user?uid={}' 或 '{}session/{}'。
        key: 行数据中作为 URL 参数的字段名。
        col_name: 新增列的名称，默认 '链接'。
    """
    base = request.host_url.rstrip('/')
    for row in rows:
        val = row.get(key, '')
        row[col_name] = url_template.format(base, val) if val else ''


def _make_csv_response(rows, fieldnames, filename, text_cols=None):
    """Build a Flask CSV response from a list of dicts.

    Adds BOM for Excel UTF-8 detection. Long numeric fields (e.g. user_id)
    get a leading tab to prevent Excel from displaying them as scientific notation.

    Supports ``?cols=`` query parameter to select which columns to export.
    When provided, only matching fieldnames are included (preserving order).

    Args:
        rows: List of dicts.
        fieldnames: CSV column order (all available columns).
        filename: Download filename.
        text_cols: Set of column names to force as text (\\t prefix).
    """
    # ── 列选择过滤 ──
    selected = request.args.get('cols', '').strip()
    if selected:
        allowed = set(selected.split(','))
        fieldnames = [f for f in fieldnames if f in allowed]

    if text_cols is None:
        text_cols = {'user_id', 'real_user_id'}
    si = io.StringIO()
    si.write('﻿')  # BOM — tells Excel this is UTF-8
    writer = csv.DictWriter(si, fieldnames=fieldnames, extrasaction='ignore')
    writer.writeheader()
    for row in rows:
        for col in text_cols:
            if col in row and row[col]:
                row[col] = '\t' + str(row[col])
        writer.writerow(row)
    out = si.getvalue()
    si.close()
    return (out, 200, {
        'Content-Type': 'text/csv; charset=utf-8',
        'Content-Disposition': f'attachment; filename="{filename}"',
    })


@app.route('/api/leaderboard/csv')
def api_leaderboard_csv():
    threshold = request.args.get('threshold', 1000, type=int)
    period = request.args.get('period', 'session')
    sort_by = request.args.get('sort_by', 'consume')
    year_month = request.args.get('year_month', '')
    min_consume = request.args.get('min_consume', 0, type=int)
    session_id = request.args.get('session_id', None, type=int)

    if period == 'session' and session_id is None:
        conn = _get_conn()
        s = conn.execute("SELECT id FROM sessions WHERE status='live' ORDER BY id DESC LIMIT 1").fetchone()
        if s:
            session_id = s[0]
        else:
            return _make_csv_response([], ['user_id', 'user_name', 'consume'], 'leaderboard.csv')

    # Fetch ALL pages (size=999999)
    data = query_leaderboard(threshold, period, 1, 999999, session_id, year_month, min_consume)
    users = data.get('users', [])

    if sort_by == 'sessions' and users:
        users.sort(key=lambda u: (-u.get('sessions_count', 0), -u.get('consume', 0)))
        for i, u in enumerate(users):
            u['rank'] = i + 1

    filename = f'leaderboard_{period}_{year_month or "all"}.csv'
    fieldnames = ['rank', 'user_id', 'user_name', 'grade', 'fans_club', 'consume', 'sessions_count',
                  'qualified_1000', 'qualified_3000', 'qualified_10000', 'qualified_100000']
    # Filter fieldnames to only those present in data
    if not any('qualified_1000' in u for u in users):
        fieldnames = ['rank', 'user_id', 'user_name', 'grade', 'fans_club', 'consume', 'sessions_count']
    _add_link_column(users, '{}/user?uid={}', 'user_id')
    fieldnames.append('链接')
    return _make_csv_response(users, fieldnames, filename)


@app.route('/api/chat/csv')
def api_chat_csv():
    user_id = request.args.get('user_id', '')
    keyword = request.args.get('keyword', '')
    data = query_chat(user_id, keyword, 1, 999999)
    chats = data.get('chats', [])
    _add_link_column(chats, '{}/user?uid={}', 'user_id')
    return _make_csv_response(chats,
        ['time', 'user_id', 'user_name', 'anchor_name', 'content', 'grade', 'fans_club', '链接'],
        'chat_logs.csv')


@app.route('/api/anonymous/csv')
def api_anonymous_csv():
    search = request.args.get('search', '')
    data = query_anonymous(1, 999999, search)
    users = data.get('users', [])
    _add_link_column(users, '{}/user?uid={}', 'real_user_id')
    return _make_csv_response(users,
        ['real_user_id', 'user_name', 'anonymous_label', 'grade', 'fans_club', 'consume', 'sessions_count', 'last_seen', '链接'],
        'anonymous_users.csv')


@app.route('/api/sessions/csv')
def api_sessions_csv():
    sessions = query_sessions(limit=999999)
    _add_link_column(sessions, '{}/session/{}', 'id')
    return _make_csv_response(sessions,
        ['id', 'room_id', 'anchor_name', 'start_time', 'end_time', 'status',
         'user_count', 'total_diamonds', 'total_gifts', 'total_chats', '链接'],
        'sessions.csv')


@app.route('/api/sessions/<int:session_id>/csv')
def api_session_csv(session_id):
    threshold = request.args.get('threshold', 0, type=int)

    if threshold > 0:
        # Use leaderboard query which handles qualified_{threshold} filtering
        data = query_leaderboard(threshold, 'session', 1, 999999, session_id)
        users = data.get('users', [])
    else:
        data = query_session_detail(session_id, top_n=999999)
        if not data:
            return jsonify({'error': 'session not found'}), 404
        # Assign rank to users
        users = data.get('top_users', [])
        for i, u in enumerate(users):
            u['rank'] = i + 1

    rows = []
    for u in users:
        rows.append({
            'rank': u.get('rank', 0),
            'user_id': u.get('user_id', ''),
            'user_name': u.get('user_name', ''),
            'grade': u.get('grade', ''),
            'fans_club': u.get('fans_club', ''),
            'consume': u.get('consume', 0),
            'gift_count': u.get('gift_count', 0),
            'chat_count': u.get('chat_count', 0),
            'qualified_1000': u.get('qualified_1000', 0),
            'qualified_3000': u.get('qualified_3000', 0),
            'qualified_10000': u.get('qualified_10000', 0),
            'qualified_100000': u.get('qualified_100000', 0),
        })

    suffix = f'_threshold_{threshold}' if threshold > 0 else ''
    _add_link_column(rows, '{}/user?uid={}', 'user_id')
    return _make_csv_response(rows,
        ['rank', 'user_id', 'user_name', 'grade', 'fans_club', 'consume',
         'gift_count', 'chat_count',
         'qualified_1000', 'qualified_3000', 'qualified_10000', 'qualified_100000', '链接'],
        f'session_{session_id}_contributors{suffix}.csv')


@app.route('/api/sessions/<int:session_id>/gifts/csv')
def api_session_gifts_csv(session_id):
    data = query_session_detail(session_id, top_n=999999)
    if not data:
        return jsonify({'error': 'session not found'}), 404

    gifts = data.get('top_gifts', [])
    for g in gifts:
        total_count = g.get('total_count', 0)
        g['avg_diamonds'] = round(g.get('total_diamonds', 0) / total_count) if total_count > 0 else 0
    return _make_csv_response(gifts,
        ['gift_name', 'times', 'total_count', 'total_diamonds', 'avg_diamonds'],
        f'session_{session_id}_gifts.csv')


@app.route('/api/user/<user_id>/csv')
def api_user_csv(user_id):
    data = query_user_detail(user_id)
    if not data:
        return jsonify({'error': 'user not found'}), 404

    rows = []
    for s in data.get('sessions', []):
        base = {
            'user_id': user_id,
            'user_name': data.get('user_name', ''),
            'grade': s.get('grade', data.get('grade', '')),
            'fans_club': s.get('fans_club', data.get('fans_club', '')),
            'session_id': s.get('id', ''),
            'anchor': s.get('anchor_name', ''),
            'start_time': s.get('start_time', ''),
            'end_time': s.get('end_time', ''),
            'consume': s.get('consume', 0),
            'gift_count': s.get('gift_count', 0),
            'chat_count': s.get('chat_count', 0),
        }
        rows.append(base)

    _add_link_column(rows, '{}/session/{}', 'session_id', col_name='场次链接')
    return _make_csv_response(rows,
        ['user_id', 'user_name', 'grade', 'fans_club', 'session_id', 'anchor',
         'start_time', 'end_time', 'consume', 'gift_count', 'chat_count', '场次链接'],
        f'user_{user_id}_sessions.csv')


@app.route('/api/user/<user_id>/timeline/csv')
def api_user_timeline_csv(user_id):
    data = query_user_timeline(user_id, 'all', '', 1, 999999)
    timeline = data.get('timeline', [])
    base = request.host_url.rstrip('/')
    user_link = f'{base}/user?uid={user_id}'
    for row in timeline:
        row['用户链接'] = user_link
    return _make_csv_response(timeline,
        ['time', 'type', 'anchor_name', 'content', 'amount', 'grade', '用户链接'],
        f'user_{user_id}_timeline.csv')


def main():
    parser = argparse.ArgumentParser(description='弹幕后台管理面板')
    parser.add_argument('--host', default=_web_config['host'])
    parser.add_argument('--port', default=_web_config['port'], type=int)
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--auto-start', action='store_true',
                        help='Auto-start streamers that were enabled on last shutdown')
    args = parser.parse_args()
    os.makedirs(os.path.join(os.path.dirname(__file__), 'data'), exist_ok=True)

    # 初始化数据库（创建 sessions 等表，避免首次打开首页报错）
    try:
        init_db()
    except Exception as e:
        print(f'[启动] 数据库初始化失败: {e}')

    # Seed streamer config from rooms.txt (one-time)
    _manager.seed_from_rooms_txt()

    # Optionally auto-start previously-enabled streamers
    if args.auto_start:
        conn = _get_conn()
        rows = conn.execute(
            'SELECT live_id FROM streamer_config WHERE enabled = 1').fetchall()
        for row in rows:
            ok, msg = _manager.start_streamer(row['live_id'])
            print(f'[StreamerManager] auto-start {row["live_id"]}: {msg}')

    print(f'[Flask] Starting http://{args.host}:{args.port}')
    print(f'[Flask] Database: {DB_PATH}')
    try:
        app.run(host=args.host, port=args.port, debug=args.debug)
    finally:
        _manager.shutdown_all()


if __name__ == '__main__':
    main()

"""采集器主类：WebSocket 连接管理、消息分发、心跳、看门狗、等待开播。

DouyinBarrage 是整个采集流程的协调中心，组合以下模块：
    base.parser      消息解析与分发表
    base.utils       配置加载、Cookie、工具函数
    base.output      日志、数据记录
    service.network  HTTP 请求、WebSocket 构建、房间 API
    service.signer   签名生成

线程模型：
    主线程        同步 HTTP 预请求 + 等待开播监控
    async 线程    asyncio 事件循环（WebSocket 接收/心跳/看门狗/统计）
    处理线程      消息解析/去重/记录（从 async 线程解耦，避免高峰期拥塞）
"""

import asyncio
import gc
import gzip
import json
import logging
import os
import queue
import random
import re
import sys
import threading
import time
import urllib.parse
from datetime import datetime
from socket import SOL_SOCKET, SO_RCVBUF
from concurrent.futures import ThreadPoolExecutor

import requests
from requests.adapters import HTTPAdapter
import websockets
from websockets.exceptions import ConnectionClosed, ConnectionClosedOK

logging.getLogger('urllib3').setLevel(logging.CRITICAL)

from base.messages import PushFrame, Response, parse_proto
from base.parser import HANDLERS
from base.utils import (
    load_config, load_cookies,
    USER_AGENTS, LOW_VALUE_TYPES, INTERACTIVE_TYPES, USER_MSG_TYPES, METHOD_TO_CONFIG,
    generate_user_unique_id, extract_ua_version,
    rotate_ua, set_current_anchor, set_anchor_name,
)
from base.output import setup_logger, ThroughputCounter, BARRAGE, RoomLogFilter, display_width, is_ci_environment
from base.parser import get_dedup_stats, init_db, create_session, end_session, flush_to_sqlite, record_chat, record_gift, upsert_user, _get_conn
from service.network import (
    fetch_ttwid, enter_room_api, download_image,
    fetch_user_info,
    build_http_headers,
    build_websocket_url, build_ws_cookie,
)
from service.signer import generate_signature

logger = logging.getLogger(__name__)


class DouyinBarrage:
    """抖音直播间弹幕数据采集器。

    通过 WebSocket 长连接实时获取 13 种消息类型，输出 CSV/JSONL。
    支持登录态、自动重连、等待开播、弱网容错。
    """

    # 统一默认配置
    _DEFAULT_CONFIG = {
        'log_level': 'INFO',
        'output': {
            'chat': True, 'lucky_bag': True, 'gift': True, 'like': True,
            'member': True, 'social': True, 'rank': True, 'stats': True,
            'fansclub': True, 'emoji': True, 'room': True, 'roomstats': True,
            'pk_event': True, 'dump_pk_raw': False,
            'contribution': True,
            'control': True, 'file_dir': 'data',
        },
        'network': {
            'http_timeout': 15, 'ws_connect_timeout': 30, 'silence_timeout': 60,
            'user_msg_timeout': 120,
            'heartbeat_interval': 10, 'rcvbuf_kb': 2048, 'proxy': None,
        },
        'max_reconnects': 0,
        'reconnect_base_delay': 2,
        'reconnect_max_delay': 120,
        'stats_interval': 60,
        'cookie_file': 'cookie.txt',
        'live_stop': False,
        'live_check_interval': 30,
    }

    def __init__(self, live_id, config_file='config.yaml', log_level=None, on_room_info=None, multi_room=False,
                 cookie_file=None, data_dir=None, ws_host=None, bind_ip=None, ws_path=None, dump_raw=False):
        self._on_room_info = on_room_info
        # ── 配置 ──
        self.config = load_config(config_file, self._DEFAULT_CONFIG)

        # ── CLI 覆盖 ──
        if cookie_file:
            self.config['cookie_file'] = cookie_file
        if data_dir:
            self.config.setdefault('output', {})['file_dir'] = data_dir
        self._ws_host = ws_host
        self._bind_ip = bind_ip
        self._dump_raw = dump_raw
        self._raw_file = None
        self._ws_path = ws_path

        # ── 日志 ──
        effective_level = (log_level or self.config.get('log_level', 'INFO')).upper()
        self._logger, self._queue_handler = setup_logger(
            log_dir='logs',
            log_level=effective_level,
            multi_room=multi_room,
        )

        self._enable_outputs = self.config.get('output', {})

        # ── UA（一次选定，全局一致）──
        self._ua = random.choice(USER_AGENTS)
        self._ua_version = extract_ua_version(self._ua)
        self._user_unique_id = generate_user_unique_id()

        # ── 网络超时参数 ──
        net_cfg = self.config.get('network', {})
        self._http_timeout = net_cfg.get('http_timeout', 15)
        self._ws_connect_timeout = net_cfg.get('ws_connect_timeout', 30)
        self._silence_timeout = net_cfg.get('silence_timeout', 60)
        self._user_msg_timeout = net_cfg.get('user_msg_timeout', 120)
        self._heartbeat_interval = net_cfg.get('heartbeat_interval', 10)
        self._proxy = net_cfg.get('proxy', None)
        self._rcvbuf = net_cfg.get('rcvbuf_kb', 256) * 1024

        # ── HTTP Session ──
        self.session = requests.Session()
        self.session.trust_env = False
        adapter = HTTPAdapter(pool_connections=10, pool_maxsize=20, max_retries=2)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)
        self.session.headers.update(build_http_headers(self._ua, self._ua_version))
        if self._proxy:
            self.session.proxies.update(self._proxy)
            logger.info(f"[启动] 使用代理: {self._proxy}")

        # ── 登录 Cookie ──
        self._cookie_file = self.config.get('cookie_file', 'cookie.txt')
        self._login_cookies = load_cookies(self._cookie_file)
        if self._login_cookies:
            for name, value in self._login_cookies.items():
                self.session.cookies.set(name, value, domain='.douyin.com')
            has_session = bool(self._login_cookies.get('sessionid') or
                               self._login_cookies.get('sessionid_ss'))
            if has_session:
                logger.info(f"[启动] 已加载 Cookie（{len(self._login_cookies)} 项），包含 sessionid，待连接后验证登录态")
            else:
                logger.info(f"[启动] 已加载 Cookie（{len(self._login_cookies)} 项），未包含 sessionid，将以游客身份采集")
        else:
            logger.info("[启动] 未加载 cookie.txt，以游客身份采集（礼物等信息可能受限）")

        # ── 直播间 ──
        self.live_id = live_id

        # ── 连接状态 ──
        self._ws = None
        self._connected_event = threading.Event()
        self._stop_event = threading.Event()
        self._started = False
        self._connected_at = 0.0
        self._last_error = ''

        # ── 线程引用 ──
        self._loop = None
        self._loop_thread = None

        # ── 健康检测 ──
        self._last_msg_time = 0.0
        self._last_msg_time_lock = threading.Lock()

        # ── 业务消息健康检测 ──
        self._last_business_msg_time = 0.0
        self._last_business_msg_time_lock = threading.Lock()
        self._last_user_msg_time = 0.0
        self._last_user_msg_time_lock = threading.Lock()
        self._ws_connected_at = 0.0

        # ── 吞吐量 ──
        self._counter = ThroughputCounter()
        self._unknown_seen = set()

        # ── 帧丢失检测 ──
        self._last_seq_id = 0
        self._frame_gaps = 0
        self._frame_total = 0

        # ── SQLite ──
        self._session_id = None
        self._db_inited = False

        # ── 输出目录（首次连接后初始化，用于 raw frame 等）──
        self._output_dir = None
        # ── 匿名用户解析跟踪（避免重复 API 调用）──
        self._resolving_anon = set()

        # ── 统计定时打印 ──
        self._stats_interval = self.config.get('stats_interval', 60)

        # ── 连接重试 ──
        self._reconnect_count = 0
        self._ttwid_refresh_needed = False

        # ── ttwid 缓存 ──
        self._ttwid = None
        self._login_info = {'is_login': False, 'nickname': '', 'uid': ''}

        # ── 房间信息 ──
        self._room_id = None
        self._room_info = None

        # ── 等待开播 ──
        self._live_lock = threading.Lock()
        self._waiting_live = False
        self._live_event = threading.Event()
        self._monitor_stop = None
        self._monitor_done = None

        # ── 预计算 enable_outputs 缓存（连接建立时更新）──
        self._eo_cached = dict(self._enable_outputs)

        # ── 面板刷新节流 ──
        self._panel_last = 0.0

        # ── 线程池（用于在 asyncio 中执行同步 HTTP 请求）──
        self._executor = ThreadPoolExecutor(max_workers=2)

        # ── 消息处理队列（收/算分离，防高峰拥塞）──
        self._msg_queue = queue.Queue(maxsize=5000)
        self._process_thread = None
        self._pending_signal = None  # 处理线程检测到的控制信号

    @property
    def anchor_name(self):
        return self._room_info.get('anchor_name', '') if self._room_info else ''

    @property
    def display_name(self):
        """显示用名称：优先主播名，降级为 live_id。"""
        return self.anchor_name or self.live_id

    def get_status(self):
        """返回采集器当前状态的摘要 dict，供 Web 面板展示。

        Returns:
            dict 包含 status, anchor_name, room_id, live_status,
            message_count, uptime_seconds, last_error 等字段。
        """
        info = {
            'live_id': self.live_id,
            'anchor_name': self.anchor_name,
            'room_id': self._room_id or '',
            'live_status': self._room_info.get('status', 0) if self._room_info else 0,
            'message_count': self._counter._count if hasattr(self, '_counter') else 0,
            'last_error': getattr(self, '_last_error', ''),
        }

        # 检查后台线程是否存活
        loop_alive = self._loop_thread is not None and self._loop_thread.is_alive()
        if self._started and not loop_alive and not self._stop_event.is_set():
            info['last_error'] = info['last_error'] or '后台线程意外退出，请查看日志'
            info['status'] = 'error'
            return info

        # 连接状态
        if not self._started:
            info['status'] = 'stopped'
        elif self._connected_event.is_set():
            info['status'] = 'collecting'
            info['uptime_seconds'] = int(time.monotonic() - self._connected_at) if hasattr(self, '_connected_at') else 0
        elif self._room_info is None:
            info['status'] = 'connecting'
        elif self._room_info.get('status') == 4:
            info['status'] = 'waiting'  # 未开播，等待中
        else:
            info['status'] = 'connecting'

        return info

    # ── 懒加载属性 ────────────────────────────────

    @property
    def ttwid(self):
        if self._ttwid:
            return self._ttwid
        self._ttwid, self._login_info = fetch_ttwid(
            self.session, self.live_id,
            self._login_cookies, self._http_timeout,
        )
        self._cookie_expire_str = ''
        self._cookie_mtime = 0
        if os.path.exists(self._cookie_file):
            self._cookie_mtime = os.path.getmtime(self._cookie_file)

        has_cookie = bool(self._login_cookies.get('sessionid') or
                          self._login_cookies.get('sessionid_ss'))
        expire_date = ''
        sid_guard = self._login_cookies.get('sid_guard', '')
        if sid_guard:
            decoded = urllib.parse.unquote(sid_guard)
            parts = decoded.split('|')
            if len(parts) >= 4:
                date_str = parts[3].replace('+', ' ').strip()
                m_date = re.search(r'(\d+)-(\w+)-(\d+)', date_str)
                if m_date:
                    day, mon_str, year = m_date.group(1), m_date.group(2), m_date.group(3)
                    months = {'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06',
                              'Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}
                    mon = months.get(mon_str[:3], '00')
                    expire_date = f'{year}-{mon}-{day}'
        self._cookie_expire_str = expire_date

        if self._login_info['is_login']:
            nick = self._login_info['nickname']
            logger.info(f"[房间] 已登录「{nick}」")
            if expire_date:
                logger.info(f"[房间] Cookie 有效期至 {expire_date}")
                self._check_cookie_expiry_warning(expire_date)
        elif has_cookie:
            logger.warning("[房间] Cookie 中存在 sessionid，但服务端返回未登录状态，"
                           "cookie 可能已过期，请重新从浏览器导出")
            logger.info("[房间] 以游客模式采集（礼物等信息可能受限）")
        else:
            logger.info("[房间] 无登录凭证，以游客模式采集（礼物等信息可能受限）")
        return self._ttwid

    def _check_cookie_expiry_warning(self, expire_str):
        """Check if cookie expires within 24h and warn."""
        try:
            expire_dt = datetime.strptime(expire_str, '%Y-%m-%d')
            remaining = (expire_dt - datetime.now()).days
            if remaining <= 1:
                logger.warning(f"[Cookie] ⚠️ 将在 {remaining} 天后过期 ({expire_str})，请更新 cookie.txt")
            elif remaining <= 3:
                logger.info(f"[Cookie] 将在 {remaining} 天后过期 ({expire_str})")
        except Exception:
            pass

    def _reload_cookie_if_updated(self):
        """Reload cookie.txt if modified since last load."""
        if not os.path.exists(self._cookie_file):
            return False
        try:
            new_mtime = os.path.getmtime(self._cookie_file)
            if new_mtime > self._cookie_mtime:
                new_cookies = load_cookies(self._cookie_file)
                if new_cookies and new_cookies != self._login_cookies:
                    self._login_cookies = new_cookies
                    self._cookie_mtime = new_mtime
                    for name, value in new_cookies.items():
                        self.session.cookies.set(name, value, domain='.douyin.com')
                    self._ttwid = None  # force refresh on next use
                    logger.info(f"[Cookie] 检测到更新，已重载 ({len(new_cookies)} 项)")
                    return True
        except Exception as e:
            logger.debug(f"[Cookie] 重载检查失败: {e}")
        return False

    def _refresh_cookie_via_http(self):
        """Hit Douyin homepage to extend cookie session lifetime."""
        try:
            resp = self.session.get(
                'https://www.douyin.com/',
                headers={'User-Agent': self._ua},
                timeout=15, allow_redirects=True
            )
            if resp.status_code == 200 and 'passport' not in resp.url:
                # Save refreshed cookies back to file
                new_cookie_str = '; '.join(
                    f'{k}={v}' for k, v in self.session.cookies.get_dict().items()
                    if k in self._login_cookies or k in ('sessionid', 'sessionid_ss', 'sid_guard', 's_v_web_id')
                )
                if new_cookie_str:
                    with open(self._cookie_file, 'w', encoding='utf-8') as f:
                        f.write(new_cookie_str)
                    self._cookie_mtime = os.path.getmtime(self._cookie_file)
                    logger.info("[Cookie] 已刷新并保存到 cookie.txt")
                    return True
            logger.debug(f"[Cookie] 刷新请求异常: status={resp.status_code} url={resp.url[:60]}")
        except Exception as e:
            logger.debug(f"[Cookie] 刷新失败: {e}")
        return False

    def _try_playwright_refresh(self):
        """Use Playwright browser to fully rotate cookies. Returns True on success."""
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            logger.debug("[Cookie] Playwright 未安装，跳过深度刷新")
            return False

        if not self._login_cookies:
            return False

        ua = self._ua
        logger.info("[Cookie] Playwright 深度刷新...")
        try:
            with sync_playwright() as p:
                profile_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                           '..', 'browser_profile')
                context = p.chromium.launch_persistent_context(
                    os.path.abspath(profile_dir),
                    headless=True, user_agent=ua,
                    viewport={'width': 1920, 'height': 1080},
                    locale='zh-CN',
                )
                cookie_objects = []
                for name, value in self._login_cookies.items():
                    cookie_objects.append({
                        'name': name, 'value': value,
                        'domain': '.douyin.com', 'path': '/',
                        'httpOnly': False, 'secure': True,
                    })
                context.add_cookies(cookie_objects)

                page = context.new_page()
                page.goto('https://www.douyin.com/', wait_until='domcontentloaded', timeout=30000)
                page.wait_for_timeout(5000)
                page.goto('https://live.douyin.com/', wait_until='domcontentloaded', timeout=30000)
                page.wait_for_timeout(3000)

                raw = context.cookies()
                new_cookies = {}
                for c in raw:
                    if 'douyin.com' in c.get('domain', '') or 'snssdk.com' in c.get('domain', ''):
                        new_cookies[c['name']] = c['value']

                # Preserve critical cookies that Playwright may drop
                critical = ['sso_uid_tt', 'sso_uid_tt_ss', 'passport_sso_user_id',
                           'sid_guard', 'sid_tt', 'uid_tt', 'uid_tt_ss',
                           'sessionid', 'sessionid_ss', 'odin_tt']
                for key in critical:
                    if key in self._login_cookies and key not in new_cookies:
                        new_cookies[key] = self._login_cookies[key]

                context.close()

                if new_cookies.get('sessionid') or new_cookies.get('sessionid_ss'):
                    # Write back in Netscape format
                    import time as _time
                    expiry = int(_time.time()) + 365 * 24 * 3600
                    lines = ['# Netscape HTTP Cookie File',
                             '# https://curl.haxx.se/rfc/cookie_spec.html',
                             '# This is a generated file! Do not edit.', '']
                    for name, value in sorted(new_cookies.items()):
                        lines.append(f'.douyin.com\tTRUE\t/\tTRUE\t{expiry}\t{name}\t{value}')
                    tmp = self._cookie_file + '.tmp'
                    with open(tmp, 'w', encoding='utf-8') as f:
                        f.write('\n'.join(lines) + '\n')
                    os.replace(tmp, self._cookie_file)
                    logger.info(f"[Cookie] Playwright 刷新成功 ({len(self._login_cookies)}→{len(new_cookies)} cookies)")
                    return True
                else:
                    logger.warning("[Cookie] Playwright 刷新后丢失 sessionid，保留旧 cookie")
                    return False
        except Exception as e:
            logger.warning(f"[Cookie] Playwright 刷新失败: {e}")
            return False

    def _cookie_watchdog_loop(self):
        """Background thread: auto-reload, HTTP keepalive, Playwright deep refresh."""
        check_interval = 600       # 10 minutes
        http_interval = 6 * 3600   # HTTP keepalive every 6 hours
        playwright_interval = 24 * 3600  # Playwright deep refresh every 24 hours
        last_http = time.time()
        last_playwright = time.time()  # don't run immediately on startup
        while not self._stop_event.is_set():
            self._stop_event.wait(check_interval)
            if self._stop_event.is_set():
                break
            try:
                # 1. Reload if cookie.txt was updated externally (by Playwright or manual)
                if self._reload_cookie_if_updated():
                    last_http = time.time()
                    continue

                # 2. Expiry warning → trigger Playwright immediately if < 24h
                if self._cookie_expire_str:
                    try:
                        expire_dt = datetime.strptime(self._cookie_expire_str, '%Y-%m-%d')
                        remaining_hours = (expire_dt - datetime.now()).total_seconds() / 3600
                        if remaining_hours < 12:
                            logger.warning(f"[Cookie] ⚠️ 仅剩 {remaining_hours:.0f}h 过期，Playwright 深度刷新...")
                            if self._try_playwright_refresh():
                                self._ttwid = None
                                last_http = time.time()
                                last_playwright = time.time()
                                if self._reload_cookie_if_updated():
                                    continue
                    except Exception:
                        pass

                # 3. HTTP keepalive (every 6 hours)
                if time.time() - last_http > http_interval and self._login_cookies:
                    logger.info("[Cookie] HTTP keepalive...")
                    if self._refresh_cookie_via_http():
                        last_http = time.time()
                        self._ttwid = None

                # 4. Playwright deep refresh (every 24 hours)
                if time.time() - last_playwright > playwright_interval and self._login_cookies:
                    if self._try_playwright_refresh():
                        last_playwright = time.time()
                        last_http = time.time()
                        self._ttwid = None
                        if self._reload_cookie_if_updated():
                            continue
            except Exception as e:
                logger.debug(f"[Cookie] watchdog 异常: {e}")

    @property
    def room_id(self):
        if self._room_id:
            return self._room_id
        self._room_info = enter_room_api(
            self.ttwid, self._ua, self._ua_version,
            self.live_id, self._http_timeout, session=self.session,
        )
        self._room_id = self._room_info['room_id']
        set_anchor_name(self.live_id, self.anchor_name)
        status = self._room_info['status']
        status_text = {2: '直播中', 4: '未开播'}.get(status, f'未知({status})')
        logger.info(f'[房间] room_id={self._room_id}, 状态={status_text}, 主播={self.anchor_name}')
        return self._room_id

    # ── 启动 / 停止 ──────────────────────────────

    def start(self):
        """启动采集：HTTP 预请求在主线程完成，asyncio 事件循环在后台线程运行。"""
        self._started = True
        logger.info(f"[启动] live_id: {self.live_id}")
        logger.info(f"[启动] UA: {self._ua}")
        logger.info(f"[启动] user_unique_id: {self._user_unique_id}")
        logger.info(f"[启动] 网络配置: http_timeout={self._http_timeout}s, "
                     f"ws_connect_timeout={self._ws_connect_timeout}s, "
                     f"silence_timeout={self._silence_timeout}s, "
                     f"heartbeat_interval={self._heartbeat_interval}s, "
                     f"rcvbuf={self._rcvbuf // 1024}KB"
                     f"{', proxy=on' if self._proxy else ''}")

        # 预请求：在主线程完成 HTTP 调用
        try:
            _ = self.ttwid
            _ = self.room_id
        except Exception as e:
            logger.error(f"[启动] HTTP 预请求失败: {e}")
            raise

        self._loop = asyncio.new_event_loop()
        self._loop_thread = threading.Thread(
            target=self._run_event_loop, daemon=False, name='async-loop'
        )
        self._loop_thread.start()

        # Cookie watchdog: auto-reload on file change, refresh before expiry
        if self._login_cookies:
            self._cookie_watchdog_thread = threading.Thread(
                target=self._cookie_watchdog_loop, daemon=True, name='cookie-watchdog'
            )
            self._cookie_watchdog_thread.start()

    def _run_event_loop(self):
        """在后台线程运行 asyncio 事件循环。"""
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._connect_loop())
        except Exception as e:
            if not self._stop_event.is_set():
                logger.error(f"[控制] 事件循环异常: {e}")
        finally:
            self._loop.run_until_complete(self._loop.shutdown_asyncgens())
            self._loop.close()

    def stop(self):
        """停止采集，关闭 WebSocket，等待事件循环退出。"""
        if self._stop_event.is_set():
            return
        logger.info("[控制] 停止采集")
        self._stop_event.set()
        self._live_event.set()
        self._connected_event.clear()
        self._stop_monitor_loop()
        self._queue_handler.clear_room_status(self.live_id)

        # 通过事件循环关闭 WebSocket
        if self._loop and self._loop.is_running() and self._ws:
            try:
                future = asyncio.run_coroutine_threadsafe(self._close_ws(), self._loop)
                future.result(timeout=5)
            except Exception:
                pass

        if self._loop_thread and self._loop_thread.is_alive():
            self._loop_thread.join(timeout=10)

        self._executor.shutdown(wait=False)

        self._stop_processor()

        # 结束 SQLite 场次
        if self._session_id:
            try:
                end_session(self._session_id)
            except Exception:
                pass

        logger.info(f"[统计] 最终: {self._counter.report()}")
        if self._raw_file:
            try:
                self._raw_file.close()
            except Exception:
                pass
            self._raw_file = None

    async def _close_ws(self):
        try:
            await self._ws.close()
        except Exception:
            pass

    # ── 状态消息 ──────────────────────────────────

    def _state_json(self, event, live, message, **extra):
        return json.dumps({
            'type': 'system',
            'event': event,
            'live': live,
            'room_id': self.live_id,
            'anchor_name': self.anchor_name,
            'message': message,
            **extra,
        }, ensure_ascii=False)

    def _log_status(self, event, live, message, **extra):
        prefix = f"[直播状态] {self.anchor_name} " if self.anchor_name else "[直播状态] "
        logger.info(f"{prefix}{message}"
                    + (f" ({', '.join(f'{k}={v}' for k, v in extra.items())})" if extra else ''))
        return self._state_json(event, live, message, **extra)

    # ── 等待开播 ──────────────────────────────────

    def _enter_wait_mode(self):
        """直播结束，进入等待开播模式。"""
        with self._live_lock:
            if self._waiting_live:
                return
            self._waiting_live = True

        # 结束当前数据库场次，避免已结束的直播仍显示"直播中"
        if self._session_id:
            try:
                end_session(self._session_id)
            except Exception:
                pass
            self._session_id = None

        poll_interval = self.config.get('live_check_interval', 30)
        label = self.display_name
        logger.info(f'[控制] {label} 监测中（间隔 {poll_interval}s）')
        if self._queue_handler.multi_room:
            self._queue_handler.set_room_status(
                self.live_id, 'waiting',
                anchor=self.display_name,
                interval=poll_interval,
            )
        else:
            sys.stderr.write('\n')
        self._counter = ThroughputCounter()
        self._unknown_seen.clear()
        self._last_seq_id = 0
        self._frame_gaps = 0
        self._frame_total = 0
        self._reset_output_dir()
        self._start_monitor_loop()

    def _is_waiting_live(self):
        with self._live_lock:
            return self._waiting_live

    def _reset_output_dir(self):
        if self._raw_file:
            try:
                self._raw_file.close()
            except Exception:
                pass
            self._raw_file = None
        self._output_dir = None
        self._resolving_anon.clear()

    def _start_monitor_loop(self):
        if self._monitor_stop is not None:
            return
        stop_event = threading.Event()
        done_event = threading.Event()
        self._monitor_stop = stop_event
        self._monitor_done = done_event

        poll_interval = self.config.get('live_check_interval', 30)
        room_label = f'{self.anchor_name} ' if self.anchor_name else f'{self.live_id} '
        is_multi = self._queue_handler.multi_room

        def loop():
            try:
                stop_event.wait(0.3)
                if stop_event.is_set() or self._stop_event.is_set():
                    return
                start_time = time.time()
                while not stop_event.is_set() and not self._stop_event.is_set():
                    try:
                        info = enter_room_api(
                            self.ttwid, self._ua, self._ua_version,
                            self.live_id, self._http_timeout, session=self.session,
                        )
                        if info['status'] == 2:
                            self._room_id = info['room_id']
                            self._room_info = info
                            set_anchor_name(self.live_id, info.get('anchor_name', ''))
                            self._on_live_started(source='api')
                            return
                    except Exception as e:
                        logger.warning(f'[监控] API 检查失败: {e}')
                        if any(kw in str(e).lower() for kw in ('sign', '403', 'unauthorized', 'cookie')):
                            logger.warning(f'[监控] 检测到认证异常，强制刷新 ttwid')
                            self._ttwid = None

                    if is_multi:
                        self._queue_handler.set_room_status(
                            self.live_id, 'waiting',
                            anchor=self.display_name,
                            interval=poll_interval,
                        )
                        for _ in range(int(poll_interval / 0.5)):
                            if stop_event.is_set() or self._stop_event.is_set():
                                break
                            time.sleep(0.5)
                    else:
                        for _ in range(int(poll_interval / 0.5)):
                            if stop_event.is_set() or self._stop_event.is_set():
                                break
                            elapsed = time.time() - start_time
                            remaining = max(0, int(poll_interval - elapsed))
                            cursor = '|' if int(elapsed * 2) % 2 == 0 else ' '
                            text = f'[等待开播] {room_label}轮询中{cursor} {remaining}s'
                            old_len = self._queue_handler._polling_len
                            pad = max(old_len - display_width(text), 0)
                            if is_ci_environment():
                                print(text)
                            else:
                                sys.stderr.write('\r' + text + ' ' * pad)
                                sys.stderr.flush()
                            self._queue_handler._polling_len = display_width(text)
                            time.sleep(0.5)
                    start_time = time.time()
            finally:
                if is_multi:
                    self._queue_handler.clear_room_status(self.live_id)
                else:
                    if not is_ci_environment():
                        sys.stderr.write('\r' + ' ' * self._queue_handler._polling_len + '\r')
                    self._queue_handler._polling_len = 0
                done_event.set()
                if self._monitor_stop is stop_event:
                    self._monitor_stop = None
                    self._monitor_done = None

        t = threading.Thread(target=loop, daemon=True, name=f'monitor-{self.live_id}')
        t.start()

    def _stop_monitor_loop(self):
        stop = self._monitor_stop
        done = self._monitor_done
        if stop is not None:
            stop.set()
        if done is not None:
            done.wait(timeout=3)

    def _on_live_started(self, source):
        with self._live_lock:
            if not self._waiting_live:
                return
            self._waiting_live = False
        self._stop_monitor_loop()
        self._reset_output_dir()
        self._counter = ThroughputCounter()
        self._unknown_seen.clear()
        self._last_seq_id = 0
        self._frame_gaps = 0
        self._frame_total = 0
        self._reconnect_count = 0
        self._live_event.set()
        self._queue_handler.set_room_status(
            self.live_id, 'collecting',
            anchor=self.display_name,
            msg_count=0,
            elapsed=0,
        )
        label = self.display_name
        logger.info(f'[房间] {label} 已开播')
        logger.info(f"[房间] 检测到开播 (来源:{source})，重新连接...")

    # ── 异步 WebSocket 连接循环 ────────────────────

    async def _connect_loop(self):
        """异步 WebSocket 连接主循环（含重连逻辑）。"""
        max_reconnects = self.config.get('max_reconnects', 0)
        base_delay = self.config.get('reconnect_base_delay', 2)
        max_delay = self.config.get('reconnect_max_delay', 120)
        self._reconnect_count = 0

        while not self._stop_event.is_set():
            try:
                logger.info(f"[连接] 第 {self._reconnect_count + 1} 次连接")

                # ── 状态感知（HTTP API，在线程池执行）──
                # 重连时复用缓存的 room_id，跳过 enter_room_api 以避免
                # 同一 cookie 多次"进入房间"触发服务端 session 冲突（导致手机被踢）
                if self._reconnect_count == 1 and self._room_id:
                    logger.info(f"[连接] 重连复用 room_id={self._room_id}，跳过 enter_room_api")
                    status = 2
                else:
                    self._room_id = None
                    info = await asyncio.get_event_loop().run_in_executor(
                        self._executor,
                        lambda: enter_room_api(
                            self.ttwid, self._ua, self._ua_version,
                            self.live_id, self._http_timeout, session=self.session,
                        )
                    )
                    self._room_id = info['room_id']
                    self._room_info = info
                    set_anchor_name(self.live_id, info.get('anchor_name', ''))

                    anchor = info.get('anchor_name', '')
                    if anchor:
                        RoomLogFilter.update_anchor(self.live_id, anchor)

                    if self._on_room_info and self._reconnect_count == 0:
                        try:
                            self._on_room_info(self.live_id, info.get('anchor_name', ''))
                        except Exception:
                            pass

                    status = info['status']

                if status != 2:
                    status_text = {4: '未开播'}.get(status, f'未知({status})')
                    poll_interval = self.config.get('live_check_interval', 30)
                    if self._ttwid_refresh_needed:
                        self._ttwid_refresh_needed = False
                        self._ttwid = None
                        try:
                            _ = self.ttwid
                            logger.info("[房间] ttwid 刷新成功")
                        except RuntimeError as e:
                            logger.error(f"[房间] ttwid 刷新失败: {e}，无法继续连接，请检查网络")
                            break
                    if not self._is_waiting_live():
                        self._enter_wait_mode()
                    while not self._stop_event.is_set():
                        if self._live_event.is_set():
                            self._live_event.clear()
                            break
                        await asyncio.sleep(1.0)
                    if self._stop_event.is_set():
                        break
                    self._reconnect_count = 0
                    continue
                else:
                    if self._is_waiting_live():
                        self._on_live_started(source='reconnect')
                        logger.info("[连接] 检测到开播，等待 5 秒后建立 WebSocket（让服务端路由就绪）")
                        await asyncio.sleep(5)
                        self._ttwid = None
                        try:
                            _ = self.ttwid
                            logger.info("[连接] ttwid 已刷新")
                        except RuntimeError as e:
                            logger.warning(f"[连接] ttwid 刷新失败: {e}，使用现有值继续")
                    label = self.display_name
                    logger.info(f'[房间] {label} 直播中')
                    if not self._is_waiting_live():
                        self._queue_handler.set_room_status(
                            self.live_id, 'collecting',
                            anchor=self.display_name,
                            msg_count=0,
                            elapsed=0,
                        )

                # ttwid 签名校验失败时自动刷新
                if self._ttwid_refresh_needed:
                    self._ttwid_refresh_needed = False
                    self._ttwid = None
                    try:
                        _ = self.ttwid
                        logger.info("[房间] ttwid 刷新成功")
                    except RuntimeError as e:
                        logger.error(f"[房间] ttwid 刷新失败: {e}，无法继续连接，请检查网络")
                        break

                # 初始化 SQLite + 创建场次（仅首次）
                if not self._db_inited:
                    try:
                        init_db()
                        self._db_inited = True
                    except Exception as e:
                        logger.warning(f"[DB] 初始化失败: {e}")

                if self._session_id is None:
                    try:
                        self._session_id = create_session(self.live_id, self.anchor_name)
                    except Exception as e:
                        logger.warning(f"[DB] 创建场次失败: {e}")

                # 将主播信息写入 users 表（sec_uid、头像），确保主播在榜单/用户页可见
                if self._room_info:
                    try:
                        anchor_uid = self._room_info.get('anchor_user_id', '')
                        if anchor_uid:
                            upsert_user(
                                anchor_uid,
                                self._room_info.get('anchor_name', ''),
                                sec_uid=self._room_info.get('sec_uid', ''),
                                avatar_url=self._room_info.get('anchor_avatar', ''),
                            )
                    except Exception:
                        pass

                # 每次 WebSocket 连接前重新生成 user_unique_id
                old_uid = self._user_unique_id
                self._user_unique_id = generate_user_unique_id()
                logger.info(f"[连接] user_unique_id 已刷新: {old_uid} → {self._user_unique_id}")

                # 构建 WebSocket URL 并签名
                wss = build_websocket_url(self._room_id, self._user_unique_id, self._ua_version, self._ws_host, self._ws_path)
                signature = generate_signature(self._room_id, self._user_unique_id)
                if not signature:
                    self._last_error = "X-Bogus 签名生成失败，请确认 Node.js 已安装"
                    logger.error(f"[签名] {self._last_error}，停止采集")
                    break
                wss += f"&signature={signature}"
                logger.debug(f"[签名] 生成: signature='{signature}', 长度={len(signature)}, "
                             f"user_unique_id={self._user_unique_id}, room_id={self._room_id}")

                additional_headers = {
                    "Cookie": build_ws_cookie(self.ttwid, self._login_cookies),
                    "User-Agent": self._ua,
                }
                logger.debug(f"[连接] WS Cookie 前 80 字符: {additional_headers['Cookie'][:80]}...")

                # ── 连接 WebSocket ──
                connect_kwargs = {
                    'additional_headers': additional_headers,
                    'ping_interval': 30,
                    'ping_timeout': 10,
                    'max_size': 2 ** 23,
                    'max_queue': None,
                    'compression': None,
                    'open_timeout': self._ws_connect_timeout,
                    'origin': 'https://live.douyin.com',
                    'proxy': None,
                }
                if self._bind_ip:
                    connect_kwargs['local_addr'] = (self._bind_ip, 0)
                    logger.info(f"[连接] 绑定源 IP: {self._bind_ip}")
                async with websockets.connect(wss, **connect_kwargs) as ws:
                    self._ws = ws

                    # 直播采集期间关闭自动 GC，避免高并发时 GC 暂停阻塞事件循环
                    gc.disable()

                    # 设置接收缓冲区
                    sock = ws.transport.get_extra_info('socket')
                    if sock is not None:
                        try:
                            sock.setsockopt(SOL_SOCKET, SO_RCVBUF, self._rcvbuf)
                        except Exception as e:
                            logger.debug(f"[连接] rcvbuf 设置失败: {e}")

                    # ── 连接状态初始化 ──
                    self._connected_event.set()
                    self._connected_at = time.monotonic()
                    self._last_error = ''
                    with self._last_msg_time_lock:
                        self._last_msg_time = time.time()
                    self._ws_connected_at = time.time()
                    self._last_seq_id = 0
                    self._frame_gaps = 0
                    self._frame_total = 0

                    # 预计算 enable_outputs
                    self._eo_cached = dict(self._enable_outputs)
                    self._eo_cached['live_stop'] = self.config.get('live_stop', False)
                    self._dump_pk_raw = self._enable_outputs.get('dump_pk_raw', False)

                    # 创建输出目录（用于 raw frame 等文件）
                    if self._output_dir is None:
                        file_dir = self.config.get('output', {}).get('file_dir', 'data')
                        ts = time.strftime('%Y%m%d_%H%M')
                        self._output_dir = os.path.join(file_dir, self.live_id, f"{ts}_{self.room_id}")
                    os.makedirs(self._output_dir, exist_ok=True)
                    self._save_room_info()

                    if self._dump_raw:
                        raw_path = os.path.join(self._output_dir, 'raw_frames.bin.gz')
                        import gzip as _gz
                        self._raw_file = _gz.open(raw_path, 'ab', compresslevel=6)
                        logger.info(f"[落盘] {raw_path}")

                    logger.info("[连接] WebSocket 已建立")

                    # ── 启动消息处理线程 ──
                    self._start_processor()

                    # ── 启动监控任务 ──
                    tasks = []
                    if self._heartbeat_interval > 0:
                        tasks.append(asyncio.create_task(self._heartbeat_task()))
                    tasks.append(asyncio.create_task(self._watchdog_task()))
                    tasks.append(asyncio.create_task(self._stats_task()))

                    try:
                        # ── 消息接收循环 ──
                        async for message in ws:
                            if self._stop_event.is_set():
                                break
                            with self._last_msg_time_lock:
                                self._last_msg_time = time.time()
                            try:
                                await self._handle_message(message)
                            except _WaitLiveSignal:
                                await ws.close()
                                break
                            except _StopSignal:
                                await ws.close()
                                break
                    finally:
                        for t in tasks:
                            t.cancel()
                        await asyncio.gather(*tasks, return_exceptions=True)
                        # 停止消息处理线程
                        self._stop_processor()
                        # 采集结束后手动回收内存并恢复自动 GC
                        gc.collect()
                        gc.enable()

                # 正常退出（ws context manager 已关闭连接）
                self._connected_event.clear()

            except asyncio.CancelledError:
                break
            except (ConnectionClosedOK, ConnectionClosed) as e:
                logger.info(f"[连接] WebSocket 已关闭 (code={e.code if hasattr(e, 'code') else '?'})")
                self._connected_event.clear()
            except RuntimeError as e:
                logger.error(f"[连接] WebSocket 不可恢复错误，停止采集: {e}")
                break
            except ValueError as e:
                err_str = str(e)
                if '4001038' in err_str or 'API 响应非 JSON' in err_str:
                    logger.error(f"[房间] 直播间无效（live_id={self.live_id}），停止采集: {e}")
                    break
                logger.error(f"[网络] API 异常: {e}")
            except Exception as e:
                err_str = str(e)
                # 过滤优雅关闭时的噪音日志
                if self._stop_event.is_set() and (err_str == '0' or not err_str or err_str == 'None'):
                    pass
                elif 'sign check' in err_str or 'signature' in err_str:
                    logger.warning("[签名] ttwid 签名校验失败，将在重连前尝试刷新 ttwid")
                    self._ttwid_refresh_needed = True
                elif 'DEVICE_BLOCKED' in err_str:
                    def _extract(key):
                        m = re.search(rf"['\"]?{re.escape(key)}['\"]?\s*[:=]\s*['\"]([^'\"]+)['\"]", err_str)
                        return m.group(1) if m else '(未知)'
                    handshake_status = _extract('handshake-status')
                    handshake_msg = _extract('handshake-msg')
                    trace_id = _extract('x-tt-trace-id')
                    logger.error(
                        f"[签名] DEVICE_BLOCKED，握手被拒，签名或端点不可用，停止采集\n"
                        f"  handshake-status={handshake_status}, msg={handshake_msg}, trace-id={trace_id}\n"
                        f"  请检查 sign.js 是否过期或尝试其他端点"
                    )
                    self._last_error = f"DEVICE_BLOCKED: {handshake_msg or '签名或端点不可用'}"
                    self._stop_event.set()
                else:
                    self._last_error = f"WebSocket 连接失败: {str(e)[:200]}"
                    logger.error(f"[连接] WebSocket 异常: {e}")
                self._connected_event.clear()

            if self._stop_event.is_set():
                break

            self._reconnect_count += 1
            if max_reconnects > 0 and self._reconnect_count >= max_reconnects:
                self._last_error = f"达到最大重连次数 ({max_reconnects})"
                logger.error(f"[重连] {self._last_error}，停止")
                break

            # 重连前切换 UA
            old_ua = self._ua
            self._ua, self._ua_version = rotate_ua(self._ua)
            if self._ua != old_ua:
                logger.debug(f"[重连] 刷新 UA: {old_ua[:50]}... → {self._ua[:50]}...")
                self.session.headers.update(build_http_headers(self._ua, self._ua_version))

            delay = min(base_delay * (2 ** min(self._reconnect_count - 1, 6)), max_delay)
            delay += random.uniform(0, 2)
            logger.warning(f"[重连] 断开，{delay:.1f}s 后重连 ({self._reconnect_count}"
                           f"{'/' + str(max_reconnects) if max_reconnects > 0 else ''})")
            await asyncio.sleep(delay)

        logger.info("[控制] 采集主循环退出")
        if self._queue_handler.multi_room:
            self._queue_handler.clear_room_status(self.live_id)

    # ── 消息处理 worker ──────────────────────────

    def _start_processor(self):
        """启动消息处理线程。"""
        if self._process_thread and self._process_thread.is_alive():
            return
        self._pending_signal = None
        self._process_thread = threading.Thread(
            target=self._process_loop, daemon=False, name='msg-processor'
        )
        self._process_thread.start()
        logger.debug("[处理] 消息处理线程已启动")

    def _stop_processor(self):
        """停止消息处理线程，等待队列清空。"""
        if not self._process_thread:
            return
        try:
            self._msg_queue.put_nowait(None)  # sentinel
        except queue.Full:
            pass
        self._process_thread.join(timeout=10)
        if self._process_thread.is_alive():
            logger.warning("[处理] 消息处理线程未在 10s 内退出")
        self._process_thread = None

    def _process_loop(self):
        """处理线程主循环：从队列取消息，解析 protobuf → 去重 → 记录。"""
        while not self._stop_event.is_set():
            try:
                item = self._msg_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            if item is None:  # sentinel
                break

            try:
                self._process_item(item)
            except Exception:
                # 单条消息处理异常不应杀死处理线程
                pass

        # 退出前排空队列中剩余数据
        self._drain_queue()

    def _drain_queue(self):
        """处理线程退出前排空队列。"""
        drained = 0
        while True:
            try:
                item = self._msg_queue.get_nowait()
                if item is None:
                    continue
                try:
                    self._process_item(item)
                    drained += 1
                except Exception:
                    pass
            except queue.Empty:
                break
        if drained:
            logger.info(f"[处理] 退出前排空 {drained} 条")

    def _process_item(self, item):
        """处理单条消息组（一个 Response 包中的所有内部消息）。"""
        messages_list, eo_cached, dump_pk_raw = item

        # 设置线程本地主播名，供 fmt_fans_club 解析多粉丝团时使用
        set_current_anchor(self.anchor_name)

        for msg in messages_list:
            if self._stop_event.is_set():
                break
            handler = HANDLERS.get(msg.method)
            if handler:
                try:
                    kwargs = {'enable_outputs': eo_cached or {}}
                    results = handler(msg.payload, **kwargs)
                    config_key = METHOD_TO_CONFIG.get(msg.method)
                    is_enabled = eo_cached.get(config_key, True) if config_key else True
                    short_name = msg.method.replace('Webcast', '').replace('Message', '').lower()
                    self._counter.inc(short_name, enabled=is_enabled)

                    if msg.method not in LOW_VALUE_TYPES:
                        with self._last_business_msg_time_lock:
                            prev = self._last_business_msg_time
                            self._last_business_msg_time = time.time()
                            if prev == 0:
                                delay = time.time() - getattr(self, '_ws_connected_at', 0)
                                logger.info(f"[连接] 开始采集 首条业务消息到达: {msg.method} (连接后 {delay:.1f}s)")

                    # 单独追踪用户互动消息（排除 roomstats 等系统定时推送）
                    if msg.method in USER_MSG_TYPES:
                        with self._last_user_msg_time_lock:
                            self._last_user_msg_time = time.time()

                    now = time.monotonic()
                    if now - self._panel_last >= 3.0:
                        self._panel_last = now
                        elapsed = now - self._counter._start
                        self._queue_handler.set_room_status(
                            self.live_id, 'collecting',
                            anchor=self.display_name,
                            msg_count=self._counter._count,
                            elapsed=elapsed,
                        )

                    for r in results:
                        if 'action' in r:
                            if r['action'] == 'stop':
                                logger.warning("[控制] 直播间已结束，停止采集")
                                self._stop_event.set()
                                self._pending_signal = 'stop'
                                # 结束当前场次，避免已结束的直播仍显示"直播中"
                                if self._session_id:
                                    try:
                                        end_session(self._session_id)
                                    except Exception:
                                        pass
                                    self._session_id = None
                                return
                            elif r['action'] == 'wait_live':
                                self._enter_wait_mode()
                                self._pending_signal = 'wait_live'
                                return
                            continue

                        msg_text = r.get('msg', '')
                        if msg_text:
                            logger.log(BARRAGE, msg_text)

                        rec_type = r.get('type', '')
                        rec_data = r.get('data')

                        # 同步写入 SQLite
                        if self._session_id and rec_data:
                            try:
                                uid = rec_data.get('user_id', '')
                                uname = rec_data.get('user_name', '')
                                ugrade = rec_data.get('grade', '')
                                uclub = rec_data.get('fans_club', '')
                                usec_uid = rec_data.get('sec_uid', '')
                                uavatar = rec_data.get('avatar_url', '')
                                # 只要有用户信息就更新 users 表（记录财富等级、粉丝团、sec_uid、头像）
                                if uid:
                                    upsert_user(uid, uname, ugrade, uclub, usec_uid, uavatar)
                                    # 匿名用户（神秘人/dou前缀）自动解析真实昵称
                                    if uname and (uname.startswith('神秘人') or re.match(r'dou\d+$', uname, re.IGNORECASE)) and uid not in self._resolving_anon:
                                        self._resolving_anon.add(uid)
                                        try:
                                            info = fetch_user_info(uid)
                                            if info and info.get('nickname') and not info['nickname'].startswith('神秘人') and not re.match(r'dou\d+$', info['nickname'], re.IGNORECASE):
                                                resolved = info['nickname']
                                                conn = _get_conn()
                                                conn.execute(
                                                    'UPDATE users SET user_name = ?, sec_uid = CASE WHEN ? != "" THEN ? ELSE sec_uid END, avatar_url = CASE WHEN ? != "" THEN ? ELSE avatar_url END WHERE user_id = ?',
                                                    (resolved,
                                                     info.get('sec_uid', ''), info.get('sec_uid', ''),
                                                     info.get('avatar_url', ''), info.get('avatar_url', ''),
                                                     uid)
                                                )
                                                conn.commit()
                                                logger.info(f"[匿名] 已解析 {uid}: {uname} → {resolved}")
                                        except Exception:
                                            logger.debug(f"[匿名] 解析 {uid} 失败，保留原名称")
                                if rec_type == 'chat' and rec_data.get('content'):
                                    record_chat(self._session_id,
                                        uid, uname,
                                        rec_data.get('content', ''),
                                        ugrade, uclub)
                                elif rec_type == 'gift' and rec_data.get('gift_name'):
                                    record_gift(self._session_id,
                                        uid, uname,
                                        rec_data.get('gift_name', ''),
                                        rec_data.get('gift_count', 0),
                                        rec_data.get('diamond_total', 0),
                                        ugrade, uclub)
                                elif rec_type == 'subscribe' and rec_data.get('diamond'):
                                    # 会员/星守护订阅：从 DB 查找用户已有的 name/grade/fans_club
                                    sub_name = rec_data.get('event', '') + rec_data.get('sub_type', '')
                                    sub_douyin_id = rec_data.get('douyin_id', '')
                                    sub_uname = rec_data.get('user_name', '')
                                    sub_uid = sub_douyin_id or uid
                                    sub_grade = ''
                                    sub_club = ''
                                    # Try to resolve user by name OR by douyin_id
                                    try:
                                        db = _get_conn()
                                        row = None
                                        if sub_uname:
                                            # Lookup by name (prefer records with real data)
                                            row = db.execute(
                                                "SELECT user_id, user_name, grade, fans_club FROM users WHERE user_name = ? AND user_id != ? AND user_name != '' ORDER BY last_seen DESC LIMIT 1",
                                                (sub_uname, sub_uname)
                                            ).fetchone()
                                        if not row and sub_douyin_id:
                                            # Fallback: lookup by douyin_id
                                            row = db.execute(
                                                "SELECT user_id, user_name, grade, fans_club FROM users WHERE user_id = ? AND user_name != '' ORDER BY last_seen DESC LIMIT 1",
                                                (sub_douyin_id,)
                                            ).fetchone()
                                        if row:
                                            sub_uid = row['user_id']
                                            sub_uname = sub_uname or row['user_name']
                                            sub_grade = row['grade'] or ''
                                            sub_club = row['fans_club'] or ''
                                    except Exception:
                                        pass
                                    # Register/update the user record
                                    if sub_douyin_id and sub_uname:
                                        upsert_user(sub_douyin_id, sub_uname, sub_grade, sub_club, '', '')
                                    elif sub_douyin_id:
                                        upsert_user(sub_douyin_id, '用户' + sub_douyin_id[-6:], '', '', '', '')
                                    record_gift(self._session_id,
                                        sub_uid,
                                        sub_uname or ('用户' + sub_douyin_id[-6:]),
                                        sub_name or '订阅',
                                        1,
                                        rec_data.get('diamond', 0),
                                        sub_grade, sub_club)
                            except Exception as e:
                                logger.error(f"[DB] SQLite write failed in _process_item: {e} | type={rec_type} user={uid}")

                        # PK 消息原始 payload dump
                        if dump_pk_raw:
                            raw_bytes = r.get('_dump_raw')
                            if raw_bytes:
                                import os as _os
                                dump_dir = _os.path.join(self._output_dir, r.get('_dump_dir', 'pk_dumps'))
                                _os.makedirs(dump_dir, exist_ok=True)
                                dump_path = _os.path.join(dump_dir, r['_dump_name'])
                                with open(dump_path, 'wb') as _f:
                                    _f.write(raw_bytes)

                except Exception as e:
                    logger.error(f"[数据] 处理 {msg.method} 失败: {e}")

            else:
                if msg.method not in LOW_VALUE_TYPES:
                    self._counter.inc('unknown')
                    if msg.method not in self._unknown_seen:
                        self._unknown_seen.add(msg.method)
                        payload_preview = msg.payload[:80].hex() if isinstance(msg.payload, bytes) else str(msg.payload)[:80]
                        logger.info(f"[数据] 未注册消息类型: {msg.method} | payload_len={len(msg.payload)} | preview={payload_preview}")

    # ── 消息处理 ──────────────────────────────────

    async def _handle_message(self, message):
        """处理单条 WebSocket 消息。"""
        if self._dump_raw and self._raw_file and isinstance(message, bytes):
            try:
                self._raw_file.write(len(message).to_bytes(4, 'big'))
                self._raw_file.write(message)
            except Exception:
                pass
        try:
            package = parse_proto(PushFrame, message)
        except Exception as e:
            raw_preview = message[:64].hex() if isinstance(message, bytes) else str(message)[:64]
            logger.info(f"[连接] PushFrame 解析失败: {e} | len={len(message)} | preview={raw_preview}")
            return

        # 帧丢失检测（hb 帧也更新 seq_id，避免误报跳号）
        sid = package.seq_id or 0
        if package.payload_type != 'hb':
            self._frame_total += 1
        if self._last_seq_id > 0 and sid > self._last_seq_id + 1:
            gap = sid - self._last_seq_id - 1
            self._frame_gaps += gap
            logger.info(f"[帧序] seq_id 跳号: {self._last_seq_id} → {sid} (缺 {gap} 帧)")
        self._last_seq_id = sid

        if package.payload_type == 'hb':
            return

        # gzip 解压
        try:
            raw_payload = gzip.decompress(package.payload)
        except gzip.BadGzipFile:
            raw_payload = package.payload
        except Exception as e:
            logger.error(f"[连接] gzip 解压异常: {e}")
            return

        # Response 解析
        try:
            response = parse_proto(Response, raw_payload)
        except Exception as e:
            raw_preview = raw_payload[:64].hex() if isinstance(raw_payload, bytes) else str(raw_payload)[:64]
            logger.info(f"[数据] Response 解析失败: {e} | len={len(raw_payload)} | preview={raw_preview}")
            return

        # ACK 发送（fire-and-forget，不阻塞收包循环）
        if response.need_ack:
            try:
                ack = PushFrame(
                    log_id=package.log_id,
                    payload_type='ack',
                    payload=response.internal_ext.encode('utf-8'),
                )._pb.SerializeToString()
                asyncio.create_task(self._ws.send(ack))
            except Exception as e:
                pass

        # 消息分发 → 推入处理队列（收/算分离，防高峰拥塞）
        try:
            self._msg_queue.put_nowait(
                (list(response.messages_list), self._eo_cached, self._dump_pk_raw)
            )
        except queue.Full:
            self._counter.inc('dropped_queue')
            if not getattr(self, '_queue_full_warned', False):
                logger.warning("[队列] 消息处理队列已满 (5000)，开始丢弃新消息 — 可能处理速度跟不上接收")
                self._queue_full_warned = True

        # 检查处理线程是否检测到控制信号
        if self._pending_signal:
            signal = self._pending_signal
            self._pending_signal = None
            if signal == 'stop':
                logger.warning("[控制] 处理线程检测到直播结束信号")
                self._stop_event.set()
                raise _StopSignal()
            elif signal == 'wait_live':
                raise _WaitLiveSignal()

    # ── 异步监控任务 ──────────────────────────────

    async def _heartbeat_task(self):
        """异步心跳：每 N 秒发送二进制心跳包。"""
        interval = max(self._heartbeat_interval, 3)
        while not self._stop_event.is_set():
            await asyncio.sleep(interval + random.uniform(0, 2))
            if self._connected_event.is_set() and self._ws:
                try:
                    await self._ws.send(
                        PushFrame(payload_type="hb")._pb.SerializeToString()
                    )
                except Exception:
                    break

    async def _watchdog_task(self):
        """异步看门狗：检测静默断连。"""
        check_interval = max(min(self._silence_timeout // 3, 10), 3)
        first_check_done = False
        first_check_timeout = check_interval + 10   # 必须 > check_interval，否则首次检查必触发
        normal_check_timeout = 30.0

        while not self._stop_event.is_set():
            await asyncio.sleep(check_interval)
            if not self._connected_event.is_set():
                continue

            with self._last_msg_time_lock:
                silence = time.time() - self._last_msg_time
            if silence > self._silence_timeout:
                logger.warning(f"[看门狗] {silence:.0f}s 无数据 (阈值={self._silence_timeout}s)，触发重连")
                try:
                    await self._ws.close()
                except Exception:
                    pass
                break

            if self._last_business_msg_time > 0:
                with self._last_business_msg_time_lock:
                    business_silence = time.time() - self._last_business_msg_time
            else:
                business_silence = time.time() - self._ws_connected_at

            if not first_check_done:
                if business_silence > first_check_timeout:
                    logger.info(f"[看门狗] 首次检测 {business_silence:.0f}s 无业务消息，快速重连")
                    first_check_done = True
                    try:
                        await self._ws.close()
                    except Exception:
                        pass
                    break
                elif self._last_business_msg_time > 0:
                    first_check_done = True
                    logger.debug("[看门狗] 首次检测通过，已收到业务消息")
            else:
                if business_silence > normal_check_timeout:
                    logger.info(f"[看门狗] {business_silence:.0f}s 无业务消息 (仅有低价值消息)，触发重连")
                    try:
                        await self._ws.close()
                    except Exception:
                        pass
                    break

            # 用户互动消息静默检测（roomstats 不停但用户无操作时重连）
            with self._last_user_msg_time_lock:
                user_silence = time.time() - self._last_user_msg_time if self._last_user_msg_time > 0 else time.time() - self._ws_connected_at
            if user_silence > self._user_msg_timeout:
                logger.info(f"[看门狗] {user_silence:.0f}s 无用户互动消息 (阈值={self._user_msg_timeout}s)，触发重连")
                try:
                    await self._ws.close()
                except Exception:
                    pass
                break

    async def _stats_task(self):
        """异步统计：每 N 秒打印吞吐量报告。"""
        while not self._stop_event.is_set():
            await asyncio.sleep(self._stats_interval)
            if self._connected_event.is_set() and not self._is_waiting_live():
                logger.info(f"[统计] {self._counter.report()}")
                dd = get_dedup_stats()
                if dd['raw'] > 0:
                    pct = dd['rejected'] / dd['raw'] * 100 if dd['raw'] else 0
                    logger.info(f"[去重] raw={dd['raw']} passed={dd['passed']} rejected={dd['rejected']}({pct:.1f}%) "
                                f"| repeat_zero={dd['repeat_zero']} combo_block={dd['combo_block']} "
                                f"counter_reset={dd['counter_reset']} delta_zero={dd['delta_zero']} "
                                f"out_of_order={dd['out_of_order']} rc1_dup={dd['rc1_dup']} bulk_dup={dd['bulk_dup']}")
                if self._frame_total > 0:
                    gap_pct = self._frame_gaps / (self._frame_total + self._frame_gaps) * 100
                    logger.info(f"[帧序] frames={self._frame_total} gaps={self._frame_gaps} loss={gap_pct:.2f}%")
                # ── 贡献用户写入 SQLite ──
                if self._session_id:
                    try:
                        flush_to_sqlite(self._session_id)
                    except Exception as e:
                        logger.debug(f"[DB] flush 异常: {e}")



    # ── 辅助方法 ──────────────────────────────────

    def _save_room_info(self):
        if not self._room_info:
            return
        file_dir = self.config.get('output', {}).get('file_dir', 'data')
        room_dir = os.path.join(file_dir, self.live_id)
        meta_file = os.path.join(room_dir, 'meta.json')
        if os.path.exists(meta_file):
            return
        try:
            os.makedirs(room_dir, exist_ok=True)
            meta = {
                'live_id': self.live_id,
                **self._room_info,
                'saved_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }
            with open(meta_file, 'w', encoding='utf-8') as f:
                json.dump(meta, f, ensure_ascii=False, indent=2)
            logger.info(f"[数据] 主播信息已保存: {meta_file}")
            if download_image(self.session, self._room_info['anchor_avatar'],
                              os.path.join(room_dir, 'avatar.jpg')):
                logger.info(f"[数据] 主播头像已下载")
            if download_image(self.session, self._room_info['room_cover'],
                              os.path.join(room_dir, 'cover.jpg')):
                logger.info(f"[数据] 直播间封面已下载")
        except Exception as e:
            logger.warning(f"[数据] 保存主播信息失败: {e}")


# ── 内部信号异常（不在模块级暴露）──

class _WaitLiveSignal(Exception):
    """直播结束，进入等待模式。"""
    pass


class _StopSignal(Exception):
    """直播间结束，停止采集。"""
    pass

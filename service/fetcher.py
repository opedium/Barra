"""é‡‡é›†å™¨ä¸»ç±»ï¼šWebSocket è¿žæŽ¥ç®¡ç†ã€æ¶ˆæ¯åˆ†å‘ã€å¿ƒè·³ã€çœ‹é—¨ç‹—ã€ç­‰å¾…å¼€æ’­ã€‚

DouyinBarrage æ˜¯æ•´ä¸ªé‡‡é›†æµç¨‹çš„åè°ƒä¸­å¿ƒï¼Œç»„åˆä»¥ä¸‹æ¨¡å—ï¼š
    base.parser      æ¶ˆæ¯è§£æžä¸Žåˆ†å‘è¡¨
    base.utils       é…ç½®åŠ è½½ã€Cookieã€å·¥å…·å‡½æ•°
    base.output      æ—¥å¿—ã€æ•°æ®è®°å½•
    service.network  HTTP è¯·æ±‚ã€WebSocket æž„å»ºã€æˆ¿é—´ API
    service.signer   ç­¾åç”Ÿæˆ

çº¿ç¨‹æ¨¡åž‹ï¼š
    ä¸»çº¿ç¨‹        åŒæ­¥ HTTP é¢„è¯·æ±‚ + ç­‰å¾…å¼€æ’­ç›‘æŽ§
    async çº¿ç¨‹    asyncio äº‹ä»¶å¾ªçŽ¯ï¼ˆWebSocket æŽ¥æ”¶/å¿ƒè·³/çœ‹é—¨ç‹—/ç»Ÿè®¡ï¼‰
    å¤„ç†çº¿ç¨‹      æ¶ˆæ¯è§£æž/åŽ»é‡/è®°å½•ï¼ˆä»Ž async çº¿ç¨‹è§£è€¦ï¼Œé¿å…é«˜å³°æœŸæ‹¥å¡žï¼‰
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
from base.parser import get_dedup_stats, init_db, create_session, end_session, flush_to_sqlite, flush_writes, record_chat, record_gift, upsert_user, _get_conn, flush_all_buffers, flush_combo_buffer, set_gift_finalize_callback, remove_gift_finalize_callback
from service.network import (
    fetch_ttwid, enter_room_api, download_image,
    fetch_user_info,
    build_http_headers,
    build_websocket_url, build_ws_cookie,
)
from service.signer import generate_signature

logger = logging.getLogger(__name__)


class DouyinBarrage:
    """æŠ–éŸ³ç›´æ’­é—´å¼¹å¹•æ•°æ®é‡‡é›†å™¨ã€‚

    é€šè¿‡ WebSocket é•¿è¿žæŽ¥å®žæ—¶èŽ·å– 13 ç§æ¶ˆæ¯ç±»åž‹ï¼Œè¾“å‡º CSV/JSONLã€‚
    æ”¯æŒç™»å½•æ€ã€è‡ªåŠ¨é‡è¿žã€ç­‰å¾…å¼€æ’­ã€å¼±ç½‘å®¹é”™ã€‚
    """

    # ç»Ÿä¸€é»˜è®¤é…ç½®
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
        # â”€â”€ é…ç½® â”€â”€
        self.config = load_config(config_file, self._DEFAULT_CONFIG)

        # â”€â”€ CLI è¦†ç›– â”€â”€
        if cookie_file:
            self.config['cookie_file'] = cookie_file
        if data_dir:
            self.config.setdefault('output', {})['file_dir'] = data_dir
        self._ws_host = ws_host
        self._bind_ip = bind_ip
        self._dump_raw = dump_raw
        self._raw_file = None
        self._ws_path = ws_path

        # â”€â”€ æ—¥å¿— â”€â”€
        effective_level = (log_level or self.config.get('log_level', 'INFO')).upper()
        self._logger, self._queue_handler = setup_logger(
            log_dir='logs',
            log_level=effective_level,
            multi_room=multi_room,
        )

        self._enable_outputs = self.config.get('output', {})

        # â”€â”€ UAï¼ˆä¸€æ¬¡é€‰å®šï¼Œå…¨å±€ä¸€è‡´ï¼‰â”€â”€
        self._ua = random.choice(USER_AGENTS)
        self._ua_version = extract_ua_version(self._ua)
        self._user_unique_id = generate_user_unique_id()

        # â”€â”€ ç½‘ç»œè¶…æ—¶å‚æ•° â”€â”€
        net_cfg = self.config.get('network', {})
        self._http_timeout = net_cfg.get('http_timeout', 15)
        self._ws_connect_timeout = net_cfg.get('ws_connect_timeout', 30)
        self._silence_timeout = net_cfg.get('silence_timeout', 60)
        self._user_msg_timeout = net_cfg.get('user_msg_timeout', 120)
        self._heartbeat_interval = net_cfg.get('heartbeat_interval', 10)
        self._proxy = net_cfg.get('proxy', None)
        self._rcvbuf = net_cfg.get('rcvbuf_kb', 256) * 1024

        # â”€â”€ HTTP Session â”€â”€
        self.session = requests.Session()
        self.session.trust_env = False
        adapter = HTTPAdapter(pool_connections=50, pool_maxsize=100, max_retries=2)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)
        self.session.headers.update(build_http_headers(self._ua, self._ua_version))
        if self._proxy:
            self.session.proxies.update(self._proxy)
            logger.info(f"[å¯åŠ¨] ä½¿ç”¨ä»£ç†: {self._proxy}")

        # â”€â”€ ç™»å½• Cookie â”€â”€
        self._cookie_file = self.config.get('cookie_file', 'cookie.txt')
        self._login_cookies = load_cookies(self._cookie_file)
        if self._login_cookies:
            for name, value in self._login_cookies.items():
                self.session.cookies.set(name, value, domain='.douyin.com')
            has_session = bool(self._login_cookies.get('sessionid') or
                               self._login_cookies.get('sessionid_ss'))
            if has_session:
                logger.info(f"[å¯åŠ¨] å·²åŠ è½½ Cookieï¼ˆ{len(self._login_cookies)} é¡¹ï¼‰ï¼ŒåŒ…å« sessionidï¼Œå¾…è¿žæŽ¥åŽéªŒè¯ç™»å½•æ€")
            else:
                logger.info(f"[å¯åŠ¨] å·²åŠ è½½ Cookieï¼ˆ{len(self._login_cookies)} é¡¹ï¼‰ï¼ŒæœªåŒ…å« sessionidï¼Œå°†ä»¥æ¸¸å®¢èº«ä»½é‡‡é›†")
        else:
            logger.info("[å¯åŠ¨] æœªåŠ è½½ cookie.txtï¼Œä»¥æ¸¸å®¢èº«ä»½é‡‡é›†ï¼ˆç¤¼ç‰©ç­‰ä¿¡æ¯å¯èƒ½å—é™ï¼‰")

        # â”€â”€ ç›´æ’­é—´ â”€â”€
        self.live_id = live_id

        # â”€â”€ è¿žæŽ¥çŠ¶æ€ â”€â”€
        self._ws = None
        self._connected_event = threading.Event()
        self._stop_event = threading.Event()
        self._started = False
        self._connected_at = 0.0
        self._last_error = ''

        # â”€â”€ çº¿ç¨‹å¼•ç”¨ â”€â”€
        self._loop = None
        self._loop_thread = None

        # â”€â”€ å¥åº·æ£€æµ‹ â”€â”€
        self._last_msg_time = 0.0
        self._last_msg_time_lock = threading.Lock()

        # â”€â”€ ä¸šåŠ¡æ¶ˆæ¯å¥åº·æ£€æµ‹ â”€â”€
        self._last_business_msg_time = 0.0
        self._last_business_msg_time_lock = threading.Lock()
        self._last_user_msg_time = 0.0
        self._last_user_msg_time_lock = threading.Lock()
        self._ws_connected_at = 0.0

        # â”€â”€ åžåé‡ â”€â”€
        self._counter = ThroughputCounter()
        self._unknown_seen = set()

        # â”€â”€ å¸§ä¸¢å¤±æ£€æµ‹ â”€â”€
        self._last_seq_id = 0
        self._frame_gaps = 0
        self._frame_total = 0

        # â”€â”€ SQLite â”€â”€
        self._session_id = None
        self._db_inited = False

        # â”€â”€ è¾“å‡ºç›®å½•ï¼ˆé¦–æ¬¡è¿žæŽ¥åŽåˆå§‹åŒ–ï¼Œç”¨äºŽ raw frame ç­‰ï¼‰â”€â”€
        self._output_dir = None
        # â”€â”€ åŒ¿åç”¨æˆ·è§£æžè·Ÿè¸ªï¼ˆé¿å…é‡å¤ API è°ƒç”¨ï¼‰â”€â”€
        self._resolving_anon = set()
        self._last_anon_resolve = 0
        self._subscribe_dedup = {}  # sub_key -> timestampï¼Œè®¢é˜…æ¶ˆæ¯ 30s çª—å£åŽ»é‡

        # â”€â”€ ç»Ÿè®¡å®šæ—¶æ‰“å° â”€â”€
        self._stats_interval = self.config.get('stats_interval', 60)

        # â”€â”€ è¿žæŽ¥é‡è¯• â”€â”€
        self._reconnect_count = 0
        self._ttwid_refresh_needed = False

        # â”€â”€ ttwid ç¼“å­˜ â”€â”€
        self._ttwid = None
        self._login_info = {'is_login': False, 'nickname': '', 'uid': ''}

        # â”€â”€ æˆ¿é—´ä¿¡æ¯ â”€â”€
        self._room_id = None
        self._room_info = None

        # â”€â”€ ç­‰å¾…å¼€æ’­ â”€â”€
        self._live_lock = threading.Lock()
        self._waiting_live = False
        self._live_event = threading.Event()
        self._monitor_stop = None
        self._monitor_done = None

        # â”€â”€ é¢„è®¡ç®— enable_outputs ç¼“å­˜ï¼ˆè¿žæŽ¥å»ºç«‹æ—¶æ›´æ–°ï¼‰â”€â”€
        self._eo_cached = dict(self._enable_outputs)

        # â”€â”€ é¢æ¿åˆ·æ–°èŠ‚æµ â”€â”€
        self._panel_last = 0.0

        # â”€â”€ çº¿ç¨‹æ± ï¼ˆç”¨äºŽåœ¨ asyncio ä¸­æ‰§è¡ŒåŒæ­¥ HTTP è¯·æ±‚ï¼‰â”€â”€
        self._executor = ThreadPoolExecutor(max_workers=2)

        # â”€â”€ æ¶ˆæ¯å¤„ç†é˜Ÿåˆ—ï¼ˆæ”¶/ç®—åˆ†ç¦»ï¼Œé˜²é«˜å³°æ‹¥å¡žï¼‰â”€â”€
        self._msg_queue = queue.Queue(maxsize=20000)
        self._process_thread = None
        self._pending_signal = None  # å¤„ç†çº¿ç¨‹æ£€æµ‹åˆ°çš„æŽ§åˆ¶ä¿¡å·

    @property
    def anchor_name(self):
        return self._room_info.get('anchor_name', '') if self._room_info else ''

    @property
    def display_name(self):
        """æ˜¾ç¤ºç”¨åç§°ï¼šä¼˜å…ˆä¸»æ’­åï¼Œé™çº§ä¸º live_idã€‚"""
        return self.anchor_name or self.live_id

    def get_status(self):
        """è¿”å›žé‡‡é›†å™¨å½“å‰çŠ¶æ€çš„æ‘˜è¦ dictï¼Œä¾› Web é¢æ¿å±•ç¤ºã€‚

        Returns:
            dict åŒ…å« status, anchor_name, room_id, live_status,
            message_count, uptime_seconds, last_error ç­‰å­—æ®µã€‚
        """
        info = {
            'live_id': self.live_id,
            'anchor_name': self.anchor_name,
            'room_id': self._room_id or '',
            'live_status': self._room_info.get('status', 0) if self._room_info else 0,
            'message_count': self._counter._count if hasattr(self, '_counter') else 0,
            'message_rate': round(self._counter._count / (time.monotonic() - self._counter._start), 1) if hasattr(self, '_counter') and (time.monotonic() - self._counter._start) > 0.1 else 0,
            'by_type': dict(getattr(self._counter, '_by_type', {})) if hasattr(self, '_counter') else {},
            'last_error': getattr(self, '_last_error', ''),
        }

        # æ£€æŸ¥åŽå°çº¿ç¨‹æ˜¯å¦å­˜æ´»
        loop_alive = self._loop_thread is not None and self._loop_thread.is_alive()
        if self._started and not loop_alive and not self._stop_event.is_set():
            info['last_error'] = info['last_error'] or 'åŽå°çº¿ç¨‹æ„å¤–é€€å‡ºï¼Œè¯·æŸ¥çœ‹æ—¥å¿—'
            info['status'] = 'error'
            return info

        # è¿žæŽ¥çŠ¶æ€
        if not self._started:
            info['status'] = 'stopped'
        elif self._connected_event.is_set():
            info['status'] = 'collecting'
            info['uptime_seconds'] = int(time.monotonic() - self._connected_at) if hasattr(self, '_connected_at') else 0
        elif self._room_info is None:
            info['status'] = 'connecting'
        elif self._room_info.get('status') == 4:
            info['status'] = 'waiting'  # æœªå¼€æ’­ï¼Œç­‰å¾…ä¸­
        else:
            info['status'] = 'connecting'

        return info

    # â”€â”€ æ‡’åŠ è½½å±žæ€§ â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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
            logger.info(f"[æˆ¿é—´] å·²ç™»å½•ã€Œ{nick}ã€")
            if expire_date:
                logger.info(f"[æˆ¿é—´] Cookie æœ‰æ•ˆæœŸè‡³ {expire_date}")
                self._check_cookie_expiry_warning(expire_date)
        elif has_cookie:
            logger.warning("[æˆ¿é—´] Cookie ä¸­å­˜åœ¨ sessionidï¼Œä½†æœåŠ¡ç«¯è¿”å›žæœªç™»å½•çŠ¶æ€ï¼Œ"
                           "cookie å¯èƒ½å·²è¿‡æœŸï¼Œè¯·é‡æ–°ä»Žæµè§ˆå™¨å¯¼å‡º")
            logger.info("[æˆ¿é—´] ä»¥æ¸¸å®¢æ¨¡å¼é‡‡é›†ï¼ˆç¤¼ç‰©ç­‰ä¿¡æ¯å¯èƒ½å—é™ï¼‰")
        else:
            logger.info("[æˆ¿é—´] æ— ç™»å½•å‡­è¯ï¼Œä»¥æ¸¸å®¢æ¨¡å¼é‡‡é›†ï¼ˆç¤¼ç‰©ç­‰ä¿¡æ¯å¯èƒ½å—é™ï¼‰")
        return self._ttwid

    def _check_cookie_expiry_warning(self, expire_str):
        """Check if cookie expires within 24h and warn."""
        try:
            expire_dt = datetime.strptime(expire_str, '%Y-%m-%d')
            remaining = (expire_dt - datetime.now()).days
            if remaining <= 1:
                logger.warning(f"[Cookie] âš ï¸ å°†åœ¨ {remaining} å¤©åŽè¿‡æœŸ ({expire_str})ï¼Œè¯·æ›´æ–° cookie.txt")
            elif remaining <= 3:
                logger.info(f"[Cookie] å°†åœ¨ {remaining} å¤©åŽè¿‡æœŸ ({expire_str})")
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
                    logger.info(f"[Cookie] æ£€æµ‹åˆ°æ›´æ–°ï¼Œå·²é‡è½½ ({len(new_cookies)} é¡¹)")
                    return True
        except Exception as e:
            logger.debug(f"[Cookie] é‡è½½æ£€æŸ¥å¤±è´¥: {e}")
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
                    logger.info("[Cookie] å·²åˆ·æ–°å¹¶ä¿å­˜åˆ° cookie.txt")
                    return True
            logger.debug(f"[Cookie] åˆ·æ–°è¯·æ±‚å¼‚å¸¸: status={resp.status_code} url={resp.url[:60]}")
        except Exception as e:
            logger.debug(f"[Cookie] åˆ·æ–°å¤±è´¥: {e}")
        return False

    def _try_playwright_refresh(self):
        """Use Playwright browser to fully rotate cookies. Returns True on success."""
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            logger.debug("[Cookie] Playwright æœªå®‰è£…ï¼Œè·³è¿‡æ·±åº¦åˆ·æ–°")
            return False

        if not self._login_cookies:
            return False

        ua = self._ua
        logger.info("[Cookie] Playwright æ·±åº¦åˆ·æ–°...")
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
                    logger.info(f"[Cookie] Playwright åˆ·æ–°æˆåŠŸ ({len(self._login_cookies)}â†’{len(new_cookies)} cookies)")
                    return True
                else:
                    logger.warning("[Cookie] Playwright åˆ·æ–°åŽä¸¢å¤± sessionidï¼Œä¿ç•™æ—§ cookie")
                    return False
        except Exception as e:
            logger.warning(f"[Cookie] Playwright åˆ·æ–°å¤±è´¥: {e}")
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

                # 2. Expiry warning â†’ trigger Playwright immediately if < 24h
                if self._cookie_expire_str:
                    try:
                        expire_dt = datetime.strptime(self._cookie_expire_str, '%Y-%m-%d')
                        remaining_hours = (expire_dt - datetime.now()).total_seconds() / 3600
                        if remaining_hours < 12:
                            logger.warning(f"[Cookie] âš ï¸ ä»…å‰© {remaining_hours:.0f}h è¿‡æœŸï¼ŒPlaywright æ·±åº¦åˆ·æ–°...")
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
                logger.debug(f"[Cookie] watchdog å¼‚å¸¸: {e}")

    @property
    def room_id(self):
        if self._room_id:
            return self._room_id
        try:
            self._room_info = enter_room_api(
                self.ttwid, self._ua, self._ua_version,
                self.live_id, self._http_timeout, session=self.session,
            )
            self._room_id = self._room_info['room_id']
        except Exception as e:
            logger.warning(f"[å¯åŠ¨] HTTP é¢„è¯·æ±‚å¤±è´¥: {e}")
            self._room_id = None
        set_anchor_name(self.live_id, self.anchor_name)
        status = self._room_info['status']
        status_text = {2: 'ç›´æ’­ä¸­', 4: 'æœªå¼€æ’­'}.get(status, f'æœªçŸ¥({status})')
        logger.info(f'[æˆ¿é—´] room_id={self._room_id}, çŠ¶æ€={status_text}, ä¸»æ’­={self.anchor_name}')
        return self._room_id

    # â”€â”€ å¯åŠ¨ / åœæ­¢ â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def start(self):
        """å¯åŠ¨é‡‡é›†ï¼šHTTP é¢„è¯·æ±‚åœ¨ä¸»çº¿ç¨‹å®Œæˆï¼Œasyncio äº‹ä»¶å¾ªçŽ¯åœ¨åŽå°çº¿ç¨‹è¿è¡Œã€‚"""
        self._started = True
        logger.info(f"[å¯åŠ¨] live_id: {self.live_id}")
        logger.info(f"[å¯åŠ¨] UA: {self._ua}")
        logger.info(f"[å¯åŠ¨] user_unique_id: {self._user_unique_id}")
        logger.info(f"[å¯åŠ¨] ç½‘ç»œé…ç½®: http_timeout={self._http_timeout}s, "
                     f"ws_connect_timeout={self._ws_connect_timeout}s, "
                     f"silence_timeout={self._silence_timeout}s, "
                     f"heartbeat_interval={self._heartbeat_interval}s, "
                     f"rcvbuf={self._rcvbuf // 1024}KB"
                     f"{', proxy=on' if self._proxy else ''}")

        # é¢„è¯·æ±‚ï¼šåœ¨ä¸»çº¿ç¨‹å®Œæˆ HTTP è°ƒç”¨
        try:
            _ = self.ttwid
            _ = self.room_id
        except Exception as e:
            logger.error(f"[å¯åŠ¨] HTTP é¢„è¯·æ±‚å¤±è´¥: {e}")
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
        """åœ¨åŽå°çº¿ç¨‹è¿è¡Œ asyncio äº‹ä»¶å¾ªçŽ¯ã€‚"""
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._connect_loop())
        except Exception as e:
            if not self._stop_event.is_set():
                logger.error(f"[æŽ§åˆ¶] äº‹ä»¶å¾ªçŽ¯å¼‚å¸¸: {e}")
        finally:
            self._loop.run_until_complete(self._loop.shutdown_asyncgens())
            self._loop.close()

    def stop(self):
        """åœæ­¢é‡‡é›†ï¼Œå…³é—­ WebSocketï¼Œç­‰å¾…äº‹ä»¶å¾ªçŽ¯é€€å‡ºã€‚"""
        if self._stop_event.is_set():
            return
        logger.info("[æŽ§åˆ¶] åœæ­¢é‡‡é›†")
        self._stop_event.set()
        self._live_event.set()
        self._connected_event.clear()
        self._stop_monitor_loop()
        self._queue_handler.clear_room_status(self.live_id)

        # é€šè¿‡äº‹ä»¶å¾ªçŽ¯å…³é—­ WebSocket
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

        # æ³¨é”€å›žè°ƒï¼Œé¿å…å½±å“å…¶ä»–æˆ¿é—´
        try:
            remove_gift_finalize_callback(self._gift_callback_anchor)
        except Exception:
            pass

        # å¼ºåˆ¶ commit å‰©ä½™æ‰¹é‡ + æ¸…ç©ºæ‰€æœ‰ç¼“å†²åŒº
        try:
            flush_writes()
        except Exception:
            pass
        try:
            flush_all_buffers()
        except Exception:
            pass

        # ç»“æŸ SQLite åœºæ¬¡ï¼ˆç©ºåœºæ¬¡è‡ªåŠ¨åˆ é™¤ï¼‰
        if self._session_id:
            try:
                from base.parser import _get_conn as _gc, delete_session as _ds
                cnt = _gc().execute('SELECT COUNT(*) FROM gift_logs WHERE session_id = ?', (self._session_id,)).fetchone()[0]
                if cnt == 0:
                    _ds(self._session_id)
                    logger.info(f"[DB] ç©ºåœºæ¬¡ #{self._session_id} å·²åˆ é™¤")
                else:
                    end_session(self._session_id)
            except Exception:
                pass

        logger.info(f"[ç»Ÿè®¡] æœ€ç»ˆ: {self._counter.report()}")
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

    # â”€â”€ çŠ¶æ€æ¶ˆæ¯ â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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
        prefix = f"[ç›´æ’­çŠ¶æ€] {self.anchor_name} " if self.anchor_name else "[ç›´æ’­çŠ¶æ€] "
        logger.info(f"{prefix}{message}"
                    + (f" ({', '.join(f'{k}={v}' for k, v in extra.items())})" if extra else ''))
        return self._state_json(event, live, message, **extra)

    # â”€â”€ ç­‰å¾…å¼€æ’­ â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def _enter_wait_mode(self):
        """ç›´æ’­ç»“æŸï¼Œè¿›å…¥ç­‰å¾…å¼€æ’­æ¨¡å¼ã€‚"""
        with self._live_lock:
            if self._waiting_live:
                return
            self._waiting_live = True

        # ç»“æŸåœºæ¬¡ï¼Œä½†ä¸æ¸…ç† _session_idï¼Œé¿å…é‡è¿žæ—¶æ–°å»ºç©ºåœºæ¬¡
        if self._session_id:
            try:
                end_session(self._session_id)
            except Exception:
                pass

        poll_interval = self.config.get('live_check_interval', 30)
        label = self.display_name
        logger.info(f'[æŽ§åˆ¶] {label} ç›‘æµ‹ä¸­ï¼ˆé—´éš” {poll_interval}sï¼‰')
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
        self._subscribe_dedup.clear()

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
                        logger.warning(f'[ç›‘æŽ§] API æ£€æŸ¥å¤±è´¥: {e}')
                        if any(kw in str(e).lower() for kw in ('sign', '403', 'unauthorized', 'cookie')):
                            logger.warning(f'[ç›‘æŽ§] æ£€æµ‹åˆ°è®¤è¯å¼‚å¸¸ï¼Œå¼ºåˆ¶åˆ·æ–° ttwid')
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
                            text = f'[ç­‰å¾…å¼€æ’­] {room_label}è½®è¯¢ä¸­{cursor} {remaining}s'
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
        logger.info(f'[æˆ¿é—´] {label} å·²å¼€æ’­')
        logger.info(f"[æˆ¿é—´] æ£€æµ‹åˆ°å¼€æ’­ (æ¥æº:{source})ï¼Œé‡æ–°è¿žæŽ¥...")

    # â”€â”€ å¼‚æ­¥ WebSocket è¿žæŽ¥å¾ªçŽ¯ â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    async def _connect_loop(self):
        """å¼‚æ­¥ WebSocket è¿žæŽ¥ä¸»å¾ªçŽ¯ï¼ˆå«é‡è¿žé€»è¾‘ï¼‰ã€‚"""
        max_reconnects = self.config.get('max_reconnects', 0)
        base_delay = self.config.get('reconnect_base_delay', 2)
        max_delay = self.config.get('reconnect_max_delay', 120)
        self._reconnect_count = 0

        while not self._stop_event.is_set():
            try:
                logger.info(f"[è¿žæŽ¥] ç¬¬ {self._reconnect_count + 1} æ¬¡è¿žæŽ¥")

                # â”€â”€ çŠ¶æ€æ„ŸçŸ¥ï¼ˆHTTP APIï¼Œåœ¨çº¿ç¨‹æ± æ‰§è¡Œï¼‰â”€â”€
                # é‡è¿žæ—¶å¤ç”¨ç¼“å­˜çš„ room_idï¼Œè·³è¿‡ enter_room_api ä»¥é¿å…
                # åŒä¸€ cookie å¤šæ¬¡"è¿›å…¥æˆ¿é—´"è§¦å‘æœåŠ¡ç«¯ session å†²çªï¼ˆå¯¼è‡´æ‰‹æœºè¢«è¸¢ï¼‰
                if self._reconnect_count == 1 and self._room_id:
                    logger.info(f"[è¿žæŽ¥] é‡è¿žå¤ç”¨ room_id={self._room_id}ï¼Œè·³è¿‡ enter_room_api")
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
                    poll_interval = self.config.get('live_check_interval', 30)
                    if self._ttwid_refresh_needed:
                        self._ttwid_refresh_needed = False
                        self._ttwid = None
                        try:
                            _ = self.ttwid
                            logger.info("[æˆ¿é—´] ttwid åˆ·æ–°æˆåŠŸ")
                        except RuntimeError as e:
                            logger.error(f"[æˆ¿é—´] ttwid åˆ·æ–°å¤±è´¥: {e}ï¼Œæ— æ³•ç»§ç»­è¿žæŽ¥ï¼Œè¯·æ£€æŸ¥ç½‘ç»œ")
                            break
                    if status == 4:
                        # ä¸»æ’­ä¸‹æ’­ï¼Œè¿›å…¥ç­‰å¾…å¼€æ’­
                        if not self._is_waiting_live():
                            self._enter_wait_mode()
                    else:
                        # ä¸´æ—¶é”™è¯¯ï¼ˆ30003 ç­‰ï¼‰ï¼Œä¸ç»“æŸåœºæ¬¡
                        logger.debug(f"[ç›‘æŽ§] API è¿”å›žçŠ¶æ€ {status}ï¼Œæš‚ä¸ç»“æŸåœºæ¬¡")
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
                        logger.info("[è¿žæŽ¥] æ£€æµ‹åˆ°å¼€æ’­ï¼Œç­‰å¾… 5 ç§’åŽå»ºç«‹ WebSocketï¼ˆè®©æœåŠ¡ç«¯è·¯ç”±å°±ç»ªï¼‰")
                        await asyncio.sleep(5)
                        self._ttwid = None
                        try:
                            _ = self.ttwid
                            logger.info("[è¿žæŽ¥] ttwid å·²åˆ·æ–°")
                        except RuntimeError as e:
                            logger.warning(f"[è¿žæŽ¥] ttwid åˆ·æ–°å¤±è´¥: {e}ï¼Œä½¿ç”¨çŽ°æœ‰å€¼ç»§ç»­")
                    label = self.display_name
                    logger.info(f'[æˆ¿é—´] {label} ç›´æ’­ä¸­')
                    if not self._is_waiting_live():
                        self._queue_handler.set_room_status(
                            self.live_id, 'collecting',
                            anchor=self.display_name,
                            msg_count=0,
                            elapsed=0,
                        )

                # ttwid ç­¾åæ ¡éªŒå¤±è´¥æ—¶è‡ªåŠ¨åˆ·æ–°
                if self._ttwid_refresh_needed:
                    self._ttwid_refresh_needed = False
                    self._ttwid = None
                    try:
                        _ = self.ttwid
                        logger.info("[æˆ¿é—´] ttwid åˆ·æ–°æˆåŠŸ")
                    except RuntimeError as e:
                        logger.error(f"[æˆ¿é—´] ttwid åˆ·æ–°å¤±è´¥: {e}ï¼Œæ— æ³•ç»§ç»­è¿žæŽ¥ï¼Œè¯·æ£€æŸ¥ç½‘ç»œ")
                        break

                # åˆå§‹åŒ– SQLite + åˆ›å»ºåœºæ¬¡ï¼ˆä»…é¦–æ¬¡ï¼‰
                if not self._db_inited:
                    try:
                        init_db()
                        if not self._db_inited:
                            self._batch_resolve_anonymous()
                        self._db_inited = True
                    except Exception as e:
                        logger.warning(f"[DB] åˆå§‹åŒ–å¤±è´¥: {e}")

                if self._session_id is None:
                    try:
                        self._session_id = create_session(self.live_id, self.anchor_name)
                    except Exception as e:
                        logger.warning(f"[DB] åˆ›å»ºåœºæ¬¡å¤±è´¥: {e}")
                else:
                    # å¦‚æžœå·²æœ‰åœºæ¬¡ä½†å·²ç»“æŸï¼Œå»ºæ–°åœºæ¬¡ï¼ˆæ­£å¸¸æµç¨‹ï¼šç›´æ’­ç»“æŸâ†’é‡å¼€æ’­ï¼‰
                    try:
                        cur = _get_conn().execute('SELECT status FROM sessions WHERE id = ?', (self._session_id,))
                        row = cur.fetchone()
                        if row and row['status'] == 'ended':
                            # æ£€æŸ¥ä¸Šä¸€åœºæ¬¡æ˜¯å¦ä¸ºè™šæµ®åœºæ¬¡ï¼ˆæžå°‘æ•°æ® -> ç›´æ’­å·²ç»“æŸï¼ŒAPI æœªåŠæ—¶æ›´æ–°ï¼‰
                            cur2 = _get_conn().execute(
                                'SELECT (SELECT COUNT(*) FROM gift_logs WHERE session_id = ?) as gc, (SELECT COUNT(*) FROM chat_logs WHERE session_id = ?) as cc',
                                (self._session_id, self._session_id)
                            )
                            r2 = cur2.fetchone()
                            if r2 and r2['gc'] < 5 and r2['cc'] < 10:
                                logger.info(f"[æŽ§åˆ¶] ä¸Šä¸€åœºæ¬¡ #{self._session_id} ä¸ºè™šæµ®åœºæ¬¡ (ç¤¼ç‰©={r2['gc']} å¼¹å¹•={r2['cc']})ï¼Œè¿›å…¥ç­‰å¾…æ¨¡å¼")
                                self._enter_wait_mode()
                                continue  # é‡æ–°æ£€æŸ¥ API çŠ¶æ€
                            else:
                                self._session_id = create_session(self.live_id, self.anchor_name)
                    except Exception:
                        pass

                # combo æœ€ç»ˆåŒ–å›žè°ƒï¼šåªæŽ¨å…¥é˜Ÿåˆ—ï¼Œä¸ç›´æŽ¥å†™DB (ç”± _process_thread çº¿ç¨‹ç»Ÿä¸€å†™å…¥ï¼Œé¿å…çº¿ç¨‹ç´¢)
                if not hasattr(self, '_combo_pending'):
                    self._combo_pending = []
                def _on_gift_finalize(data):
                    if data:
                        self._combo_pending.append(dict(data))
                set_gift_finalize_callback(self.anchor_name or self.live_id, _on_gift_finalize)
                self._gift_callback_anchor = self.anchor_name or self.live_id

                # å°†ä¸»æ’­ä¿¡æ¯å†™å…¥ users è¡¨ï¼ˆsec_uidã€å¤´åƒï¼‰ï¼Œç¡®ä¿ä¸»æ’­åœ¨æ¦œå•/ç”¨æˆ·é¡µå¯è§
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

                # æ¯æ¬¡ WebSocket è¿žæŽ¥å‰é‡æ–°ç”Ÿæˆ user_unique_id
                old_uid = self._user_unique_id
                self._user_unique_id = generate_user_unique_id()
                logger.info(f"[è¿žæŽ¥] user_unique_id å·²åˆ·æ–°: {old_uid} â†’ {self._user_unique_id}")

                # æž„å»º WebSocket URL å¹¶ç­¾å
                wss = build_websocket_url(self._room_id, self._user_unique_id, self._ua_version, self._ws_host, self._ws_path)
                signature = generate_signature(self._room_id, self._user_unique_id)
                if not signature:
                    self._last_error = "X-Bogus ç­¾åç”Ÿæˆå¤±è´¥ï¼Œè¯·ç¡®è®¤ Node.js å·²å®‰è£…"
                    logger.error(f"[ç­¾å] {self._last_error}ï¼Œåœæ­¢é‡‡é›†")
                    break
                wss += f"&signature={signature}"
                logger.debug(f"[ç­¾å] ç”Ÿæˆ: signature='{signature}', é•¿åº¦={len(signature)}, "
                             f"user_unique_id={self._user_unique_id}, room_id={self._room_id}")

                additional_headers = [
                    ("Cookie", build_ws_cookie(self.ttwid, self._login_cookies)),
                    ("User-Agent", self._ua),
                ]
                logger.debug(f"[è¿žæŽ¥] WS Cookie å‰ 80 å­—ç¬¦: {additional_headers[0][1][:80]}...")

                # â”€â”€ è¿žæŽ¥ WebSocket â”€â”€
                # websockets v10- ç”¨ extra_headers, v14+ ç”¨ additional_headers
                _ws_ver = tuple(int(x) for x in websockets.__version__.split('.')[:2])
                _headers_kw = 'additional_headers' if _ws_ver >= (14, 0) else 'extra_headers'
                connect_kwargs = {
                    _headers_kw: additional_headers,
                    'ping_interval': 30,
                    'ping_timeout': 10,
                    'max_size': 2 ** 23,
                    'max_queue': None,
                    'compression': None,
                    'open_timeout': self._ws_connect_timeout,
                    'origin': 'https://live.douyin.com',
                }
                if self._bind_ip:
                    connect_kwargs['local_addr'] = (self._bind_ip, 0)
                    logger.info(f"[è¿žæŽ¥] ç»‘å®šæº IP: {self._bind_ip}")
                async with websockets.connect(wss, **connect_kwargs) as ws:
                    self._ws = ws

                    # ç›´æ’­é‡‡é›†æœŸé—´å…³é—­è‡ªåŠ¨ GCï¼Œé¿å…é«˜å¹¶å‘æ—¶ GC æš‚åœé˜»å¡žäº‹ä»¶å¾ªçŽ¯
                    gc.disable()

                    # è®¾ç½®æŽ¥æ”¶ç¼“å†²åŒº
                    sock = ws.transport.get_extra_info('socket')
                    if sock is not None:
                        try:
                            sock.setsockopt(SOL_SOCKET, SO_RCVBUF, self._rcvbuf)
                        except Exception as e:
                            logger.debug(f"[è¿žæŽ¥] rcvbuf è®¾ç½®å¤±è´¥: {e}")

                    # â”€â”€ è¿žæŽ¥çŠ¶æ€åˆå§‹åŒ– â”€â”€
                    self._connected_event.set()
                    self._connected_at = time.monotonic()
                    self._last_error = ''
                    with self._last_msg_time_lock:
                        self._last_msg_time = time.time()
                    self._ws_connected_at = time.time()
                    self._last_seq_id = 0
                    self._frame_gaps = 0
                    self._frame_total = 0

                    # é¢„è®¡ç®— enable_outputs
                    self._eo_cached = dict(self._enable_outputs)
                    self._eo_cached['live_stop'] = self.config.get('live_stop', False)
                    self._dump_pk_raw = self._enable_outputs.get('dump_pk_raw', False)

                    # åˆ›å»ºè¾“å‡ºç›®å½•ï¼ˆç”¨äºŽ raw frame ç­‰æ–‡ä»¶ï¼‰
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
                        logger.info(f"[è½ç›˜] {raw_path}")

                    logger.info("[è¿žæŽ¥] WebSocket å·²å»ºç«‹")

                    # â”€â”€ å¯åŠ¨æ¶ˆæ¯å¤„ç†çº¿ç¨‹ â”€â”€
                    self._start_processor()

                    # â”€â”€ å¯åŠ¨ç›‘æŽ§ä»»åŠ¡ â”€â”€
                    tasks = []
                    if self._heartbeat_interval > 0:
                        tasks.append(asyncio.create_task(self._heartbeat_task()))
                    tasks.append(asyncio.create_task(self._watchdog_task()))
                    tasks.append(asyncio.create_task(self._stats_task()))

                    try:
                        # â”€â”€ æ¶ˆæ¯æŽ¥æ”¶å¾ªçŽ¯ â”€â”€
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
                        # åœæ­¢æ¶ˆæ¯å¤„ç†çº¿ç¨‹
                        self._stop_processor()
                        # é‡‡é›†ç»“æŸåŽæ‰‹åŠ¨å›žæ”¶å†…å­˜å¹¶æ¢å¤è‡ªåŠ¨ GC
                        gc.collect()
                        gc.enable()

                # æ­£å¸¸é€€å‡ºï¼ˆws context manager å·²å…³é—­è¿žæŽ¥ï¼‰
                # é‡è¿žå‰åˆ·æ–° combo ç¼“å†²ï¼Œé¿å… PK/é«˜å³°æœŸé‡è¿žå¤±åŽ»æœªå®Œæˆçš„è¿žå‡»ç¤¼ç‰©
                flush_combo_buffer()
                flush_writes()
                self._connected_event.clear()

            except asyncio.CancelledError:
                break
            except (ConnectionClosedOK, ConnectionClosed) as e:
                logger.info(f"[è¿žæŽ¥] WebSocket å·²å…³é—­ (code={e.code if hasattr(e, 'code') else '?'})")
                self._connected_event.clear()
            except RuntimeError as e:
                logger.error(f"[è¿žæŽ¥] WebSocket ä¸å¯æ¢å¤é”™è¯¯ï¼Œåœæ­¢é‡‡é›†: {e}")
                break
            except ValueError as e:
                err_str = str(e)
                if '4001038' in err_str or 'API å“åº”éž JSON' in err_str:
                    logger.error(f"[æˆ¿é—´] ç›´æ’­é—´æ— æ•ˆï¼ˆlive_id={self.live_id}ï¼‰ï¼Œåœæ­¢é‡‡é›†: {e}")
                    break
                logger.error(f"[ç½‘ç»œ] API å¼‚å¸¸: {e}")
            except Exception as e:
                err_str = str(e)
                # è¿‡æ»¤ä¼˜é›…å…³é—­æ—¶çš„å™ªéŸ³æ—¥å¿—
                if self._stop_event.is_set() and (err_str == '0' or not err_str or err_str == 'None'):
                    pass
                elif 'sign check' in err_str or 'signature' in err_str:
                    logger.warning("[ç­¾å] ttwid ç­¾åæ ¡éªŒå¤±è´¥ï¼Œå°†åœ¨é‡è¿žå‰å°è¯•åˆ·æ–° ttwid")
                    self._ttwid_refresh_needed = True
                elif 'DEVICE_BLOCKED' in err_str:
                    def _extract(key):
                        m = re.search(rf"['\"]?{re.escape(key)}['\"]?\s*[:=]\s*['\"]([^'\"]+)['\"]", err_str)
                        return m.group(1) if m else '(æœªçŸ¥)'
                    handshake_status = _extract('handshake-status')
                    handshake_msg = _extract('handshake-msg')
                    trace_id = _extract('x-tt-trace-id')
                    logger.error(
                        f"[ç­¾å] DEVICE_BLOCKEDï¼Œæ¡æ‰‹è¢«æ‹’ï¼Œç­¾åæˆ–ç«¯ç‚¹ä¸å¯ç”¨ï¼Œåœæ­¢é‡‡é›†\n"
                        f"  handshake-status={handshake_status}, msg={handshake_msg}, trace-id={trace_id}\n"
                        f"  è¯·æ£€æŸ¥ sign.js æ˜¯å¦è¿‡æœŸæˆ–å°è¯•å…¶ä»–ç«¯ç‚¹"
                    )
                    self._last_error = f"DEVICE_BLOCKED: {handshake_msg or 'ç­¾åæˆ–ç«¯ç‚¹ä¸å¯ç”¨'}"
                    self._stop_event.set()
                else:
                    self._last_error = f"WebSocket è¿žæŽ¥å¤±è´¥: {str(e)[:200]}"
                    logger.error(f"[è¿žæŽ¥] WebSocket å¼‚å¸¸: {e}")
                self._connected_event.clear()

            if self._stop_event.is_set():
                break

            self._reconnect_count += 1
            if max_reconnects > 0 and self._reconnect_count >= max_reconnects:
                self._last_error = f"è¾¾åˆ°æœ€å¤§é‡è¿žæ¬¡æ•° ({max_reconnects})"
                logger.error(f"[é‡è¿ž] {self._last_error}ï¼Œåœæ­¢")
                break

            # é‡è¿žå‰åˆ‡æ¢ UA
            old_ua = self._ua
            self._ua, self._ua_version = rotate_ua(self._ua)
            if self._ua != old_ua:
                logger.debug(f"[é‡è¿ž] åˆ·æ–° UA: {old_ua[:50]}... â†’ {self._ua[:50]}...")
                self.session.headers.update(build_http_headers(self._ua, self._ua_version))

            delay = min(base_delay * (2 ** min(self._reconnect_count - 1, 6)), max_delay)
            delay += random.uniform(0, 2)
            logger.warning(f"[é‡è¿ž] æ–­å¼€ï¼Œ{delay:.1f}s åŽé‡è¿ž ({self._reconnect_count}"
                           f"{'/' + str(max_reconnects) if max_reconnects > 0 else ''})")
            await asyncio.sleep(delay)

        logger.info("[æŽ§åˆ¶] é‡‡é›†ä¸»å¾ªçŽ¯é€€å‡º")
        if self._queue_handler.multi_room:
            self._queue_handler.clear_room_status(self.live_id)

    # â”€â”€ æ¶ˆæ¯å¤„ç† worker â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def _start_processor(self):
        """å¯åŠ¨æ¶ˆæ¯å¤„ç†çº¿ç¨‹ã€‚"""
        if self._process_thread and self._process_thread.is_alive():
            return
        self._pending_signal = None
        self._process_thread = threading.Thread(
            target=self._process_loop, daemon=False, name='msg-processor'
        )
        self._process_thread.start()
        logger.debug("[å¤„ç†] æ¶ˆæ¯å¤„ç†çº¿ç¨‹å·²å¯åŠ¨")

    def _stop_processor(self):
        """åœæ­¢æ¶ˆæ¯å¤„ç†çº¿ç¨‹ï¼Œç­‰å¾…é˜Ÿåˆ—æ¸…ç©ºã€‚"""
        if not self._process_thread:
            return
        try:
            self._msg_queue.put_nowait(None)  # sentinel
        except queue.Full:
            pass
        self._process_thread.join(timeout=10)
        if self._process_thread.is_alive():
            logger.warning("[å¤„ç†] æ¶ˆæ¯å¤„ç†çº¿ç¨‹æœªåœ¨ 10s å†…é€€å‡º")
        self._process_thread = None

    def _process_loop(self):
        """å¤„ç†çº¿ç¨‹ä¸»å¾ªçŽ¯ï¼šä»Žé˜Ÿåˆ—å–æ¶ˆæ¯ï¼Œè§£æž protobuf â†’ åŽ»é‡ â†’ è®°å½•ã€‚"""
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
                # å•æ¡æ¶ˆæ¯å¤„ç†å¼‚å¸¸ä¸åº”æ€æ­»å¤„ç†çº¿ç¨‹
                pass

        # é€€å‡ºå‰æŽ’ç©ºé˜Ÿåˆ—ä¸­å‰©ä½™æ•°æ®
        self._drain_queue()

    def _drain_queue(self):
        """å¤„ç†çº¿ç¨‹é€€å‡ºå‰æŽ’ç©ºé˜Ÿåˆ—ã€‚"""
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
            logger.info(f"[å¤„ç†] é€€å‡ºå‰æŽ’ç©º {drained} æ¡")

    def _process_item(self, item):
        """å¤„ç†å•æ¡æ¶ˆæ¯ç»„ï¼ˆä¸€ä¸ª Response åŒ…ä¸­çš„æ‰€æœ‰å†…éƒ¨æ¶ˆæ¯ï¼‰ã€‚"""
        messages_list, eo_cached, dump_pk_raw = item

        # è®¾ç½®çº¿ç¨‹æœ¬åœ°ä¸»æ’­åï¼Œä¾› fmt_fans_club è§£æžå¤šç²‰ä¸å›¢æ—¶ä½¿ç”¨
        set_current_anchor(self.anchor_name or str(self.live_id))

        # æ¶ˆè€— combo ç¼“å†²ï¼ˆç»Ÿä¸€åœ¨ _process_item çº¿ç¨‹å†™ DBï¼Œé¿å…å®šæ—¶å™¨çº¿ç¨‹ä¸Žæ­¤çº¿ç¨‹äº‰æ‰§ï¼‰
        if hasattr(self, '_combo_pending') and self._combo_pending:
            pending = self._combo_pending[:]
            self._combo_pending.clear()
            for data in pending:
                try:
                    uid = data.get('user_id', '')
                    uname = data.get('user_name', '')
                    if uid:
                        upsert_user(uid, uname,
                            grade=data.get('grade',''), fans_club=data.get('fans_club',''),
                            sec_uid=data.get('sec_uid',''), avatar_url=data.get('avatar_url',''))
                    record_gift(self._session_id, uid, uname,
                        data.get('gift_name',''), data.get('gift_count',0),
                        data.get('diamond_total',0), data.get('grade',''), data.get('fans_club',''))
                except Exception as e:
                    logger.error(f"[DB] combo finalize write failed: {e}")

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
                                logger.info(f"[è¿žæŽ¥] å¼€å§‹é‡‡é›† é¦–æ¡ä¸šåŠ¡æ¶ˆæ¯åˆ°è¾¾: {msg.method} (è¿žæŽ¥åŽ {delay:.1f}s)")

                    # å•ç‹¬è¿½è¸ªç”¨æˆ·äº’åŠ¨æ¶ˆæ¯ï¼ˆæŽ’é™¤ roomstats ç­‰ç³»ç»Ÿå®šæ—¶æŽ¨é€ï¼‰
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
                                logger.warning("[æŽ§åˆ¶] ç›´æ’­é—´å·²ç»“æŸï¼Œåœæ­¢é‡‡é›†")
                                self._stop_event.set()
                                self._pending_signal = 'stop'
                                # ç»“æŸå½“å‰åœºæ¬¡ï¼Œé¿å…å·²ç»“æŸçš„ç›´æ’­ä»æ˜¾ç¤º"ç›´æ’­ä¸­"
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

                        # åŒæ­¥å†™å…¥ SQLite
                        if self._session_id and rec_data:
                            try:
                                uid = rec_data.get('user_id', '')
                                uname = rec_data.get('user_name', '')
                                ugrade = rec_data.get('grade', '')
                                uclub = rec_data.get('fans_club', '')
                                usec_uid = rec_data.get('sec_uid', '')
                                uavatar = rec_data.get('avatar_url', '')
                                # åªè¦æœ‰ç”¨æˆ·ä¿¡æ¯å°±æ›´æ–° users è¡¨ï¼ˆè®°å½•è´¢å¯Œç­‰çº§ã€ç²‰ä¸å›¢ã€sec_uidã€å¤´åƒï¼‰
                                if uid:
                                    # 升级检测前从 DB 读取旧值（upsert_user 会覆盖为新值）
                                    _old_lv_saved = 0
                                    _old_fc_saved = 0
                                    try:
                                        _row = _get_conn().execute("SELECT grade, fans_club FROM users WHERE user_id = ?", (uid,)).fetchone()
                                        if _row:
                                            _m = re.search(r"(\d+)", _row["grade"] or "")
                                            if _m:
                                                _old_lv_saved = int(_m.group(1))
                                            _m = re.search(r"Lv(\d+)", _row["fans_club"] or "")
                                            if _m:
                                                _old_fc_saved = int(_m.group(1))
                                    except Exception:
                                        _old_lv_saved = 0
                                        _old_fc_saved = 0
                                    upsert_user(uid, uname, ugrade, uclub, usec_uid, uavatar)
                                    # è¡¥å……ç­‰å¾…ä¸­çš„è®¢é˜… user_idï¼ˆç”¨æˆ·ç¡®è®¤èº«ä»½åŽå¡«å……ï¼‰
                                    if uid:
                                        self._fill_subscription_uid(uname, uid)
                                    # åŒ¿åç”¨æˆ·ï¼ˆç¥žç§˜äºº/douå‰ç¼€ï¼‰è‡ªåŠ¨è§£æžçœŸå®žæ˜µç§°
                                    _is_anon = uname and (uname.startswith('ç¥žç§˜äºº') or re.match(r'dou\d+$', uname, re.IGNORECASE))
                                    if _is_anon:
                                        # å…ˆæŸ¥ DBï¼šå¦‚æžœå·²ç»è§£æžè¿‡äº†ï¼ˆåå­—ä¸å†åŒ¿åï¼‰ï¼Œè·³è¿‡
                                        try:
                                            cur = _get_conn().execute('SELECT user_name FROM users WHERE user_id = ?', (uid,))
                                            db_name = (cur.fetchone() or {}).get('user_name', '')
                                            if db_name and not (db_name.startswith('ç¥žç§˜äºº') or re.match(r'dou\d+$', db_name, re.IGNORECASE)):
                                                _is_anon = False
                                        except Exception:
                                            pass
                                    if _is_anon:
                                        # å†·å´ï¼š5 åˆ†é’Ÿå†…ä¸é‡å¤è§£æžåŒä¸ªç”¨æˆ·
                                        import time as _time
                                        cooldown_key = f'anon_{uid}'
                                        last_attempt = getattr(self, '_anon_cooldown', {}).get(cooldown_key, 0)
                                        now = _time.time()
                                        if now - last_attempt > 300:
                                            if not hasattr(self, '_anon_cooldown'):
                                                self._anon_cooldown = {}
                                            self._anon_cooldown[cooldown_key] = now
                                            try:
                                                if re.match(r'dou\d+$', uid, re.IGNORECASE):
                                                    from service.network import fetch_user_info_by_unique_id
                                                    info = fetch_user_info_by_unique_id(uid, session=self.session)
                                                else:
                                                    info = fetch_user_info(uid, session=self.session)
                                                if info and info.get('nickname') and not info['nickname'].startswith('ç¥žç§˜äºº') and not re.match(r'dou\d+$', info['nickname'], re.IGNORECASE):
                                                    resolved = info['nickname']
                                                    resolved_uid = info.get('user_id', uid)
                                                    resolved_sec = info.get('sec_uid', '')
                                                    resolved_avatar = info.get('avatar_url', '')
                                                    conn = _get_conn()
                                                    for muid in ({resolved_uid, uid} if resolved_uid and resolved_uid != uid else {uid}):
                                                        conn.execute(
                                                            'UPDATE users SET user_name = ?, sec_uid = CASE WHEN ? != "" THEN ? ELSE sec_uid END, avatar_url = CASE WHEN ? != "" THEN ? ELSE avatar_url END, is_anonymous = 0 WHERE user_id = ?',
                                                            (resolved, resolved_sec, resolved_sec, resolved_avatar, resolved_avatar, muid)
                                                        )
                                                    conn.commit()
                                                    logger.info(f"[åŒ¿å] å·²è§£æž {uid}: {uname} â†’ {resolved}")
                                            except Exception:
                                                logger.debug(f"[åŒ¿å] è§£æž {uid} å¤±è´¥ï¼Œä¿ç•™åŽŸåç§°")
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
                                # â”€â”€ å‡çº§æ£€æµ‹ â”€â”€
                                if uid and rec_type in ('gift', 'chat', 'fansclub'):
                                    try:
                                        from base.parser import record_upgrade
                                        import re as _re
                                        # è´¢å¯Œç­‰çº§å‡çº§æ£€æµ‹ï¼ˆ>= 40 çº§ï¼‰
                                        if ugrade:
                                            m = _re.search(r'(\d+)', ugrade)
                                            if m:
                                                new_lv = int(m.group(1))
                                                if new_lv >= 40:
                                                    old_lv = _old_lv_saved
                                                    if old_lv > 0 and new_lv > old_lv:
                                                        record_upgrade(self._session_id, uid, uname, 'grade', old_lv, new_lv, self.anchor_name)
                                                        logger.info(f"[å‡çº§] è´¢å¯Œç­‰çº§ {uname}[{uid}]: {old_lv} â†’ {new_lv}")
                                        # ç²‰ä¸å›¢ç¯ç‰Œå‡çº§æ£€æµ‹ï¼ˆ>= 15 çº§ï¼‰
                                        if uclub:
                                            m = _re.search(r'Lv(\d+)', uclub)
                                            if m:
                                                new_fc = int(m.group(1))
                                                if new_fc >= 15:
                                                    old_fc = _old_fc_saved
                                                    if old_fc > 0 and new_fc > old_fc:
                                                        record_upgrade(self._session_id, uid, uname, 'fansclub', old_fc, new_fc, self.anchor_name)
                                                        logger.info(f"[å‡çº§] ç²‰ä¸å›¢ {uname}[{uid}]: Lv{old_fc} â†’ Lv{new_fc}")
                                    except Exception as _upgrade_err:
                                        logger.error(f"[升级] 检测异常: {_upgrade_err}")
                                elif rec_type == 'subscribe' and rec_data.get('diamond'):
                                    # ä¼šå‘˜/æ˜Ÿå®ˆæŠ¤è®¢é˜…ï¼šä¼˜å…ˆç”¨ protobuf User å¯¹è±¡çš„çœŸå®žä¿¡æ¯
                                    sub_name = rec_data.get('event', '') + rec_data.get('sub_type', '')
                                    sub_douyin_id = rec_data.get('douyin_id', '')  # display_id (10-12ä½)
                                    sub_uname = rec_data.get('user_name', '')
                                    # sub_uid: ä¼˜å…ˆ protobuf è§£æžçš„ user_idï¼Œå…¶æ¬¡å­—èŠ‚æ‰«æçš„ user_id
                                    proto_uid = rec_data.get('user_id', '')
                                    sub_uid = proto_uid if (proto_uid and re.match(r'^\d+$', str(proto_uid))) else (sub_douyin_id or uid)
                                    sub_grade = rec_data.get('grade', '')
                                    sub_club = rec_data.get('fans_club', '')
                                    sub_sec_uid = rec_data.get('sec_uid', '')
                                    sub_avatar = rec_data.get('avatar_url', '')

                                    # ç”¨æˆ·åå®½æ¾éªŒè¯ï¼Œæ— æ•ˆæ—¶ç”Ÿæˆå…šåº•åä½†ä¸è·³è¿‡
                                    if not sub_uname or len(sub_uname.strip()) < 1:
                                        logger.warning(f"[è®¢é˜…] ç”¨æˆ·åè¯†åˆ«å¤±è´¥ (douyin_id={sub_douyin_id}, uid={sub_uid})ï¼Œä½¿ç”¨å…šåº•å")
                                        sub_uname = ''

                                    # åŽ»é‡ + ä¿å­˜ï¼ˆä»…åœ¨æ²¡æœ‰ä»»ä½•èº«ä»½æ ‡è¯†æ—¶è·³è¿‡ï¼‰
                                    dedup_key = sub_douyin_id or sub_uid or ''
                                    if not dedup_key and not sub_uname:
                                        logger.warning(f"[è®¢é˜…] å®Œå…¨æ— æ³•è¯†åˆ«ç”¨æˆ·ï¼Œè·³è¿‡è®°å½• (douyin_id={sub_douyin_id}, uid={sub_uid})")
                                    else:
                                        now_ts = time.time()
                                        sk = (str(dedup_key or sub_uid or 'unknown'), str(sub_name))
                                        if sk in self._subscribe_dedup and now_ts - self._subscribe_dedup[sk] < 120:
                                            logger.debug(f"[è®¢é˜…] åŽ»é‡è·³è¿‡ {sk}")
                                        else:
                                            self._subscribe_dedup[sk] = now_ts
                                            stale = [k for k, t in list(self._subscribe_dedup.items()) if now_ts - t > 180]
                                            for k in stale: del self._subscribe_dedup[k]

                                            # ç¡®å®šæœ€ç»ˆ user_id
                                            final_uid = sub_uid if re.match(r'^\d+$', str(sub_uid)) else ''
                                            final_name = sub_uname or ('ç”¨æˆ·' + str(sub_douyin_id or sub_uid)[-6:])

                                            # ç”¨æˆ·åæ˜¯å”¯ä¸€å¯é çš„ç¼–è¯†ï¼Œé¿å…ç”¨ msg_id å½“ user_id æŸ¥
                                            if not final_uid or final_uid == '0':
                                                if sub_uname:
                                                    found = _get_conn().execute(
                                                        'SELECT user_id FROM users WHERE user_name = ? LIMIT 1',
                                                        (sub_uname,)
                                                    ).fetchone()
                                                    if found:
                                                        final_uid = found['user_id']
                                                        logger.debug(f"[subscribe] found user by name: {final_uid}")

                                            if final_uid and final_uid != '0':
                                                upsert_user(final_uid, final_name, sub_grade, sub_club, sub_sec_uid, sub_avatar)
                                                record_gift(self._session_id, final_uid, final_name, sub_name or 'è®¢é˜…',
                                                    1, rec_data.get('diamond', 0), sub_grade, sub_club)
                                            else:
                                                # ç­å¾…ç”¨æˆ·èº«ä»½ï¼šå…ˆç”¨ç©º user_id è®°å½•ï¼ŒåŽç»­ top up å¡«å…… real_uid
                                                if sub_uname:
                                                    record_gift(self._session_id, '', sub_uname, sub_name or 'è®¢é˜…',
                                                                1, rec_data.get('diamond', 0), sub_grade, sub_club)
                                                    logger.info(f"[subscribe] pending user: {sub_uname} (waiting for real_uid)")
                            except Exception as e:
                                logger.error(f"[DB] SQLite write failed in _process_item: {e} | type={rec_type} user={uid}")

                        # PK æ¶ˆæ¯åŽŸå§‹ payload dump
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
                    logger.error(f"[æ•°æ®] å¤„ç† {msg.method} å¤±è´¥: {e}")

            else:
                if msg.method not in LOW_VALUE_TYPES:
                    self._counter.inc('unknown')
                    if msg.method not in self._unknown_seen:
                        self._unknown_seen.add(msg.method)
                        payload_preview = msg.payload[:80].hex() if isinstance(msg.payload, bytes) else str(msg.payload)[:80]
                        logger.info(f"[æ•°æ®] æœªæ³¨å†Œæ¶ˆæ¯ç±»åž‹: {msg.method} | payload_len={len(msg.payload)} | preview={payload_preview}")

        # æ¯ 100 æ¡æ¶ˆæ¯è§¦å‘ä¸€æ¬¡ç»Ÿè®¡å†™å…¥ï¼ˆåœ¨å¤„ç†çº¿ç¨‹æ‰§è¡Œï¼Œå…äº‰æ‰§ï¼‰
        self._proc_count = getattr(self, '_proc_count', 0) + 1
        if self._proc_count % 100 == 0 and self._session_id:
            try:
                flush_to_sqlite(self._session_id)
            except Exception:
                pass

    # â”€â”€ æ¶ˆæ¯å¤„ç† â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    async def _handle_message(self, message):
        """å¤„ç†å•æ¡ WebSocket æ¶ˆæ¯ã€‚"""
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
            logger.info(f"[è¿žæŽ¥] PushFrame è§£æžå¤±è´¥: {e} | len={len(message)} | preview={raw_preview}")
            return

        # å¸§ä¸¢å¤±æ£€æµ‹ï¼ˆhb å¸§ä¹Ÿæ›´æ–° seq_idï¼Œé¿å…è¯¯æŠ¥è·³å·ï¼‰
        sid = package.seq_id or 0
        if package.payload_type != 'hb':
            self._frame_total += 1
        if self._last_seq_id > 0 and sid > self._last_seq_id + 1:
            gap = sid - self._last_seq_id - 1
            self._frame_gaps += gap
            logger.info(f"[å¸§åº] seq_id è·³å·: {self._last_seq_id} â†’ {sid} (ç¼º {gap} å¸§)")
        self._last_seq_id = sid

        if package.payload_type == 'hb':
            return

        # gzip è§£åŽ‹
        try:
            raw_payload = gzip.decompress(package.payload)
        except gzip.BadGzipFile:
            raw_payload = package.payload
        except Exception as e:
            logger.error(f"[è¿žæŽ¥] gzip è§£åŽ‹å¼‚å¸¸: {e}")
            return

        # Response è§£æž
        try:
            response = parse_proto(Response, raw_payload)
        except Exception as e:
            raw_preview = raw_payload[:64].hex() if isinstance(raw_payload, bytes) else str(raw_payload)[:64]
            logger.info(f"[æ•°æ®] Response è§£æžå¤±è´¥: {e} | len={len(raw_payload)} | preview={raw_preview}")
            return

        # ACK å‘é€ï¼ˆfire-and-forgetï¼Œä¸é˜»å¡žæ”¶åŒ…å¾ªçŽ¯ï¼‰
        if response.need_ack:
            try:
                ack = PushFrame(
                    log_id=package.log_id,
                    payload_type='ack',
                    payload=response.internal_ext.encode('utf-8'),
                )._pb.SerializeToString()
                async def _safe_ack():
                    try:
                        await self._ws.send(ack)
                    except Exception:
                        pass
                asyncio.create_task(_safe_ack())
            except Exception as e:
                pass

        # æ¶ˆæ¯åˆ†å‘ â†’ æŽ¨å…¥å¤„ç†é˜Ÿåˆ—ï¼ˆæ”¶/ç®—åˆ†ç¦»ï¼Œé˜²é«˜å³°æ‹¥å¡žï¼‰
        try:
            self._msg_queue.put_nowait(
                (list(response.messages_list), self._eo_cached, self._dump_pk_raw)
            )
        except queue.Full:
            self._counter.inc('dropped_queue')
            if not getattr(self, '_queue_full_warned', False):
                logger.warning("[é˜Ÿåˆ—] æ¶ˆæ¯å¤„ç†é˜Ÿåˆ—å·²æ»¡ (5000)ï¼Œå¼€å§‹ä¸¢å¼ƒæ–°æ¶ˆæ¯ â€” å¯èƒ½å¤„ç†é€Ÿåº¦è·Ÿä¸ä¸ŠæŽ¥æ”¶")
                self._queue_full_warned = True

        # æ£€æŸ¥å¤„ç†çº¿ç¨‹æ˜¯å¦æ£€æµ‹åˆ°æŽ§åˆ¶ä¿¡å·
        if self._pending_signal:
            signal = self._pending_signal
            self._pending_signal = None
            if signal == 'stop':
                logger.warning("[æŽ§åˆ¶] å¤„ç†çº¿ç¨‹æ£€æµ‹åˆ°ç›´æ’­ç»“æŸä¿¡å·")
                self._stop_event.set()
                raise _StopSignal()
            elif signal == 'wait_live':
                raise _WaitLiveSignal()

    # â”€â”€ å¼‚æ­¥ç›‘æŽ§ä»»åŠ¡ â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    async def _heartbeat_task(self):
        """å¼‚æ­¥å¿ƒè·³ï¼šæ¯ N ç§’å‘é€äºŒè¿›åˆ¶å¿ƒè·³åŒ…ã€‚"""
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
        """å¼‚æ­¥çœ‹é—¨ç‹—ï¼šæ£€æµ‹é™é»˜æ–­è¿žã€‚"""
        check_interval = max(min(self._silence_timeout // 3, 10), 3)
        first_check_done = False
        first_check_timeout = check_interval + 10   # å¿…é¡» > check_intervalï¼Œå¦åˆ™é¦–æ¬¡æ£€æŸ¥å¿…è§¦å‘
        normal_check_timeout = 180.0

        while not self._stop_event.is_set():
            await asyncio.sleep(check_interval)
            if not self._connected_event.is_set():
                continue

            with self._last_msg_time_lock:
                silence = time.time() - self._last_msg_time
            if silence > self._silence_timeout:
                logger.warning(f"[çœ‹é—¨ç‹—] {silence:.0f}s æ— æ•°æ® (é˜ˆå€¼={self._silence_timeout}s)ï¼Œè§¦å‘é‡è¿ž")
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
                    logger.info(f"[çœ‹é—¨ç‹—] é¦–æ¬¡æ£€æµ‹ {business_silence:.0f}s æ— ä¸šåŠ¡æ¶ˆæ¯ï¼Œå é™¤ç©ºåœºæ¬¡")
                    first_check_done = True
                    # å é™¤æ­¤æ¬¡å»ºç«‹çš„ç©ºåœºæ¬¡ï¼ˆé¿å…èšæµ®åœºæ¬¡ç§¯ç´¯ï¼‰
                    try:
                        from base.parser import delete_session as _ds
                        if self._session_id:
                            _ds(self._session_id)
                            self._session_id = None
                    except Exception:
                        pass
                    self._enter_wait_mode()
                    break
                elif self._last_business_msg_time > 0:
                    first_check_done = True
                    logger.debug("[çœ‹é—¨ç‹—] é¦–æ¬¡æ£€æµ‹é€šè¿‡ï¼Œå·²æ”¶åˆ°ä¸šåŠ¡æ¶ˆæ¯")
            else:
                if business_silence > normal_check_timeout:
                    logger.info(f"[çœ‹é—¨ç‹—] {business_silence:.0f}s æ— ä¸šåŠ¡æ¶ˆæ¯ (ä»…æœ‰ä½Žä»·å€¼æ¶ˆæ¯)ï¼Œè§¦å‘é‡è¿ž")
                    try:
                        await self._ws.close()
                    except Exception:
                        pass
                    break

            # ç”¨æˆ·äº’åŠ¨æ¶ˆæ¯é™é»˜æ£€æµ‹ï¼ˆroomstats ä¸åœä½†ç”¨æˆ·æ— æ“ä½œæ—¶é‡è¿žï¼‰
            with self._last_user_msg_time_lock:
                user_silence = time.time() - self._last_user_msg_time if self._last_user_msg_time > 0 else time.time() - self._ws_connected_at
            if user_silence > self._user_msg_timeout:
                logger.info(f"[çœ‹é—¨ç‹—] {user_silence:.0f}s æ— ç”¨æˆ·äº’åŠ¨æ¶ˆæ¯ (é˜ˆå€¼={self._user_msg_timeout}s)ï¼Œè§¦å‘é‡è¿ž")
                try:
                    await self._ws.close()
                except Exception:
                    pass
                break

    async def _stats_task(self):
        """å¼‚æ­¥ç»Ÿè®¡ï¼šæ¯ N ç§’æ‰“å°åžåé‡æŠ¥å‘Šã€‚"""
        while not self._stop_event.is_set():
            await asyncio.sleep(self._stats_interval)
            if self._connected_event.is_set() and not self._is_waiting_live():
                logger.info(f"[ç»Ÿè®¡] {self._counter.report()}")
                dd = get_dedup_stats()
                if dd['raw'] > 0:
                    pct = dd['rejected'] / dd['raw'] * 100 if dd['raw'] else 0
                    logger.info(f"[åŽ»é‡] raw={dd['raw']} passed={dd['passed']} rejected={dd['rejected']}({pct:.1f}%) "
                                f"| repeat_zero={dd['repeat_zero']} combo_block={dd['combo_block']} "
                                f"counter_reset={dd['counter_reset']} delta_zero={dd['delta_zero']} "
                                f"out_of_order={dd['out_of_order']} rc1_dup={dd['rc1_dup']} bulk_dup={dd['bulk_dup']}")
                if self._frame_total > 0:
                    gap_pct = self._frame_gaps / (self._frame_total + self._frame_gaps) * 100
                    logger.info(f"[å¸§åº] frames={self._frame_total} gaps={self._frame_gaps} loss={gap_pct:.2f}%")
                # â”€â”€ fd çŠ¶æ€ç›‘æŽ§ï¼ˆé¿å… fd æ³„æ¼å¯¼è‡´ crashï¼‰â”€â”€
                import os as _os
                try:
                    _fd_count = len(_os.listdir(f'/proc/{_os.getpid()}/fd'))
                    if _fd_count > 200:
                        logger.warning(f"[FD] å½“å‰ fd æ•°: {_fd_count} (è­¦æˆ’ > 200)")
                    elif self._stats_count % 5 == 0:
                        logger.info(f"[FD] fd: {_fd_count}")
                except Exception:
                    pass
                # â”€â”€ æ¯ 5 åˆ†é’Ÿè§¦å‘ä¸€æ¬¡åŒ¿åè§£æž + ç¼©æ”¾ HTTP è¿žæŽ¥æ± â”€â”€
                self._stats_count = getattr(self, '_stats_count', 0) + 1
                if self._stats_count % 10 == 0:
                    self._batch_resolve_anonymous()
                    try:
                        # æ¸…ç† HTTP è¿žæŽ¥æ± ï¼Œé‡Šæ”¾é—²ç½®é•¿è¿žæŽ¥
                        for proto in ('https://', 'http://'):
                            adp = self.session.get_adapter(proto)
                            adp.pool_manager.clear()
                    except Exception:
                        pass
                # â”€â”€ å¼ºåˆ¶åˆ·æ–° combo ç¼“å†² + è´¡çŒ®å†™å…¥ SQLite + æ‰¹é‡ commit â”€â”€
                flush_writes()
                try:
                    flush_combo_buffer()
                except Exception:
                    pass
                # (flush_to_sqlite å·²ç§»è‡³ _process_item çº¿ç¨‹ï¼Œæ­¤å¤„ä¸å†è°ƒç”¨ä»¥å…äº‰æ‰§)
                # â”€â”€ æ¯ 5 åˆ†é’Ÿå…³é—­é—²ç½® HTTP è¿žæŽ¥ï¼Œé˜²æ­¢ fd æ³„æ¼ â”€â”€
                if int(time.time()) % 300 < self._stats_interval:
                    try:
                        self.session.close()
                        logger.debug("[è¿žæŽ¥] HTTP ç©ºé—²è¿žæŽ¥å·²å…³é—­")
                    except Exception:
                        pass



    # â”€â”€ è¾…åŠ©æ–¹æ³• â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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
            logger.info(f"[æ•°æ®] ä¸»æ’­ä¿¡æ¯å·²ä¿å­˜: {meta_file}")
            if download_image(self.session, self._room_info['anchor_avatar'],
                              os.path.join(room_dir, 'avatar.jpg')):
                logger.info(f"[æ•°æ®] ä¸»æ’­å¤´åƒå·²ä¸‹è½½")
            if download_image(self.session, self._room_info['room_cover'],
                              os.path.join(room_dir, 'cover.jpg')):
                logger.info(f"[æ•°æ®] ç›´æ’­é—´å°é¢å·²ä¸‹è½½")
        except Exception as e:
            logger.warning(f"[æ•°æ®] ä¿å­˜ä¸»æ’­ä¿¡æ¯å¤±è´¥: {e}")


    # â”€â”€ æ‰¹é‡åŒ¿åç”¨æˆ·è§£æž â”€â”€

    def _batch_resolve_anonymous(self):
        """åŽå°æ‰¹é‡è§£æžæœªè§£å†³çš„åŒ¿åç”¨æˆ·ï¼ˆdou/ç¥žç§˜äººå‰ç¼€ï¼‰ã€‚"""
        import re as _re
        from base.parser import _get_conn
        from service.network import fetch_user_info, fetch_user_info_by_unique_id
        import threading

        def _resolve():
            conn = _get_conn()
            users = conn.execute("""
                SELECT user_id, user_name FROM users
                WHERE is_anonymous = 1 AND (user_name LIKE 'dou%' OR user_name LIKE 'ç¥žç§˜äºº%')
                ORDER BY last_seen DESC LIMIT 200
            """).fetchall()
            if not users:
                return
            done = 0
            for u in users:
                uid = u['user_id']
                try:
                    if _re.match(r'dou\d+$', uid, _re.IGNORECASE):
                        info = fetch_user_info_by_unique_id(uid, session=self.session)
                    else:
                        info = fetch_user_info(uid, session=self.session)
                    if info and info.get('nickname'):
                        nick = info['nickname']
                        if not nick.startswith('ç¥žç§˜äºº') and not _re.match(r'dou\d+$', nick, _re.IGNORECASE):
                            rid = info.get('user_id', uid)
                            sec = info.get('sec_uid', '')
                            avatar = info.get('avatar_url', '')
                            conn.execute(
                                'UPDATE users SET user_name = ?, sec_uid = CASE WHEN ? != "" THEN ? ELSE sec_uid END, avatar_url = CASE WHEN ? != "" THEN ? ELSE avatar_url END, is_anonymous = 0 WHERE user_id = ?',
                                (nick, sec, sec, avatar, avatar, uid)
                            )
                            if rid and rid != uid:
                                conn.execute(
                                    'UPDATE users SET user_name = ?, sec_uid = CASE WHEN ? != "" THEN ? ELSE sec_uid END, avatar_url = CASE WHEN ? != "" THEN ? ELSE avatar_url END, is_anonymous = 0 WHERE user_id = ?',
                                    (nick, sec, sec, avatar, avatar, rid)
                                )
                            conn.commit()
                            done += 1
                            import time
                            time.sleep(2.5)
                            continue
                    # API è¿”å›žç©º + æ— è´¡çŒ® â†’ å‡ç”¨æˆ·ï¼Œç§»å‡ºåŒ¿åç»Ÿè®¡
                    has_c = conn.execute('SELECT COUNT(*) FROM contributions WHERE user_id = ?', (uid,)).fetchone()[0]
                    if has_c == 0:
                        conn.execute('UPDATE users SET is_anonymous = 0, anonymous_label = "fake" WHERE user_id = ?', (uid,))
                        conn.commit()
                        done += 1
                    import time
                    time.sleep(2.5)
                except Exception:
                    pass
            logger.info(f"[åŒ¿å] æ‰¹é‡è§£æžå®Œæˆ: {done}/{len(users)}")

        t = threading.Thread(target=_resolve, daemon=True, name='anon-resolve')
        t.start()

    def _fill_subscription_uid(self, user_name, real_uid):
        """当用户身份确认后，补填等待中的订阅记录 user_id。"""
        if not user_name or not real_uid:
            return
        try:
            cur = _get_conn().execute(
                'UPDATE gift_logs SET user_id = ?, user_name = ? WHERE user_id = \'\' AND user_name = ? AND (gift_name LIKE \'%会员%\' OR gift_name LIKE \'%星守护%\')',
                (real_uid, user_name, user_name)
            )
            if cur.rowcount > 0:
                logger.info(f"[è®¢é˜…] å¡«è¡¥ user_id: {user_name} â†’ {real_uid} (å…± {cur.rowcount} æ¡)")
        except Exception:
            pass


# â”€â”€ å†…éƒ¨ä¿¡å·å¼‚å¸¸ï¼ˆä¸åœ¨æ¨¡å—çº§æš´éœ²ï¼‰â”€â”€

class _WaitLiveSignal(Exception):
    """ç›´æ’­ç»“æŸï¼Œè¿›å…¥ç­‰å¾…æ¨¡å¼ã€‚"""
    pass


class _StopSignal(Exception):
    """ç›´æ’­é—´ç»“æŸï¼Œåœæ­¢é‡‡é›†ã€‚"""
    pass


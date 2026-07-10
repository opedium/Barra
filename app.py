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
import sqlite3
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
    set_sse_callback,
    init_gift_prices_table, recalculate_gift_price, get_price_change_history,
    query_user_retention, query_big_spenders, query_silent_whales,
    get_flow_counters, get_combo_buffer_size,
)
from service.fetcher import DouyinBarrage
from service.network import fetch_user_info_by_sec_uid, fetch_user_info_by_user_id, fetch_user_info

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
        try:
            conn.commit()
        except sqlite3.OperationalError as e:
            if 'not an error' not in str(e):
                raise

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
                    'message_rate': info.get('message_rate', 0),
                    'by_type': info.get('by_type', {}),
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


# ── Collection Stats API ──

@app.route('/api/collection-stats')
@require_auth
def api_collection_stats():
    """返回当前采集中的实时消息速率。"""
    statuses = _manager.get_all_status()
    live = [s for s in statuses if s.get('running')]
    return jsonify({
        'active_collectors': len(live),
        'streamers': [{
            'anchor_name': s.get('anchor_name'),
            'message_count': s.get('message_count', 0),
            'message_rate': s.get('message_rate', 0),
            'uptime_seconds': s.get('uptime_seconds', 0),
            'by_type': s.get('by_type', {}),
        } for s in live],
    })


# ═══════════════════════════════════════════════════════════════
#  Cookie Manager — manual cookie management
# ═══════════════════════════════════════════════════════════════

class CookieLoginManager:
    """Manages cookie file loading, validation, and manual cookie input."""

    def __init__(self):
        self._lock = threading.Lock()

    # ── Cookie file I/O ──────────────────────────────────

    def _cookie_file_path(self, filename='cookie.txt'):
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)

    def _save_cookies_file(self, cookies_dict, filename='cookie.txt'):
        """Write cookies to cookie.txt (or specified filename) in Netscape format."""
        path = self._cookie_file_path(filename)
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

    # ── Manual cookie save ──────────────────────────────

    def manual_save(self, cookie_string, filename='cookie.txt'):
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

        # Save to cookie file
        self._save_cookies_file(cookies, filename)
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

    def get_cookie_overview(self, filename='cookie.txt'):
        """Return a quick summary of the cookie file on disk."""
        path = self._cookie_file_path(filename)
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
        anchor_name = live['anchor_name']
        top = conn.execute('''
            SELECT c.user_id, COALESCE(NULLIF(u.user_name, ''), c.user_name) as user_name, c.consume,
                   CASE WHEN ? != '' THEN '[粉丝团:' || ? || ']' ELSE '' END AS fans_club,
                   COALESCE(u.grade, (SELECT grade FROM chat_logs WHERE user_id=c.user_id AND grade!='' ORDER BY id DESC LIMIT 1), '') as grade,
                   u.sec_uid, u.avatar_url,
                   (SELECT COUNT(*) FROM gift_logs WHERE session_id=c.session_id AND user_id=c.user_id) as gift_count,
                   (SELECT COUNT(*) FROM chat_logs WHERE session_id=c.session_id AND user_id=c.user_id) as chat_count,
                   (SELECT COALESCE(NULLIF(g.to_user_name, ''), s2.anchor_name, '')
                    FROM gift_logs g
                    LEFT JOIN sessions s2 ON s2.id = g.session_id
                    WHERE g.session_id=c.session_id AND g.user_id=c.user_id
                    GROUP BY COALESCE(NULLIF(g.to_user_name, ''), s2.anchor_name, '')
                    ORDER BY SUM(g.diamond_total) DESC LIMIT 1) as top_recipient
            FROM contributions c
            LEFT JOIN users u ON u.user_id = c.user_id
            WHERE c.session_id=? ORDER BY c.consume DESC LIMIT 10
        ''', (anchor_name, anchor_name, live['id'],)).fetchall()
        top_users = [dict(r) for r in top]
        # 尝试通过 API 解析匿名用户真实昵称
        if top_users:
            for u in top_users:
                if '***' in (u['user_name'] or '') and u['user_id'] and _rate_limiter.acquire():
                    try:
                        info = fetch_user_info(u['user_id'])
                        if info and info.get('nickname'):
                            new_name = info['nickname']
                            conn.execute('UPDATE users SET user_name = ? WHERE user_id = ?', (new_name, u['user_id']))
                            u['user_name'] = new_name
                    except Exception:
                        pass

    # 查找主播头像（从 users 表中匹配主播昵称 + sec_uid）
    anchor_avatar = ''
    if live:
        anchor_row = conn.execute(
            'SELECT avatar_url FROM users WHERE sec_uid != "" AND user_name = ? LIMIT 1',
            (live['anchor_name'],)
        ).fetchone()
        if anchor_row and anchor_row['avatar_url']:
            anchor_avatar = anchor_row['avatar_url']

    flow = get_flow_counters()
    flow['combo_buf'] = get_combo_buffer_size()
    return render_template('index.html',
        session=dict(live) if live else None,
        anchor_avatar=anchor_avatar,
        live_sessions=[dict(r) for r in live_sessions],
        stats={'total_gifts': total_gifts, 'total_chats': total_chats, 'total_sessions': total_sessions, 'today_gifts': today, 'today_chats': today_chats, 'today_users': today_users},
        sessions=[dict(r) for r in recent],
        recent_chats=[dict(r) for r in recent_chats],
        top_users=top_users,
        flow=flow,
        db_path=DB_PATH,
        auto_refresh=_web_config['auto_refresh'],
        auth_enabled=bool(_web_config['password']))


@app.route('/audit')
@require_auth
def audit():
    """数据完整性审计页。"""
    from base.parser import query_audit as _qa
    return render_template('audit.html',
        data=_qa(),
        auth_enabled=bool(_web_config['password']))


@app.route('/api/audit')
@require_auth
def api_audit():
    """审计诊断数据 API。"""
    from base.parser import query_audit as _qa
    return jsonify(_qa())


# ── 升级记录 ──

@app.route('/upgrades')
@require_auth
def upgrades():
    return render_template('upgrades.html', auth_enabled=bool(_web_config['password']))


@app.route('/api/upgrades')
@require_auth
def api_upgrades():
    from base.parser import query_upgrades
    upgrade_type = request.args.get('type', '')
    session_id = request.args.get('session_id', type=int)
    min_level = request.args.get('min_level', 0, type=int)
    page = request.args.get('page', 1, type=int)
    anchor_name = request.args.get('anchor_name', '').strip()
    room_id = request.args.get('room_id', '').strip()
    return jsonify(query_upgrades(upgrade_type=upgrade_type, session_id=session_id, min_level=min_level,
                                  anchor_name=anchor_name, room_id=room_id, page=page))


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


# ── Gift Price Management ──

@app.route('/gift-prices')
@require_auth
def gift_prices():
    return render_template('gift_prices.html', auth_enabled=bool(_web_config['password']))


@app.route('/api/gift-prices')
@require_auth
def api_gift_prices():
    """List gift prices with search, filter, and pagination."""
    conn = _get_conn()
    search = request.args.get('search', '').strip()
    source_filter = request.args.get('source', '').strip()
    page = request.args.get('page', 1, type=int)
    size = request.args.get('size', 50, type=int)
    offset = (page - 1) * size

    where_clauses = ['1=1']
    rg_where_clauses = ['1=1']
    params = []
    rg_params = []

    if search:
        where_clauses.append('gift_name LIKE ?')
        params.append(f'%{search}%')
        rg_where_clauses.append('r.gift_name LIKE ?')
        rg_params.append(f'%{search}%')

    if source_filter:
        where_clauses.append('source = ?')
        params.append(source_filter)
    # rg subquery has no source column — skip filter there

    w = ' AND '.join(where_clauses)
    rg_w = ' AND '.join(rg_where_clauses)

    total = conn.execute(f'''
        SELECT COUNT(*) FROM (
            SELECT gift_name FROM gift_prices WHERE {w}
            UNION
            SELECT r.gift_name FROM gift_id_registry r
            LEFT JOIN gift_prices p ON r.gift_name = p.gift_name
            WHERE p.id IS NULL AND {rg_w}
        )
    ''', params + rg_params).fetchone()[0]

    rows = conn.execute(f'''
        SELECT * FROM (
            SELECT
                g.id,
                COALESCE(g.gift_id, 0) AS gift_id,
                g.gift_name,
                g.diamond_count,
                g.source,
                g.is_limited_skin,
                COALESCE(d.logs, 0) AS logs,
                COALESCE(d.total_dia, 0) AS total_dia
            FROM gift_prices g
            LEFT JOIN (
                SELECT gift_name, COUNT(*) AS logs, SUM(diamond_total) AS total_dia
                FROM gift_logs GROUP BY gift_name
            ) d ON d.gift_name = g.gift_name
            WHERE {w}
            UNION ALL
            SELECT
                -r.gift_id AS id,
                r.gift_id AS gift_id,
                r.gift_name,
                r.diamond_count,
                r.source AS source,
                0 AS is_limited_skin,
                COALESCE(d2.logs, 0) AS logs,
                COALESCE(d2.total_dia, 0) AS total_dia
            FROM gift_id_registry r
            LEFT JOIN gift_prices p ON r.gift_name = p.gift_name
            LEFT JOIN (
                SELECT gift_name, COUNT(*) AS logs, SUM(diamond_total) AS total_dia
                FROM gift_logs GROUP BY gift_name
            ) d2 ON d2.gift_name = r.gift_name
            WHERE p.id IS NULL AND {rg_w}
        )
        ORDER BY total_dia DESC, gift_name
        LIMIT ? OFFSET ?
    ''', params + rg_params + [size, offset]).fetchall()

    # Confirmed count (entries in gift_prices — has a user-set price)
    confirmed = conn.execute('SELECT COUNT(*) FROM gift_prices').fetchone()[0]
    unregistered = conn.execute('''
        SELECT COUNT(*) FROM gift_id_registry r
        LEFT JOIN gift_prices p ON r.gift_name = p.gift_name
        WHERE p.id IS NULL AND r.source = 'auto'
    ''').fetchone()[0]
    registry_only = conn.execute('''
        SELECT COUNT(*) FROM gift_id_registry r
        LEFT JOIN gift_prices p ON r.gift_name = p.gift_name
        WHERE p.id IS NULL AND r.source = 'official'
    ''').fetchone()[0]

    return jsonify({
        'prices': [dict(r) for r in rows],
        'total': total,
        'confirmed': confirmed,
        'unregistered': unregistered,
        'registry_only': registry_only,
        'page': page,
        'size': size,
    })


@app.route('/api/gift-prices', methods=['POST'])
@require_auth
def api_gift_price_create():
    """Create a new gift price entry."""
    data = request.get_json(force=True, silent=True) or {}
    gift_name = (data.get('gift_name') or '').strip()
    if not gift_name:
        return jsonify({'error': 'gift_name is required'}), 400
    diamond_count = int(data.get('diamond_count', 0))
    if diamond_count < 0:
        return jsonify({'error': 'price must be >= 0'}), 400

    conn = _get_conn()
    # Check if already exists in gift_prices (exact match)
    existing = conn.execute('SELECT id FROM gift_prices WHERE gift_name = ?', (gift_name,)).fetchone()
    if existing:
        return jsonify({'error': f'礼物 "{gift_name}" 已存在'}), 409

    # Check for similar names (case-insensitive, to catch typos/case differences)
    similar = conn.execute('SELECT gift_name, diamond_count, source FROM gift_prices WHERE LOWER(gift_name) = LOWER(?)', (gift_name,)).fetchall()
    if similar:
        s = similar[0]
        return jsonify({
            'error': f'礼物 "{gift_name}" 与已有记录 "{s["gift_name"]}" 仅大小写不同',
            'similar_name': s['gift_name'],
        }), 409

    # Check if name exists in gift_logs data (not yet in gift_prices)
    log_count = conn.execute('SELECT COUNT(*) AS cnt FROM gift_logs WHERE gift_name = ?', (gift_name,)).fetchone()[0]
    if log_count > 0:
        # Name already exists in live data — warn but allow creation
        pass  # handled in response below

    conn.execute('''
        INSERT INTO gift_prices (gift_name, gift_id, diamond_count, source, is_limited_skin)
        VALUES (?, ?, ?, ?, ?)
    ''', (
        gift_name,
        int(data.get('gift_id', 0)),
        diamond_count,
        data.get('source', 'manual'),
        1 if data.get('is_limited_skin') else 0,
    ))
    conn.commit()

    new_id = conn.execute('SELECT id FROM gift_prices WHERE gift_name = ?', (gift_name,)).fetchone()[0]
    result = {'success': True, 'id': new_id, 'gift_name': gift_name}
    if log_count > 0:
        result['warning'] = f'礼物名 "{gift_name}" 已在 {log_count} 条直播记录中出现，价格已覆盖自动检测值'
    return jsonify(result), 201


@app.route('/api/gift-prices/<int:price_id>')
@require_auth
def api_gift_price(price_id):
    conn = _get_conn()
    row = conn.execute('SELECT * FROM gift_prices WHERE id = ?', (price_id,)).fetchone()
    if not row:
        return jsonify({'error': 'not found'}), 404
    return jsonify(dict(row))


@app.route('/api/gift-prices/<int:price_id>', methods=['POST'])
@require_auth
def api_gift_price_update(price_id):
    """Update a gift price. If diamond_count changes, optionally recalculate."""
    conn = _get_conn()
    row = conn.execute('SELECT * FROM gift_prices WHERE id = ?', (price_id,)).fetchone()
    if not row:
        return jsonify({'error': 'not found'}), 404

    data = request.get_json(force=True, silent=True) or {}

    updates = []
    params = []

    if 'diamond_count' in data:
        new_price = int(data['diamond_count'])
        if new_price < 0:
            return jsonify({'error': 'price must be >= 0'}), 400
        old_price = row['diamond_count']
        updates.append('diamond_count = ?')
        params.append(new_price)
    else:
        old_price = row['diamond_count']
        new_price = old_price

    if 'gift_id' in data:
        updates.append('gift_id = ?')
        params.append(int(data['gift_id']))

    if 'source' in data:
        updates.append('source = ?')
        params.append(data['source'])

    if 'is_limited_skin' in data:
        updates.append('is_limited_skin = ?')
        params.append(1 if data['is_limited_skin'] else 0)

    if updates:
        updates.append('updated_at = datetime("now", "+8 hours")')
        params.append(price_id)
        conn.execute(f'UPDATE gift_prices SET {", ".join(updates)} WHERE id = ?', params)
        conn.commit()

    recalculate = data.get('recalculate', False)
    recalc_result = None
    if recalculate and new_price != old_price:
        recalc_result = recalculate_gift_price(
            row['gift_name'], new_price, old_price,
            notes=data.get('notes', '')
        )

    return jsonify({
        'success': True,
        'gift_name': row['gift_name'],
        'old_price': old_price,
        'new_price': new_price if 'diamond_count' in data else old_price,
        'recalculated': recalc_result is not None,
        'recalc_result': recalc_result,
    })


@app.route('/api/gift-prices/bulk-recalculate', methods=['POST'])
@require_auth
def api_gift_prices_bulk_recalculate():
    """Recalculate all gift_logs rows for a specific gift_name using its current price."""
    data = request.get_json(force=True, silent=True) or {}
    price_id = data.get('price_id')
    gift_name = data.get('gift_name')

    conn = _get_conn()

    if price_id:
        row = conn.execute('SELECT * FROM gift_prices WHERE id = ?', (price_id,)).fetchone()
    elif gift_name:
        row = conn.execute('SELECT * FROM gift_prices WHERE gift_name = ?', (gift_name,)).fetchone()
    else:
        return jsonify({'error': 'price_id or gift_name required'}), 400

    if not row:
        return jsonify({'error': 'gift price not found'}), 404

    result = recalculate_gift_price(row['gift_name'], row['diamond_count'], row['diamond_count'],
                                    notes='Bulk recalculation')
    return jsonify({'success': True, 'result': result})


@app.route('/api/gift-prices/audit')
@require_auth
def api_gift_prices_audit():
    """Return audit data: discrepancies, needs-review items, change history."""
    conn = _get_conn()

    # Section 1: Price discrepancies — same gift_name, different unit prices in gift_logs
    discrepancies = conn.execute('''
        SELECT gift_name, diamond_total / MAX(gift_count, 1) AS unit_price,
               COUNT(*) AS occurrences,
               COUNT(DISTINCT session_id) AS sessions
        FROM gift_logs
        WHERE gift_count > 0
        GROUP BY gift_name, unit_price
        HAVING COUNT(*) > 0
        ORDER BY gift_name, occurrences DESC
    ''').fetchall()

    # Group by gift_name, find conflicts
    disc_dict = {}
    for r in discrepancies:
        name = r['gift_name']
        if name not in disc_dict:
            disc_dict[name] = {'gift_name': name, 'prices': [], 'total_occurrences': 0}
        disc_dict[name]['prices'].append({
            'unit_price': r['unit_price'],
            'occurrences': r['occurrences'],
            'sessions': r['sessions'],
        })
        disc_dict[name]['total_occurrences'] += r['occurrences']

    audit_discrepancies = [
        d for d in disc_dict.values() if len(d['prices']) > 1
    ]

    # Cross-reference with gift_prices table
    for d in audit_discrepancies:
        db_row = conn.execute(
            'SELECT diamond_count, source FROM gift_prices WHERE gift_name = ?',
            (d['gift_name'],)
        ).fetchone()
        d['db_price'] = db_row['diamond_count'] if db_row else None
        d['db_source'] = db_row['source'] if db_row else 'unknown'

    # Section 2: Change history
    history = get_price_change_history(30)

    return jsonify({
        'discrepancies': audit_discrepancies,
        'history': history,
    })


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

@app.route('/leaderboard')
@require_auth
def leaderboard():
    return render_template('leaderboard.html', auth_enabled=bool(_web_config['password']))


@app.route('/api/leaderboard')
def api_leaderboard():
    threshold = request.args.get('threshold', 1000, type=int)
    period = request.args.get('period', 'session')
    page = request.args.get('page', 1, type=int)
    size = request.args.get('size', 50, type=int)
    sort_by = request.args.get('sort_by', 'consume')
    year_month = request.args.get('year_month', '')
    min_consume = request.args.get('min_consume', 0, type=int)
    streamer = request.args.get('streamer', '').strip()
    room_id = request.args.get('room_id', '').strip()
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')

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

    data = query_leaderboard(threshold, period, page, size, session_id, year_month, min_consume, anchor_name=streamer, room_id=room_id, start_date=start_date, end_date=end_date)
    if sort_by == 'sessions' and data.get('users'):
        data['users'].sort(key=lambda u: (-u.get('sessions_count', 0), -u.get('consume', 0)))
        for i, u in enumerate(data['users']):
            u['rank'] = i + 1
    return jsonify(data)


@app.route('/api/leaderboard/streamers')
def api_leaderboard_streamers():
    """返回所有有贡献记录的主播名+room_id（用于榜单筛选下拉框）。"""
    conn = _get_conn()
    rows = conn.execute('''
        SELECT DISTINCT TRIM(s.anchor_name) AS anchor_name, s.room_id
        FROM sessions s
        WHERE s.anchor_name != '' AND s.anchor_name IS NOT NULL
        ORDER BY s.anchor_name
    ''').fetchall()
    return jsonify([{'name': r['anchor_name'], 'id': r['room_id']} for r in rows])


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

    # 头像/sec_uid 缺失时尝试从抖音 API 补全（受速率限制保护）
    # 安全流程：先通过 ID 获取 sec_uid，再用 sec_uid 获取完整用户信息
    need_avatar = not data.get('avatar_url')
    need_sec_uid = not data.get('sec_uid')
    if need_avatar or need_sec_uid:
        existing_sec_uid = data.get('sec_uid', '') or ''
        fetched = None
        if _rate_limiter.acquire():
            if existing_sec_uid:
                # 已有 sec_uid → 直接获取完整信息（1 次 API 调用，最高效）
                fetched = fetch_user_info_by_sec_uid(existing_sec_uid)
            if not fetched:
                # 无 sec_uid 或 sec_uid API 失败
                # → 两步式安全流程：ID → sec_uid → 完整信息
                fetched = fetch_user_info(uid)
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
            if fetched.get('nickname'):
                data['user_name'] = fetched['nickname']
                updates.append('user_name = ?')
                params.append(fetched['nickname'])
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
            '''SELECT g.gift_name, g.gift_id, g.gift_count, g.diamond_total, g.created_at,
                      g.to_user_name,
                      COALESCE(NULLIF(g.to_user_name, ''), s.anchor_name, '') as to_user,
                      COALESCE(s.anchor_name, '') as anchor_name,
                      COALESCE(NULLIF(g.grade, ''), u.grade, '') as grade,
                      COALESCE(NULLIF(g.fans_club, ''), u.fans_club, '') as fans_club
               FROM gift_logs g
               LEFT JOIN sessions s ON s.id = g.session_id
               LEFT JOIN users u ON u.user_id = g.user_id
               WHERE g.user_id=? AND g.session_id=?
               ORDER BY g.created_at DESC LIMIT 5000''',
            (user_id, session_id)).fetchall()
    else:
        rows = conn.execute(
            '''SELECT g.gift_name, g.gift_id, g.gift_count, g.diamond_total, g.created_at,
                      g.to_user_name,
                      COALESCE(NULLIF(g.to_user_name, ''), s.anchor_name, '') as to_user,
                      COALESCE(s.anchor_name, '') as anchor_name,
                      COALESCE(NULLIF(g.grade, ''), u.grade, '') as grade,
                      COALESCE(NULLIF(g.fans_club, ''), u.fans_club, '') as fans_club
               FROM gift_logs g
               LEFT JOIN sessions s ON s.id = g.session_id
               LEFT JOIN users u ON u.user_id = g.user_id
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

    # 有 sec_uid 但无 avatar_url → 调用抖音 API 补全（也会返回最新昵称）
    if user['sec_uid']:
        if not _rate_limiter.acquire():
            return jsonify({'error': 'rate_limited', 'message': '请求频率过高，请稍后重试'}), 429
        info = fetch_user_info_by_sec_uid(user['sec_uid'])
        if info:
            updates = []
            params = []
            if info.get('avatar_url'):
                updates.append('avatar_url = ?')
                params.append(info['avatar_url'])
            if info.get('nickname') and info['nickname'] != user['user_name']:
                updates.append('user_name = ?')
                params.append(info['nickname'])
            if updates:
                params.append(user_id)
                conn.execute(
                    f'UPDATE users SET {", ".join(updates)} WHERE user_id = ?',
                    params
                )
                conn.commit()
            return jsonify({
                'user_id': user['user_id'],
                'nickname': info.get('nickname', user['user_name']),
                'sec_uid': user['sec_uid'],
                'avatar_url': info.get('avatar_url', user['avatar_url'] or ''),
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

    # 无 sec_uid：两步式安全获取 — 先用 ID 获取 sec_uid，再用 sec_uid 获取完整信息
    if not _rate_limiter.acquire():
        return jsonify({'error': 'rate_limited', 'message': '请求频率过高，请稍后重试'}), 429
    info = fetch_user_info(user_id)
    if info:
        # 回写获取到的数据到本地 DB
        updates = []
        params = []
        if info.get('avatar_url'):
            updates.append('avatar_url = ?')
            params.append(info['avatar_url'])
        if info.get('sec_uid'):
            updates.append('sec_uid = ?')
            params.append(info['sec_uid'])
        if info.get('nickname'):
            updates.append('user_name = ?')
            params.append(info['nickname'])
        if updates:
            params.append(user_id)
            conn.execute(
                f'UPDATE users SET {", ".join(updates)} WHERE user_id = ?',
                params
            )
            conn.commit()
        return jsonify({
            'user_id': user_id,
            'nickname': info.get('nickname', user['user_name']),
            'sec_uid': info.get('sec_uid', ''),
            'display_id': info.get('display_id', ''),
            'avatar_url': info.get('avatar_url', ''),
            'grade': user['grade'] or '',
            'fans_club': user['fans_club'] or '',
            'source': 'api',
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


@app.route('/api/anonymous/resolve', methods=['POST'])
@require_auth
def api_anonymous_resolve():
    """批量解析所有未解决的匿名用户。"""
    from base.parser import _get_conn
    from service.network import fetch_user_info, fetch_user_info_by_unique_id
    import re as _re
    conn = _get_conn()
    users = conn.execute("""
        SELECT user_id, user_name FROM users
        WHERE is_anonymous = 1 AND (user_name LIKE 'dou%' OR user_name LIKE '神秘人%')
        ORDER BY last_seen DESC
    """).fetchall()
    import time
    resolved = 0
    failed = 0
    for u in users:
        uid = u['user_id']
        try:
            if _re.match(r'dou\d+$', uid, _re.IGNORECASE):
                info = fetch_user_info_by_unique_id(uid)
            else:
                info = fetch_user_info(uid)
            if info and info.get('nickname'):
                nick = info['nickname']
                if not nick.startswith('神秘人') and not _re.match(r'dou\d+$', nick, _re.IGNORECASE):
                    resolved_id = info.get('user_id', uid)
                    sec = info.get('sec_uid', '')
                    avatar = info.get('avatar_url', '')
                    conn.execute(
                        'UPDATE users SET user_name = ?, sec_uid = CASE WHEN ? != "" THEN ? ELSE sec_uid END, avatar_url = CASE WHEN ? != "" THEN ? ELSE avatar_url END, is_anonymous = 0 WHERE user_id = ?',
                        (nick, sec, sec, avatar, avatar, uid)
                    )
                    if resolved_id and resolved_id != uid:
                        conn.execute(
                            'UPDATE users SET user_name = ?, sec_uid = CASE WHEN ? != "" THEN ? ELSE sec_uid END, avatar_url = CASE WHEN ? != "" THEN ? ELSE avatar_url END, is_anonymous = 0 WHERE user_id = ?',
                            (nick, sec, sec, avatar, avatar, resolved_id)
                        )
                    conn.commit()
                    resolved += 1
                    continue
            # API 返回空（用户不存在）+ 无贡献 → 假用户（如订阅垃圾ID），移出匿名统计
            has_consume = conn.execute('SELECT COUNT(*) FROM contributions WHERE user_id = ?', (uid,)).fetchone()[0]
            if has_consume == 0:
                conn.execute('UPDATE users SET is_anonymous = 0, anonymous_label = "fake" WHERE user_id = ?', (uid,))
                conn.commit()
                resolved += 1  # 算作"已处理"
                continue
            failed += 1
        except Exception:
            failed += 1
        time.sleep(2.5)
    return jsonify({'resolved': resolved, 'failed': failed, 'total': len(users)})


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
    limit = request.args.get('limit', 9999, type=int)
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


@app.route('/api/cookie-login/manual', methods=['POST'])
def api_cookie_login_manual():
    """Save manually-pasted cookie string."""
    data = request.get_json(force=True, silent=True) or {}
    cookie_string = data.get('cookie_string', '').strip()
    ok, message, details = _cookie_manager.manual_save(cookie_string)
    return jsonify({'success': ok, 'message': message, 'details': details})


# ── Dual WebSocket Config API ──

@app.route('/api/dual-ws-config')
@require_auth
def api_dual_ws_config():
    """Return current dual WS config from config.yaml network section."""
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.yaml')
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f) or {}
    except Exception:
        cfg = {}
    net = cfg.get('network', {})
    return jsonify({
        'dual_ws': net.get('dual_ws', False),
        'dual_ws_identity': net.get('dual_ws_identity', 'smart'),
        'ws_host2': net.get('ws_host2', None),
        'ws_cookie2': net.get('ws_cookie2', None),
    })


@app.route('/api/dual-ws-config', methods=['POST'])
@require_auth
def api_dual_ws_config_save():
    """Save dual WS config to config.yaml network section."""
    data = request.get_json(force=True, silent=True) or {}
    allowed_keys = {'dual_ws', 'dual_ws_identity', 'ws_host2', 'ws_cookie2'}
    updates = {k: v for k, v in data.items() if k in allowed_keys}

    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.yaml')
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f) or {}
    except Exception:
        cfg = {}

    if 'network' not in cfg:
        cfg['network'] = {}
    cfg['network'].update(updates)

    tmp = config_path + '.tmp'
    try:
        with open(tmp, 'w', encoding='utf-8') as f:
            yaml.dump(cfg, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
        os.replace(tmp, config_path)
    except Exception as e:
        return jsonify({'success': False, 'message': f'保存失败: {e}'}), 500

    return jsonify({'success': True, 'message': '双 WS 配置已保存'})


@app.route('/api/cookie2-status')
@require_auth
def api_cookie2_status():
    """Return cookie2.txt overview (same shape as /api/cookie-status)."""
    return jsonify(_cookie_manager.get_cookie_overview('cookie2.txt'))


@app.route('/api/cookie2-login/manual', methods=['POST'])
@require_auth
def api_cookie2_login_manual():
    """Save manually-pasted cookie string to cookie2.txt."""
    data = request.get_json(force=True, silent=True) or {}
    cookie_string = data.get('cookie_string', '').strip()
    ok, message, details = _cookie_manager.manual_save(cookie_string, 'cookie2.txt')
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
    streamer = request.args.get('streamer', '').strip()
    room_id = request.args.get('room_id', '').strip()
    session_id = request.args.get('session_id', None, type=int)
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')

    if period == 'session' and session_id is None:
        conn = _get_conn()
        s = conn.execute("SELECT id FROM sessions WHERE status='live' ORDER BY id DESC LIMIT 1").fetchone()
        if s:
            session_id = s[0]
        else:
            return _make_csv_response([], ['user_id', 'user_name', 'consume'], 'leaderboard.csv')

    # Fetch ALL pages (size=999999)
    data = query_leaderboard(threshold, period, 1, 999999, session_id, year_month, min_consume, anchor_name=streamer, room_id=room_id, start_date=start_date, end_date=end_date)
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


# ═══════════════════════════════════════════════════════════════
#  User Behavior Analytics
# ═══════════════════════════════════════════════════════════════

@app.route('/analytics')
@require_auth
def analytics():
    return render_template('analytics.html', auth_enabled=bool(_web_config['password']))


@app.route('/api/analytics/retention')
@require_auth
def api_analytics_retention():
    anchor = request.args.get('anchor', '')
    period = request.args.get('period', '30d')
    tier = request.args.get('tier', 0, type=int)
    page = request.args.get('page', 1, type=int)
    size = request.args.get('size', 20, type=int)
    try:
        data = query_user_retention(anchor, period, tier, page, size)
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/analytics/big-spenders')
@require_auth
def api_analytics_big_spenders():
    min_consume = request.args.get('min_consume', 10000, type=int)
    trend = request.args.get('trend', 'all')
    anchor = request.args.get('anchor', '')
    page = request.args.get('page', 1, type=int)
    size = request.args.get('size', 50, type=int)
    try:
        data = query_big_spenders(min_consume, trend, anchor, page, size)
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/analytics/silent-whales')
@require_auth
def api_analytics_silent_whales():
    threshold = request.args.get('threshold', 30000, type=int)
    silent_days = request.args.get('silent_days', 7, type=int)
    anchor = request.args.get('anchor', '')
    try:
        data = query_silent_whales(threshold, silent_days, anchor)
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def main():
    parser = argparse.ArgumentParser(description='弹幕后台管理面板')
    parser.add_argument('--host', default=_web_config['host'])
    parser.add_argument('--port', default=_web_config['port'], type=int)
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--auto-start', action='store_true',
                        help='Auto-start streamers that were enabled on last shutdown')
    args = parser.parse_args()
    os.makedirs(os.path.join(os.path.dirname(__file__), 'data'), exist_ok=True)

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

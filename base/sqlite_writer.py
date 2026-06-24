"""SQLite 数据库模块 — 替代 CSV 输出

所有采集数据写入 SQLite，供 Flask Web 端查询。
每60s由 fetcher._stats_task 调用 flush_to_sqlite()。
"""

import logging
import os
import sqlite3
import threading
from datetime import datetime

logger = logging.getLogger(__name__)

DB_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
DB_PATH = os.path.join(DB_DIR, 'douyin_barrage.db')
_local = threading.local()

# ── 启动时自动迁移旧表结构 ──
try:
    os.makedirs(DB_DIR, exist_ok=True)
    _migrate_conn = sqlite3.connect(DB_PATH)
    _migrate_conn.execute('PRAGMA journal_mode=WAL')
    try:
        _migrate_conn.execute('ALTER TABLE users ADD COLUMN grade TEXT DEFAULT ""')
        _migrate_conn.commit()
    except sqlite3.OperationalError:
        pass
    try:
        _migrate_conn.execute('ALTER TABLE gift_logs ADD COLUMN grade TEXT DEFAULT ""')
        _migrate_conn.execute('ALTER TABLE gift_logs ADD COLUMN fans_club TEXT DEFAULT ""')
        _migrate_conn.commit()
    except sqlite3.OperationalError:
        pass
    _migrate_conn.close()
except Exception:
    pass


def _get_conn():
    if not hasattr(_local, 'conn') or _local.conn is None:
        os.makedirs(DB_DIR, exist_ok=True)
        _local.conn = sqlite3.connect(DB_PATH)
        _local.conn.execute('PRAGMA journal_mode=WAL')
        _local.conn.execute('PRAGMA busy_timeout=5000')
        _local.conn.row_factory = sqlite3.Row
    return _local.conn


def init_db():
    conn = _get_conn()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            room_id TEXT NOT NULL,
            anchor_name TEXT DEFAULT '',
            start_time DATETIME DEFAULT CURRENT_TIMESTAMP,
            end_time DATETIME,
            status TEXT DEFAULT 'live'
        );
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT UNIQUE NOT NULL,
            user_name TEXT NOT NULL,
            fans_club TEXT DEFAULT '',
            grade TEXT DEFAULT '',
            is_anonymous INTEGER DEFAULT 0,
            anonymous_label TEXT DEFAULT '',
            first_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
            last_seen DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS contributions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL REFERENCES sessions(id),
            user_id TEXT NOT NULL,
            user_name TEXT NOT NULL,
            consume INTEGER DEFAULT 0,
            rank INTEGER DEFAULT 0,
            fans_club TEXT DEFAULT '',
            source TEXT DEFAULT 'websocket',
            qualified_1000 INTEGER DEFAULT 0,
            qualified_3000 INTEGER DEFAULT 0,
            qualified_10000 INTEGER DEFAULT 0,
            qualified_100000 INTEGER DEFAULT 0,
            recorded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(session_id, user_id)
        );
        CREATE TABLE IF NOT EXISTS chat_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER REFERENCES sessions(id),
            user_id TEXT NOT NULL,
            user_name TEXT NOT NULL,
            content TEXT NOT NULL,
            grade TEXT DEFAULT '',
            fans_club TEXT DEFAULT '',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS gift_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER REFERENCES sessions(id),
            user_id TEXT NOT NULL,
            user_name TEXT NOT NULL,
            gift_name TEXT NOT NULL,
            gift_count INTEGER DEFAULT 1,
            diamond_total INTEGER DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS daily_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            user_name TEXT NOT NULL,
            date TEXT NOT NULL,
            total_consume INTEGER DEFAULT 0,
            sessions_1000 INTEGER DEFAULT 0,
            sessions_3000 INTEGER DEFAULT 0,
            sessions_10000 INTEGER DEFAULT 0,
            sessions_100000 INTEGER DEFAULT 0,
            gift_count INTEGER DEFAULT 0,
            chat_count INTEGER DEFAULT 0,
            UNIQUE(user_id, date)
        );
        CREATE TABLE IF NOT EXISTS monthly_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            user_name TEXT NOT NULL,
            year_month TEXT NOT NULL,
            total_consume INTEGER DEFAULT 0,
            sessions_1000 INTEGER DEFAULT 0,
            sessions_3000 INTEGER DEFAULT 0,
            sessions_10000 INTEGER DEFAULT 0,
            sessions_100000 INTEGER DEFAULT 0,
            days_active INTEGER DEFAULT 0,
            max_rank INTEGER DEFAULT 0,
            UNIQUE(user_id, year_month)
        );
        CREATE INDEX IF NOT EXISTS idx_contributions_session ON contributions(session_id);
        CREATE INDEX IF NOT EXISTS idx_contributions_user ON contributions(user_id);
        CREATE INDEX IF NOT EXISTS idx_chat_logs_user ON chat_logs(user_id);
        CREATE INDEX IF NOT EXISTS idx_monthly_stats ON monthly_stats(year_month, sessions_1000 DESC);
        CREATE INDEX IF NOT EXISTS idx_daily_stats ON daily_stats(date, sessions_1000 DESC);
        CREATE INDEX IF NOT EXISTS idx_contributions_qualified ON contributions(qualified_1000);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_gift_dedup ON gift_logs(session_id, user_id, gift_name, diamond_total, gift_count);
    ''')
    # 兼容旧表：给 users 表补充 grade 字段
    try:
        conn.execute('ALTER TABLE users ADD COLUMN grade TEXT DEFAULT ""')
    except Exception:
        pass
    conn.commit()
    logger.info(f"[DB] 已初始化: {DB_PATH}")
    return True


def create_session(room_id, anchor_name=''):
    conn = _get_conn()
    cur = conn.execute('INSERT INTO sessions (room_id, anchor_name) VALUES (?, ?)', (room_id, anchor_name))
    conn.commit()
    sid = cur.lastrowid
    logger.info(f"[DB] 新场次 #{sid}: {anchor_name} ({room_id})")
    return sid


def end_session(session_id):
    conn = _get_conn()
    conn.execute('UPDATE sessions SET end_time = datetime("now"), status = "ended" WHERE id = ?', (session_id,))
    conn.commit()
    logger.info(f"[DB] 场次 #{session_id} 已结束")


def upsert_user(user_id, user_name, grade='', fans_club=''):
    """更新或插入用户信息（财富等级、粉丝团等）。线程安全。"""
    if not user_id:
        return
    conn = _get_conn()
    conn.execute('''
        INSERT INTO users (user_id, user_name, grade, fans_club, last_seen)
        VALUES (?, ?, ?, ?, datetime("now"))
        ON CONFLICT(user_id) DO UPDATE SET
            user_name = CASE WHEN ? != '' THEN ? ELSE user_name END,
            grade = CASE WHEN ? != '' THEN ? ELSE grade END,
            fans_club = CASE WHEN ? != '' THEN ? ELSE fans_club END,
            last_seen = datetime("now")
    ''', (user_id, user_name, grade, fans_club,
          user_name, user_name,
          grade, grade,
          fans_club, fans_club))
    conn.commit()


def flush_to_sqlite(session_id, contribution_users):
    """将贡献缓存写入 SQLite（含达标计数，防重复）。"""
    conn = _get_conn()
    if not contribution_users:
        return
    today = datetime.now().strftime('%Y-%m-%d')
    month = datetime.now().strftime('%Y-%m')

    for u in contribution_users:
        uid = u.get('user_id', '')
        nick = u.get('nick', '')
        consume = u.get('consume', 0) or 0
        fans_club = u.get('fans_club', '')
        grade = u.get('grade', '')
        source = 'websocket'

        if not uid:
            continue

        # 同步更新 users 表的粉丝团和财富等级信息
        conn.execute('''
            INSERT INTO users (user_id, user_name, fans_club, grade, last_seen)
            VALUES (?, ?, ?, ?, datetime("now"))
            ON CONFLICT(user_id) DO UPDATE SET
                fans_club = CASE WHEN ? != '' THEN ? ELSE fans_club END,
                grade = CASE WHEN ? != '' THEN ? ELSE grade END,
                last_seen = datetime("now")
        ''', (uid, nick, fans_club, grade, fans_club, fans_club, grade, grade))

        prev = conn.execute(
            'SELECT qualified_1000, qualified_3000, qualified_10000, qualified_100000 FROM contributions WHERE session_id=? AND user_id=?',
            (session_id, uid)
        ).fetchone()

        pq_1000 = prev['qualified_1000'] if prev else 0
        pq_3000 = prev['qualified_3000'] if prev else 0
        q_1000 = 1 if consume >= 1000 else 0
        q_3000 = 1 if consume >= 3000 else 0
        q_10000 = 1 if consume >= 10000 else 0
        q_100000 = 1 if consume >= 100000 else 0

        conn.execute('''
            INSERT INTO contributions
                (session_id, user_id, user_name, consume, rank, fans_club, source,
                 qualified_1000, qualified_3000, qualified_10000, qualified_100000)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(session_id, user_id) DO UPDATE SET
                consume = excluded.consume,
                qualified_1000 = MAX(qualified_1000, excluded.qualified_1000),
                qualified_3000 = MAX(qualified_3000, excluded.qualified_3000),
                qualified_10000 = MAX(qualified_10000, excluded.qualified_10000),
                qualified_100000 = MAX(qualified_100000, excluded.qualified_100000)
        ''', (session_id, uid, nick, consume, 0, fans_club, source,
              q_1000, q_3000, q_10000, q_100000))

        if q_1000 and not pq_1000:
            conn.execute('''
                INSERT INTO daily_stats (user_id, user_name, date, sessions_1000, total_consume)
                VALUES (?, ?, ?, 1, ?)
                ON CONFLICT(user_id, date) DO UPDATE SET
                    sessions_1000 = sessions_1000 + 1,
                    total_consume = total_consume + ?
            ''', (uid, nick, today, consume, consume))
            conn.execute('''
                INSERT INTO monthly_stats (user_id, user_name, year_month, sessions_1000, total_consume)
                VALUES (?, ?, ?, 1, ?)
                ON CONFLICT(user_id, year_month) DO UPDATE SET
                    sessions_1000 = sessions_1000 + 1,
                    total_consume = total_consume + ?
            ''', (uid, nick, month, consume, consume))

        if q_3000 and not pq_3000:
            conn.execute('UPDATE daily_stats SET sessions_3000 = sessions_3000 + 1 WHERE user_id = ? AND date = ?', (uid, today))
            conn.execute('UPDATE monthly_stats SET sessions_3000 = sessions_3000 + 1 WHERE user_id = ? AND year_month = ?', (uid, month))

        if q_10000 and not pq_10000:
            conn.execute('UPDATE daily_stats SET sessions_10000 = sessions_10000 + 1 WHERE user_id = ? AND date = ?', (uid, today))
            conn.execute('UPDATE monthly_stats SET sessions_10000 = sessions_10000 + 1 WHERE user_id = ? AND year_month = ?', (uid, month))

        if q_100000 and not pq_100000:
            conn.execute('UPDATE daily_stats SET sessions_100000 = sessions_100000 + 1 WHERE user_id = ? AND date = ?', (uid, today))
            conn.execute('UPDATE monthly_stats SET sessions_100000 = sessions_100000 + 1 WHERE user_id = ? AND year_month = ?', (uid, month))

    conn.commit()


def record_chat(session_id, user_id, user_name, content, grade='', fans_club=''):
    conn = _get_conn()
    conn.execute('INSERT OR IGNORE INTO chat_logs (session_id, user_id, user_name, content, grade, fans_club) VALUES (?, ?, ?, ?, ?, ?)',
                 (session_id, user_id, user_name, content, grade, fans_club))
    conn.commit()


def record_gift(session_id, user_id, user_name, gift_name, gift_count, diamond_total, grade='', fans_club=''):
    """记录礼物（UNIQUE 约束防重复，parser.py 的 delta 去重配合使用）。"""
    conn = _get_conn()
    # 兼容旧表：没有 grade/fans_club 列时静默忽略
    try:
        conn.execute('INSERT OR IGNORE INTO gift_logs (session_id, user_id, user_name, gift_name, gift_count, diamond_total, grade, fans_club) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                     (session_id, user_id, user_name, gift_name, gift_count, diamond_total, grade, fans_club))
    except Exception:
        conn.execute('INSERT OR IGNORE INTO gift_logs (session_id, user_id, user_name, gift_name, gift_count, diamond_total) VALUES (?, ?, ?, ?, ?, ?)',
                     (session_id, user_id, user_name, gift_name, gift_count, diamond_total))
    conn.commit()


# ── 查询接口 ──

def query_leaderboard(threshold=1000, period='session', page=1, size=100, session_id=None, year_month=''):
    conn = _get_conn()
    offset = (page - 1) * size

    if period == 'session' and session_id:
        if threshold > 0:
            col = f'qualified_{threshold}'
            where_extra = f'AND c.{col} = 1'
            total = conn.execute(f'SELECT COUNT(*) FROM contributions WHERE session_id = ? AND {col} = 1', (session_id,)).fetchone()[0]
        else:
            where_extra = ''
            total = conn.execute('SELECT COUNT(*) FROM contributions WHERE session_id = ? AND consume > 0', (session_id,)).fetchone()[0]
        rows = conn.execute(f'''
            SELECT c.user_id, c.user_name, c.consume,
                   COALESCE(
                       NULLIF(u.fans_club, ''),
                       NULLIF(c.fans_club, ''),
                       (SELECT fans_club FROM chat_logs WHERE user_id = c.user_id AND fans_club != '' ORDER BY id DESC LIMIT 1),
                       ''
                   ) AS fans_club,
                   COALESCE(
                       u.grade,
                       (SELECT grade FROM chat_logs WHERE user_id = c.user_id AND grade != '' ORDER BY id DESC LIMIT 1),
                       ''
                   ) AS grade,
                   c.qualified_1000, c.qualified_3000, c.qualified_10000, c.qualified_100000,
                   (SELECT COUNT(DISTINCT s.id) FROM sessions s JOIN contributions c2 ON c2.session_id = s.id WHERE c2.user_id = c.user_id AND c2.consume > 0 AND strftime('%Y-%m', s.start_time) = strftime('%Y-%m', 'now')) AS sessions_count
            FROM contributions c
            LEFT JOIN users u ON u.user_id = c.user_id
            WHERE c.session_id = ? AND c.consume > 0 {where_extra}
            ORDER BY c.consume DESC LIMIT ? OFFSET ?
        ''', (session_id, size, offset)).fetchall()
        users_list = []
        for i, r in enumerate(rows):
            d = dict(r)
            d['rank'] = offset + i + 1
            users_list.append(d)
        return {'users': users_list, 'total': total, 'page': page}
    elif period == 'today':
        today = datetime.now().strftime('%Y-%m-%d')
        rows = conn.execute(f'''
            SELECT d.user_id, d.user_name, d.total_consume AS consume,
                   COALESCE(u.fans_club, '') AS fans_club,
                   COALESCE(u.grade, '') AS grade,
                   d.sessions_{threshold} AS sessions_count
            FROM daily_stats d
            LEFT JOIN users u ON u.user_id = d.user_id
            WHERE d.date = ? AND d.sessions_{threshold} > 0
            ORDER BY d.total_consume DESC LIMIT ? OFFSET ?
        ''', (today, size, offset)).fetchall()
        total = conn.execute('SELECT COUNT(*) FROM daily_stats WHERE date = ? AND sessions_1000 > 0', (today,)).fetchone()[0]
    elif period == 'month':
        month = year_month or datetime.now().strftime('%Y-%m')
        rows = conn.execute(f'''
            SELECT m.user_id, m.user_name, m.total_consume AS consume,
                   COALESCE(u.fans_club, '') AS fans_club,
                   COALESCE(u.grade, '') AS grade,
                   m.sessions_{threshold} AS sessions_count
            FROM monthly_stats m
            LEFT JOIN users u ON u.user_id = m.user_id
            WHERE m.year_month = ? AND m.sessions_{threshold} > 0
            ORDER BY m.total_consume DESC LIMIT ? OFFSET ?
        ''', (month, size, offset)).fetchall()
        total = conn.execute('SELECT COUNT(*) FROM monthly_stats WHERE year_month = ? AND sessions_1000 > 0', (month,)).fetchone()[0]
    else:
        month_filter = year_month if year_month else ''
        if month_filter:
            rows = conn.execute(f'''
                SELECT m.user_id, m.user_name, m.total_consume AS consume,
                       COALESCE(u.fans_club, '') AS fans_club,
                       COALESCE(u.grade, '') AS grade,
                       m.sessions_{threshold} AS sessions_count
                FROM monthly_stats m
                LEFT JOIN users u ON u.user_id = m.user_id
                WHERE m.year_month = ? AND m.sessions_{threshold} > 0
                ORDER BY m.total_consume DESC LIMIT ? OFFSET ?
            ''', (month_filter, size, offset)).fetchall()
            total = conn.execute('SELECT COUNT(*) FROM monthly_stats WHERE year_month = ? AND sessions_1000 > 0', (month_filter,)).fetchone()[0]
        else:
            rows = conn.execute(f'''
                SELECT m.user_id, m.user_name, SUM(m.total_consume) AS consume,
                       COALESCE(u.fans_club, '') AS fans_club,
                       COALESCE(u.grade, '') AS grade,
                       SUM(m.sessions_{threshold}) AS sessions_count
                FROM monthly_stats m
                LEFT JOIN users u ON u.user_id = m.user_id
                WHERE m.sessions_{threshold} > 0
                GROUP BY m.user_id ORDER BY consume DESC LIMIT ? OFFSET ?
            ''', (size, offset)).fetchall()
            total = conn.execute('SELECT COUNT(DISTINCT user_id) FROM monthly_stats WHERE sessions_1000 > 0').fetchone()[0]

    users_list = []
    for i, r in enumerate(rows):
        d = dict(r)
        d['rank'] = offset + i + 1
        d['sessions_count'] = d.pop('sessions_count', 0)
        users_list.append(d)
    return {'users': users_list, 'total': total, 'page': page}


def query_user(user_id):
    conn = _get_conn()
    user = conn.execute('SELECT * FROM users WHERE user_id = ?', (user_id,)).fetchone()
    if not user:
        user = conn.execute('SELECT user_id, user_name, fans_club, "" as grade FROM contributions WHERE user_id = ? LIMIT 1', (user_id,)).fetchone()
    if not user:
        user = conn.execute('SELECT DISTINCT user_id, user_name, grade, fans_club FROM chat_logs WHERE user_id = ? LIMIT 1', (user_id,)).fetchone()
    if not user:
        return None

    monthly = conn.execute('''
        SELECT year_month, total_consume, sessions_1000, sessions_3000, sessions_10000, sessions_100000, days_active
        FROM monthly_stats WHERE user_id = ? ORDER BY year_month DESC
    ''', (user_id,)).fetchall()

    total_consume = conn.execute('SELECT SUM(consume) FROM contributions WHERE user_id = ?', (user_id,)).fetchone()[0] or 0
    sessions_all = conn.execute('SELECT COUNT(*) FROM contributions WHERE user_id = ? AND qualified_1000 = 1', (user_id,)).fetchone()[0]

    # 从最新弹幕中获取财富等级
    latest_chat = conn.execute(
        'SELECT grade, fans_club FROM chat_logs WHERE user_id = ? AND (grade != "" OR fans_club != "") ORDER BY id DESC LIMIT 1',
        (user_id,)
    ).fetchone()

    result = dict(user)
    if latest_chat:
        result['grade'] = latest_chat['grade'] or result.get('grade', '')
        if latest_chat['fans_club']:
            result['fans_club'] = latest_chat['fans_club']
    elif not result.get('grade'):
        result['grade'] = ''
    result['total_consume'] = total_consume
    result['total_sessions_1000'] = sessions_all
    result['monthly'] = [dict(r) for r in monthly]
    return result


def query_user_detail(user_id):
    """查询用户深度记录：按场次分组，含每场音浪/礼物/发言/时间线。"""
    conn = _get_conn()
    user = conn.execute('SELECT * FROM users WHERE user_id = ?', (user_id,)).fetchone()
    if not user:
        user = conn.execute('SELECT user_id, user_name, fans_club, "" as grade FROM contributions WHERE user_id = ? LIMIT 1', (user_id,)).fetchone()
    if not user:
        user = conn.execute('SELECT DISTINCT user_id, user_name, grade, fans_club FROM chat_logs WHERE user_id = ? LIMIT 1', (user_id,)).fetchone()
    if not user:
        return None

    total_consume = conn.execute('SELECT SUM(consume) FROM contributions WHERE user_id = ?', (user_id,)).fetchone()[0] or 0
    total_gifts = conn.execute('SELECT COUNT(*) FROM gift_logs WHERE user_id = ?', (user_id,)).fetchone()[0]
    total_chats = conn.execute('SELECT COUNT(*) FROM chat_logs WHERE user_id = ?', (user_id,)).fetchone()[0]

    # 从最新弹幕中获取财富等级和粉丝团信息
    latest_chat = conn.execute(
        'SELECT grade, fans_club FROM chat_logs WHERE user_id = ? AND (grade != "" OR fans_club != "") ORDER BY id DESC LIMIT 1',
        (user_id,)
    ).fetchone()

    rows = conn.execute("""
        SELECT s.id, s.anchor_name, s.start_time, s.end_time,
               COALESCE(c.consume, 0) as consume,
               (SELECT COUNT(*) FROM gift_logs WHERE session_id = s.id AND user_id = ?) as gift_count,
               (SELECT COUNT(*) FROM chat_logs WHERE session_id = s.id AND user_id = ?) as chat_count
        FROM sessions s
        LEFT JOIN contributions c ON c.session_id = s.id AND c.user_id = ?
        WHERE (SELECT COUNT(*) FROM gift_logs WHERE session_id = s.id AND user_id = ?) > 0
           OR (SELECT COUNT(*) FROM chat_logs WHERE session_id = s.id AND user_id = ?) > 0
           OR (c.id IS NOT NULL)
        ORDER BY s.id DESC
    """, (user_id, user_id, user_id, user_id, user_id)).fetchall()

    result = dict(user)
    result['total_consume'] = total_consume
    result['total_gifts'] = total_gifts
    result['total_chats'] = total_chats
    result['sessions'] = []

    # 补充财富等级和粉丝团（优先取最新弹幕中的值）
    if latest_chat:
        result['grade'] = latest_chat['grade'] or result.get('grade', '')
        if latest_chat['fans_club']:
            result['fans_club'] = latest_chat['fans_club']
    elif not result.get('grade'):
        result['grade'] = ''

    for s in rows:
        sd = dict(s)
        # 查询该场次的最后财富等级和粉丝团
        sess_info = conn.execute(
            'SELECT grade, fans_club FROM chat_logs WHERE session_id = ? AND user_id = ? AND (grade != "" OR fans_club != "") ORDER BY id DESC LIMIT 1',
            (s['id'], user_id)
        ).fetchone()
        if sess_info and sess_info['grade']:
            sd['grade'] = sess_info['grade']
        if sess_info and sess_info['fans_club']:
            sd['fans_club'] = sess_info['fans_club']
        # 如果 chat_logs 没有，从 users 表获取
        if not sd.get('grade') or not sd.get('fans_club'):
            u_info = conn.execute('SELECT grade, fans_club FROM users WHERE user_id = ?', (user_id,)).fetchone()
            if u_info:
                if not sd.get('grade') and u_info['grade']:
                    sd['grade'] = u_info['grade']
                if not sd.get('fans_club') and u_info['fans_club']:
                    sd['fans_club'] = u_info['fans_club']
        tl = []
        for row in conn.execute("SELECT created_at as time, 'chat' as type, content, '' as amount FROM chat_logs WHERE session_id = ? AND user_id = ? ORDER BY created_at DESC LIMIT 20", (s['id'], user_id)).fetchall():
            tl.append(dict(row))
        for row in conn.execute("SELECT created_at as time, 'gift' as type, gift_name || ' x' || gift_count as content, diamond_total as amount FROM gift_logs WHERE session_id = ? AND user_id = ? ORDER BY created_at DESC LIMIT 20", (s['id'], user_id)).fetchall():
            tl.append(dict(row))
        tl.sort(key=lambda x: str(x.get('time', '')), reverse=True)
        sd['timeline'] = tl[:30]
        result['sessions'].append(sd)

    return result


def query_user_timeline(user_id, type_filter='all', keyword='', page=1, size=50):
    conn = _get_conn()
    offset = (page - 1) * size
    results = []

    if type_filter in ('all', 'chat'):
        sql = 'SELECT created_at as time, "chat" as type, content, "" as amount, grade FROM chat_logs WHERE user_id = ?'
        params = [user_id]
        if keyword:
            sql += ' AND content LIKE ?'
            params.append(f'%{keyword}%')
        sql += ' ORDER BY created_at DESC LIMIT ? OFFSET ?'
        for row in conn.execute(sql, params + [size, offset]):
            results.append(dict(row))

    if type_filter in ('all', 'gift'):
        sql = 'SELECT created_at as time, "gift" as type, gift_name || " x" || gift_count as content, diamond_total as amount FROM gift_logs WHERE user_id = ?'
        params = [user_id]
        if keyword:
            sql += ' AND (gift_name LIKE ?)'
            params.append(f'%{keyword}%')
        sql += ' ORDER BY created_at DESC LIMIT ? OFFSET ?'
        for row in conn.execute(sql, params + [size, offset]):
            results.append(dict(row))

    results.sort(key=lambda x: str(x.get('time', '')), reverse=True)
    results = results[:size]

    total = conn.execute('SELECT COUNT(*) FROM chat_logs WHERE user_id = ?', (user_id,)).fetchone()[0] + \
            conn.execute('SELECT COUNT(*) FROM gift_logs WHERE user_id = ?', (user_id,)).fetchone()[0]
    return {'timeline': results, 'total': total, 'page': page}


def query_chat(user_id='', keyword='', page=1, size=50):
    conn = _get_conn()
    offset = (page - 1) * size
    where = []
    params = []
    if user_id:
        where.append('user_id = ?')
        params.append(user_id)
    if keyword:
        where.append('content LIKE ?')
        params.append(f'%{keyword}%')
    w = ' AND '.join(where) if where else '1=1'
    total = conn.execute(f'SELECT COUNT(*) FROM chat_logs WHERE {w}', params).fetchone()[0]
    rows = conn.execute(f'SELECT created_at as time, user_id, user_name, content, grade, fans_club FROM chat_logs WHERE {w} ORDER BY created_at DESC LIMIT ? OFFSET ?',
                        params + [size, offset]).fetchall()
    return {'chats': [dict(r) for r in rows], 'total': total, 'page': page}


def query_anonymous(page=1, size=50, search=''):
    conn = _get_conn()
    offset = (page - 1) * size
    where = 'u.is_anonymous = 1'
    params = []
    if search:
        where += ' AND (u.user_name LIKE ? OR u.user_id LIKE ?)'
        params.extend([f'%{search}%', f'%{search}%'])
    total = conn.execute(f'SELECT COUNT(*) FROM users u WHERE {where}', params).fetchone()[0]
    rows = conn.execute(f'''
        SELECT u.user_id AS real_user_id, u.user_name,
               u.anonymous_label,
               COALESCE(SUM(c.consume), 0) AS consume,
               COUNT(c.id) AS sessions_count, MAX(c.recorded_at) AS last_seen
        FROM users u LEFT JOIN contributions c ON c.user_id = u.user_id
        WHERE {where}
        GROUP BY u.user_id ORDER BY consume DESC LIMIT ? OFFSET ?
    ''', params + [size, offset]).fetchall()
    return {'users': [dict(r) for r in rows], 'total': total, 'page': page}


def query_million(year_month='', page=1, size=100):
    conn = _get_conn()
    if not year_month:
        year_month = datetime.now().strftime('%Y-%m')
    offset = (page - 1) * size
    rows = conn.execute('''
        SELECT user_id, user_name, total_consume, days_active
        FROM monthly_stats WHERE year_month = ? AND total_consume >= 1000000
        ORDER BY total_consume DESC LIMIT ? OFFSET ?
    ''', (year_month, size, offset)).fetchall()
    total = conn.execute('SELECT COUNT(*) FROM monthly_stats WHERE year_month = ? AND total_consume >= 1000000', (year_month,)).fetchone()[0]
    users = []
    for i, r in enumerate(rows):
        d = dict(r)
        d['rank'] = offset + i + 1
        users.append(d)
    return {'users': users, 'total': total, 'page': page}


def query_sessions(limit=20):
    conn = _get_conn()
    rows = conn.execute('''
        SELECT s.*, (SELECT COUNT(*) FROM contributions WHERE session_id = s.id AND qualified_1000 = 1) as user_count
        FROM sessions s ORDER BY id DESC LIMIT ?
    ''', (limit,)).fetchall()
    return [dict(r) for r in rows]


def query_session_detail(session_id, top_n=50):
    """查询单场次的详细信息：基础信息 + 贡献用户分层 + Top贡献用户 + 礼物/弹幕统计。"""
    conn = _get_conn()
    session = conn.execute('SELECT * FROM sessions WHERE id = ?', (session_id,)).fetchone()
    if not session:
        return None
    result = dict(session)

    # 礼物统计
    gift_stats = conn.execute('''
        SELECT COUNT(*) as total_gifts, COALESCE(SUM(diamond_total), 0) as total_diamonds,
               COUNT(DISTINCT user_id) as gift_users
        FROM gift_logs WHERE session_id = ?
    ''', (session_id,)).fetchone()
    result['gift_stats'] = dict(gift_stats) if gift_stats else {}

    # 弹幕统计
    chat_stats = conn.execute('''
        SELECT COUNT(*) as total_chats, COUNT(DISTINCT user_id) as chat_users
        FROM chat_logs WHERE session_id = ?
    ''', (session_id,)).fetchone()
    result['chat_stats'] = dict(chat_stats) if chat_stats else {}

    # 达标用户层次统计
    tier_counts = conn.execute('''
        SELECT
            COUNT(*) as total_contributors,
            SUM(CASE WHEN qualified_1000 = 1 THEN 1 ELSE 0 END) as tier_1000,
            SUM(CASE WHEN qualified_3000 = 1 THEN 1 ELSE 0 END) as tier_3000,
            SUM(CASE WHEN qualified_10000 = 1 THEN 1 ELSE 0 END) as tier_10000,
            SUM(CASE WHEN qualified_100000 = 1 THEN 1 ELSE 0 END) as tier_100000
        FROM contributions WHERE session_id = ?
    ''', (session_id,)).fetchone()
    result['tier_counts'] = dict(tier_counts) if tier_counts else {}

    # 贡献用户 Top N（含粉丝团、财富等级、达标层级）
    top = conn.execute('''
        SELECT c.user_id, c.user_name, c.consume,
               COALESCE(
                   NULLIF(u.fans_club, ''),
                   NULLIF(c.fans_club, ''),
                   (SELECT fans_club FROM chat_logs WHERE user_id = c.user_id AND fans_club != '' ORDER BY id DESC LIMIT 1),
                   ''
               ) AS fans_club,
               COALESCE(
                   u.grade,
                   (SELECT grade FROM chat_logs WHERE user_id = c.user_id AND grade != '' ORDER BY id DESC LIMIT 1),
                   ''
               ) AS grade,
               c.qualified_1000, c.qualified_3000, c.qualified_10000, c.qualified_100000
        FROM contributions c
        LEFT JOIN users u ON u.user_id = c.user_id
        WHERE c.session_id = ? AND c.consume > 0
        ORDER BY c.consume DESC LIMIT ?
    ''', (session_id, top_n)).fetchall()
    result['top_users'] = [dict(r) for r in top]

    # 礼物类型分布 Top N
    gifts = conn.execute('''
        SELECT gift_name, COUNT(*) as times, SUM(gift_count) as total_count, SUM(diamond_total) as total_diamonds
        FROM gift_logs WHERE session_id = ?
        GROUP BY gift_name ORDER BY total_diamonds DESC LIMIT ?
    ''', (session_id, top_n)).fetchall()
    result['top_gifts'] = [dict(r) for r in gifts]

    return result


def query_search(q, page=1, size=20):
    conn = _get_conn()
    offset = (page - 1) * size
    rows = conn.execute('''
        SELECT DISTINCT c.user_id, c.user_name,
               COALESCE(m.total_consume, 0) as total_consume,
               COALESCE(m.sessions_1000, 0) as sessions_1000, c.fans_club
        FROM contributions c
        LEFT JOIN monthly_stats m ON m.user_id = c.user_id AND m.year_month = strftime('%Y-%m', 'now')
        WHERE c.user_id = ?
        ORDER BY c.consume DESC LIMIT ? OFFSET ?
    ''', (q, size, offset)).fetchall()
    if not rows:
        rows = conn.execute('''
            SELECT DISTINCT c.user_id, c.user_name,
                   COALESCE(m.total_consume, 0) as total_consume,
                   COALESCE(m.sessions_1000, 0) as sessions_1000, c.fans_club
            FROM contributions c
            LEFT JOIN monthly_stats m ON m.user_id = c.user_id AND m.year_month = strftime('%Y-%m', 'now')
            WHERE c.user_name LIKE ?
            ORDER BY c.consume DESC LIMIT ? OFFSET ?
        ''', (f'%{q}%', size, offset)).fetchall()
    return {'users': [dict(r) for r in rows], 'total': len(rows), 'page': page}

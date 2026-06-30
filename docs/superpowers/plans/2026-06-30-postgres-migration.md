# SQLite → PostgreSQL Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace SQLite with PostgreSQL to eliminate "database is locked" errors under concurrent read/write access.

**Architecture:** Direct replacement using `psycopg2` connection pool instead of raw `sqlite3` thread-local connections. No ORM. All 9 tables re-created in PostgreSQL with adapted DDL. Writer-queue pattern retained. Existing function signatures preserved.

**Tech Stack:** PostgreSQL 16, psycopg2-binary, Python 3.11+

> **部署环境:** 阿里云 ECS 2C2G 40GB，PostgreSQL 原生安装。本地开发如需 PG 也可用 docker-compose.yml。

## Global Constraints

- All public function signatures must remain unchanged: `record_chat`, `record_gift`, `upsert_user`, `flush_to_sqlite`, `create_session`, `end_session`, `delete_session`, `record_upgrade`, all `query_*` functions
- Time values must remain UTC+8 (same semantics as `datetime('now', '+8 hours')`)
- Rollback is via `git checkout` on changed files; old SQLite DB file is never modified
- Migration script must be idempotent (safe to re-run)

## File Map

| File | Change | Responsibility |
|------|--------|---------------|
| `config.yaml` | **Edit** (append) | Database connection settings |
| `requirements.txt` | **Edit** (append) | Add `psycopg2-binary` |
| `base/parser.py` | **Major rewrite** | Connection pool, all SQL, schema init |
| `scripts/migrate_sqlite_to_pg.py` | **Create** | One-shot data migration |
| `app.py` | **Edit** (minor) | SQL placeholder + date function fixes |

---

### Task 1: Install PostgreSQL on ECS + Configuration

**Files:**
- Modify: `config.yaml` (add database section)
- Modify: `requirements.txt` (add psycopg2-binary)

- [ ] **Step 1: Install PostgreSQL 16 on the Alibaba ECS**

SSH into your server and run:

```bash
# Ubuntu/Debian (阿里云 ECS 默认系统)
sudo apt update
sudo apt install -y postgresql postgresql-client

# 查看版本
psql --version
```

- [ ] **Step 2: Create database and user**

```bash
sudo -u postgres psql -c "CREATE USER barrage WITH PASSWORD 'barrage';"
sudo -u postgres psql -c "CREATE DATABASE douyin_barrage OWNER barrage;"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE douyin_barrage TO barrage;"
```

- [ ] **Step 3: Configure pg_hba.conf for md5 auth**

```bash
# 找到 pg_hba.conf
sudo find /etc/postgresql -name pg_hba.conf
# 编辑它，将 local 行的 peer 改为 md5, host 行的 scram-sha-256 改为 md5
sudo systemctl restart postgresql
```

- [ ] **Step 4: Verify connection**

```bash
PGPASSWORD=barrage psql -h localhost -U barrage -d douyin_barrage -c "SELECT 1 AS ok;"
```
Expected: `ok` = `1`

- [ ] **Step 5: Add database config to config.yaml**

Append at the end of `config.yaml`:

```yaml
# ==================== 数据库配置 ====================
database:
  host: localhost
  port: 5432
  dbname: douyin_barrage
  user: barrage
  password: barrage
  pool_min: 2
  pool_max: 10
```

- [ ] **Step 6: Add psycopg2-binary to requirements.txt**

```txt
psycopg2-binary>=2.9.10,<3.0
```

Then install: `pip install -r requirements.txt`

- [ ] **Step 7: Commit**

```bash
git add config.yaml requirements.txt
git commit -m "feat: add PostgreSQL config (ECS native install)"
```

> **本地开发备选:** 如需在本地用 Docker，创建 `docker-compose.yml`:
> ```yaml
> services:
>   postgres:
>     image: postgres:16
>     environment:
>       POSTGRES_DB: douyin_barrage
>       POSTGRES_USER: barrage
>       POSTGRES_PASSWORD: barrage
>     ports:
>       - "5432:5432"
>     volumes:
>       - pgdata:/var/lib/postgresql/data
> volumes:
>   pgdata:
> ```

---

### Task 2: Rewrite Connection Layer (`base/parser.py`)

**Files:**
- Modify: `base/parser.py` (lines 1683–1786 and imports)

**Key design:**
- Replace `threading.local()` SQLite connections with `psycopg2.pool.ThreadedConnectionPool`
- Create a `_PGConnection` wrapper class that provides `conn.execute(sql, params)` — same API as sqlite3
- Use `RealDictCursor` under the hood so rows support both `r['col']` and `r[0]`
- Remove all SQLite PRAGMAs; remove the `atexit` connection registry; remove the module-level migration block

- [ ] **Step 1: Add new imports at top of file**

After existing imports (around line 22), add:

```python
import psycopg2
from psycopg2 import pool as pg_pool
from psycopg2.extras import RealDictCursor
```

- [ ] **Step 2: Replace the SQLite connection block (lines 1683–1786)**

Replace everything from `# ── SQLite 写入 ──` (line 1683) through the end of `_get_conn()` (line 1786) with:

```python
# ── PostgreSQL 连接池 ──────────────────────────────

DB_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
DB_PATH = os.path.join(DB_DIR, 'douyin_barrage.db')  # kept for migration script reference only


def _load_db_config():
    """Load database config from config.yaml, with env var overrides."""
    cfg = {
        'host': 'localhost', 'port': 5432, 'dbname': 'douyin_barrage',
        'user': 'barrage', 'password': 'barrage', 'pool_min': 2, 'pool_max': 10,
    }
    try:
        import yaml
        yaml_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.yaml')
        if os.path.exists(yaml_path):
            with open(yaml_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f) or {}
                dbc = data.get('database', {})
                for k in ('host', 'port', 'dbname', 'user', 'password', 'pool_min', 'pool_max'):
                    if k in dbc:
                        cfg[k] = dbc[k]
    except Exception:
        pass
    cfg['host'] = os.environ.get('PGHOST', cfg['host'])
    cfg['port'] = int(os.environ.get('PGPORT', cfg['port']))
    cfg['dbname'] = os.environ.get('PGDATABASE', cfg['dbname'])
    cfg['user'] = os.environ.get('PGUSER', cfg['user'])
    cfg['password'] = os.environ.get('PGPASSWORD', cfg['password'])
    cfg['pool_min'] = int(os.environ.get('PGPOOL_MIN', cfg['pool_min']))
    cfg['pool_max'] = int(os.environ.get('PGPOOL_MAX', cfg['pool_max']))
    return cfg


_db_config = _load_db_config()


def _create_pool():
    try:
        pool = pg_pool.ThreadedConnectionPool(
            _db_config['pool_min'], _db_config['pool_max'],
            host=_db_config['host'], port=_db_config['port'],
            dbname=_db_config['dbname'], user=_db_config['user'],
            password=_db_config['password'],
        )
        logger.info(f"[DB] PostgreSQL 连接池已创建 ({_db_config['host']}:{_db_config['port']}/{_db_config['dbname']})")
        return pool
    except Exception as e:
        logger.critical(f"[DB] PostgreSQL 连接失败: {e}")
        raise


_pool = _create_pool()


class _PGConnection:
    """Wraps a psycopg2 connection to provide sqlite3-compatible .execute() interface.

    conn.execute(sql, params) → cursor with RealDictRow support (r['col'] AND r[0] both work).
    """

    def __init__(self, conn):
        self._conn = conn
        self.autocommit = conn.autocommit
        self.closed = conn.closed

    def execute(self, query, params=None):
        cur = self._conn.cursor(cursor_factory=RealDictCursor)
        try:
            cur.execute(query, params)
            return cur
        except Exception:
            cur.close()
            raise

    def commit(self):
        return self._conn.commit()

    def rollback(self):
        return self._conn.rollback()

    def close(self):
        return self._conn.close()

    def cursor(self):
        return self._conn.cursor()

    def __getattr__(self, name):
        return getattr(self._conn, name)


def _get_conn():
    """Borrow a wrapped connection from the pool (thread-safe)."""
    try:
        raw = _pool.getconn()
        if raw.closed:
            _pool.putconn(raw)
            raw = _pool.getconn()
        raw.autocommit = False
        return _PGConnection(raw)
    except Exception as e:
        logger.error(f"[DB] 获取连接失败: {e}")
        raise


def _put_conn(conn):
    if conn is not None and hasattr(conn, '_conn'):
        try:
            _pool.putconn(conn._conn)
        except Exception:
            pass
    elif conn is not None:
        try:
            _pool.putconn(conn)
        except Exception:
            pass


class _db_conn:
    """Context manager: borrow from pool, return on exit."""
    def __enter__(self):
        self.conn = _get_conn()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            try:
                self.conn.rollback()
            except Exception:
                pass
        _put_conn(self.conn)
        return False


def _close_all_connections():
    global _pool
    try:
        if _pool is not None:
            _pool.closeall()
            logger.info("[DB] 连接池已关闭")
    except Exception:
        pass


import atexit
atexit.register(_close_all_connections)

_db_schema_inited = False
_db_schema_lock = threading.Lock()
```

- [ ] **Step 3: Remove the old migration block (lines 1706–1756)**

Delete the entire `# ── 启动时自动迁移旧表结构 ──` block.

- [ ] **Step 4: Update `_db_write_with_retry`**

Replace the function (retry logic no longer needed for PostgreSQL):

```python
def _db_write_with_retry(fn, max_retries=5, base_delay=0.1):
    """保留接口兼容性 — PostgreSQL 不需要锁重试。"""
    return fn()
```

- [ ] **Step 5: Commit**

```bash
git add base/parser.py
git commit -m "feat: replace SQLite connection layer with PostgreSQL connection pool"
```

---

### Task 3: Rewrite init_db() with PostgreSQL DDL

**Files:**
- Modify: `base/parser.py` (replace `init_db()`, currently lines 1789–1932)

- [ ] **Step 1: Replace init_db()**

```python
def init_db():
    """Initialize PostgreSQL schema — tables, indexes, startup cleanup."""
    global _db_schema_inited
    with _db_schema_lock:
        if _db_schema_inited:
            return True
        conn = _get_conn()
        try:
            conn.execute('''CREATE TABLE IF NOT EXISTS sessions (
                id SERIAL PRIMARY KEY, room_id TEXT NOT NULL,
                anchor_name TEXT DEFAULT '',
                start_time TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'Asia/Shanghai'),
                end_time TIMESTAMP, status TEXT DEFAULT 'live')''')
            conn.execute('''CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY, user_id TEXT UNIQUE NOT NULL,
                user_name TEXT NOT NULL, fans_club TEXT DEFAULT '',
                grade TEXT DEFAULT '', sec_uid TEXT DEFAULT '',
                avatar_url TEXT DEFAULT '', notes TEXT DEFAULT '',
                tags TEXT DEFAULT '', is_anonymous INTEGER DEFAULT 0,
                anonymous_label TEXT DEFAULT '',
                first_seen TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'Asia/Shanghai'),
                last_seen TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'Asia/Shanghai'))''')
            conn.execute('''CREATE TABLE IF NOT EXISTS contributions (
                id SERIAL PRIMARY KEY,
                session_id INTEGER NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                user_id TEXT NOT NULL, user_name TEXT NOT NULL,
                consume INTEGER DEFAULT 0, rank INTEGER DEFAULT 0,
                fans_club TEXT DEFAULT '', source TEXT DEFAULT 'websocket',
                qualified_1000 INTEGER DEFAULT 0, qualified_3000 INTEGER DEFAULT 0,
                qualified_10000 INTEGER DEFAULT 0, qualified_100000 INTEGER DEFAULT 0,
                recorded_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'Asia/Shanghai'),
                UNIQUE(session_id, user_id))''')
            conn.execute('''CREATE TABLE IF NOT EXISTS chat_logs (
                id SERIAL PRIMARY KEY,
                session_id INTEGER REFERENCES sessions(id) ON DELETE CASCADE,
                user_id TEXT NOT NULL, user_name TEXT NOT NULL,
                content TEXT NOT NULL, grade TEXT DEFAULT '',
                fans_club TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'Asia/Shanghai'))''')
            conn.execute('''CREATE TABLE IF NOT EXISTS upgrade_logs (
                id SERIAL PRIMARY KEY, session_id INTEGER DEFAULT 0,
                user_id TEXT NOT NULL, user_name TEXT NOT NULL,
                upgrade_type TEXT NOT NULL, from_level INTEGER DEFAULT 0,
                to_level INTEGER DEFAULT 0, anchor_name TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'Asia/Shanghai'))''')
            conn.execute('''CREATE TABLE IF NOT EXISTS gift_logs (
                id SERIAL PRIMARY KEY,
                session_id INTEGER REFERENCES sessions(id) ON DELETE CASCADE,
                user_id TEXT NOT NULL, user_name TEXT NOT NULL,
                gift_name TEXT NOT NULL, gift_count INTEGER DEFAULT 1,
                diamond_total INTEGER DEFAULT 0, grade TEXT DEFAULT '',
                fans_club TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'Asia/Shanghai'))''')
            conn.execute('''CREATE TABLE IF NOT EXISTS daily_stats (
                id SERIAL PRIMARY KEY, user_id TEXT NOT NULL,
                user_name TEXT NOT NULL, date TEXT NOT NULL,
                total_consume INTEGER DEFAULT 0, sessions_1000 INTEGER DEFAULT 0,
                sessions_3000 INTEGER DEFAULT 0, sessions_10000 INTEGER DEFAULT 0,
                sessions_100000 INTEGER DEFAULT 0, gift_count INTEGER DEFAULT 0,
                chat_count INTEGER DEFAULT 0, UNIQUE(user_id, date))''')
            conn.execute('''CREATE TABLE IF NOT EXISTS monthly_stats (
                id SERIAL PRIMARY KEY, user_id TEXT NOT NULL,
                user_name TEXT NOT NULL, year_month TEXT NOT NULL,
                total_consume INTEGER DEFAULT 0, sessions_1000 INTEGER DEFAULT 0,
                sessions_3000 INTEGER DEFAULT 0, sessions_10000 INTEGER DEFAULT 0,
                sessions_100000 INTEGER DEFAULT 0, days_active INTEGER DEFAULT 0,
                max_rank INTEGER DEFAULT 0, UNIQUE(user_id, year_month))''')
            conn.execute('''CREATE TABLE IF NOT EXISTS streamer_config (
                live_id TEXT PRIMARY KEY, anchor_name TEXT DEFAULT '',
                enabled INTEGER DEFAULT 0,
                added_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'Asia/Shanghai'))''')
            conn.execute('''CREATE TABLE IF NOT EXISTS pk_rounds (
                id SERIAL PRIMARY KEY,
                session_id INTEGER REFERENCES sessions(id) ON DELETE CASCADE,
                start_time TEXT NOT NULL, end_time TEXT,
                duration_sec INTEGER DEFAULT 0, mode TEXT DEFAULT '',
                participants TEXT DEFAULT '', participant_count INTEGER DEFAULT 0,
                self_score INTEGER DEFAULT 0, opponent_score INTEGER DEFAULT 0,
                result TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'Asia/Shanghai'))''')

            # Indexes
            conn.execute('CREATE INDEX IF NOT EXISTS idx_contributions_session ON contributions(session_id)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_contributions_user ON contributions(user_id)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_chat_logs_user ON chat_logs(user_id)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_monthly_stats ON monthly_stats(year_month, sessions_1000 DESC)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_daily_stats ON daily_stats(date, sessions_1000 DESC)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_contributions_qualified ON contributions(qualified_1000)')
            conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_gift_dedup_ts ON gift_logs(session_id, user_id, gift_name, diamond_total, gift_count, created_at)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_upgrade_logs_session ON upgrade_logs(session_id)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_upgrade_logs_type ON upgrade_logs(upgrade_type)')
            conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_upgrade_logs_dedup ON upgrade_logs(session_id, user_id, upgrade_type, from_level, to_level)')

            # Cleanup zombie sessions
            conn.execute("""
                UPDATE sessions SET end_time = start_time, status = 'ended'
                WHERE status = 'live' AND start_time < (NOW() AT TIME ZONE 'Asia/Shanghai' - INTERVAL '12 hours')
            """)

            conn.commit()
            _db_schema_inited = True
            logger.info("[DB] PostgreSQL schema initialized")
        except Exception:
            conn.rollback()
            raise
        finally:
            _put_conn(conn)
    return True
```

- [ ] **Step 2: Ensure init_db() is called at module level**

After the `atexit.register` line, add:

```python
init_db()
```

- [ ] **Step 3: Commit**

```bash
git add base/parser.py
git commit -m "feat: rewrite init_db() with PostgreSQL DDL (9 tables + indexes)"
```

---

### Task 4: Rewrite All Write Functions

**Files:**
- Modify: `base/parser.py` (create_session, end_session, delete_session, _flush_write_batch, flush_to_sqlite, record_upgrade)

**SQL Mapping:** `?` → `%s`, `datetime("now", "+8 hours")` → `(NOW() AT TIME ZONE 'Asia/Shanghai')`, `INSERT OR IGNORE` → `ON CONFLICT DO NOTHING`, `MAX` → `GREATEST`, `strftime` → `TO_CHAR`.

- [ ] **Step 1: Rewrite create_session**

```python
def create_session(room_id, anchor_name=''):
    with _db_conn() as conn:
        old = conn.execute(
            'SELECT id FROM sessions WHERE room_id = %s AND status = %s',
            (room_id, 'live')
        ).fetchall()
        for row in old:
            conn.execute(
                'UPDATE sessions SET end_time = (NOW() AT TIME ZONE \'Asia/Shanghai\'), status = %s WHERE id = %s',
                ('ended', row[0])
            )
            logger.info(f"[DB] 自动结束旧场次 #{row[0]} (新场次创建)")
        cur = conn.execute(
            'INSERT INTO sessions (room_id, anchor_name) VALUES (%s, %s) RETURNING id',
            (room_id, anchor_name)
        )
        sid = cur.fetchone()[0]
        conn.commit()
        logger.info(f"[DB] 新场次 #{sid}: {anchor_name} ({room_id})")
        return sid
```

- [ ] **Step 2: Rewrite end_session**

```python
def end_session(session_id):
    def _do():
        with _db_conn() as conn:
            conn.execute(
                'UPDATE sessions SET end_time = (NOW() AT TIME ZONE \'Asia/Shanghai\'), status = %s WHERE id = %s',
                ('ended', session_id)
            )
            conn.commit()
    _db_write_with_retry(_do)
    logger.info(f"[DB] 场次 #{session_id} 已结束")
```

- [ ] **Step 3: Rewrite delete_session**

```python
def delete_session(session_id):
    def _do():
        with _db_conn() as conn:
            gift_count = conn.execute('SELECT COUNT(*) FROM gift_logs WHERE session_id = %s', (session_id,)).fetchone()[0]
            chat_count = conn.execute('SELECT COUNT(*) FROM chat_logs WHERE session_id = %s', (session_id,)).fetchone()[0]
            contrib_count = conn.execute('SELECT COUNT(*) FROM contributions WHERE session_id = %s', (session_id,)).fetchone()[0]
            conn.execute('DELETE FROM gift_logs WHERE session_id = %s', (session_id,))
            conn.execute('DELETE FROM chat_logs WHERE session_id = %s', (session_id,))
            conn.execute('DELETE FROM contributions WHERE session_id = %s', (session_id,))
            conn.execute('DELETE FROM sessions WHERE id = %s', (session_id,))
            conn.commit()
            return (gift_count, chat_count, contrib_count)
    gift_count, chat_count, contrib_count = _db_write_with_retry(_do)
    logger.info(f"[DB] 场次 #{session_id} 已删除（礼物:{gift_count} 弹幕:{chat_count} 贡献:{contrib_count}）")
    return {'deleted': True, 'gifts': gift_count, 'chats': chat_count, 'contributions': contrib_count}
```

- [ ] **Step 4: Rewrite _flush_write_batch**

```python
def _flush_write_batch(conn, batch):
    for item in batch:
        op = item[0]
        try:
            if op == 'chat':
                _, sid, uid, uname, content, grade, club = item
                conn.execute(
                    'INSERT INTO chat_logs (session_id, user_id, user_name, content, grade, fans_club) '
                    'VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING',
                    (sid, uid, uname, content, grade, club)
                )
            elif op == 'gift':
                _, sid, uid, uname, gname, cnt, dia, grade, club = item
                if cnt > 1 and uid:
                    conn.execute(
                        'DELETE FROM gift_logs WHERE session_id = %s AND user_id = %s '
                        'AND gift_name = %s AND diamond_total = %s AND gift_count < %s',
                        (sid, uid, gname, dia, cnt)
                    )
                conn.execute(
                    'INSERT INTO gift_logs (session_id, user_id, user_name, gift_name, gift_count, diamond_total, grade, fans_club) '
                    'VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING',
                    (sid, uid, uname, gname, cnt, dia, grade, club)
                )
            elif op == 'upsert':
                _, uid, uname, grade, club, sec, av = item
                is_anon, anon_label = _detect_anonymous(uname)
                if club:
                    existing = conn.execute(
                        'SELECT fans_club FROM users WHERE user_id = %s', (uid,)
                    ).fetchone()
                    if existing and existing[0]:
                        club = _merge_fans_club_strings(existing[0], club)
                conn.execute('''
                    INSERT INTO users (user_id, user_name, grade, fans_club, sec_uid, avatar_url, is_anonymous, anonymous_label, last_seen)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW() AT TIME ZONE 'Asia/Shanghai')
                    ON CONFLICT(user_id) DO UPDATE SET
                        user_name = CASE WHEN %s != '' THEN %s ELSE users.user_name END,
                        grade = CASE WHEN %s != '' THEN %s ELSE users.grade END,
                        fans_club = CASE WHEN %s != '' THEN %s ELSE users.fans_club END,
                        sec_uid = CASE WHEN %s != '' THEN %s ELSE users.sec_uid END,
                        avatar_url = CASE WHEN %s != '' THEN %s ELSE users.avatar_url END,
                        is_anonymous = CASE WHEN %s = 1 THEN 1 ELSE users.is_anonymous END,
                        anonymous_label = CASE WHEN %s != '' THEN %s ELSE users.anonymous_label END,
                        last_seen = NOW() AT TIME ZONE 'Asia/Shanghai'
                ''', (uid, uname, grade, club, sec, av, is_anon, anon_label,
                      uname, uname, grade, grade, club, club, sec, sec, av, av,
                      is_anon, anon_label, anon_label))
        except Exception as e:
            logger.debug(f"[DB] writer batch op failed: {op} {e}")
    conn.commit()
```

- [ ] **Step 5: Rewrite flush_to_sqlite**

Replace `MAX(excluded.col, ...)` with `GREATEST(table.col, excluded.col)`, `?` → `%s`, `datetime` → `NOW() AT TIME ZONE 'Asia/Shanghai'`.

```python
def flush_to_sqlite(session_id):
    with _db_conn() as conn:
        today = datetime.now().strftime('%Y-%m-%d')
        month = datetime.now().strftime('%Y-%m')
        rows = conn.execute('''
            SELECT user_id, user_name, SUM(diamond_total) as consume
            FROM gift_logs WHERE session_id = %s GROUP BY user_id
        ''', (session_id,)).fetchall()
        if not rows:
            return
        with _flush_lock:
            for r in rows:
                uid = r['user_id']; nick = r['user_name']; consume = r['consume']
                info = conn.execute(
                    "SELECT fans_club, grade FROM chat_logs WHERE user_id = %s AND (fans_club != '' OR grade != '') ORDER BY id DESC LIMIT 1",
                    (uid,)).fetchone()
                fans_club = info['fans_club'] if info else ''
                grade = info['grade'] if info else ''
                # sync users
                conn.execute('''
                    INSERT INTO users (user_id, user_name, fans_club, grade, last_seen)
                    VALUES (%s, %s, %s, %s, NOW() AT TIME ZONE 'Asia/Shanghai')
                    ON CONFLICT(user_id) DO UPDATE SET
                        fans_club = CASE WHEN %s != '' THEN %s ELSE users.fans_club END,
                        grade = CASE WHEN %s != '' THEN %s ELSE users.grade END,
                        last_seen = NOW() AT TIME ZONE 'Asia/Shanghai'
                ''', (uid, nick, fans_club, grade, fans_club, fans_club, grade, grade))
                prev = conn.execute(
                    'SELECT qualified_1000, qualified_3000, qualified_10000, qualified_100000 FROM contributions WHERE session_id=%s AND user_id=%s',
                    (session_id, uid)).fetchone()
                pq_1000 = prev['qualified_1000'] if prev else 0
                pq_3000 = prev['qualified_3000'] if prev else 0
                pq_10000 = prev['qualified_10000'] if prev else 0
                pq_100000 = prev['qualified_100000'] if prev else 0
                q_1000 = 1 if consume >= 1000 else 0; q_3000 = 1 if consume >= 3000 else 0
                q_10000 = 1 if consume >= 10000 else 0; q_100000 = 1 if consume >= 100000 else 0
                conn.execute('''
                    INSERT INTO contributions (session_id, user_id, user_name, consume, rank, fans_club, source,
                        qualified_1000, qualified_3000, qualified_10000, qualified_100000)
                    VALUES (%s, %s, %s, %s, 0, %s, 'websocket', %s, %s, %s, %s)
                    ON CONFLICT(session_id, user_id) DO UPDATE SET
                        consume = excluded.consume,
                        qualified_1000 = GREATEST(contributions.qualified_1000, excluded.qualified_1000),
                        qualified_3000 = GREATEST(contributions.qualified_3000, excluded.qualified_3000),
                        qualified_10000 = GREATEST(contributions.qualified_10000, excluded.qualified_10000),
                        qualified_100000 = GREATEST(contributions.qualified_100000, excluded.qualified_100000)
                ''', (session_id, uid, nick, consume, fans_club, q_1000, q_3000, q_10000, q_100000))
                # 达标次数（仅在首次达标时 +1）
                if q_1000 and not pq_1000:
                    conn.execute('INSERT INTO daily_stats (user_id, user_name, date, sessions_1000, total_consume) VALUES (%s, %s, %s, 1, 0) ON CONFLICT(user_id, date) DO UPDATE SET sessions_1000 = daily_stats.sessions_1000 + 1', (uid, nick, today))
                    conn.execute('INSERT INTO monthly_stats (user_id, user_name, year_month, sessions_1000, total_consume) VALUES (%s, %s, %s, 1, 0) ON CONFLICT(user_id, year_month) DO UPDATE SET sessions_1000 = monthly_stats.sessions_1000 + 1', (uid, nick, month))
                if q_3000 and not pq_3000:
                    conn.execute('UPDATE daily_stats SET sessions_3000 = sessions_3000 + 1 WHERE user_id = %s AND date = %s', (uid, today))
                    conn.execute('UPDATE monthly_stats SET sessions_3000 = sessions_3000 + 1 WHERE user_id = %s AND year_month = %s', (uid, month))
                if q_10000 and not pq_10000:
                    conn.execute('UPDATE daily_stats SET sessions_10000 = sessions_10000 + 1 WHERE user_id = %s AND date = %s', (uid, today))
                    conn.execute('UPDATE monthly_stats SET sessions_10000 = sessions_10000 + 1 WHERE user_id = %s AND year_month = %s', (uid, month))
                if q_100000 and not pq_100000:
                    conn.execute('UPDATE daily_stats SET sessions_100000 = sessions_100000 + 1 WHERE user_id = %s AND date = %s', (uid, today))
                    conn.execute('UPDATE monthly_stats SET sessions_100000 = sessions_100000 + 1 WHERE user_id = %s AND year_month = %s', (uid, month))
                # 总消费
                total_day = conn.execute(
                    "SELECT COALESCE(SUM(c.consume), 0) FROM contributions c JOIN sessions s ON c.session_id = s.id WHERE c.user_id = %s AND DATE(s.start_time) = %s",
                    (uid, today)).fetchone()[0]
                total_month = conn.execute(
                    "SELECT COALESCE(SUM(c.consume), 0) FROM contributions c JOIN sessions s ON c.session_id = s.id WHERE c.user_id = %s AND TO_CHAR(s.start_time, 'YYYY-MM') = %s",
                    (uid, month)).fetchone()[0]
                conn.execute('UPDATE daily_stats SET total_consume = %s, user_name = %s WHERE user_id = %s AND date = %s', (total_day, nick, uid, today))
                conn.execute('UPDATE monthly_stats SET total_consume = %s, user_name = %s WHERE user_id = %s AND year_month = %s', (total_month, nick, uid, month))
            conn.commit()
```

- [ ] **Step 6: Rewrite record_upgrade**

```python
def record_upgrade(session_id, user_id, user_name, upgrade_type, from_level, to_level, anchor_name=''):
    if not user_id or not upgrade_type or to_level <= 0 or from_level <= 0:
        return
    try:
        with _db_conn() as conn:
            conn.execute(
                'INSERT INTO upgrade_logs (session_id, user_id, user_name, upgrade_type, from_level, to_level, anchor_name) '
                'VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING',
                (session_id or 0, user_id, user_name, upgrade_type, from_level, to_level, anchor_name))
            conn.commit()
    except Exception as e:
        logger.debug(f"[DB] record_upgrade failed: {e}")
```

- [ ] **Step 7: Commit**

```bash
git add base/parser.py
git commit -m "feat: rewrite write functions for PostgreSQL (create/end/delete session, writer batch, flush, upgrade)"
```

---

### Task 5: Rewrite All Query Functions

**Files:**
- Modify: `base/parser.py` (query_upgrades, query_leaderboard, query_user, query_user_detail, query_user_timeline, query_chat, query_anonymous, query_million, query_sessions, query_session_detail, query_search, query_audit)

**SQL Mappings:**
- `?` → `%s`
- `strftime('%Y-%m', ts)` → `TO_CHAR(ts, 'YYYY-MM')`
- `strftime('%H', ts)` → `TO_CHAR(ts, 'HH24')`
- `strftime('%Y-%m-%d %H:%M:%S', ts)` → `TO_CHAR(ts, 'YYYY-MM-DD HH24:MI:SS')`
- `strftime('%Y-%m', 'now')` → `TO_CHAR(NOW(), 'YYYY-MM')`
- `date(ts)` → `DATE(ts)` (same)
- `datetime('now', '-30 days')` → `NOW() - INTERVAL '30 days'`
- `datetime('now')` → `NOW()`
- `date('now')` → `CURRENT_DATE`
- `instr(str, x'02')` → `POSITION(chr(2) IN str)`
- `instr(str, x'03')` → `POSITION(chr(3) IN str)`
- `||`, `COALESCE`, `NULLIF`, `CASE WHEN`: same

The `_PGConnection.execute()` returns RealDictCursor rows supporting both `r['col']` and `r[0]` — all existing row access patterns work unchanged.

- [ ] **Step 1: Go through each query function and apply the SQL mappings**

For each function (query_upgrades, query_leaderboard, query_user, query_user_detail, query_user_timeline, query_chat, query_anonymous, query_million, query_sessions, query_session_detail, query_search, query_audit):

1. Replace every `?` with `%s` in SQL strings
2. Replace `strftime('...', ...)` with equivalent `TO_CHAR(...)` or `DATE(...)`
3. Replace `datetime('now', ...)` with `NOW() ... INTERVAL`
4. Replace `date('now')` with `CURRENT_DATE`
5. Replace `instr(str, x'02')` with `POSITION(chr(2) IN str)`
6. Replace `instr(str, x'03')` with `POSITION(chr(3) IN str)`

**query_upgrades:** `?` → `%s`, rest identical.

**query_leaderboard:** Change `strftime('%Y-%m', s.start_time)` → `TO_CHAR(s.start_time, 'YYYY-MM')`, `date(s.start_time) = ?` → `DATE(s.start_time) = %s`, `datetime('now', '-30 days')` → `NOW() - INTERVAL '30 days'`, all `?` → `%s`.

**query_user, query_user_detail, query_user_timeline, query_chat, query_anonymous, query_million, query_sessions, query_session_detail:** `?` → `%s` everywhere. No function-level changes.

**query_search:** `strftime('%Y-%m', 'now')` → `TO_CHAR(NOW(), 'YYYY-MM')`, `?` → `%s`.

**query_audit:** `instr(user_name, x'02')` → `POSITION(chr(2) IN user_name)`, `instr(user_name, x'03')` → `POSITION(chr(3) IN user_name)`, `?` → `%s`.

- [ ] **Step 2: Commit**

```bash
git add base/parser.py
git commit -m "feat: rewrite all query functions for PostgreSQL (%s, TO_CHAR, NOW, POSITION)"
```

---

### Task 6: Update app.py

**Files:**
- Modify: `app.py` (SQL placeholder and function fixes)

- [ ] **Step 1: Replace `?` with `%s` in all SQL queries**

Search for every `conn.execute(` in app.py and change `?` → `%s` in all SQL strings. This is mechanical — ~40 calls.

- [ ] **Step 2: Replace SQLite date functions**

- `date(created_at)=date('now')` → `DATE(created_at) = CURRENT_DATE`
- `date('now')` → `CURRENT_DATE`
- `datetime('now', '+8 hours')` → `(NOW() AT TIME ZONE 'Asia/Shanghai')`

- [ ] **Step 3: Commit**

```bash
git add app.py
git commit -m "fix: adapt app.py SQL for PostgreSQL (%s placeholders, CURRENT_DATE)"
```

---

### Task 7: Create Data Migration Script

**Files:**
- Create: `scripts/migrate_sqlite_to_pg.py`

- [ ] **Step 1: Create scripts/migrate_sqlite_to_pg.py**

```python
#!/usr/bin/env python3
"""One-shot migration: copy all data from SQLite to PostgreSQL.
Safe to re-run (uses IF NOT EXISTS + ON CONFLICT DO NOTHING).
SQLite file is never modified.
"""
import os, sys, sqlite3, psycopg2

SQLITE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'douyin_barrage.db')
PG_CONFIG = {
    'host': os.environ.get('PGHOST', 'localhost'),
    'port': int(os.environ.get('PGPORT', 5432)),
    'dbname': os.environ.get('PGDATABASE', 'douyin_barrage'),
    'user': os.environ.get('PGUSER', 'barrage'),
    'password': os.environ.get('PGPASSWORD', 'barrage'),
}
TABLES = ['sessions', 'users', 'contributions', 'chat_logs', 'gift_logs',
          'upgrade_logs', 'daily_stats', 'monthly_stats', 'streamer_config', 'pk_rounds']


def get_sqlite_cols(cur, table):
    cur.execute(f'PRAGMA table_info({table})')
    return [r[1] for r in cur.fetchall()]


def get_pg_cols(cur, table):
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s ORDER BY ordinal_position", (table,))
    return [r[0] for r in cur.fetchall()]


def migrate_table(sqlite_conn, pg_conn, table):
    sqlite_cur = sqlite_conn.cursor()
    pg_cur = pg_conn.cursor()
    sqlite_cols = get_sqlite_cols(sqlite_cur, table)
    pg_cols = get_pg_cols(pg_cur, table)
    common = [c for c in sqlite_cols if c in pg_cols]
    if not common:
        print(f"  ⚠ No common columns for {table}, skipping"); return 0
    col_list = ', '.join(common)
    placeholders = ', '.join(['%s'] * len(common))
    sqlite_cur.execute(f'SELECT {col_list} FROM {table} ORDER BY id')
    rows = sqlite_cur.fetchall()
    if not rows:
        print(f"  {table}: 0 rows (empty)"); return 0
    insert_sql = f'INSERT INTO {table} ({col_list}) VALUES ({placeholders}) ON CONFLICT DO NOTHING'
    inserted = 0
    for i in range(0, len(rows), 500):
        batch = rows[i:i+500]
        try:
            pg_cur.executemany(insert_sql, batch)
            inserted += len(batch)
        except Exception as e:
            print(f"  ⚠ Batch error in {table}: {e}")
            for row in batch:
                try:
                    pg_cur.execute(insert_sql, row); inserted += 1
                except Exception:
                    pg_conn.rollback()
    print(f"  {table}: {len(rows)} read, {inserted} inserted")
    return inserted


def main():
    print("=" * 60)
    print("SQLite → PostgreSQL Migration")
    print("=" * 60)
    print(f"SQLite: {SQLITE_PATH}")
    print(f"PostgreSQL: {PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['dbname']}")
    if not os.path.exists(SQLITE_PATH):
        print(f"✗ SQLite DB not found: {SQLITE_PATH}"); sys.exit(1)
    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    print("✓ Connected to SQLite")
    try:
        pg_conn = psycopg2.connect(**PG_CONFIG)
        pg_conn.autocommit = False
        print("✓ Connected to PostgreSQL")
    except Exception as e:
        print(f"✗ PostgreSQL connection failed: {e}"); sqlite_conn.close(); sys.exit(1)
    # Init PG schema
    print("\nInitializing PostgreSQL schema...")
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from base.parser import init_db as pg_init_db
    pg_init_db()
    print("✓ Schema initialized")
    # Migrate
    print("\nMigrating data...")
    total = 0
    for table in TABLES:
        total += migrate_table(sqlite_conn, pg_conn, table)
        pg_conn.commit()
    print(f"\n{'=' * 60}\nMigration complete: {total} total rows migrated\n{'=' * 60}")
    print(f"\nSQLite file untouched at: {SQLITE_PATH}")
    sqlite_conn.close(); pg_conn.close()


if __name__ == '__main__':
    main()
```

- [ ] **Step 2: Run the migration**

```bash
cd C:\Users\xinyi\downloads\barra
python scripts/migrate_sqlite_to_pg.py
```

Expected: Shows all 10 tables with row counts.

- [ ] **Step 3: Commit**

```bash
git add scripts/migrate_sqlite_to_pg.py
git commit -m "feat: add SQLite-to-PostgreSQL data migration script"
```

---

### Task 8: Verify the Migration

**Files:** No file changes — just run and verify.

- [ ] **Step 1: Syntax check**

```bash
python -c "import py_compile; py_compile.compile('base/parser.py', doraise=True)"
```

- [ ] **Step 2: Run web panel**

```bash
python app.py --port 8080
```

Check: Dashboard loads, sessions visible, user detail works, leaderboard works.

- [ ] **Step 3: Quick data collector test**

```bash
python main.py <live_id> --live-stop
```

Check: No "database is locked" errors, data appears in panel.

- [ ] **Step 4: Final commit**

```bash
git add -A
git commit -m "fix: post-migration adjustments"
```

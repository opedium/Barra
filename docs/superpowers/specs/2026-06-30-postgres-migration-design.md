# SQLite → PostgreSQL Migration Design

**Date:** 2026-06-30
**Status:** Approved for implementation

## 1. Problem Statement

The application uses SQLite as its database backend. Under concurrent access — the WebSocket recorder writing data while the Flask web panel reads it — SQLite's single-writer limitation causes "database is locked" errors. PostgreSQL's MVCC architecture eliminates this contention: multiple readers and a writer can operate simultaneously without locking.

## 2. Approach

**psycopg2 direct replacement** — replace raw `sqlite3` calls with `psycopg2` using the same direct-SQL style. No ORM. The existing writer-queue (producer-consumer) pattern is retained as a write-serialization layer, but PostgreSQL's concurrency model means readers never block on it.

## 3. PostgreSQL Setup

### Docker Compose

A `docker-compose.yml` at the project root provides a local PostgreSQL:

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_DB: douyin_barrage
      POSTGRES_USER: barrage
      POSTGRES_PASSWORD: barrage
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
```

Usage: `docker compose up -d`

### Configuration

Add a `database:` section to `config.yaml`:

```yaml
database:
  host: localhost
  port: 5432
  dbname: douyin_barrage
  user: barrage
  password: barrage
  pool_min: 2
  pool_max: 10
```

The existing `PGHOST`/`PGPORT`/`PGDATABASE`/`PGUSER`/`PGPASSWORD` environment variables serve as fallback overrides.

> **Note:** No `type` field is needed — the project uses PostgreSQL directly. Rollback is handled via git checkout.

## 4. Connection Layer

Replace the SQLite thread-local connection factory (`_get_conn()`) with a **`psycopg2.pool.ThreadedConnectionPool`**:

| Aspect | SQLite (before) | PostgreSQL (after) |
|--------|----------------|-------------------|
| Factory | `threading.local()` cache | `ThreadedConnectionPool` singleton |
| Per-thread | First call creates connection | `pool.getconn()` borrows from pool |
| Cleanup | `atexit` registry | `pool.putconn()` after use + `atexit` closeall |
| PRAGMAs | WAL, synchronous, cache, etc. | None needed (PG defaults are correct) |

### Connection wrapper

A context manager (`_get_conn` → `_with_conn`) ensures connections are always returned to the pool:

```python
def _with_conn(fn):
    """Decorator: borrow pool connection, call fn(conn, *args), return to pool."""
    def wrapper(*args, **kwargs):
        conn = _pool.getconn()
        try:
            return fn(conn, *args, **kwargs)
        finally:
            _pool.putconn(conn)
    return wrapper
```

The writer queue's `_flush_write_batch` and all query functions (`query_leaderboard`, etc.) will use this wrapper.

## 5. SQL Syntax Changes

### Placeholders

```
SQLite:  ?       →  PostgreSQL:  %s
```

### DDL Changes

| SQLite | PostgreSQL |
|--------|-----------|
| `INTEGER PRIMARY KEY AUTOINCREMENT` | `SERIAL PRIMARY KEY` |
| `TEXT` | `TEXT` (same, no change) |
| `INTEGER` | `INTEGER` (same, no change) |
| `DATETIME` | `TIMESTAMP` (native type) |

### Default Value Expressions

| SQLite | PostgreSQL |
|--------|-----------|
| `datetime('now', '+8 hours')` | `(NOW() AT TIME ZONE 'Asia/Shanghai')` |
| `datetime('now', '-30 days')` | `NOW() - INTERVAL '30 days'` |

### DML Changes

| SQLite | PostgreSQL |
|--------|-----------|
| `INSERT OR IGNORE INTO t ...` | `INSERT INTO t ... ON CONFLICT DO NOTHING` |
| `INSERT INTO t ... ON CONFLICT(u) DO UPDATE SET ...` | Same syntax — already compatible! |
| `MAX(excluded.col, t.col)` | `GREATEST(excluded.col, t.col)` |
| `COALESCE(a, b)` | `COALESCE(a, b)` — same |

### Function Changes

| SQLite | PostgreSQL |
|--------|-----------|
| `strftime('%Y-%m', ts)` | `TO_CHAR(ts, 'YYYY-MM')` |
| `strftime('%H', ts)` | `TO_CHAR(ts, 'HH24')` |
| `strftime('%Y-%m-%d %H:%M:%S', ts)` | `TO_CHAR(ts, 'YYYY-MM-DD HH24:MI:SS')` |
| `date(ts)` | `DATE(ts)` — same |
| `datetime('now')` | `NOW()` |
| `instr(str, x'02')` | `POSITION(chr(2) IN str)` |
| `instr(str, x'03')` | `POSITION(chr(3) IN str)` |

## 6. Schema Definition

All 9 tables are re-created in PostgreSQL with adapted DDL. The schema is defined in a single `init_db()` function called at module import, identical to the current pattern.

### Table: sessions

```sql
CREATE TABLE IF NOT EXISTS sessions (
    id SERIAL PRIMARY KEY,
    room_id TEXT NOT NULL,
    anchor_name TEXT DEFAULT '',
    start_time TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'Asia/Shanghai'),
    end_time TIMESTAMP,
    status TEXT DEFAULT 'live'
);
```

### Table: users

```sql
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    user_id TEXT UNIQUE NOT NULL,
    user_name TEXT NOT NULL,
    fans_club TEXT DEFAULT '',
    grade TEXT DEFAULT '',
    sec_uid TEXT DEFAULT '',
    avatar_url TEXT DEFAULT '',
    notes TEXT DEFAULT '',
    tags TEXT DEFAULT '',
    is_anonymous INTEGER DEFAULT 0,
    anonymous_label TEXT DEFAULT '',
    first_seen TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'Asia/Shanghai'),
    last_seen TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'Asia/Shanghai')
);
```

### Table: contributions

```sql
CREATE TABLE IF NOT EXISTS contributions (
    id SERIAL PRIMARY KEY,
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
    recorded_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'Asia/Shanghai'),
    UNIQUE(session_id, user_id)
);
```

### Table: chat_logs

```sql
CREATE TABLE IF NOT EXISTS chat_logs (
    id SERIAL PRIMARY KEY,
    session_id INTEGER REFERENCES sessions(id),
    user_id TEXT NOT NULL,
    user_name TEXT NOT NULL,
    content TEXT NOT NULL,
    grade TEXT DEFAULT '',
    fans_club TEXT DEFAULT '',
    created_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'Asia/Shanghai')
);
```

### Table: gift_logs

```sql
CREATE TABLE IF NOT EXISTS gift_logs (
    id SERIAL PRIMARY KEY,
    session_id INTEGER REFERENCES sessions(id),
    user_id TEXT NOT NULL,
    user_name TEXT NOT NULL,
    gift_name TEXT NOT NULL,
    gift_count INTEGER DEFAULT 1,
    diamond_total INTEGER DEFAULT 0,
    grade TEXT DEFAULT '',
    fans_club TEXT DEFAULT '',
    created_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'Asia/Shanghai')
);
```

### Table: upgrade_logs

```sql
CREATE TABLE IF NOT EXISTS upgrade_logs (
    id SERIAL PRIMARY KEY,
    session_id INTEGER DEFAULT 0,
    user_id TEXT NOT NULL,
    user_name TEXT NOT NULL,
    upgrade_type TEXT NOT NULL,
    from_level INTEGER DEFAULT 0,
    to_level INTEGER DEFAULT 0,
    anchor_name TEXT DEFAULT '',
    created_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'Asia/Shanghai')
);
```

### Table: daily_stats

```sql
CREATE TABLE IF NOT EXISTS daily_stats (
    id SERIAL PRIMARY KEY,
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
```

### Table: monthly_stats

```sql
CREATE TABLE IF NOT EXISTS monthly_stats (
    id SERIAL PRIMARY KEY,
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
```

### Table: streamer_config

```sql
CREATE TABLE IF NOT EXISTS streamer_config (
    live_id TEXT PRIMARY KEY,
    anchor_name TEXT DEFAULT '',
    enabled INTEGER DEFAULT 0,
    added_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'Asia/Shanghai')
);
```

### Table: pk_rounds

```sql
CREATE TABLE IF NOT EXISTS pk_rounds (
    id SERIAL PRIMARY KEY,
    session_id INTEGER REFERENCES sessions(id),
    start_time TEXT NOT NULL,
    end_time TEXT,
    duration_sec INTEGER DEFAULT 0,
    mode TEXT DEFAULT '',
    participants TEXT DEFAULT '',
    participant_count INTEGER DEFAULT 0,
    self_score INTEGER DEFAULT 0,
    opponent_score INTEGER DEFAULT 0,
    result TEXT DEFAULT '',
    created_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'Asia/Shanghai')
);
```

### Indexes

All existing indexes re-created with adapted names:
- `idx_contributions_session` on `contributions(session_id)`
- `idx_contributions_user` on `contributions(user_id)`
- `idx_chat_logs_user` on `chat_logs(user_id)`
- `idx_monthly_stats` on `monthly_stats(year_month, sessions_1000 DESC)`
- `idx_daily_stats` on `daily_stats(date, sessions_1000 DESC)`
- `idx_contributions_qualified` on `contributions(qualified_1000)`
- `idx_gift_dedup_ts` (UNIQUE) on `gift_logs(session_id, user_id, gift_name, diamond_total, gift_count, created_at)`
- `idx_upgrade_logs_session` on `upgrade_logs(session_id)`
- `idx_upgrade_logs_type` on `upgrade_logs(upgrade_type)`
- `idx_upgrade_logs_dedup` (UNIQUE) on `upgrade_logs(session_id, user_id, upgrade_type, from_level, to_level)`

## 7. Data Migration

A one-shot script `scripts/migrate_sqlite_to_pg.py`:

1. Connect to both SQLite (`data/douyin_barrage.db`) and PostgreSQL (from config)
2. Create all PostgreSQL tables (call `init_db()` on the PG connection)
3. Migrate each table in dependency order:
   - `sessions` → `users` → `contributions` → `chat_logs` → `gift_logs` → `upgrade_logs` → `daily_stats` → `monthly_stats` → `streamer_config` → `pk_rounds`
4. For each table: `SELECT *` from SQLite, then `INSERT INTO pg_table (...) VALUES ...` with `ON CONFLICT DO NOTHING`
5. Re-map SQLite `INTEGER` PKs to PostgreSQL `SERIAL` (the serial auto-generates on insert; explicit insert of the old PK works fine without specifying the serial column)
6. Wrap in a transaction for atomicity
7. Report row counts per table

## 8. Rollback

Rollback is handled via version control:
- `git checkout` on `base/parser.py`, `config.yaml`, and `requirements.txt` restores the SQLite version
- The old SQLite data file (`data/douyin_barrage.db`) is never deleted or modified by the migration script
- The migration script is safe to re-run (uses `IF NOT EXISTS` + `ON CONFLICT DO NOTHING`)

## 9. Files Changed

| File | Change Type |
|------|------------|
| `docker-compose.yml` | **New** |
| `config.yaml` | **Edit** — add `database:` section |
| `requirements.txt` | **Edit** — add `psycopg2-binary` |
| `base/parser.py` | **Major edit** — connection pool, SQL rewrites, init_db |
| `app.py` | **Minor edit** — adapt a few query patterns |
| `scripts/migrate_sqlite_to_pg.py` | **New** |

## 10. What Stays the Same

- Writer-queue pattern (producer-consumer with `queue.Queue`)
- All public function signatures: `record_chat`, `record_gift`, `upsert_user`, etc.
- Flask route logic
- `service/fetcher.py`, `service/network.py`, `base/utils.py`, `base/output.py` — untouched
- All time values stored as UTC+8 (same behavioral semantics)
- `auto_vacuum` startup check (replaced by PG `VACUUM` settings, which default to auto)

#!/usr/bin/env python3
"""One-shot migration: copy all data from SQLite to PostgreSQL.

Safe to re-run (uses IF NOT EXISTS + ON CONFLICT DO NOTHING).
SQLite file is never modified.
"""
import os
import sys
import sqlite3

SQLITE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'douyin_barrage.db')

TABLES = [
    ('sessions', 'id'),
    ('users', 'id'),
    ('contributions', 'id'),
    ('chat_logs', 'id'),
    ('gift_logs', 'id'),
    ('upgrade_logs', 'id'),
    ('daily_stats', 'id'),
    ('monthly_stats', 'id'),
    ('streamer_config', None),
    ('pk_rounds', 'id'),
]


def get_sqlite_cols(cur, table):
    cur.execute(f'PRAGMA table_info({table})')
    return [r[1] for r in cur.fetchall()]


def get_pg_cols(cur, table):
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = %s ORDER BY ordinal_position", (table,))
    return [r[0] for r in cur.fetchall()]


def migrate_table(sqlite_conn, pg_conn, table, order_col):
    sqlite_cur = sqlite_conn.cursor()
    # Check if table exists in SQLite
    sqlite_cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    if not sqlite_cur.fetchone():
        print(f"  {table}: not found in SQLite, skipping")
        return 0
    sqlite_cols = get_sqlite_cols(sqlite_cur, table)

    pg_cur = pg_conn.cursor()
    pg_cols = get_pg_cols(pg_cur, table)

    common = [c for c in sqlite_cols if c in pg_cols]
    if not common:
        print(f"  No common columns for {table}, skipping"); return 0
    col_list = ', '.join(common)
    ph = ', '.join(['%s'] * len(common))

    order_sql = f'ORDER BY {order_col}' if order_col else ''
    sqlite_cur.execute(f'SELECT {col_list} FROM {table} {order_sql}')
    rows = sqlite_cur.fetchall()
    if not rows:
        print(f"  {table}: 0 rows (empty)"); return 0

    insert_sql = f'INSERT INTO {table} ({col_list}) VALUES ({ph}) ON CONFLICT DO NOTHING'
    inserted = 0
    for i in range(0, len(rows), 500):
        batch = rows[i:i + 500]
        try:
            pg_cur.executemany(insert_sql, batch)
            inserted += len(batch)
        except Exception as e:
            print(f"  Batch error at {i} in {table}: {e}")
            for row in batch:
                try:
                    pg_cur.execute(insert_sql, row); inserted += 1
                except Exception:
                    pg_conn.rollback()
    print(f"  {table}: {len(rows)} read, {inserted} inserted")
    return inserted


def main():
    print("=" * 60)
    print("SQLite -> PostgreSQL Migration")
    print("=" * 60)
    print(f"SQLite: {SQLITE_PATH}")
    if not os.path.exists(SQLITE_PATH):
        print(f"DB not found: {SQLITE_PATH}"); sys.exit(1)
    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    print("Connected to SQLite")

    print("\nInitializing PostgreSQL schema...")
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    os.environ.setdefault('PGPOOL_MIN', '1')
    os.environ.setdefault('PGPOOL_MAX', '2')
    from base.parser import _get_conn, _put_conn
    pg_wrapped = _get_conn()
    pg_conn = pg_wrapped._conn
    pg_conn.autocommit = False
    print("Schema initialized")

    print("\nMigrating data...")
    total = 0
    for table, order_col in TABLES:
        total += migrate_table(sqlite_conn, pg_conn, table, order_col)
        pg_conn.commit()

    # Repair SERIAL sequences after data migration
    print("\nRepairing sequences...")
    seq_tables = [('sessions', 'id'), ('users', 'id'), ('contributions', 'id'),
                  ('chat_logs', 'id'), ('gift_logs', 'id'), ('upgrade_logs', 'id'),
                  ('daily_stats', 'id'), ('monthly_stats', 'id'), ('pk_rounds', 'id')]
    for tbl, col in seq_tables:
        try:
            pg_cur = pg_conn.cursor()
            pg_cur.execute(f"SELECT setval('{tbl}_{col}_seq', COALESCE((SELECT MAX({col}) FROM {tbl}), 1))")
            seq_val = pg_cur.fetchone()[0]
            print(f"  {tbl}_{col}_seq -> {seq_val}")
        except Exception as e:
            print(f"  {tbl}: skipped ({e})")
    pg_conn.commit()

    print(f"\n{'=' * 60}")
    print(f"Migration complete: {total} total rows migrated")
    print(f"{'=' * 60}")
    print(f"\nSQLite file untouched: {SQLITE_PATH}")
    _put_conn(pg_wrapped)
    sqlite_conn.close()


if __name__ == '__main__':
    main()

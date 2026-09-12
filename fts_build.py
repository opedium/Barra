"""Build FTS5 full-text search indexes for faster user/chat/gift searching.

Run once:  python3 fts_build.py
Then update query_search to use the FTS tables.
"""

import sqlite3
import time
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'douyin_barrage.db')

def log(msg):
    print(f'  {msg}')

def build_fts():
    t0 = time.time()
    conn = sqlite3.connect(DB_PATH, timeout=120)
    conn.execute('PRAGMA journal_mode=DELETE')
    conn.execute('PRAGMA synchronous=OFF')

    # 1. Users FTS
    log('Creating users_fts...')
    conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS users_fts USING fts5(
            user_id, user_name, tokenize='unicode61'
        )
    """)
    cnt = conn.execute('SELECT COUNT(*) FROM users_fts').fetchone()[0]
    total_users = conn.execute('SELECT COUNT(*) FROM users').fetchone()[0]
    if cnt < total_users:
        conn.execute('DELETE FROM users_fts')
        conn.execute("INSERT INTO users_fts(user_id, user_name) SELECT user_id, user_name FROM users")
        log(f'  Indexed {total_users} users')
    else:
        log(f'  Skipped ({cnt} already indexed)')

    # 2. Chat FTS
    log('Creating chat_logs_fts...')
    conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS chat_logs_fts USING fts5(
            user_name, content, tokenize='unicode61'
        )
    """)
    cnt = conn.execute('SELECT COUNT(*) FROM chat_logs_fts').fetchone()[0]
    total_chats = conn.execute('SELECT COUNT(*) FROM chat_logs').fetchone()[0]
    if cnt < total_chats:
        conn.execute('DELETE FROM chat_logs_fts')
        conn.execute("INSERT INTO chat_logs_fts(rowid, user_name, content) SELECT rowid, user_name, content FROM chat_logs")
        log(f'  Indexed {total_chats} chat messages')
    else:
        log(f'  Skipped ({cnt} already indexed)')

    # 3. Gift FTS
    log('Creating gift_logs_fts...')
    conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS gift_logs_fts USING fts5(
            user_name, gift_name, tokenize='unicode61'
        )
    """)
    cnt = conn.execute('SELECT COUNT(*) FROM gift_logs_fts').fetchone()[0]
    total_gifts = conn.execute('SELECT COUNT(*) FROM gift_logs').fetchone()[0]
    if cnt < total_gifts:
        conn.execute('DELETE FROM gift_logs_fts')
        conn.execute("INSERT INTO gift_logs_fts(rowid, user_name, gift_name) SELECT rowid, user_name, gift_name FROM gift_logs")
        log(f'  Indexed {total_gifts} gift logs')
    else:
        log(f'  Skipped ({cnt} already indexed)')

    conn.commit()
    conn.close()
    log(f'Done in {time.time()-t0:.1f}s')

if __name__ == '__main__':
    build_fts()

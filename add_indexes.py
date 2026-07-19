import sqlite3
c = sqlite3.connect('data/douyin_barrage.db')

indexes = [
    "CREATE INDEX IF NOT EXISTS idx_chat_logs_session ON chat_logs(session_id)",
    "CREATE INDEX IF NOT EXISTS idx_chat_logs_time ON chat_logs(created_at)",
    "CREATE INDEX IF NOT EXISTS idx_chat_logs_session_user ON chat_logs(session_id, user_id)",
    "CREATE INDEX IF NOT EXISTS idx_gift_logs_session ON gift_logs(session_id)",
    "CREATE INDEX IF NOT EXISTS idx_gift_logs_time ON gift_logs(created_at)",
    "CREATE INDEX IF NOT EXISTS idx_gift_logs_session_user ON gift_logs(session_id, user_id)",
    "CREATE INDEX IF NOT EXISTS idx_gift_logs_diamond ON gift_logs(diamond_total)",
    "CREATE INDEX IF NOT EXISTS idx_users_name ON users(user_name)",
    "CREATE INDEX IF NOT EXISTS idx_users_sec_uid ON users(sec_uid)",
]

for sql in indexes:
    name = sql.split('ON ')[1].split('(')[0].strip()
    print(f'  Adding {name}...', end=' ')
    try:
        c.execute(sql)
        print('OK')
    except Exception as e:
        print(f'FAIL: {e}')

c.commit()
c.close()
print('Done. Indexes added.')

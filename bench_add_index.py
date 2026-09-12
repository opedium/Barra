import sqlite3, time

conn = sqlite3.connect("/mnt/Barra/data/douyin_barrage.db", timeout=120)
conn.execute("PRAGMA journal_mode=DELETE")

# Add indexes
t0 = time.time()
conn.execute("CREATE INDEX IF NOT EXISTS idx_gift_logs_created ON gift_logs(created_at)")
conn.commit()
print("idx_gift_logs_created: %.1fs" % (time.time() - t0))

t0 = time.time()
conn.execute("CREATE INDEX IF NOT EXISTS idx_chat_logs_created ON chat_logs(created_at)")
conn.commit()
print("idx_chat_logs_created: %.1fs" % (time.time() - t0))

# Test optimized query
t0 = time.time()
r = conn.execute("SELECT COUNT(DISTINCT user_id) FROM gift_logs WHERE created_at >= datetime('now', 'start of day')").fetchone()
print("optimized today users: %s %.2fs" % (r[0], time.time() - t0))

t0 = time.time()
r = conn.execute("SELECT COUNT(*) FROM gift_logs WHERE created_at >= datetime('now', 'start of day')").fetchone()
print("optimized today gifts: %s %.2fs" % (r[0], time.time() - t0))

t0 = time.time()
r = conn.execute("SELECT COUNT(*) FROM chat_logs WHERE created_at >= datetime('now', 'start of day')").fetchone()
print("optimized today chats: %s %.2fs" % (r[0], time.time() - t0))

conn.close()

import sqlite3, time

conn = sqlite3.connect("/mnt/Barra/data/douyin_barrage.db", timeout=60)
conn.execute("PRAGMA journal_mode=DELETE")
conn.execute("PRAGMA synchronous=NORMAL")

queries = [
    ("gift_logs total", "SELECT COUNT(*) FROM gift_logs"),
    ("chat_logs total", "SELECT COUNT(*) FROM chat_logs"),
    ("sessions total", "SELECT COUNT(*) FROM sessions"),
    ("today gifts", "SELECT COUNT(*) FROM gift_logs WHERE date(created_at)=date('now')"),
    ("today chats", "SELECT COUNT(*) FROM chat_logs WHERE date(created_at)=date('now')"),
    ("today users", "SELECT COUNT(DISTINCT user_id) FROM gift_logs WHERE date(created_at)=date('now')"),
]

for label, q in queries:
    t0 = time.time()
    r = conn.execute(q).fetchone()
    print("%-20s %10s  %.2fs" % (label, r[0], time.time() - t0))

# Find live session
t0 = time.time()
sid_row = conn.execute("SELECT id FROM sessions WHERE status='live' ORDER BY id DESC LIMIT 1").fetchone()
print("%-20s %.2fs" % ("find live session", time.time() - t0))

if sid_row:
    sid = sid_row[0]
    t0 = time.time()
    g = conn.execute("SELECT COUNT(*) FROM gift_logs WHERE session_id=?", (sid,)).fetchone()[0]
    c = conn.execute("SELECT COUNT(*) FROM chat_logs WHERE session_id=?", (sid,)).fetchone()[0]
    u = conn.execute("SELECT COUNT(*) FROM contributions WHERE session_id=? AND qualified_1000=1", (sid,)).fetchone()[0]
    print("%-20s %.2fs (g=%s c=%s u=%s)" % ("live session counts", time.time() - t0, g, c, u))

    t0 = time.time()
    r = conn.execute(
        "SELECT c.user_id, c.consume FROM contributions c "
        "WHERE c.session_id=? AND c.qualified_1000=1 "
        "ORDER BY c.consume DESC LIMIT 50", (sid,)
    ).fetchall()
    print("%-20s %.2fs (%d rows)" % ("top 50 contributors", time.time() - t0, len(r)))

t0 = time.time()
r = conn.execute("SELECT user_name, content FROM chat_logs ORDER BY id DESC LIMIT 50").fetchall()
print("%-20s %.2fs" % ("recent 50 chats", time.time() - t0))

t0 = time.time()
r = conn.execute("SELECT id FROM sessions ORDER BY id DESC LIMIT 10").fetchall()
print("%-20s %.2fs (%d rows)" % ("recent 10 sessions", time.time() - t0))

conn.close()

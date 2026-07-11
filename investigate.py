import sqlite3, sys
conn = sqlite3.connect('data/douyin_barrage.db')
conn.row_factory = sqlite3.Row

# Find the top contributor with empty/malformed user_id
rows = conn.execute("""
    SELECT c.user_id, c.user_name, c.consume, c.session_id, s.anchor_name
    FROM contributions c
    JOIN sessions s ON s.id = c.session_id
    WHERE c.user_id IS NULL OR c.user_id = '' OR c.user_id LIKE 'W%'
    ORDER BY c.consume DESC
    LIMIT 10
""").fetchall()
print('=== Suspicious contribution records ===')
for r in rows:
    print(f'  uid={r["user_id"]!r} name={r["user_name"]!r} consume={r["consume"]} session={r["session_id"]} anchor={r["anchor_name"]!r}')

# Check gift_logs for WebcastRoomMessage as user_name or user_id
print()
rows2 = conn.execute("""
    SELECT user_id, user_name, gift_name, diamond_total, session_id
    FROM gift_logs
    WHERE diamond_total > 5000
    ORDER BY diamond_total DESC
    LIMIT 10
""").fetchall()
print('=== Top 10 gift records by diamond ===')
for r in rows2:
    print(f'  uid={r["user_id"]!r} name={r["user_name"]!r} gift={r["gift_name"]} diamonds={r["diamond_total"]} session={r["session_id"]}')

# Direct check for user with 38940
print()
rows3 = conn.execute("""
    SELECT user_id, user_name, consume, session_id, qualified_1000, qualified_3000
    FROM contributions
    WHERE consume BETWEEN 35000 AND 45000
    ORDER BY consume DESC
    LIMIT 10
""").fetchall()
print('=== Contributions around 38k-40k ===')
for r in rows3:
    print(f'  uid={r["user_id"]!r} name={r["user_name"]!r} consume={r["consume"]} session={r["session_id"]}')

conn.close()

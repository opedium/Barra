import sqlite3
c = sqlite3.connect('data/douyin_barrage.db')
sid = c.execute('SELECT id FROM sessions ORDER BY id DESC LIMIT 1').fetchone()[0]
rows = c.execute("SELECT user_id, user_name, sec_uid, display_id FROM gift_logs WHERE session_id=? AND (user_id='111111' OR user_name LIKE '%神秘人%')", (sid,)).fetchall()
print(f'S{sid} bad rows: {len(rows)}')
for r in rows:
    print(f'  uid={r[0]} name={r[1]} sec_uid={str(r[2])[:30] if r[2] else "-"} display_id={r[3]}')
# Also check the protobuf debug - what fields are available in the raw frame for these users
c.close()

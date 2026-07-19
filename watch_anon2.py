import sqlite3, sys
sys.stdout.reconfigure(encoding='utf-8')
c = sqlite3.connect('data/douyin_barrage.db')
sid = c.execute('SELECT id FROM sessions ORDER BY id DESC LIMIT 1').fetchone()[0]
rows = c.execute("SELECT user_id, user_name, sec_uid, display_id FROM gift_logs WHERE session_id=? AND user_name LIKE 'dou%' LIMIT 10", (sid,)).fetchall()
print(f'Session {sid} - dou users:')
for r in rows:
    print(f'  uid={r[0]} name={r[1]} sec_uid={str(r[2])[:25] if r[2] else "-"} display_id={r[3]}')
all_anon = c.execute("SELECT user_id, user_name FROM gift_logs WHERE session_id=? AND (user_id='111111' OR user_name LIKE 'dou%' OR user_name LIKE '%神秘人%')", (sid,)).fetchall()
print(f'\nAll anon-like rows: {len(all_anon)}')
for r in all_anon:
    print(f'  uid={r[0]} name={r[1]}')
c.close()

import sqlite3
import sys
try:
    sys.stdout.reconfigure(encoding='utf-8')
except:
    pass

c = sqlite3.connect('data/douyin_barrage.db')
sid = c.execute('SELECT id FROM sessions ORDER BY id DESC LIMIT 1').fetchone()[0]

rows = c.execute("SELECT user_name, sec_uid, display_id FROM gift_logs WHERE session_id=? AND user_id='111111'", (sid,)).fetchall()
print('Rows with 111111:', len(rows))
for r in rows:
    sec = str(r[1])[:35] if r[1] else 'EMPTY'
    did = r[2] if r[2] else 'EMPTY'
    print(f'  name={r[0]} sec_uid={sec} display_id={did}')

rows2 = c.execute("SELECT user_name, sec_uid FROM gift_logs WHERE session_id=? AND user_name LIKE 'dou%'", (sid,)).fetchall()
print(f'\nDou users: {len(rows2)}')
for r in rows2[:5]:
    sec = str(r[1])[:35] if r[1] else 'EMPTY'
    print(f'  name={r[0]} sec_uid={sec}')
c.close()

import sqlite3
c = sqlite3.connect('data/douyin_barrage.db')
sid = c.execute('SELECT id FROM sessions ORDER BY id DESC LIMIT 1').fetchone()
if sid:
    sid = sid[0]
    total = c.execute('SELECT COUNT(*) FROM gift_logs WHERE session_id=?', (sid,)).fetchone()[0]
    bad = c.execute("SELECT COUNT(*) FROM gift_logs WHERE session_id=? AND (user_id='111111' OR user_name LIKE '%神秘人%')", (sid,)).fetchone()[0]
    dou = c.execute("SELECT COUNT(*) FROM gift_logs WHERE session_id=? AND (user_name LIKE 'dou%')", (sid,)).fetchone()[0]
    print(f'S{sid}: {total} gifts, {bad} with 111111/神秘人, {dou} dou users')
c.close()

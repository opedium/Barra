import sqlite3
c = sqlite3.connect('data/douyin_barrage.db')
# Latest session
sid = c.execute('SELECT id FROM sessions ORDER BY id DESC LIMIT 1').fetchone()
if sid:
    sid = sid[0]
    print(f'Session {sid}')
    # Count gifts
    total = c.execute('SELECT COUNT(*) FROM gift_logs WHERE session_id=?', (sid,)).fetchone()[0]
    bad = c.execute("SELECT COUNT(*) FROM gift_logs WHERE session_id=? AND user_id='111111'", (sid,)).fetchone()[0]
    good = total - bad
    print(f'  Total: {total}, 111111: {bad}, Good: {good}')
    # Show sample bad rows
    if bad:
        rows = c.execute("SELECT user_id, user_name, sec_uid, display_id FROM gift_logs WHERE session_id=? AND user_id='111111' LIMIT 5", (sid,)).fetchall()
        print(f'  Sample 111111 rows:')
        for r in rows:
            print(f'    user_id={r[0]} user_name={r[1]} sec_uid={r[2][:30] if r[2] else "(empty)"} display_id={r[3]}')
    # Show some good rows from dou users
    rows = c.execute("SELECT user_id, user_name, sec_uid, display_id FROM gift_logs WHERE session_id=? AND user_name LIKE 'dou%' LIMIT 5", (sid,)).fetchall()
    if rows:
        print(f'  Sample dou rows:')
        for r in rows:
            print(f'    user_id={r[0]} user_name={r[1]} sec_uid={r[2][:30] if r[2] else "(empty)"} display_id={r[3]}')
    # Check for the cookie account
    me = c.execute("SELECT user_id, user_name FROM users WHERE user_name LIKE '%坐看%' LIMIT 3").fetchall()
    if me:
        print(f'\n  Cookie account match:')
        for r in me:
            print(f'    user_id={r[0]} user_name={r[1]}')
c.close()

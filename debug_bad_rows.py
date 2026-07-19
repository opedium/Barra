import sqlite3
c = sqlite3.connect('data/douyin_barrage.db')
sid = c.execute('SELECT id FROM sessions ORDER BY id DESC LIMIT 1').fetchone()[0]

# Count totals
total = c.execute('SELECT COUNT(*) FROM gift_logs WHERE session_id=?', (sid,)).fetchone()[0]
bad = c.execute("SELECT COUNT(*) FROM gift_logs WHERE session_id=? AND user_id='111111'", (sid,)).fetchone()[0]
print(f'S{sid}: {total} gifts total, {bad} with 111111')

# Show bad rows
rows = c.execute("SELECT user_id, user_name, sec_uid, display_id, badge_url, fansclub_badge FROM gift_logs WHERE session_id=? AND (user_id='111111' OR user_name LIKE '%神秘人%')", (sid,)).fetchall()
for r in rows:
    print(f'  uid={r[0]} name={r[1]} sec_uid={str(r[2])[:30] if r[2] else "EMPTY"} display_id={r[3]} badge={r[4][:40] if r[4] else "-"}')

# Check for ANY dou users in session
dou = c.execute("SELECT user_id, user_name, sec_uid, display_id FROM gift_logs WHERE session_id=? AND user_name LIKE 'dou%'", (sid,)).fetchall()
print(f'\nDou users: {len(dou)}')
for r in dou:
    print(f'  uid={r[0]} name={r[1]} sec_uid={str(r[2])[:30] if r[2] else "EMPTY"} display_id={r[3]}')

# Check the new schema columns were created
cols = c.execute("PRAGMA table_info(gift_logs)").fetchall()
for col in cols:
    if col[1] in ('display_id', 'sec_uid', 'badge_url', 'fansclub_badge'):
        print(f'  Column: {col[1]} type={col[2]} default={col[4]} already_exists=True')
c.close()

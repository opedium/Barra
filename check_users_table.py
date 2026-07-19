import sqlite3
c = sqlite3.connect('data/douyin_barrage.db')

# Check the latest resolved dou user - does users table have their info?
rows = c.execute("SELECT user_id, user_name, sec_uid, display_id FROM users WHERE user_name LIKE 'dou%' OR user_name LIKE '%神秘人%' LIMIT 10").fetchall()
print('Anonymous users in users table:')
for r in rows:
    sec = str(r[2])[:30] if r[2] else 'EMPTY'
    print(f'  id={r[0]} name={r[1]} sec_uid={sec} display_id={r[3]}')

# Check if newly resolved dou users exist with real IDs
rows2 = c.execute("SELECT user_id, user_name, sec_uid FROM users WHERE user_id NOT LIKE 'anon%' AND user_id != '111111' AND user_id != '' AND user_name LIKE 'dou%' LIMIT 10").fetchall()
print(f'\nResolved dou users (have real user_id): {len(rows2)}')
for r in rows2:
    sec = str(r[2])[:30] if r[2] else 'EMPTY'
    print(f'  id={r[0]} name={r[1]} sec_uid={sec}')

c.close()

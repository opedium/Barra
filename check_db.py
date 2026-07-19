import sqlite3
c = sqlite3.connect('data/douyin_barrage.db')
idx = c.execute("SELECT name, sql FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'").fetchall()
print('INDEXES:')
for r in idx:
    print(' ', r[0], r[1][:80] if r[1] else '')
tables = c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
for t in tables:
    cnt = c.execute(f'SELECT COUNT(*) FROM "{t[0]}"').fetchone()[0]
    print(f'  {t[0]}: {cnt} rows')
c.close()

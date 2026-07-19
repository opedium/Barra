from base.utils import load_cookies
import requests, json

s = requests.Session()
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64) AppleWebKit/537.36'}
s.headers.update(headers)

display_id = 'dou5519115'

# Without cookies
r1 = s.get(f'https://www.douyin.com/aweme/v1/web/query/user/?unique_id={display_id}', timeout=15)
print('=== query/user WITHOUT cookies ===')
print('Status:', r1.status_code)
d1 = r1.json()
print('user_uid:', d1.get('user_uid'))
print('status_code:', d1.get('status_code'))
print('Full:', json.dumps(d1, indent=2, ensure_ascii=False)[:500])

# With cookies
cookies = load_cookies('cookie.txt')
for name, value in cookies.items():
    s.cookies.set(name, value, domain='.douyin.com')

r2 = s.get(f'https://www.douyin.com/aweme/v1/web/query/user/?unique_id={display_id}', timeout=15)
print('\n=== query/user WITH cookies ===')
print('Status:', r2.status_code)
d2 = r2.json()
print('user_uid:', d2.get('user_uid'))
print('status_code:', d2.get('status_code'))
print('Full:', json.dumps(d2, indent=2, ensure_ascii=False)[:500])

# Test by sec_uid: get a known sec_uid from the DB
import sqlite3
c = sqlite3.connect('data/douyin_barrage.db')
sid = c.execute('SELECT id FROM sessions ORDER BY id DESC LIMIT 1').fetchone()[0]
sec = c.execute("SELECT sec_uid FROM gift_logs WHERE session_id=? AND sec_uid != '' LIMIT 1", (sid,)).fetchone()
display = c.execute("SELECT display_id FROM gift_logs WHERE session_id=? AND display_id != '' LIMIT 1", (sid,)).fetchone()
if sec:
    print('\n=== fetch_user_info by sec_uid ===')
    sec_val = sec[0]
    r3 = s.get(f'https://www.douyin.com/web/api/v2/user/info/?sec_uid={sec_val}', timeout=15)
    print('Status:', r3.status_code)
    d3 = r3.json()
    print('Full:', json.dumps(d3, indent=2, ensure_ascii=False)[:500])
if display:
    print('\n=== fetch_user_info by user_id (from display_id) ===')
    print('display_id from DB:', display[0])
c.close()

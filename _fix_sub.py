import sys

with open('service/fetcher.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find subscribe block start
start = None
for i, line in enumerate(lines):
    if "rec_type == 'subscribe'" in line:
        start = i
        break
if start is None:
    print("NOT FOUND")
    sys.exit(1)

# Get the indentation of the original line
indent = lines[start][:len(lines[start]) - len(lines[start].lstrip())]

# Find end of subscribe block - the "if dump_pk_raw:" line
end = None
for i in range(start + 1, len(lines)):
    if lines[i].lstrip().startswith("if dump_pk_raw:"):
        end = i
        break
if end is None:
    print("END NOT FOUND")
    sys.exit(1)

new_block = f"""\
{indent}elif rec_type == 'subscribe' and rec_data.get('diamond'):
{indent}    # 订阅消息不含 user_id，只能通过用户名查 DB
{indent}    sub_name = rec_data.get('event', '') + rec_data.get('sub_type', '')
{indent}    sub_douyin_id = rec_data.get('douyin_id', '')  # Common.msg_id，仅用于去重
{indent}    sub_uname = rec_data.get('user_name', '')
{indent}    sub_grade = rec_data.get('grade', '')
{indent}    sub_club = rec_data.get('fans_club', '')
{indent}    sub_sec_uid = rec_data.get('sec_uid', '')
{indent}    sub_avatar = rec_data.get('avatar_url', '')

{indent}    if not sub_uname or len(sub_uname.strip()) < 1:
{indent}        logger.warning(f\"[订阅] 无法识别订阅用户，跳过 (msg_id={{sub_douyin_id}})\")
{indent}    else:
{indent}        try:
{indent}            found = _get_conn().execute(
{indent}                'SELECT user_id, sec_uid, avatar_url FROM users WHERE user_name = ? LIMIT 1',
{indent}                (sub_uname,)
{indent}            ).fetchone()
{indent}        except Exception:
{indent}            found = None
{indent}        if found:
{indent}            final_uid = found['user_id']
{indent}            if not sub_sec_uid and found['sec_uid']:
{indent}                sub_sec_uid = found['sec_uid']
{indent}            if not sub_avatar and found['avatar_url']:
{indent}                sub_avatar = found['avatar_url']
{indent}            now_ts = time.time()
{indent}            sk = (str(sub_douyin_id or sub_uname), str(sub_name))
{indent}            if sk in self._subscribe_dedup and now_ts - self._subscribe_dedup[sk] < 120:
{indent}                logger.debug(f\"[订阅] 去重跳过 {{sk}}\")
{indent}            else:
{indent}                self._subscribe_dedup[sk] = now_ts
{indent}                stale = [k for k, t in list(self._subscribe_dedup.items()) if now_ts - t > 180]
{indent}                for k in stale: del self._subscribe_dedup[k]
{indent}                upsert_user(final_uid, sub_uname, sub_grade, sub_club, sub_sec_uid, sub_avatar)
{indent}                record_gift(self._session_id, final_uid, sub_uname, sub_name or '订阅',
{indent}                            1, rec_data.get('diamond', 0), sub_grade, sub_club)
{indent}        else:
{indent}            logger.warning(f\"[订阅] 未找到用户: {{sub_uname}}，跳过记录\")
"""

result = lines[:start] + [new_block] + lines[end:]
with open('service/fetcher.py', 'w', encoding='utf-8') as f:
    f.writelines(result)
print("OK")

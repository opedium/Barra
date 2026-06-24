"""Add query_user_detail to sqlite_writer.py"""
f = 'base/sqlite_writer.py'
content = open(f, 'r', encoding='utf-8').read()

new_func = '''
def query_user_detail(user_id):
    """查询用户深度记录：按场次分组，含每场音浪/礼物/发言/时间线。"""
    conn = _get_conn()
    user = conn.execute('SELECT * FROM users WHERE user_id = ?', (user_id,)).fetchone()
    if not user:
        user = conn.execute('SELECT user_id, user_name FROM contributions WHERE user_id = ? LIMIT 1', (user_id,)).fetchone()
    if not user:
        return None

    total_consume = conn.execute('SELECT SUM(consume) FROM contributions WHERE user_id = ?', (user_id,)).fetchone()[0] or 0
    total_gifts = conn.execute('SELECT COUNT(*) FROM gift_logs WHERE user_id = ?', (user_id,)).fetchone()[0]
    total_chats = conn.execute('SELECT COUNT(*) FROM chat_logs WHERE user_id = ?', (user_id,)).fetchone()[0]

    rows = conn.execute('''
        SELECT s.id, s.anchor_name, s.start_time, s.end_time,
               COALESCE(c.consume, 0) as consume,
               (SELECT COUNT(*) FROM gift_logs WHERE session_id = s.id AND user_id = ?) as gift_count,
               (SELECT COUNT(*) FROM chat_logs WHERE session_id = s.id AND user_id = ?) as chat_count
        FROM sessions s
        LEFT JOIN contributions c ON c.session_id = s.id AND c.user_id = ?
        WHERE (SELECT COUNT(*) FROM gift_logs WHERE session_id = s.id AND user_id = ?) > 0
           OR (SELECT COUNT(*) FROM chat_logs WHERE session_id = s.id AND user_id = ?) > 0
           OR (c.id IS NOT NULL)
        ORDER BY s.id DESC
    ''', (user_id, user_id, user_id, user_id, user_id)).fetchall()

    result = dict(user)
    result['total_consume'] = total_consume
    result['total_gifts'] = total_gifts
    result['total_chats'] = total_chats
    result['sessions'] = []

    for s in rows:
        sd = dict(s)
        tl = []
        for row in conn.execute('''
            SELECT created_at as time, "chat" as type, content, "" as amount, grade
            FROM chat_logs WHERE session_id = ? AND user_id = ?
            ORDER BY created_at DESC LIMIT 20
        ''', (s['id'], user_id)).fetchall():
            tl.append(dict(row))
        for row in conn.execute('''
            SELECT created_at as time, "gift" as type, gift_name || " x" || gift_count as content, diamond_total as amount, "" as grade
            FROM gift_logs WHERE session_id = ? AND user_id = ?
            ORDER BY created_at DESC LIMIT 20
        ''', (s['id'], user_id)).fetchall():
            tl.append(dict(row))
        tl.sort(key=lambda x: str(x.get('time', '')), reverse=True)
        sd['timeline'] = tl[:30]
        result['sessions'].append(sd)

    return result
'''

# Insert before query_user function
idx = content.find('def query_user(user_id):')
if idx >= 0:
    content = content[:idx] + new_func + '\n\n' + content[idx:]
    open(f, 'w', encoding='utf-8').write(content)
    print('OK')
else:
    print('Pattern not found')

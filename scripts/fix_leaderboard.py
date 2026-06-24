"""Fix leaderboard to support sort by sessions_count."""
content = open('app.py', 'r', encoding='utf-8').read()

old = """@app.route('/api/leaderboard')
def api_leaderboard():
    threshold = request.args.get('threshold', 1000, type=int)
    period = request.args.get('period', 'session')
    page = request.args.get('page', 1, type=int)
    size = request.args.get('size', 100, type=int)
    conn = _get_conn()
    s = conn.execute("SELECT id FROM sessions WHERE status='live' ORDER BY id DESC LIMIT 1").fetchone()
    session_id = request.args.get('session_id', s[0] if s else None, type=int)
    return jsonify(query_leaderboard(threshold, period, page, size, session_id))"""

new = """@app.route('/api/leaderboard')
def api_leaderboard():
    threshold = request.args.get('threshold', 1000, type=int)
    period = request.args.get('period', 'session')
    page = request.args.get('page', 1, type=int)
    size = request.args.get('size', 100, type=int)
    sort_by = request.args.get('sort_by', 'consume')
    conn = _get_conn()
    s = conn.execute("SELECT id FROM sessions WHERE status='live' ORDER BY id DESC LIMIT 1").fetchone()
    session_id = request.args.get('session_id', s[0] if s else None, type=int)
    data = query_leaderboard(threshold, period, page, size, session_id)
    if sort_by == 'sessions' and data.get('users'):
        data['users'].sort(key=lambda u: (-u.get('sessions_count', 0), -u.get('consume', 0)))
        for i, u in enumerate(data['users']):
            u['rank'] = i + 1
    return jsonify(data)"""

if old in content:
    content = content.replace(old, new)
    open('app.py', 'w', encoding='utf-8').write(content)
    print('OK')
else:
    print('Pattern not found')

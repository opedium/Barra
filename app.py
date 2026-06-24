#!/usr/bin/python
# coding:utf-8
"""Flask Web 服务器 — 弹幕后台管理面板

用法:
  python app.py [--port=8080] [--host=0.0.0.0]
"""

import argparse
import csv
import io
import os
import sys

from flask import Flask, jsonify, render_template, request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from base.sqlite_writer import (
    query_leaderboard, query_user, query_user_detail, query_user_timeline,
    query_chat, query_anonymous, query_million,
    query_sessions, query_session_detail, query_search, _get_conn, DB_PATH
)

app = Flask(__name__)


@app.route('/')
def index():
    conn = _get_conn()
    live = conn.execute("""
        SELECT s.*,
            (SELECT COUNT(*) FROM gift_logs WHERE session_id=s.id) as total_gifts,
            (SELECT COUNT(*) FROM chat_logs WHERE session_id=s.id) as total_chats,
            (SELECT COUNT(*) FROM contributions WHERE session_id=s.id AND qualified_1000=1) as user_count
        FROM sessions s WHERE status='live' ORDER BY s.id DESC LIMIT 1
    """).fetchone()
    total_gifts = conn.execute('SELECT COUNT(*) FROM gift_logs').fetchone()[0]
    total_chats = conn.execute('SELECT COUNT(*) FROM chat_logs').fetchone()[0]
    total_sessions = conn.execute('SELECT COUNT(*) FROM sessions').fetchone()[0]
    today = conn.execute("SELECT COUNT(*) FROM gift_logs WHERE date(created_at)=date('now')").fetchone()[0]
    today_chats = conn.execute("SELECT COUNT(*) FROM chat_logs WHERE date(created_at)=date('now')").fetchone()[0]
    today_users = conn.execute("SELECT COUNT(DISTINCT user_id) FROM gift_logs WHERE date(created_at)=date('now')").fetchone()[0]
    recent = conn.execute('SELECT s.*, (SELECT COUNT(*) FROM gift_logs WHERE session_id=s.id) as total_gifts, (SELECT COUNT(*) FROM chat_logs WHERE session_id=s.id) as total_chats FROM sessions s ORDER BY s.id DESC LIMIT 10').fetchall()
    recent_chats = conn.execute('SELECT user_name, user_id, content, created_at as time FROM chat_logs ORDER BY id DESC LIMIT 10').fetchall()
    top_users = []
    if live:
        top = conn.execute('''
            SELECT c.user_id, c.user_name, c.consume, c.fans_club,
                   COALESCE(u.grade, (SELECT grade FROM chat_logs WHERE user_id=c.user_id AND grade!='' ORDER BY id DESC LIMIT 1), '') as grade,
                   (SELECT COUNT(*) FROM gift_logs WHERE session_id=c.session_id AND user_id=c.user_id) as gift_count,
                   (SELECT COUNT(*) FROM chat_logs WHERE session_id=c.session_id AND user_id=c.user_id) as chat_count
            FROM contributions c
            LEFT JOIN users u ON u.user_id = c.user_id
            WHERE c.session_id=? ORDER BY c.consume DESC LIMIT 10
        ''', (live['id'],)).fetchall()
        top_users = [dict(r) for r in top]
    return render_template('index.html',
        session=dict(live) if live else None,
        stats={'total_gifts': total_gifts, 'total_chats': total_chats, 'total_sessions': total_sessions, 'today_gifts': today, 'today_chats': today_chats, 'today_users': today_users},
        sessions=[dict(r) for r in recent],
        recent_chats=[dict(r) for r in recent_chats],
        top_users=top_users,
        db_path=DB_PATH)


@app.route('/leaderboard')
def leaderboard():
    return render_template('leaderboard.html')


@app.route('/session/<int:session_id>')
def session_detail(session_id):
    return render_template('session.html', session_id=session_id)


@app.route('/user')
def user_detail():
    return render_template('user.html', uid=request.args.get('uid', ''))


@app.route('/chat')
def chat():
    return render_template('chat.html')


@app.route('/anonymous')
def anonymous():
    return render_template('anonymous.html')


@app.route('/million')
def million():
    return render_template('million.html')


@app.route('/sessions')
def sessions():
    return render_template('sessions.html')


# ── API ──

@app.route('/api/leaderboard')
def api_leaderboard():
    threshold = request.args.get('threshold', 1000, type=int)
    period = request.args.get('period', 'session')
    page = request.args.get('page', 1, type=int)
    size = request.args.get('size', 100, type=int)
    sort_by = request.args.get('sort_by', 'consume')
    year_month = request.args.get('year_month', '')

    # If session_id passed explicitly, use it; otherwise resolve from live session
    session_id = request.args.get('session_id', None, type=int)
    if period == 'session' and session_id is None:
        conn = _get_conn()
        s = conn.execute("SELECT id FROM sessions WHERE status='live' ORDER BY id DESC LIMIT 1").fetchone()
        if s:
            session_id = s[0]
        else:
            # No live session and no explicit session_id — return empty
            return jsonify({'users': [], 'total': 0, 'page': 1})

    data = query_leaderboard(threshold, period, page, size, session_id, year_month)
    if sort_by == 'sessions' and data.get('users'):
        data['users'].sort(key=lambda u: (-u.get('sessions_count', 0), -u.get('consume', 0)))
        for i, u in enumerate(data['users']):
            u['rank'] = i + 1
    return jsonify(data)


@app.route('/api/user')
def api_user():
    uid = request.args.get('uid', '')
    if not uid:
        return jsonify({'error': 'uid required'}), 400
    if request.args.get('detail'):
        data = query_user_detail(uid)
    else:
        data = query_user(uid)
    if not data:
        return jsonify({'error': 'user not found'}), 404
    return jsonify(data)


@app.route('/api/user/<user_id>/timeline')
def api_user_timeline(user_id):
    return jsonify(query_user_timeline(user_id,
        request.args.get('type', 'all'),
        request.args.get('keyword', ''),
        request.args.get('page', 1, type=int),
        request.args.get('size', 50, type=int)))


@app.route('/api/user/<user_id>/gifts')
def api_user_gifts(user_id):
    conn = _get_conn()
    rows = conn.execute(
        'SELECT gift_name, gift_count, diamond_total, created_at FROM gift_logs WHERE user_id=? ORDER BY created_at DESC LIMIT 100',
        (user_id,)).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route('/api/chat')
def api_chat():
    return jsonify(query_chat(
        request.args.get('user_id', ''),
        request.args.get('keyword', ''),
        request.args.get('page', 1, type=int),
        request.args.get('size', 50, type=int)))


@app.route('/api/anonymous')
def api_anonymous():
    return jsonify(query_anonymous(
        request.args.get('page', 1, type=int),
        request.args.get('size', 50, type=int),
        request.args.get('search', '')))


@app.route('/api/million')
def api_million():
    return jsonify(query_million(
        request.args.get('year_month', ''),
        request.args.get('page', 1, type=int),
        request.args.get('size', 100, type=int)))


@app.route('/api/million/csv')
def api_million_csv():
    year_month = request.args.get('year_month', '')
    data = query_million(year_month, 1, 999999)
    users = data.get('users', [])
    return _make_csv_response(users,
        ['rank', 'user_id', 'user_name', 'total_consume', 'days_active'],
        f'million_{year_month or "all"}.csv')


@app.route('/api/sessions')
def api_sessions():
    return jsonify(query_sessions())

@app.route('/api/sessions/<int:session_id>')
def api_session_detail(session_id):
    data = query_session_detail(session_id)
    if not data:
        return jsonify({'error': 'session not found'}), 404
    return jsonify(data)


@app.route('/api/search')
def api_search():
    q = request.args.get('q', '')
    if not q:
        return jsonify({'users': [], 'total': 0, 'page': 1})
    return jsonify(query_search(q,
        request.args.get('page', 1, type=int),
        request.args.get('size', 20, type=int)))


@app.route('/api/stats')
def api_stats():
    conn = _get_conn()
    return jsonify({
        'total_users': conn.execute('SELECT COUNT(DISTINCT user_id) FROM contributions').fetchone()[0],
        'total_gifts': conn.execute('SELECT COUNT(*) FROM gift_logs').fetchone()[0],
        'total_chats': conn.execute('SELECT COUNT(*) FROM chat_logs').fetchone()[0],
        'total_sessions': conn.execute('SELECT COUNT(*) FROM sessions').fetchone()[0],
        'active_session': conn.execute("SELECT COUNT(*) FROM sessions WHERE status='live'").fetchone()[0] > 0,
    })


# ── CSV Export ──

def _make_csv_response(rows, fieldnames, filename, text_cols=None):
    """Build a Flask CSV response from a list of dicts.

    Adds BOM for Excel UTF-8 detection. Long numeric fields (e.g. user_id)
    get a leading tab to prevent Excel from displaying them as scientific notation.

    Args:
        rows: List of dicts.
        fieldnames: CSV column order.
        filename: Download filename.
        text_cols: Set of column names to force as text (\\t prefix).
    """
    if text_cols is None:
        text_cols = {'user_id', 'real_user_id'}
    si = io.StringIO()
    si.write('﻿')  # BOM — tells Excel this is UTF-8
    writer = csv.DictWriter(si, fieldnames=fieldnames, extrasaction='ignore')
    writer.writeheader()
    for row in rows:
        for col in text_cols:
            if col in row and row[col]:
                row[col] = '\t' + str(row[col])
        writer.writerow(row)
    out = si.getvalue()
    si.close()
    return (out, 200, {
        'Content-Type': 'text/csv; charset=utf-8',
        'Content-Disposition': f'attachment; filename="{filename}"',
    })


@app.route('/api/leaderboard/csv')
def api_leaderboard_csv():
    threshold = request.args.get('threshold', 1000, type=int)
    period = request.args.get('period', 'session')
    sort_by = request.args.get('sort_by', 'consume')
    year_month = request.args.get('year_month', '')
    session_id = request.args.get('session_id', None, type=int)

    if period == 'session' and session_id is None:
        conn = _get_conn()
        s = conn.execute("SELECT id FROM sessions WHERE status='live' ORDER BY id DESC LIMIT 1").fetchone()
        if s:
            session_id = s[0]
        else:
            return _make_csv_response([], ['user_id', 'user_name', 'consume'], 'leaderboard.csv')

    # Fetch ALL pages (size=999999)
    data = query_leaderboard(threshold, period, 1, 999999, session_id, year_month)
    users = data.get('users', [])

    if sort_by == 'sessions' and users:
        users.sort(key=lambda u: (-u.get('sessions_count', 0), -u.get('consume', 0)))
        for i, u in enumerate(users):
            u['rank'] = i + 1

    filename = f'leaderboard_{period}_{year_month or "all"}.csv'
    fieldnames = ['rank', 'user_id', 'user_name', 'grade', 'fans_club', 'consume', 'sessions_count',
                  'qualified_1000', 'qualified_3000', 'qualified_10000', 'qualified_100000']
    # Filter fieldnames to only those present in data
    if not any('qualified_1000' in u for u in users):
        fieldnames = ['rank', 'user_id', 'user_name', 'grade', 'fans_club', 'consume', 'sessions_count']
    return _make_csv_response(users, fieldnames, filename)


@app.route('/api/chat/csv')
def api_chat_csv():
    user_id = request.args.get('user_id', '')
    keyword = request.args.get('keyword', '')
    data = query_chat(user_id, keyword, 1, 999999)
    chats = data.get('chats', [])
    return _make_csv_response(chats,
        ['time', 'user_id', 'user_name', 'content', 'grade', 'fans_club'],
        'chat_logs.csv')


@app.route('/api/anonymous/csv')
def api_anonymous_csv():
    search = request.args.get('search', '')
    data = query_anonymous(1, 999999, search)
    users = data.get('users', [])
    return _make_csv_response(users,
        ['real_user_id', 'user_name', 'anonymous_label', 'consume', 'sessions_count', 'last_seen'],
        'anonymous_users.csv')


@app.route('/api/sessions/csv')
def api_sessions_csv():
    sessions = query_sessions(limit=999999)
    return _make_csv_response(sessions,
        ['id', 'room_id', 'anchor_name', 'start_time', 'end_time', 'status', 'user_count'],
        'sessions.csv')


@app.route('/api/sessions/<int:session_id>/csv')
def api_session_csv(session_id):
    threshold = request.args.get('threshold', 0, type=int)

    if threshold > 0:
        # Use leaderboard query which handles qualified_{threshold} filtering
        data = query_leaderboard(threshold, 'session', 1, 999999, session_id)
        users = data.get('users', [])
    else:
        data = query_session_detail(session_id, top_n=999999)
        if not data:
            return jsonify({'error': 'session not found'}), 404
        # Assign rank to users
        users = data.get('top_users', [])
        for i, u in enumerate(users):
            u['rank'] = i + 1

    rows = []
    for u in users:
        rows.append({
            'rank': u.get('rank', 0),
            'user_id': u.get('user_id', ''),
            'user_name': u.get('user_name', ''),
            'grade': u.get('grade', ''),
            'fans_club': u.get('fans_club', ''),
            'consume': u.get('consume', 0),
            'qualified_1000': u.get('qualified_1000', 0),
            'qualified_3000': u.get('qualified_3000', 0),
            'qualified_10000': u.get('qualified_10000', 0),
            'qualified_100000': u.get('qualified_100000', 0),
        })

    suffix = f'_threshold_{threshold}' if threshold > 0 else ''
    return _make_csv_response(rows,
        ['rank', 'user_id', 'user_name', 'grade', 'fans_club', 'consume',
         'qualified_1000', 'qualified_3000', 'qualified_10000', 'qualified_100000'],
        f'session_{session_id}_contributors{suffix}.csv')


@app.route('/api/sessions/<int:session_id>/gifts/csv')
def api_session_gifts_csv(session_id):
    data = query_session_detail(session_id, top_n=999999)
    if not data:
        return jsonify({'error': 'session not found'}), 404

    gifts = data.get('top_gifts', [])
    return _make_csv_response(gifts,
        ['gift_name', 'times', 'total_count', 'total_diamonds'],
        f'session_{session_id}_gifts.csv')


@app.route('/api/user/<user_id>/csv')
def api_user_csv(user_id):
    data = query_user_detail(user_id)
    if not data:
        return jsonify({'error': 'user not found'}), 404

    rows = []
    for s in data.get('sessions', []):
        base = {
            'user_id': user_id,
            'user_name': data.get('user_name', ''),
            'grade': s.get('grade', data.get('grade', '')),
            'fans_club': s.get('fans_club', data.get('fans_club', '')),
            'session_id': s.get('id', ''),
            'anchor': s.get('anchor_name', ''),
            'start_time': s.get('start_time', ''),
            'end_time': s.get('end_time', ''),
            'consume': s.get('consume', 0),
            'gift_count': s.get('gift_count', 0),
            'chat_count': s.get('chat_count', 0),
        }
        rows.append(base)

    return _make_csv_response(rows,
        ['user_id', 'user_name', 'grade', 'fans_club', 'session_id', 'anchor',
         'start_time', 'end_time', 'consume', 'gift_count', 'chat_count'],
        f'user_{user_id}_sessions.csv')


@app.route('/api/user/<user_id>/timeline/csv')
def api_user_timeline_csv(user_id):
    data = query_user_timeline(user_id, 'all', '', 1, 999999)
    timeline = data.get('timeline', [])
    return _make_csv_response(timeline,
        ['time', 'type', 'content', 'amount', 'grade'],
        f'user_{user_id}_timeline.csv')


def main():
    parser = argparse.ArgumentParser(description='弹幕后台管理面板')
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', default=8080, type=int)
    parser.add_argument('--debug', action='store_true')
    args = parser.parse_args()
    os.makedirs(os.path.join(os.path.dirname(__file__), 'data'), exist_ok=True)
    print(f'[Flask] 启动 http://{args.host}:{args.port}')
    print(f'[Flask] 数据库: {DB_PATH}')
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == '__main__':
    main()

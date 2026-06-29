def flush_to_sqlite(session_id):
    """从 gift_logs 聚合贡献数据写入 contributions / daily_stats / monthly_stats。"""
    conn = _get_conn()
    today = datetime.now().strftime('%Y-%m-%d')
    month = datetime.now().strftime('%Y-%m')

    rows = conn.execute('''
        SELECT user_id, user_name, SUM(diamond_total) as consume
        FROM gift_logs WHERE session_id = ?
        GROUP BY user_id
    ''', (session_id,)).fetchall()

    if not rows:
        return

    with _db_write_lock:
        for r in rows:
            uid = r['user_id']
            nick = r['user_name']
            consume = r['consume']

            info = conn.execute(
                "SELECT fans_club, grade FROM chat_logs WHERE user_id = ? AND (fans_club != '' OR grade != '') ORDER BY id DESC LIMIT 1",
                (uid,)
            ).fetchone()
            fans_club = info['fans_club'] if info else ''
            grade = info['grade'] if info else ''

            conn.execute('''
                INSERT INTO users (user_id, user_name, fans_club, grade, last_seen)
                VALUES (?, ?, ?, ?, datetime("now", "+8 hours"))
                ON CONFLICT(user_id) DO UPDATE SET
                    fans_club = CASE WHEN ? != '' THEN ? ELSE fans_club END,
                    grade = CASE WHEN ? != '' THEN ? ELSE grade END,
                    last_seen = datetime("now", "+8 hours")
            ''', (uid, nick, fans_club, grade, fans_club, fans_club, grade, grade))

            prev = conn.execute(
                'SELECT qualified_1000, qualified_3000, qualified_10000, qualified_100000 FROM contributions WHERE session_id=? AND user_id=?',
                (session_id, uid)
            ).fetchone()

            pq_1000 = prev['qualified_1000'] if prev else 0
            pq_3000 = prev['qualified_3000'] if prev else 0
            pq_10000 = prev['qualified_10000'] if prev else 0
            pq_100000 = prev['qualified_100000'] if prev else 0
            q_1000 = 1 if consume >= 1000 else 0
            q_3000 = 1 if consume >= 3000 else 0
            q_10000 = 1 if consume >= 10000 else 0
            q_100000 = 1 if consume >= 100000 else 0

            conn.execute('''
                INSERT INTO contributions
                    (session_id, user_id, user_name, consume, rank, fans_club, source,
                     qualified_1000, qualified_3000, qualified_10000, qualified_100000)
                VALUES (?, ?, ?, ?, 0, ?, 'websocket', ?, ?, ?, ?)
                ON CONFLICT(session_id, user_id) DO UPDATE SET
                    consume = excluded.consume,
                    qualified_1000 = MAX(qualified_1000, excluded.qualified_1000),
                    qualified_3000 = MAX(qualified_3000, excluded.qualified_3000),
                    qualified_10000 = MAX(qualified_10000, excluded.qualified_10000),
                    qualified_100000 = MAX(qualified_100000, excluded.qualified_100000)
            ''', (session_id, uid, nick, consume, fans_club,
                  q_1000, q_3000, q_10000, q_100000))

            if q_1000 and not pq_1000:
                conn.execute('INSERT INTO daily_stats (user_id, user_name, date, sessions_1000, total_consume) VALUES (?, ?, ?, 1, 0) ON CONFLICT(user_id, date) DO UPDATE SET sessions_1000 = sessions_1000 + 1', (uid, nick, today))
                conn.execute('INSERT INTO monthly_stats (user_id, user_name, year_month, sessions_1000, total_consume) VALUES (?, ?, ?, 1, 0) ON CONFLICT(user_id, year_month) DO UPDATE SET sessions_1000 = sessions_1000 + 1', (uid, nick, month))
            if q_3000 and not pq_3000:
                conn.execute('UPDATE daily_stats SET sessions_3000 = sessions_3000 + 1 WHERE user_id = ? AND date = ?', (uid, today))
                conn.execute('UPDATE monthly_stats SET sessions_3000 = sessions_3000 + 1 WHERE user_id = ? AND year_month = ?', (uid, month))
            if q_10000 and not pq_10000:
                conn.execute('UPDATE daily_stats SET sessions_10000 = sessions_10000 + 1 WHERE user_id = ? AND date = ?', (uid, today))
                conn.execute('UPDATE monthly_stats SET sessions_10000 = sessions_10000 + 1 WHERE user_id = ? AND year_month = ?', (uid, month))
            if q_100000 and not pq_100000:
                conn.execute('UPDATE daily_stats SET sessions_100000 = sessions_100000 + 1 WHERE user_id = ? AND date = ?', (uid, today))
                conn.execute('UPDATE monthly_stats SET sessions_100000 = sessions_100000 + 1 WHERE user_id = ? AND year_month = ?', (uid, month))

            total_day = conn.execute(
                "SELECT COALESCE(SUM(c.consume), 0) FROM contributions c JOIN sessions s ON c.session_id = s.id WHERE c.user_id = ? AND date(s.start_time) = ?",
                (uid, today)
            ).fetchone()[0]
            total_month = conn.execute(
                "SELECT COALESCE(SUM(c.consume), 0) FROM contributions c JOIN sessions s ON c.session_id = s.id WHERE c.user_id = ? AND strftime('%Y-%m', s.start_time) = ?",
                (uid, month)
            ).fetchone()[0]
            conn.execute('UPDATE daily_stats SET total_consume = ?, user_name = ? WHERE user_id = ? AND date = ?',
                         (total_day, nick, uid, today))
            conn.execute('UPDATE monthly_stats SET total_consume = ?, user_name = ? WHERE user_id = ? AND year_month = ?',
                         (total_month, nick, uid, month))

        conn.commit()

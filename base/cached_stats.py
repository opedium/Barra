"""Cached aggregate dashboard stats.

The web panel used to run full-table COUNT(*) / SUM() scans over the
(now multi-GB) SQLite database on every page load. On the small HDD VPS
this starved the disk: even a single COUNT(*) could take over a minute,
and every dashboard hit made it worse.

This module computes those aggregates once per TTL window and serves the
cached snapshot to all requests. It uses stale-while-revalidate: a
request that arrives while a refresh is already running gets the previous
snapshot instead of blocking on the disk.
"""

import logging
import threading
import time

from base.parser import _get_conn

logger = logging.getLogger(__name__)

# Zero-filled snapshot served on the first (pre-compute) load so callers
# never hit KeyError while the background compute is still running.
_EMPTY_SNAPSHOT = {
    'total_gifts': 0, 'total_chats': 0, 'total_sessions': 0, 'total_users': 0,
    'total_diamond': 0, 'gift_users': 0,
    'active_session': False, 'today_gifts': 0, 'today_chats': 0, 'today_users': 0,
    'recent_sessions': [],
}


class StatsCache:
    """Compute and cache dashboard aggregate counts.

    ``conn_getter`` is a callable returning a SQLite connection; it is
    invoked on each refresh so the connection matches the calling thread
    (thread-local connections). ``ttl_seconds`` bounds how often the full
    set of scans is re-run.
    """

    def __init__(self, conn_getter=_get_conn, ttl_seconds=60):
        self._conn_getter = conn_getter
        self._ttl = ttl_seconds
        self._refresh_lock = threading.Lock()
        self._snapshot = None
        self._last_compute = 0.0
        self._bg_thread = None

    def get(self):
        """Return the current stats snapshot.

        Never blocks a page load on cold scans: the first call serves an
        empty snapshot immediately and kicks off the real compute in the
        background; once computed, fresh snapshots are served from cache.
        """
        now = time.time()
        snap = self._snapshot
        if snap is not None and now - self._last_compute < self._ttl:
            return snap
        # A refresh is already running — serve stale rather than block.
        if snap is not None and not self._refresh_lock.acquire(blocking=False):
            return snap
        if snap is not None:
            # Refresh in the background so a page load never waits.
            self._refresh_lock.release()
            self._start_background_refresh()
            return snap
        # First call: never block on the cold compute. Serve a zero-filled
        # snapshot now and compute in the background.
        self._snapshot = dict(_EMPTY_SNAPSHOT)
        self._last_compute = now
        self._start_background_refresh()
        return self._snapshot

    def _start_background_refresh(self):
        """Compute the snapshot in a background thread if none is running."""
        if self._bg_thread is not None and self._bg_thread.is_alive():
            return
        t = threading.Thread(target=self._background_compute, daemon=True)
        self._bg_thread = t
        t.start()

    def _background_compute(self):
        """Run the compute; concurrent get() calls never wait on the lock."""
        try:
            with self._refresh_lock:
                snap = self._compute()
                self._snapshot = snap
                self._last_compute = time.time()
        except Exception as e:
            # Log instead of swallowing: a silent failure left the dashboard
            # showing stale numbers for hours with no visibility.
            logger.error(f"[StatsCache] refresh failed: {e}")

    def run_forever(self, stop_event=None):
        """Periodically refresh the snapshot in the background.

        The web app calls this in a daemon thread at startup so the
        dashboard always shows fresh data, even when nobody is hitting the
        pages. Without it, refreshes only happened on page loads — and a
        single slow/hung refresh could leave stale numbers for a long time.
        """
        while True:
            if stop_event is not None and stop_event.is_set():
                return
            try:
                # Force a refresh (bypass the TTL check) so the snapshot is
                # always kept up to date.
                with self._refresh_lock:
                    snap = self._compute()
                    self._snapshot = snap
                    self._last_compute = time.time()
            except Exception as e:
                logger.error(f"[StatsCache] periodic refresh failed: {e}")
            # Wait a bit before the next refresh (longer than compute cost).
            time.sleep(self._ttl)
            if stop_event is not None and stop_event.is_set():
                return

    def _compute(self):
        """Run the aggregate queries and return a snapshot dict."""
        conn = self._conn_getter()
        stats = {
            'total_gifts': conn.execute('SELECT COUNT(*) FROM gift_logs').fetchone()[0],
            'total_chats': conn.execute('SELECT COUNT(*) FROM chat_logs').fetchone()[0],
            'total_sessions': conn.execute('SELECT COUNT(*) FROM sessions').fetchone()[0],
            'total_users': conn.execute('SELECT COUNT(DISTINCT user_id) FROM contributions').fetchone()[0],
            'total_diamond': conn.execute('SELECT COALESCE(SUM(diamond_total), 0) FROM gift_logs').fetchone()[0],
            'gift_users': conn.execute('SELECT COUNT(DISTINCT user_id) FROM gift_logs').fetchone()[0],
            'active_session': conn.execute(
                "SELECT COUNT(*) FROM sessions WHERE status='live'"
            ).fetchone()[0] > 0,
            'today_gifts': conn.execute(
                "SELECT COUNT(*) FROM gift_logs WHERE created_at >= datetime('now', 'start of day')"
            ).fetchone()[0],
            'today_chats': conn.execute(
                "SELECT COUNT(*) FROM chat_logs WHERE created_at >= datetime('now', 'start of day')"
            ).fetchone()[0],
            'today_users': conn.execute(
                "SELECT COUNT(DISTINCT user_id) FROM gift_logs WHERE created_at >= datetime('now', 'start of day')"
            ).fetchone()[0],
        }
        # Recent sessions with per-session gift/chat counts. This query ran
        # per-request subqueries over the whole gift_logs table — minutes on a
        # multi-GB DB — so it's cached on the same TTL as the counters.
        stats['recent_sessions'] = [dict(r) for r in conn.execute('''
            SELECT s.id, s.anchor_name, s.room_id, s.start_time, s.status,
                (SELECT COUNT(*) FROM gift_logs WHERE session_id=s.id) as total_gifts,
                (SELECT COUNT(*) FROM chat_logs WHERE session_id=s.id) as total_chats
            FROM sessions s ORDER BY s.id DESC LIMIT 10
        ''').fetchall()]
        return stats

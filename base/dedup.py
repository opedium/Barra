"""Dedup cache and merge tracker for dual WebSocket union filtering.

Provides:
    LRUDedupCache — thread-safe LRU cache for dedup keys (size-based eviction, no TTL).
    MergeTracker — cross-validation buffer: records which events were seen by
        each WS connection, compares values on second arrival, flushes to SQLite.
    Per-type protobuf extractors — generate stable dedup keys and value JSON
        that are identical across both WS connections for the same logical event.

Usage:
    cache = LRUDedupCache(max_size=100000)
    if cache.is_new(('WebcastGiftMessage', uid, ts, gid, cnt, to_uid)):
        # first arrival — enqueue
    else:
        # duplicate — cross-validate
"""

import hashlib
import json
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Optional

from base.messages import (
    parse_proto,
    ChatMessage, GiftMessage, LightGiftMessage, LikeMessage, MemberMessage,
    SocialMessage, RoomUserSeqMessage, FansclubMessage,
    ControlMessage, EmojiChatMessage, RoomStatsMessage,
    RoomMessage, RoomRankMessage,
)
from base.utils import get_user_id

logger = __import__('logging').getLogger(__name__)


class LRUDedupCache:
    """Thread-safe LRU dedup cache. Evicts by size only (no TTL).

    Keys can be any hashable value (typically tuples from per-type extractors).
    is_new() returns True for unseen keys, False for duplicates.
    On a cache hit, the key is promoted to the MRU end.
    When over max_size, the LRU (oldest) entry is evicted.

    Thread-safe via threading.Lock.
    """

    def __init__(self, max_size: int = 100000):
        self._lock = threading.Lock()
        self._cache: OrderedDict = OrderedDict()
        self._max_size = max_size

    def is_new(self, key: object) -> bool:
        """Check if key is new (not a duplicate).

        Returns True if key has not been seen before (adds it to cache).
        Returns False if key exists (promotes it to MRU end).
        """
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
                return False
            self._cache[key] = True
            if len(self._cache) > self._max_size:
                self._cache.popitem(last=False)
            return True

    def clear(self) -> None:
        """Remove all entries from the cache."""
        with self._lock:
            self._cache.clear()

    @property
    def size(self) -> int:
        """Current number of entries in the cache."""
        with self._lock:
            return len(self._cache)


@dataclass
class _MergeStats:
    """Aggregate merge statistics returned by MergeTracker.flush_to_db()."""
    total: int            # total unique events in this flush batch
    confirmed: int        # events seen by both connections
    single_source: int    # events seen by only one connection
    discrepancies: int    # events where values differed


@dataclass
class _ConflictInfo:
    """Returned by record_confirmation() when values disagree."""
    field: str
    val_a: str
    val_b: str
    description: str


@dataclass
class _MergeRecord:
    """Internal record for one unique event in the merge tracker."""
    method: str
    first_source: str
    first_seen: float
    second_source: Optional[str] = None
    confirmed_at: Optional[float] = None
    value_a: str = ''            # first source's value_json
    value_b: str = ''            # second source's value_json
    discrepancy: Optional[str] = None


class MergeTracker:
    """Tracks which events were seen by which WS connection.

    When the same event arrives from both connections, compares the
    reported values and records any discrepancies for accuracy auditing.

    Thread-safe via threading.Lock. Periodically flushed to SQLite.

    Args:
        max_pending: Max in-memory records before LRU eviction.
    """

    def __init__(self, max_pending: int = 50000):
        self._lock = threading.Lock()
        self._pending: dict[str, _MergeRecord] = OrderedDict()
        self._max_pending = max_pending

    def record_first_seen(self, key_hash: str, method: str,
                          source: str, value_json: str) -> None:
        """Record that an event was first seen by one WS connection.

        Args:
            key_hash: SHA-256 hash of the dedup key tuple.
            method: WebSocket method name (e.g. 'WebcastGiftMessage').
            source: 'primary' or 'secondary'.
            value_json: JSON string of extracted values for cross-validation.
        """
        with self._lock:
            if len(self._pending) >= self._max_pending:
                # LRU eviction: remove oldest 25%
                for _ in range(self._max_pending // 4):
                    self._pending.pop(next(iter(self._pending)), None)
            self._pending[key_hash] = _MergeRecord(
                method=method,
                first_source=source,
                first_seen=time.time(),
                value_a=value_json,
            )

    def record_confirmation(self, key_hash: str, source: str,
                            value_json: str) -> Optional[_ConflictInfo]:
        """Record that an event was confirmed by the second WS connection.

        Returns _ConflictInfo if the values disagree, None otherwise.

        Args:
            key_hash: SHA-256 hash of the dedup key tuple.
            source: 'primary' or 'secondary'.
            value_json: JSON string of extracted values.

        Returns:
            _ConflictInfo if values differ, None if they match or record not found.
        """
        with self._lock:
            rec = self._pending.get(key_hash)
            if rec is None:
                return None  # already flushed, can't cross-ref

            rec.second_source = source
            rec.confirmed_at = time.time()
            rec.value_b = value_json

            if rec.value_a and rec.value_b:
                diff = _compare_values(rec.method, rec.value_a, rec.value_b)
                if diff:
                    rec.discrepancy = diff
                    return _ConflictInfo(
                        field=diff.split(':')[0],
                        val_a=rec.value_a,
                        val_b=rec.value_b,
                        description=diff,
                    )
            return None

    def flush_to_db(self, session_id: str, conn) -> _MergeStats:
        """Write pending records to event_confidence table.

        Called every 60s from _stats_task. Clears the pending buffer
        after successful write.

        Args:
            session_id: Current session ID for the FK.
            conn: SQLite connection from _get_conn().

        Returns:
            _MergeStats with counts from this flush batch.
        """
        with self._lock:
            records = dict(self._pending)

        if not records:
            return _MergeStats(0, 0, 0, 0)

        stats = _MergeStats(
            total=len(records),
            confirmed=0,
            single_source=0,
            discrepancies=0,
        )

        for key_hash, rec in records.items():
            if rec.confirmed_at:
                stats.confirmed += 1
                if rec.discrepancy:
                    stats.discrepancies += 1
            else:
                stats.single_source += 1

            conn.execute("""
                INSERT OR REPLACE INTO event_confidence
                (session_id, key_hash, method, first_source, first_seen,
                 second_source, confirmed_at, value_a, value_b, discrepancy)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                session_id, key_hash, rec.method,
                rec.first_source, rec.first_seen,
                rec.second_source, rec.confirmed_at,
                rec.value_a, rec.value_b, rec.discrepancy,
            ))
        conn.commit()

        # Clear only after successful commit
        with self._lock:
            self._pending.clear()
        return stats

    @property
    def size(self) -> int:
        """Current number of pending (unflushed) records."""
        with self._lock:
            return len(self._pending)


def _key_to_hash(key: tuple) -> str:
    """Deterministic hash of dedup key tuple.

    Uses SHA-256 truncated to 16 bytes (32 hex chars).
    Collision probability for 100k keys is negligible (~1e-12).
    """
    return hashlib.sha256(repr(key).encode()).hexdigest()[:32]


def _compare_values(method: str, json_a: str, json_b: str) -> Optional[str]:
    """Compare two value_json strings. Returns description if different.

    Compares all keys present in either JSON object.
    Returns None if values match exactly.
    Returns e.g. "combo_count: 5 vs 8 | repeat_count: 5 vs 3" if they differ.
    """
    try:
        a = json.loads(json_a)
        b = json.loads(json_b)
    except (json.JSONDecodeError, TypeError):
        return None

    if a == b:
        return None

    diffs = []
    all_keys = set(a.keys()) | set(b.keys())
    for k in sorted(all_keys):
        va = a.get(k)
        vb = b.get(k)
        if va != vb:
            diffs.append(f"{k}: {va} vs {vb}")
    return ' | '.join(diffs) if diffs else None


def _extract_user_id(user) -> str:
    """Safely extract user_id from a protobuf User object."""
    if user is None:
        return ''
    return get_user_id(user) or ''


def _key_chat(payload: bytes) -> tuple:
    msg = parse_proto(ChatMessage, payload)
    uid = _extract_user_id(msg.user)
    ts = msg.common.create_time if msg.common else 0
    return ('WebcastChatMessage', uid, ts, msg.content or '')


def _key_gift(payload: bytes) -> tuple:
    msg = parse_proto(GiftMessage, payload)
    uid = _extract_user_id(msg.user)
    ts = msg.common.create_time if msg.common else 0
    gid = str(msg.group_id) if msg.group_id else '0'
    cnt = msg.combo_count or msg.repeat_count or 1
    to_uid = str(msg.to_user.id) if msg.to_user else ''
    return ('WebcastGiftMessage', uid, ts, gid, cnt, to_uid)


def _key_like(payload: bytes) -> tuple:
    msg = parse_proto(LikeMessage, payload)
    uid = _extract_user_id(msg.user)
    ts = msg.common.create_time if msg.common else 0
    return ('WebcastLikeMessage', uid, ts)


def _key_control(payload: bytes) -> tuple:
    msg = parse_proto(ControlMessage, payload)
    ts = msg.common.create_time if msg.common else 0
    return ('WebcastControlMessage', ts, msg.status)


def _key_member(payload: bytes) -> tuple:
    msg = parse_proto(MemberMessage, payload)
    uid = _extract_user_id(msg.user)
    ts = msg.common.create_time if msg.common else 0
    return ('WebcastMemberMessage', uid, ts)


def _key_social(payload: bytes) -> tuple:
    msg = parse_proto(SocialMessage, payload)
    uid = _extract_user_id(msg.user)
    ts = msg.common.create_time if msg.common else 0
    return ('WebcastSocialMessage', uid, ts, msg.action)


def _key_fansclub(payload: bytes) -> tuple:
    msg = parse_proto(FansclubMessage, payload)
    uid = _extract_user_id(msg.user)
    ts = msg.common.create_time if msg.common else 0
    return ('WebcastFansclubMessage', uid, ts, msg.type)


def _key_emoji(payload: bytes) -> tuple:
    msg = parse_proto(EmojiChatMessage, payload)
    uid = _extract_user_id(msg.user)
    ts = msg.common.create_time if msg.common else 0
    return ('WebcastEmojiChatMessage', uid, ts, msg.emoji_id)


def _key_room(payload: bytes) -> tuple:
    msg = parse_proto(RoomMessage, payload)
    ts = msg.common.create_time if msg.common else 0
    return ('WebcastRoomMessage', ts, msg.content or '')


def _key_room_user_seq(payload: bytes) -> tuple:
    msg = parse_proto(RoomUserSeqMessage, payload)
    ts = msg.common.create_time if msg.common else 0
    return ('WebcastRoomUserSeqMessage', ts, msg.total)


def _key_room_rank(payload: bytes) -> tuple:
    msg = parse_proto(RoomRankMessage, payload)
    ts = msg.common.create_time if msg.common else 0
    return ('WebcastRoomRankMessage', ts)


def _key_room_stats(payload: bytes) -> tuple:
    msg = parse_proto(RoomStatsMessage, payload)
    ts = msg.common.create_time if msg.common else 0
    return ('WebcastRoomStatsMessage', ts, msg.total)


def _key_light_gift(payload: bytes) -> tuple:
    msg = parse_proto(LightGiftMessage, payload)
    uid = str(msg.user_id) if msg.user_id else '0'
    ts = msg.common.create_time if msg.common else 0
    gid = msg.gift_info.gift_id if msg.gift_info else 0
    cnt = msg.count or 1
    return ('WebcastLightGiftMessage', uid, ts, gid, cnt)


_DEDUP_EXTRACTORS = {
    'WebcastChatMessage': _key_chat,
    'WebcastGiftMessage': _key_gift,
    'WebcastLikeMessage': _key_like,
    'WebcastControlMessage': _key_control,
    'WebcastMemberMessage': _key_member,
    'WebcastSocialMessage': _key_social,
    'WebcastFansclubMessage': _key_fansclub,
    'WebcastEmojiChatMessage': _key_emoji,
    'WebcastRoomMessage': _key_room,
    'WebcastRoomUserSeqMessage': _key_room_user_seq,
    'WebcastRoomRankMessage': _key_room_rank,
    'WebcastRoomStatsMessage': _key_room_stats,
    'WebcastLightGiftMessage': _key_light_gift,
}


def _value_gift(payload: bytes) -> str:
    msg = parse_proto(GiftMessage, payload)
    return json.dumps({
        'gift_id': str(msg.gift_id) if msg.gift_id else str(msg.gift.id) if msg.gift else '0',
        'combo_count': msg.combo_count or 0,
        'repeat_count': msg.repeat_count or 0,
        'group_id': str(msg.group_id) if msg.group_id else '0',
        'to_user_id': str(msg.to_user.id) if msg.to_user else '',
    }, sort_keys=True)


def _value_chat(payload: bytes) -> str:
    msg = parse_proto(ChatMessage, payload)
    return json.dumps({'content': msg.content or ''}, sort_keys=True)






_VALUE_EXTRACTORS = {
    'WebcastGiftMessage': _value_gift,
    'WebcastChatMessage': _value_chat,
}


def _fallback_key(payload: bytes, method: str, timestamp: float = 0) -> tuple:
    """Fallback: MD5 of payload, windowed to 30-second buckets.

    Protobuf wire-format bytes can differ between connections (unknown
    fields, field ordering). To mitigate, the timestamp component is
    floored to 30s granularity — messages within the same 30s window
    share the same key.
    """
    h = hashlib.md5(payload).hexdigest()
    window = int(timestamp) // 30 if timestamp else 0
    return ('__md5', method, h, window)

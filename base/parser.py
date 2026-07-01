"""消息解析器：Protobuf 消息反序列化与结构化分发。

每个 parse_* 函数接收 protobuf payload bytes，返回结果列表。
结果字典结构：
    type: 消息类型标识（如 'chat'、'gift'），用于 CSV 文件路由。
    msg: 人类可读的日志文本。
    data: CSV 行数据字典（与 output.py 的 CSV_FIELDS 对应）。
    action: 可选的控制指令（'stop' 终止采集、'wait_live' 等待开播）。

HANDLERS 字典将 WebSocket method 名（如 'WebcastChatMessage'）
映射到对应的解析函数，供 fetcher.py 消息分发使用。
"""

import logging
import os
import re
import sqlite3
import threading
import queue
import time
from collections import Counter
from datetime import datetime

from base.messages import (
    parse_proto,
    ChatMessage, GiftMessage, LightGiftMessage, LikeMessage, MemberMessage,
    SocialMessage, RoomUserSeqMessage, FansclubMessage,
    ControlMessage, EmojiChatMessage, RoomStatsMessage,
    RoomMessage, RoomRankMessage, RoomStreamAdaptationMessage,
    LinkmicBattleMessage, LinkmicBattleFinishMessage,
    LinkmicArmiesMessage, LinkmicPlayModeUpdateScoreMessage,
    LinkerContributeMessage, BattleAuxiliaryMessage,
    BattleEndPunishMessage, BattlePowerContainerMessage,
)
from base.utils import (
    get_user_id, get_user_sec_uid, get_user_avatar_url,
    get_user_name, sanitize_username,
    fmt_grade, fmt_fans_club,
)

logger = logging.getLogger(__name__)

# ── SSE event callback (set by app.py to avoid circular imports) ──
_sse_callback = None

def set_sse_callback(cb):
    """Register callback for SSE events. Called as cb(event_type, data_dict)."""
    global _sse_callback
    _sse_callback = cb



# ── 礼物去重状态（delta法）────────────────────────
# 同一直播间内，相同 (group_id, gift_name) 的 repeat_count 是累计值。
# 每条消息只统计增量 delta = current_repeat - last_repeat，
# 避免把累计值当作单次数量导致超采。

_gift_dedup = {}        # (group_id, gift_id, user_id) -> (last_repeat_count, last_update_time, combo_ended)
_rc1_dedup = {}         # (gift_id, user_id) -> last_accept_time (rc=1 500ms 短窗口去重)
_dedup_lock = threading.Lock()

# ── trace_id 影子去重 (shadow mode) ────────────────
# 与现有 delta 去重并行运行，记录分歧供离线分析。
# 不改变任何 accept/reject 决策，只记录"如果只用 trace_id 会怎样"。
_seen_trace_ids = set()
_trace_shadow_lock = threading.Lock()

# ── PK 回合追踪 ──────────────────────────────────────
# BattleMethod 开始 → BattleFinishMethod 结束 → 输出 pk_round 记录
_pk_current = None          # {'start_ts', 'start_time', 'participants', 'play_mode', 'team_mode'}
_pk_round_lock = threading.Lock()

# 模板/UI 文字，从参与者名单中过滤
_PK_TEMPLATES = frozenset({
    '投票', '投票结束', '你更期待谁的表演？', '比拼结束', '比拼',
    '巅峰赛', '巅峰赛结束', 'PK结束',
    '榜50', '榜100',
    '获胜', '获胜且PK值达', '可得积分', 'PK值越高积分越高',
    '1.计算整场结束后，贡献榜上排名第三/五/十的票数，票数多的则赢。',
    '钻石及以上段位仅在巅峰赛获取积分',
    '我方', '送礼上榜', 'PK值', '积分', '距第', '成为榜单',
    '钻石3星', '钻石1星', '钻石2星', '钻石4星', '钻石5星',
    '星耀', '王者',
})

def _scan_cjk_strings(data, min_len=2, max_len=60, recurse=True, _depth=0):
    """遍历 protobuf wire format，从 LEN 字段中收集含 CJK 的字符串。"""
    if _depth > 8:
        return []
    results = []
    p = 0
    while p < len(data) - 1:
        tag, p = _read_varint(data, p)
        if tag is None:
            break
        wt = tag & 0x7
        if wt == 2:
            raw, p = _read_ld(data, p)
            if raw is None:
                continue
            # 尝试解码为字符串
            if min_len <= len(raw) <= max_len:
                try:
                    s = raw.decode('utf-8')
                    if s.isprintable() and any('一' <= c <= '鿿' for c in s):
                        if 'http' not in s and '.png' not in s and 'sslocal' not in s:
                            results.append(s)
                except Exception:
                    pass
            # 递归进入嵌套的 protobuf 子消息（跳过明显的 JSON / URL）
            if recurse and len(raw) > 2 and raw[0] != 0x7b:  # skip '{' = JSON
                sub = _scan_cjk_strings(raw, min_len, max_len, recurse=True, _depth=_depth+1)
                results.extend(sub)
        elif wt == 0:
            _, p = _read_varint(data, p)
        elif wt == 5:
            p += 4
        else:
            break
    # 去重保持顺序
    seen = set()
    unique = []
    for s in results:
        if s not in seen:
            seen.add(s)
            unique.append(s)
    return unique

def _extract_participants(payload):
    """从 BattleMethod 原始 payload 提取参与者名单。"""
    all_cjk = _scan_cjk_strings(payload)
    return [s for s in all_cjk if s not in _PK_TEMPLATES
            and len(s) <= 25
            and not s.startswith('距')
            and not s.endswith('...')
            and '差' not in s[:2]
            and not s.endswith('已开启')]

def _extract_mode_json(payload):
    """从 payload 的 JSON 字符串中提取 battle_play_mode 和 team_mode。"""
    import re
    # 搜索 battle_play_mode 数字
    m = re.search(rb'"battle_play_mode"\s*:\s*"(\d+)"', payload)
    play_mode = m.group(1).decode() if m else ''
    # 搜索 team_mode
    m2 = re.search(rb'"team_mode"\s*:\s*(\d+)', payload)
    team_mode = m2.group(1).decode() if m2 else ''
    return play_mode, team_mode

def _mode_label(play_mode):
    mapping = {'0': '1v1', '101': '多人PK', '201': '多人PK', '501': '随机匹配'}
    return mapping.get(play_mode, play_mode or '?')

# ── 礼物元数据回退表 ────────────────────────────────
# 当 protobuf GiftStruct 解析失败（gift 为 None 或缺失 name/id）
# 时，用此表按 gift_id 回退，避免数据完全丢失。
# 参考：DouyinBarrageGrab C# 上游的硬编码回退。
_GIFT_FALLBACK = {
    685: {'name': '粉丝灯牌', 'diamond_count': 1, 'combo': False},
    3389: {'name': '欢乐盲盒', 'diamond_count': 10, 'combo': False},
    4021: {'name': '欢乐拼图', 'diamond_count': 10, 'combo': False},
}

# ── 最大合理增量阈值 ──────────────────────────────
# 单条礼物消息的 delta 超过此值时记录警告，但不拒绝。
# 用于检测 dedup 状态异常（如 counter reset 后仍累积了大量 delta）。
_MAX_REASONABLE_DELTA = 10000

# ── 礼物定价覆盖表 ──────────────────────────────────
# 限定/限时礼物的皮肤变体，protobuf diamond_count 返回的是基类价格而非实际定价。
# 例：至尊超跑(12,000)的 protobuf 返回跑车基类价(1,200)。
# 此表按礼物名覆盖，优先于 protobuf 和 gift_registry。
# 维护方式：发现新限定礼物 → 查抖音实际定价 → 加入此表。
_GIFT_PRICE_OVERRIDE = {
    '至尊超跑': 12000,
    '烈焰跑车': 6000,
    '无界超跑': 36000,
    '青绿典藏版嘉年华': 35000,
    '钻石嘉年华': 36000,
    '520嘉年华': 33000,
    '御风飞机': 9000,
    '凌霄战机': 18000,
    '星际战舰': 36000,
    '闪烁星河': 99,
    '点点星光': 9,
}

# ── 礼物 ID → 钻石价格/名称反向索引 ──────────────────
# 从 gift_registry.json 构建，用于 diy_item_info 中 gift_id 的反查。
# 甄选礼盒（AI分身礼物邮箱等）是复合礼物，protobuf 外层 diamond_count
# 只报容器价（99），真实礼物信息藏在 field 27 (diy_item_info) JSON 中。
# 此映射用于：diy_item_info.gift_id → 真实 diamond_count + 真实礼物名。
_GIFT_ID_TO_PRICE = {}
_GIFT_ID_TO_NAME = {}
try:
    import json, os as _os
    _reg_path = _os.path.join(_os.path.dirname(_os.path.dirname(__file__)), 'data', 'gift_registry.json')
    with open(_reg_path, 'r', encoding='utf-8') as _f:
        _registry = json.load(_f)
    for _name, _info in _registry.items():
        _gid = _info.get('id')
        _price = _info.get('diamond_count', 0)
        if _gid and _price:
            _GIFT_ID_TO_PRICE[str(_gid)] = _price
            _GIFT_ID_TO_NAME[str(_gid)] = _name
except Exception:
    pass  # 注册表加载失败不阻塞启动，静态覆盖表兜底

# ── Gift Price Cache (DB-backed, with hardcoded fallback) ──
_gift_price_cache = None
_gift_price_cache_lock = threading.Lock()

def _load_gift_price_cache():
    """Load all gift prices from DB into a dict, then overlay hardcoded overrides.

    Hardcoded _GIFT_PRICE_OVERRIDE always wins (emergency fix path).
    """
    global _gift_price_cache
    cache = {}
    try:
        conn = _get_conn()
        for row in conn.execute('SELECT gift_name, diamond_count FROM gift_prices'):
            cache[row['gift_name']] = row['diamond_count']
    except Exception:
        pass  # DB not ready yet, use fallback only
    # Hardcoded fallbacks (gift_id-based, when protobuf is null)
    for fb in _GIFT_FALLBACK.values():
        cache[fb['name']] = fb['diamond_count']
    # Hardcoded overrides take highest precedence (safety net + emergency edits)
    cache.update(_GIFT_PRICE_OVERRIDE)
    _gift_price_cache = cache
    logger.debug(f"[礼物定价] 已加载 {len(cache)} 条礼物价格 (DB + 覆盖)")

def _lookup_gift_price(gift_name, protobuf_price):
    """Look up a gift's diamond price.

    Resolution order:
        1. In-memory cache (loaded from DB + hardcoded overrides)
        2. protobuf gift.diamond_count (fallback)

    Returns (price, was_overridden) tuple.
    """
    global _gift_price_cache
    if _gift_price_cache is None:
        with _gift_price_cache_lock:
            if _gift_price_cache is None:
                _load_gift_price_cache()
    cached = _gift_price_cache.get(gift_name)
    if cached is not None and cached != protobuf_price:
        return cached, True
    return protobuf_price, False

# ── 去重诊断计数器 ────────────────────────────────
_dedup_diag = {
    'raw': 0, 'passed': 0, 'rejected': 0,
    'repeat_zero': 0, 'combo_block': 0,
    'counter_reset': 0, 'delta_zero': 0, 'out_of_order': 0,
    'rc1_dup': 0, 'bulk_dup': 0,
}

# ── 消息流计数器（debug 用） ──────────────────────
# 追踪消息从 enqueue → DB written 的全流程
_chat_enqueued = {}     # session_id -> record_chat 调用次数
_gift_enqueued = {}     # session_id -> record_gift 调用次数
_chat_written = {}      # session_id -> _flush_write_batch 实际写入 chat_logs 行数
_gift_written = {}      # session_id -> _flush_write_batch 实际写入 gift_logs 行数
_combo_progress_count = 0  # 连击进度消息数（不计入 enq，被 buffer 吸收）


def get_dedup_stats():
    """获取去重诊断统计的当前快照（不重置）。返回 dict。"""
    with _dedup_lock:
        return dict(_dedup_diag)


def _compute_gift_count(group_id, gift_id, user_id, repeat_count, repeat_end=0):
    """计算去重后的礼物增量。返回 (delta, reject_reason)。

    delta > 0 表示本次应计数的数量，delta=0 表示重复消息需丢弃。
    reject_reason 为空表示通过，否则为拒绝原因标识。

    repeat_count 参数既可能是 combo_count（连击累计值 1,2,3...N），
    也可能是 repeat_count（单次消息内数量）。调用者（parse_gift_msg）
    优先传入 combo_count，仅在 combo_count 为 0 时退回 repeat_count。

    rc=1 使用 500ms 短窗口去重（单次送礼是事件而非状态，不应走 delta 逻辑）。
    rc>=2 使用 delta 法 + combo_end 5s TTL。
    """
    if repeat_count <= 0:
        _dedup_diag['repeat_zero'] += 1
        return 0, 'repeat_zero'
    key = (str(group_id), str(gift_id), str(user_id))
    now = time.time()

    # rc=1: 短窗口去重（500ms）。单次送礼不存在 combo 递增，delta 法会误杀。
    if repeat_count == 1:
        rc1_key = (str(gift_id), str(user_id))
        with _dedup_lock:
            last_time = _rc1_dedup.get(rc1_key, 0)
            if now - last_time < 0.5:
                _dedup_diag['rc1_dup'] += 1
                return 0, 'rc1_dup'
            _rc1_dedup[rc1_key] = now
            prev_count = _gift_dedup.get(key, (0, 0, False))[0]
            _gift_dedup[key] = (max(prev_count, 1), now, repeat_end == 1)
            return 1, ''

    with _dedup_lock:
        prev_count, prev_time, combo_ended = _gift_dedup.get(key, (0, 0, False))
        if prev_count == 0:
            delta = repeat_count
        elif combo_ended and repeat_count >= prev_count and now - prev_time <= 5:
            delta = 0
            _dedup_diag['combo_block'] += 1
        elif combo_ended:
            # counter reset after combo end, or TTL expired — accept as new
            delta = repeat_count
            _dedup_diag['counter_reset'] += 1
        elif repeat_count < prev_count:
            # 乱序到达（同combo内rc不降反退）→ 拒绝，避免重复计数
            delta = 0
            _dedup_diag['out_of_order'] += 1
        else:
            delta = repeat_count - prev_count
            if delta == 0:
                _dedup_diag['delta_zero'] += 1
        reject_reason = ''
        if delta == 0:
            if repeat_count < prev_count:
                reject_reason = 'out_of_order'
            else:
                reject_reason = 'combo_block' if combo_ended else 'delta_zero'
        if delta > 0:
            _gift_dedup[key] = (repeat_count, now, repeat_end == 1)
        elif repeat_end:
            _gift_dedup[key] = (prev_count, now, True)
        return delta, reject_reason


def _prune_dedup_state(max_age=600):
    """清理超过 max_age 秒未更新的去重状态。

    上游 C# 版本（DouyinBarrageGrab）使用 10 秒超时。
    我们使用 30 秒作为平衡——足够覆盖连击间隔，又不会让
    过期条目长时间滞留导致 delta_zero / out_of_order 误杀。
    """
    now = time.time()
    with _dedup_lock:
        stale = [k for k, (_, t, _) in _gift_dedup.items() if now - t > max_age]
        for k in stale:
            del _gift_dedup[k]
        rc1_stale = [k for k, t in _rc1_dedup.items() if now - t > max_age]
        for k in rc1_stale:
            del _rc1_dedup[k]
    # 定期清理 trace_id 集合（限制内存增长）
    with _trace_shadow_lock:
        if len(_seen_trace_ids) > 200_000:
            _seen_trace_ids.clear()


# ── 礼物 Combo 缓冲器 ──────────────────────────────
# 连击礼物（combo_count > 0）不走 delta 法，而是缓冲到
# combo 结束（repeat_end=1）或超时（5s）后一次性写入最终 count。
# 这消除了 delta_zero / out_of_order / combo_block 等误杀。
_gift_combo_buffer = {}
_gift_combo_lock = threading.Lock()
_gift_combo_timeout = 5.0
_gift_finalize_callbacks = {}  # anchor_name → callable

def set_gift_finalize_callback(anchor_name, cb):
    """按主播名注册礼物最终化回调，支持多房间共存。"""
    global _gift_finalize_callbacks
    _gift_finalize_callbacks[anchor_name or ''] = cb

def remove_gift_finalize_callback(anchor_name):
    """移除指定主播的回调。"""
    global _gift_finalize_callbacks
    _gift_finalize_callbacks.pop(anchor_name or '', None)

def _combo_finalize(key):
    with _gift_combo_lock:
        buf = _gift_combo_buffer.pop(key, None)
    if not buf:
        return
    # passed 已在 parse_gift_msg 入口处计数，这里不再重复计
    unit_price = buf['unit_price']
    cnt = buf['cnt']
    total_value = unit_price * cnt
    display_name = buf['display_name']
    if cnt > 0:
        anchor = buf.get('anchor_name', '')
        data = {
            'time': time.strftime('%H:%M:%S'),
            'user_id': buf['user_id'],
            'douyin_id': buf.get('douyin_id', ''),
            'user_name': buf['user_name'],
            'gift_name': display_name,
            'gift_count': cnt,
            'diamond_total': total_value,
            'grade': buf.get('grade', ''),
            'fans_club': buf.get('fans_club', ''),
            'sec_uid': buf.get('sec_uid', ''),
            'avatar_url': buf.get('avatar_url', ''),
            'anchor_name': anchor,
            'group_id': buf.get('group_id', ''),
        }
        cb = _gift_finalize_callbacks.get(anchor)
        if cb:
            cb(data)
    if _sse_callback and buf.get('user_id'):
        _sse_callback('gift', {
            'user_id': buf['user_id'],
            'user_name': buf['user_name'],
            'gift_name': display_name,
            'gift_count': cnt,
            'diamond_total': total_value,
        })
    diamond_info = f" ({total_value}钻石)" if total_value > 0 else ""
    msg = f"[礼物] {buf['user_name']}[{buf['user_id']}] 礼物:{display_name} x{cnt}{diamond_info}"
    logger.log(15, msg)

def get_combo_buffer_size():
    """返回当前组合礼物缓冲中的待定连击数量（group_id, gift_id, user_id 三元组去重）。"""
    with _gift_combo_lock:
        return len(_gift_combo_buffer)

def flush_combo_buffer():
    with _gift_combo_lock:
        keys = list(_gift_combo_buffer.keys())
        _gift_combo_buffer.clear()
    for key in keys:
        _combo_finalize(key)

def flush_all_buffers():
    flush_combo_buffer()

# ── 解析函数 ──────────────────────────────────────

def parse_chat_msg(payload, enable_outputs=None):
    """解析聊天消息（含福袋口令）。

    chat_by == 9 时归类为福袋口令，否则为普通弹幕。

    Args:
        payload: ChatMessage protobuf 序列化字节。
        enable_outputs: 输出开关字典，key='chat' / 'lucky_bag' 控制是否输出。

    Returns:
        结果字典列表。类型为 'chat' 或 'lucky_bag'。
    """
    msg = parse_proto(ChatMessage, payload)
    user = msg.user
    uid = get_user_id(user)
    uname = get_user_name(user)
    common = {
        'time': time.strftime('%H:%M:%S'),
        'user_id': uid,
        'douyin_id': user.display_id,
        'user_name': uname,
        'gender': {1: "男", 2: "女"}.get(user.gender, "未知"),
        'grade': fmt_grade(user),
        'fans_club': fmt_fans_club(user),
        'sec_uid': get_user_sec_uid(user),
        'avatar_url': get_user_avatar_url(user),
    }

    results = []
    if _sse_callback and uid:
        _sse_callback('chat', {**common, 'content': msg.content})

    if msg.chat_by == 9:  # 福袋口令
        if enable_outputs.get('lucky_bag', True):
            results.append({
                'type': 'lucky_bag',
                'msg': f"[福袋口令] {uname}[{uid}] 内容:{msg.content}",
                'data': {**common, 'content': msg.content},
            })
    else:  # 普通聊天
        if enable_outputs.get('chat', True):
            results.append({
                'type': 'chat',
                'msg': f"[聊天] {uname}[{uid}] 内容:{msg.content}",
                'data': {**common, 'content': msg.content},
            })
    return results


def parse_gift_msg(payload, enable_outputs=None):
    """解析礼物消息。

    混合去重策略：
    - 连击礼物（combo_count > 0）：缓冲到 combo 结束，取 MAX 累计值一次性写入。
    - 非连击礼物：用 trace_id 去重，即时写入。
    - 无 trace_id 兜底：delta 法。

    null/损坏保护：
    - gift 对象为 None 时按 gift_id 查 _GIFT_FALLBACK 表回退
    """
    if not enable_outputs.get('gift', True):
        return []
    _dedup_diag['raw'] += 1
    msg = parse_proto(GiftMessage, payload)
    user = msg.user
    uid = get_user_id(user)
    gift = msg.gift

    # ── null/gift 回退保护 ──
    if gift is None or not gift.name:
        gft_id = msg.gift_id or 0
        fallback = _GIFT_FALLBACK.get(gft_id)
        if fallback:
            class _FallbackGift:
                name = fallback['name']; id = gft_id
                diamond_count = fallback['diamond_count']; combo = fallback['combo']; type = 0; image = None
            gift = _FallbackGift()
        else:
            class _MinGift:
                name = f"gift_{gft_id}"; id = gft_id; diamond_count = 0; combo = False; type = 0; image = None
            gift = _MinGift()
    else:
        gft_id = gift.id or msg.gift_id or 0

    user_display_name = get_user_name(user)
    douyin_id = getattr(user, 'display_id', '') or ''

    # ── 提取关键字段 ──
    combo_cnt = msg.combo_count or 0
    rc = combo_cnt if combo_cnt > 0 else (msg.repeat_count or 0)
    gid = str(msg.group_id) if msg.group_id else '0'
    log_id = msg.log_id or ''
    trace_id = msg.trace_id or ''
    send_time = msg.send_time or 0
    is_combo = combo_cnt > 0
    now = time.time()

    # ── 计算礼物单价（供 combo buffer 和非连击路径共用）──
    composite_price = 0; composite_name = ''
    diy_raw = getattr(msg, 'diy_item_info', '') or ''
    if diy_raw:
        try:
            diy_items = json.loads(diy_raw)
            for item in diy_items:
                gift_id_str = str(item.get('values', {}).get('gift_id', ''))
                if gift_id_str and gift_id_str in _GIFT_ID_TO_PRICE:
                    composite_price += _GIFT_ID_TO_PRICE[gift_id_str]
                    if not composite_name:
                        composite_name = _GIFT_ID_TO_NAME.get(gift_id_str, '')
        except (json.JSONDecodeError, TypeError, AttributeError):
            pass
    if composite_price > 0:
        unit_price = composite_price; display_name = composite_name or gift.name
    else:
        unit_price, was_overridden = _lookup_gift_price(gift.name, gift.diamond_count)
        display_name = gift.name
        if was_overridden:
            logger.debug(f"[礼物-定价覆盖] {gift.name}: protobuf={gift.diamond_count} → DB={unit_price}")

    # ── 路径 A：连击礼物 → Combo 缓冲器 ──
    if is_combo:
        key = (gid, str(gft_id), uid)
        with _gift_combo_lock:
            entry = _gift_combo_buffer.get(key)
            if entry is None:
                from base.utils import get_current_anchor
                entry = {
                    'cnt': rc, 'unit_price': unit_price, 'display_name': display_name,
                    'user_name': user_display_name or str(uid), 'user_id': uid,
                    'douyin_id': douyin_id, 'grade': fmt_grade(user),
                    'fans_club': fmt_fans_club(user), 'sec_uid': get_user_sec_uid(user),
                    'avatar_url': get_user_avatar_url(user), 'group_id': gid,
                    'timer': None, 'repeat_end_seen': bool(msg.repeat_end or 0), 'last_update': now,
                    'anchor_name': get_current_anchor() or '',
                }
                _gift_combo_buffer[key] = entry
            else:
                entry['cnt'] = max(entry['cnt'], rc)
                entry['last_update'] = now
                if msg.repeat_end: entry['repeat_end_seen'] = True
                if entry.get('timer'):
                    try: entry['timer'].cancel()
                    except: pass
                    entry['timer'] = None
            should_finalize = entry['repeat_end_seen']
            cnt_final = entry['cnt']
            price = entry['unit_price']
            uname = entry['user_name']
        # 在锁外调用 _combo_finalize，避免死锁
        if should_finalize:
            _combo_finalize(key)
            _dedup_diag['passed'] += 1  # 最终化也计数
            total_value = price * cnt_final
            diamond_info = f" ({total_value}钻石)" if total_value > 0 else ""
            return [{'type': 'gift', 'msg': f"[礼物] {uname}[{uid}] 礼物:{display_name} x{cnt_final}{diamond_info}", 'data': {'_combo_progress': True}}]
        else:
            global _combo_progress_count; _combo_progress_count += 1
            _dedup_diag['passed'] += 1
            def _on_timeout(k=key): return _combo_finalize(k)
            timer = threading.Timer(_gift_combo_timeout, _on_timeout); timer.daemon = True; timer.start()
            with _gift_combo_lock:
                if key in _gift_combo_buffer:
                    _gift_combo_buffer[key]['timer'] = timer
            total_value = price * cnt_final
            diamond_info = f" ({total_value}钻石)" if total_value > 0 else ""
            return [{'type': 'gift', 'msg': f"[礼物] {uname}[{uid}] 礼物:{display_name} x{cnt_final}", 'data': {'_combo_progress': True}}]

    # ── 路径 B：非连击礼物 → 即时写入（trace_id 去重）──
    if rc == 1:
        if trace_id:
            with _trace_shadow_lock:
                if trace_id in _seen_trace_ids: cnt, reason = 0, 'rc1_dup'
                else: _seen_trace_ids.add(trace_id); cnt, reason = 1, ''
        else:
            cnt, reason = 1, ''
    elif rc > 1 and trace_id:
        with _trace_shadow_lock:
            if trace_id in _seen_trace_ids: cnt, reason = 0, 'bulk_dup'
            else: _seen_trace_ids.add(trace_id); cnt, reason = rc, ''
    else:
        cnt, reason = _compute_gift_count(gid, gft_id, uid, rc, repeat_end=msg.repeat_end or 0)

    if cnt > _MAX_REASONABLE_DELTA:
        logger.warning(f"[礼物] delta={cnt} 超过阈值({_MAX_REASONABLE_DELTA}) gift={gift.name} user={user_display_name} rc={rc} gid={gid} reason={reason}")

    if rc > 0 and (rc % 100) == 0:
        _prune_dedup_state()
    if cnt <= 0:
        _dedup_diag['rejected'] += 1; return []
    _dedup_diag['passed'] += 1

    if _sse_callback and uid:
        _sse_callback('gift', {'user_id': uid, 'user_name': user_display_name, 'gift_name': display_name, 'gift_count': cnt, 'diamond_total': unit_price * cnt, 'group_id': gid})

    total_value = unit_price * cnt
    diamond_info = f" ({total_value}钻石)" if total_value > 0 else ""
    return [{
        'type': 'gift',
        'msg': f"[礼物] {user_display_name}[{uid}] 礼物:{display_name} x{cnt}{diamond_info}",
        'data': {
            'time': time.strftime('%H:%M:%S'), 'user_id': uid, 'douyin_id': douyin_id,
            'user_name': user_display_name, 'gender': {1: "男", 2: "女"}.get(user.gender, "未知"),
            'gift_name': display_name, 'gift_count': cnt, 'diamond_total': total_value,
            'grade': fmt_grade(user), 'fans_club': fmt_fans_club(user),
            'sec_uid': get_user_sec_uid(user), 'avatar_url': get_user_avatar_url(user),
            'group_id': gid, 'raw_repeat_count': msg.repeat_count, 'raw_repeat_end': msg.repeat_end,
            'raw_combo_count': msg.combo_count, 'raw_total_count': msg.total_count,
            'log_id': log_id, 'trace_id': trace_id, 'send_time': send_time,
            'priority_score': msg.common.priority_score if msg.common else 0,
            'fold_type': msg.common.fold_type if msg.common else 0,
            'anchor_fold_type': msg.common.anchor_fold_type if msg.common else 0,
            'msg_filter_k': msg.common.msg_process_filter_k if msg.common else '',
            'msg_filter_v': msg.common.msg_process_filter_v if msg.common else '',
            'is_dispatch': 1 if (msg.common and msg.common.is_dispatch) else 0,
            'queue_priority': msg.priority.priority if msg.priority else 0,
        },
    }]


def parse_light_gift_msg(payload, enable_outputs=None):
    """解析轻礼物消息（快捷礼物、粉丝团礼物、融合产物等）。

    LightGiftMessage 与 GiftMessage 的区别：
    - 不含完整 User 对象，仅 user_id 字段
    - 不含 combo_count / repeat_count 等连击字段
    - 礼物价值在 gift_info.diamond_count 中
    - count 字段表示本次赠送数量
    - 使用独立的 gift_id 体系，无法与 GiftMessage 的 gift_id 对应

    输出到独立的 light_gift.csv，不混入 gift.csv。
    钻石计数仍以 GiftMessage → gift.csv 为准，light_gift.csv 仅作参考。

    Args:
        payload: LightGiftMessage protobuf 序列化字节。
        enable_outputs: 输出开关字典，key='light_gift' 控制是否输出。

    Returns:
        结果字典列表。类型为 'light_gift'。
    """
    if enable_outputs and not enable_outputs.get('light_gift', True):
        return []

    msg = parse_proto(LightGiftMessage, payload)
    gid = msg.gift_info.gift_id if msg.gift_info else 0
    unit_dia = msg.gift_info.diamond_count if msg.gift_info else 0
    cnt = msg.count or 1
    uid = str(msg.user_id) if msg.user_id else '0'
    total_value = unit_dia * cnt

    return [{
        'type': 'light_gift',
        'msg': f"[礼物:L] user_{uid} 礼物:gift_{gid} x{cnt} ({total_value}钻,不计入总音浪)",
        'data': {
            'time': time.strftime('%H:%M:%S'),
            'user_id': uid,
            'gift_id': str(gid),
            'diamond_count': unit_dia,
            'count': cnt,
            'diamond_total': total_value,
            'msg_type': msg.msg_type or 0,
        },
    }]


def parse_like_msg(payload, enable_outputs=None):
    """解析点赞消息。

    Args:
        payload: LikeMessage protobuf 序列化字节。
        enable_outputs: 输出开关字典，key='like' 控制是否输出。

    Returns:
        结果字典列表。类型为 'like'，data 含 'count'（本次）和 'total'（累计）。
    """
    if not enable_outputs.get('like', True):
        return []
    msg = parse_proto(LikeMessage, payload)
    user = msg.user
    uid = get_user_id(user)
    uname = get_user_name(user)
    return [{
        'type': 'like',
        'msg': f"[点赞] {uname}[{uid}] 点赞:{msg.count}个, 累计{msg.total}赞",
        'data': {
            'time': time.strftime('%H:%M:%S'),
            'user_id': uid, 'user_name': uname,
            'count': msg.count, 'total': msg.total,
            'grade': fmt_grade(user), 'fans_club': fmt_fans_club(user),
            'sec_uid': get_user_sec_uid(user),
            'avatar_url': get_user_avatar_url(user),
        },
    }]


def parse_member_msg(payload, enable_outputs=None):
    """解析进场消息。

    Args:
        payload: MemberMessage protobuf 序列化字节。
        enable_outputs: 输出开关字典，key='member' 控制是否输出。

    Returns:
        结果字典列表。类型为 'member'，data 含 'gender' 和 'member_count'。
    """
    if not enable_outputs.get('member', True):
        return []
    msg = parse_proto(MemberMessage, payload)
    user = msg.user
    uid = get_user_id(user)
    uname = get_user_name(user)
    gender = {0: "未知", 1: "男", 2: "女"}.get(user.gender, "未知")
    extras = f" (直播间人数:{msg.member_count})" if msg.member_count else ""
    return [{
        'type': 'member',
        'msg': f"[进场] {uname}[{uid}][{gender}] 进入了直播间{extras}",
        'data': {
            'time': time.strftime('%H:%M:%S'),
            'user_id': uid, 'user_name': uname, 'gender': gender,
            'grade': fmt_grade(user), 'fans_club': fmt_fans_club(user),
            'sec_uid': get_user_sec_uid(user),
            'avatar_url': get_user_avatar_url(user),
            'member_count': msg.member_count,
        },
    }]


def parse_social_msg(payload, enable_outputs=None):
    """解析关注/分享消息（仅记录关注 action=1）。

    Args:
        payload: SocialMessage protobuf 序列化字节。
        enable_outputs: 输出开关字典，key='social' 控制是否输出。

    Returns:
        结果字典列表。类型为 'social'，非关注动作（action != 1）返回空列表。
    """
    if not enable_outputs.get('social', True):
        return []
    msg = parse_proto(SocialMessage, payload)
    if msg.action != 1:
        return []
    user = msg.user
    uid = get_user_id(user)
    uname = get_user_name(user)
    action = {1: "关注了主播", 2: "分享了直播间"}.get(msg.action, "互动")
    follow = f"(第{msg.follow_count}个关注)" if msg.follow_count else ""
    return [{
        'type': 'social',
        'msg': f"[关注/分享] {uname}[{uid}] {action} {follow}",
        'data': {
            'time': time.strftime('%H:%M:%S'),
            'user_id': uid, 'user_name': uname, 'action': action,
            'follow_count': msg.follow_count or '',
            'grade': fmt_grade(user), 'fans_club': fmt_fans_club(user),
            'sec_uid': get_user_sec_uid(user),
            'avatar_url': get_user_avatar_url(user),
        },
    }]


def parse_room_user_seq_msg(payload, enable_outputs=None):
    """解析直播间实时统计消息（在线人数、音浪、贡献用户排行等）。

    除了聚合统计外，还提取 ranks_list 中的贡献用户排行数据
    （即 Web 端"1000贡献用户"列表的 Top N 用户）。

    Args:
        payload: RoomUserSeqMessage protobuf 序列化字节。
        enable_outputs: 输出开关字典，key='stats' / 'contribution' 控制是否输出。

    Returns:
        结果字典列表。类型为 'stats' 和/或 'contribution'。
    """
    msg = parse_proto(RoomUserSeqMessage, payload)
    results = []

    # ── 聚合统计 ──
    if enable_outputs.get('stats', True):
        parts = [f"当前: {msg.total}"]
        if msg.total_pv_for_anchor:
            parts.append(f"累计: {msg.total_pv_for_anchor}")
        if msg.total_user_str:
            parts.append(f"累计用户: {msg.total_user_str}")
        if msg.online_user_for_anchor:
            parts.append(f"主播端在线: {msg.online_user_for_anchor}")
        if msg.popularity:
            parts.append(f"热度: {msg.popularity}")
        if msg.up_right_stats_str:
            parts.append(f"右上: {msg.up_right_stats_str}")
        results.append({
            'type': 'stats',
            'msg': f"[统计] {', '.join(parts)}",
            'data': {
                'time': time.strftime('%H:%M:%S'),
                'popularity': msg.popularity or 0,
                'up_right_stats_str': msg.up_right_stats_str or '',
                'up_right_stats_complete': msg.up_right_stats_str_complete or '',
                'current': msg.total,
                'total_pv': msg.total_pv_for_anchor or '',
                'total_user': msg.total_user_str or '',
                'online_anchor': msg.online_user_for_anchor or '',
            },
        })

    return results


def parse_fansclub_msg(payload, enable_outputs=None):
    """解析粉丝团消息（加入/升级）。

    Args:
        payload: FansclubMessage protobuf 序列化字节。
        enable_outputs: 输出开关字典，key='fansclub' 控制是否输出。

    Returns:
        结果字典列表。类型为 'fansclub'。
    """
    if not enable_outputs.get('fansclub', True):
        return []
    msg = parse_proto(FansclubMessage, payload)
    user = msg.user
    uid = get_user_id(user)
    uname = get_user_name(user)
    t = {1: "升级", 2: "加入"}.get(msg.type, "变动")
    return [{
        'type': 'fansclub',
        'msg': f"[粉丝团] {uname}[{uid}] {t}: {msg.content}",
        'data': {
            'time': time.strftime('%H:%M:%S'),
            'user_id': uid, 'user_name': uname,
            'type': t, 'content': msg.content,
            'grade': fmt_grade(user), 'fans_club': fmt_fans_club(user),
            'sec_uid': get_user_sec_uid(user),
            'avatar_url': get_user_avatar_url(user),
        },
    }]


def parse_emoji_chat_msg(payload, enable_outputs=None):
    """解析表情消息。

    Args:
        payload: EmojiChatMessage protobuf 序列化字节。
        enable_outputs: 输出开关字典，key='emoji' 控制是否输出。

    Returns:
        结果字典列表。类型为 'emoji'，无默认内容时显示 '[表情{emoji_id}]'。
    """
    if not enable_outputs.get('emoji', True):
        return []
    msg = parse_proto(EmojiChatMessage, payload)
    user = msg.user
    uid = get_user_id(user)
    uname = get_user_name(user)
    content = msg.default_content or f"[表情{msg.emoji_id}]"
    return [{
        'type': 'emoji',
        'msg': f"[表情] {uname}[{uid}]: {content}",
        'data': {
            'time': time.strftime('%H:%M:%S'),
            'user_id': uid, 'user_name': uname,
            'emoji_id': msg.emoji_id, 'content': content,
            'grade': fmt_grade(user), 'fans_club': fmt_fans_club(user),
            'sec_uid': get_user_sec_uid(user),
            'avatar_url': get_user_avatar_url(user),
        },
    }]


def _extract_proto_strings(payload):
    """递归提取 protobuf 中所有字符串字段的值。"""
    strings = []
    stack = [(payload, 0)]
    while stack:
        data, depth = stack.pop()
        if depth > 5:
            continue
        i = 0
        while i < len(data):
            tag = 0; shift = 0
            while i < len(data):
                b = data[i]; i += 1
                tag |= (b & 0x7f) << shift; shift += 7
                if not (b & 0x80): break
            fn = tag >> 3; wt = tag & 0x7
            if wt == 0:
                val = 0; shift = 0
                while i < len(data):
                    b = data[i]; i += 1
                    val |= (b & 0x7f) << shift; shift += 7
                    if not (b & 0x80): break
            elif wt == 2:
                length = 0; shift = 0
                while i < len(data):
                    b = data[i]; i += 1
                    length |= (b & 0x7f) << shift; shift += 7
                    if not (b & 0x80): break
                nested = data[i:i+length]; i += length
                try:
                    s = nested.decode('utf-8')
                    if len(s) > 0 and len(s) < 200:
                        strings.append(s)
                except:
                    pass
                stack.append((nested, depth + 1))
            elif wt == 5:
                i += 4
            else:
                break
    return strings


def _extract_douyin_id(payload):
    """从 protobuf 中提取 douyin_id（10-12位数字，非 room_id）。"""
    # 遍历所有 varint，找 10-12 位的数字，排除已知的 room_id
    ids = []
    i = 0
    stack = [(payload, 0)]
    while stack:
        data, depth = stack.pop()
        if depth > 6:
            continue
        j = 0
        while j < len(data):
            tag = 0; shift = 0
            while j < len(data):
                b = data[j]; j += 1
                tag |= (b & 0x7f) << shift; shift += 7
                if not (b & 0x80): break
            fn = tag >> 3; wt = tag & 0x7
            if wt == 0:
                val = 0; shift = 0
                while j < len(data):
                    b = data[j]; j += 1
                    val |= (b & 0x7f) << shift; shift += 7
                    if not (b & 0x80): break
                if 10**9 < val < 10**13:
                    # 排除 Unix 时间戳（秒 1.5e9-2e9，毫秒 1.5e12-2e12）
                    if 1500000000 < val < 2000000000:
                        continue
                    if 1500000000000 < val < 2000000000000:
                        continue
                    ids.append(val)
            elif wt == 2:
                length = 0; shift = 0
                while j < len(data):
                    b = data[j]; j += 1
                    length |= (b & 0x7f) << shift; shift += 7
                    if not (b & 0x80): break
                stack.append((data[j:j+length], depth + 1))
                j += length
            elif wt == 5:
                j += 4
            else:
                break
    # 排除已知 room_id，取最常见的
    from collections import Counter
    id_counts = Counter(ids)
    for val, _ in id_counts.most_common(3):
        if val not in (7647162160514190118, 7646418124761353003, 7646787956389776170):
            return str(val)
    return ''


def _extract_user_id(payload):
    """从 RoomMessage 原始 payload 扫描 17-19 位 protobuf varint 提取 user_id。
    优先 field number = 1 的候选值（常见于 User 对象）。

    Returns:
        str 或空字符串。
    """
    ids = []
    stack = [(payload, 0)]
    while stack:
        data, depth = stack.pop()
        if depth > 6:
            continue
        j = 0
        while j < len(data):
            tag = 0; shift = 0
            while j < len(data):
                b = data[j]; j += 1
                tag |= (b & 0x7f) << shift; shift += 7
                if not (b & 0x80): break
            fn = tag >> 3; wt = tag & 0x7
            if wt == 0:
                val = 0; shift = 0
                while j < len(data):
                    b = data[j]; j += 1
                    val |= (b & 0x7f) << shift; shift += 7
                    if not (b & 0x80): break
                # 17-19 位数字 = Douyin user_id
                if 10**16 < val < 10**19:
                    ids.append((val, fn))
            elif wt == 2:
                length = 0; shift = 0
                while j < len(data):
                    b = data[j]; j += 1
                    length |= (b & 0x7f) << shift; shift += 7
                    if not (b & 0x80): break
                stack.append((data[j:j+length], depth + 1))
                j += length
            elif wt == 5:
                j += 4
            else:
                break

    if not ids:
        return ''

    # 优先选 field number = 1 的值
    fn1 = [v for v, fn in ids if fn == 1]
    if fn1:
        return str(fn1[0])

    # 否则取最常见的
    counts = Counter(v for v, _ in ids)
    return str(counts.most_common(1)[0][0])


def _extract_subscribe(payload):
    """从 RoomMessage 原始 payload 提取会员/星守护订阅信息。
    返回 dict 或 None。"""
    # 订阅事件标识
    if b'subscribe_anchor_mvp_v2' not in payload and '星守护'.encode('utf-8') not in payload:
        return None

    # 提取所有 protobuf 字符串
    all_strings = _extract_proto_strings(payload)

    # 过滤：只移除明显的噪音（URL、图片路径、模板占位符）
    noise_keywords = ('http://', 'https://', '.png', '.jpg', '.gif', '.webp', 'img/', '勋章')
    strings = [s for s in all_strings
               if not any(p in s for p in noise_keywords)
               and s not in ('', '小葵花', '送{0}')]

    # 提取 douyin_id 和 user_id
    douyin_id = _extract_douyin_id(payload)
    user_id = _extract_user_id(payload)

    # ── 辅助：从字符串列表中找用户名 ──
    def _find_username(candidates):
        """从候选字符串中提取最可能的用户名（评分法，优先含汉字的）。"""
        known_words = {'开通', '续费', '月度', '季度', '年度', '会员', '星守护',
                       'subscribe_anchor_mvp_v2', 'webcast', 'douyin', 'room'}
        best = ''
        best_score = -1
        for s in candidates:
            if not s or len(s) > 60:
                continue
            if s.startswith('{') and s.endswith('}'):
                continue
            import re as _re
            if _re.search(r'\{\d+:', s):
                continue  # 跳过 i18n 模板 {0:user} {1:string}...
            if s.isdigit():
                continue
            if s in known_words:
                continue
            # 过滤 protobuf 消息类型名（如 WebcastRoomMessage、Common、Response、PushFrame）
            if _re.match(r'^(Webcast|Common|Response|PushFrame)', s):
                continue
            clean = s.lstrip('"').rstrip('" ').strip()
            if not clean or len(clean) > 50:
                continue
            # 评分：汉字 > 字母 > 纯符号
            score = 0
            has_cjk = any('一' <= c <= '鿿' for c in clean)
            has_alpha = any(c.isalpha() for c in clean)
            if has_cjk:
                score += 100  # 含汉字的优先
            if has_alpha:
                score += 10
            if clean.isascii() and not clean.isalpha():
                score -= 20  # 纯符号/混合符号扣分
            if len(clean) >= 3:
                score += 5   # 长度 >= 3 加分
            if score > best_score:
                best_score = score
                best = clean
        return best

    # ── 会员订阅 ──
    has_vip_template = any('会员' in s for s in all_strings)
    if has_vip_template:
        action = ''
        sub_type = ''
        for s in strings:
            if s in ('开通', '续费'):
                action = s
            elif s in ('月度', '季度', '年度'):
                sub_type = s
        if action and sub_type:
            user = _find_username(strings)
            if not user:
                # 回退：用正则扫描整个 payload 中的中文名
                import re as _re
                m = _re.search(rb'[\x80-\xff]{3,18}', payload)
                if m:
                    try:
                        user = m.group().decode('utf-8', errors='ignore')
                    except Exception:
                        pass
            return {'event': '会员', 'action': action, 'type': sub_type,
                    'user': user, 'douyin_id': douyin_id, 'user_id': user_id}

    # ── 星守护 ──
    has_star_template = any('星守护' in s for s in all_strings)
    if has_star_template:
        user = _find_username(strings)
        if not user:
            import re as _re
            m = _re.search(rb'[\x80-\xff]{3,18}', payload)
            if m:
                try:
                    user = m.group().decode('utf-8', errors='ignore')
                except Exception:
                    pass
        if user or douyin_id:
            return {'event': '星守护', 'action': '开通', 'type': '月度',
                    'user': user, 'douyin_id': douyin_id, 'user_id': user_id}

    return None


# 订阅价格映射
_SUBSCRIBE_PRICES = {
    ('会员', '开通', '月度'): 980,
    ('会员', '续费', '月度'): 980,
    ('会员', '开通', '季度'): 2940,
    ('会员', '续费', '季度'): 2940,
    ('会员', '开通', '年度'): 11760,
    ('会员', '续费', '年度'): 11760,
    ('星守护', '开通', '月度'): 1280,
    ('星守护', '续费', '月度'): 1280,
    ('星守护', '开通', '年度'): 15360,
    ('星守护', '续费', '年度'): 15360,
}


def parse_room_msg(payload, enable_outputs=None):
    """解析直播间公告消息（置顶、场景、会员/星守护订阅等）。

    Args:
        payload: RoomMessage protobuf 序列化字节。
        enable_outputs: 输出开关字典，key='room'/'subscribe' 控制是否输出。

    Returns:
        结果字典列表。类型为 'room' 和/或 'subscribe'。
    """
    results = []

    # ── 订阅检测 ──
    sub_info = _extract_subscribe(payload)
    if sub_info and enable_outputs.get('subscribe', True):
        event = sub_info['event']
        action = sub_info['action']
        sub_type = sub_info['type']
        user = sub_info['user']
        douyin_id = sub_info.get('douyin_id', '')
        price = _SUBSCRIBE_PRICES.get((event, action, sub_type), 0)

        # 从 RoomMessage.common.user 获取真实用户信息（比字节扫描更可靠）
        real_uid = ''; real_sec_uid = ''; real_avatar = ''; real_grade = ''; real_fans_club = ''
        try:
            room_msg = parse_proto(RoomMessage, payload)
            # 调试：看 common.user 到底有没有
            has_common = bool(room_msg.common)
            has_user = bool(room_msg.common and room_msg.common.user)
            user_id_from_proto = ''
            user_name_from_proto = ''
            if has_user:
                u = room_msg.common.user
                user_id_from_proto = str(get_user_id(u) or '')
                user_name_from_proto = get_user_name(u) or ''
                uid = get_user_id(u)
                if uid:
                    real_uid = uid
                    douyin_id = real_uid
                    user = get_user_name(u) or user
                    real_sec_uid = get_user_sec_uid(u)
                    real_avatar = get_user_avatar_url(u)
                    real_grade = fmt_grade(u)
                    real_fans_club = fmt_fans_club(u)
            # 调试日志：记录解析结果和字节扫描结果的对比
            if not has_user:
                # protobuf 没有 user → 回退到字节扫描的 user_id
                bytescan_uid = sub_info.get('user_id', '')
                if bytescan_uid:
                    # 排除被误识别为 user_id 的 room_id
                    room_id_from_proto = str(room_msg.common.room_id) if room_msg.common and room_msg.common.room_id else ''
                    if room_id_from_proto and bytescan_uid == room_id_from_proto:
                        logger.info(f"[订阅调试] 字节扫描 user_id={bytescan_uid} 与 room_id 相同，已忽略")
                        real_uid = ''
                    else:
                        real_uid = bytescan_uid
                        logger.info(f"[订阅调试] protobuf 无 user，回退字节扫描 user_id={bytescan_uid}")
            if sub_info.get('douyin_id') != user_id_from_proto or sub_info.get('user') != user_name_from_proto:
                logger.info(f"[订阅调试] bytescan_id={sub_info.get('douyin_id','')} proto_id={user_id_from_proto} "
                           f"bytescan_name={sub_info.get('user','')} proto_name={user_name_from_proto} "
                           f"has_common={has_common} has_user={has_user}")
        except Exception as e:
            logger.debug(f"[订阅调试] protobuf 解析异常: {e}")

        results.append({
            'type': 'subscribe',
            'msg': f'[订阅] {user} {action}{sub_type}{event} ({price}钻石)',
            'data': {
                'time': time.strftime('%H:%M:%S'),
                'user_name': user,
                'douyin_id': douyin_id,
                'user_id': real_uid,
                'sec_uid': real_sec_uid,
                'avatar_url': real_avatar,
                'grade': real_grade,
                'fans_club': real_fans_club,
                'event': event,
                'action': action,
                'sub_type': sub_type,
                'diamond': price,
            },
        })

    # ── 原有 room 解析 ──
    if not enable_outputs.get('room', True):
        return results

    msg = parse_proto(RoomMessage, payload)
    is_top = "[置顶]" if msg.system_top_msg else ""
    detail = f"直播间id:{msg.common.room_id}"
    if msg.content:
        detail += f", 内容:{msg.content}"
    if msg.biz_scene:
        detail += f", 场景:{msg.biz_scene}"
    results.append({
        'type': 'room',
        'msg': f"[直播间] {is_top}{detail}",
        'data': {
            'time': time.strftime('%H:%M:%S'),
            'is_top': '是' if msg.system_top_msg else '否',
            'room_id': msg.common.room_id if msg.common else '',
            'content': msg.content or '',
            'biz_scene': msg.biz_scene or '',
        },
    })
    return results


def parse_room_stats_msg(payload, enable_outputs=None):
    """解析直播累计统计消息（观看人次等）。

    Args:
        payload: RoomStatsMessage protobuf 序列化字节。
        enable_outputs: 输出开关字典，key='roomstats' 控制是否输出。

    Returns:
        结果字典列表。类型为 'roomstats'。
    """
    if not enable_outputs.get('roomstats', True):
        return []
    msg = parse_proto(RoomStatsMessage, payload)
    detail = msg.display_long or msg.display_middle or msg.display_short or str(msg.total)
    return [{
        'type': 'roomstats',
        'msg': f"[直播统计] {detail} (数值:{msg.total})",
        'data': {
            'time': time.strftime('%H:%M:%S'),
            'detail': detail,
            'total': msg.total,
        },
    }]


def parse_rank_msg(payload, enable_outputs=None):
    """解析排行榜消息（结构化版）。

    Args:
        payload: RoomRankMessage protobuf 序列化字节。
        enable_outputs: 输出开关字典，key='rank' 控制是否输出。

    Returns:
        结果字典列表。类型为 'rank'，每条记录对应一位上榜用户。
    """
    if not enable_outputs.get('rank', True):
        return []
    msg = parse_proto(RoomRankMessage, payload)
    if not msg.ranks_list:
        return []

    results = []
    for i, r in enumerate(msg.ranks_list, 1):
        if not r.score_str:
            continue
        uid = get_user_id(r.user)
        uname = get_user_name(r.user)
        results.append({
            'type': 'rank',
            'msg': f"[排行榜] 第{i}名: {uname} 积分:{r.score_str}",
            'data': {
                'time': time.strftime('%H:%M:%S'),
                'rank_pos': i,
                'user_id': uid,
                'douyin_id': r.user.display_id,
                'user_name': uname,
                'score': r.score_str,
                'sec_uid': get_user_sec_uid(r.user),
                'avatar_url': get_user_avatar_url(r.user),
            },
        })
    return results


def parse_control_msg(payload, enable_outputs=None):
    """解析直播控制消息（开始/暂停/结束）。

    直播结束时根据 live_stop 配置返回控制指令：
    - live_stop=False → action='wait_live'（进入等待开播模式）
    - live_stop=True  → action='stop'（终止采集）

    Args:
        payload: ControlMessage protobuf 序列化字节。
        enable_outputs: 输出开关字典，key='control' 控制是否记录，
            key='live_stop' 决定结束时的行为。

    Returns:
        结果字典列表。始终包含 status='已结束' 时的 action 指令。
    """
    msg = parse_proto(ControlMessage, payload)
    status = {1: "开始", 2: "暂停", 3: "已结束"}.get(msg.status, f"未知({msg.status})")
    results = []
    if enable_outputs.get('control', True):
        results.append({
            'type': 'control',
            'msg': f"[直播状态] {status}",
            'data': {
                'time': time.strftime('%H:%M:%S'),
                'status': status,
            },
        })
    if msg.status == 3:
        if enable_outputs.get('live_stop', False):
            results.append({'action': 'stop'})
        else:
            results.append({'action': 'wait_live'})
    return results


def parse_room_stream_adaptation_msg(payload, enable_outputs=None):
    """解析流配置消息（仅日志，不写入数据文件）。

    Args:
        payload: RoomStreamAdaptationMessage protobuf 序列化字节。
        enable_outputs: 未使用，保持签名一致。

    Returns:
        类型为 '_log_only' 的结果列表，不会被写入 CSV/JSONL。
    """
    msg = parse_proto(RoomStreamAdaptationMessage, payload)
    return [{
        'type': '_log_only',
        'msg': f"[流配置] 类型:{msg.adaptation_type}",
    }]


# ── PK/连麦消息解析 ─────────────────────────────

def _read_varint(data, pos):
    """读取 protobuf varint，返回 (value, new_pos)。"""
    value = 0
    shift = 0
    while pos < len(data):
        b = data[pos]; pos += 1
        value |= (b & 0x7f) << shift
        if not (b & 0x80):
            return value, pos
        shift += 7
    return None, pos

def _read_ld(data, pos):
    """读取 length-delimited 字段，返回 (bytes, new_pos)。"""
    length, pos = _read_varint(data, pos)
    if length is None or pos + length > len(data):
        return None, pos
    return data[pos:pos + length], pos + length

def _extract_pk_fields(payload, field_specs):
    """从原始 protobuf payload 中提取指定字段（跳过 field 1 = Common）。

    Args:
        payload: 原始 protobuf 字节
        field_specs: list of (field_number, label, field_type, inner_map_or_none)
            field_type: 'varint', 'string', 'nested_v', 'nested_s'
            inner_map: for nested types, dict of {inner_fn: (inner_label, inner_type)}

    Returns:
        dict of {label: value}
    """
    # Build lookup: {fn: (label, ftype, inner_map)}
    lookup = {}
    for fn, label, ftype, inner in field_specs:
        lookup[fn] = (label, ftype, inner)

    def _decode_fields(data, lookup):
        """在给定的 protobuf 数据中按 lookup 提取字段。"""
        local = {}
        p = 0
        while p < len(data):
            tag, p = _read_varint(data, p)
            if tag is None:
                break
            fn = tag >> 3
            wt = tag & 0x7
            if fn in lookup:
                label, ftype, inner_map = lookup[fn]
                if wt == 0 and ftype == 'varint':
                    val, p = _read_varint(data, p)
                    local[label] = str(val)
                elif wt == 2 and ftype in ('string', 'nested_s'):
                    raw, p = _read_ld(data, p)
                    if raw is None:
                        continue
                    if ftype == 'string':
                        try:
                            s = raw.decode('utf-8')
                            if s.isprintable():
                                local[label] = s
                        except Exception:
                            pass
                    elif ftype == 'nested_s' and inner_map:
                        ipos = 0
                        while ipos < len(raw):
                            itag, ipos = _read_varint(raw, ipos)
                            if itag is None:
                                break
                            ifn = itag >> 3
                            iwt = itag & 0x7
                            if ifn in inner_map:
                                ilabel, itype = inner_map[ifn]
                                if iwt == 2 and itype == 'string':
                                    iraw, ipos = _read_ld(raw, ipos)
                                    if iraw:
                                        try:
                                            local[ilabel] = iraw.decode('utf-8')
                                        except Exception:
                                            pass
                                elif iwt == 0:
                                    _, ipos = _read_varint(raw, ipos)
                                elif iwt == 2:
                                    _, ipos = _read_ld(raw, ipos)
                                elif iwt == 5:
                                    ipos += 4
                            else:
                                if iwt == 0:
                                    _, ipos = _read_varint(raw, ipos)
                                elif iwt == 2:
                                    _, ipos = _read_ld(raw, ipos)
                                elif iwt == 5:
                                    ipos += 4
                else:
                    # type mismatch, skip
                    if wt == 0:
                        _, p = _read_varint(data, p)
                    elif wt == 2:
                        _, p = _read_ld(data, p)
                    elif wt == 5:
                        p += 4
            else:
                # skip uninteresting fields
                if wt == 0:
                    _, p = _read_varint(data, p)
                elif wt == 2:
                    _, p = _read_ld(data, p)
                elif wt == 5:
                    p += 4
        return local

    result = {}
    pos = 0
    while pos < len(payload):
        tag, pos = _read_varint(payload, pos)
        if tag is None:
            break
        fn = tag >> 3
        wt = tag & 0x7

        if fn == 1:  # skip Common
            if wt == 2:
                _, pos = _read_ld(payload, pos)
            elif wt == 0:
                _, pos = _read_varint(payload, pos)
            continue

        if fn in lookup:
            # direct top-level field extraction
            label, ftype, inner_map = lookup[fn]
            if wt == 0 and ftype == 'varint':
                val, pos = _read_varint(payload, pos)
                result[label] = str(val)
            elif wt == 2:
                raw, pos = _read_ld(payload, pos)
                if raw is None:
                    continue
                if ftype == 'string':
                    try:
                        s = raw.decode('utf-8')
                        if s.isprintable():
                            result[label] = s
                    except Exception:
                        pass
                elif ftype == 'nested_s' and inner_map:
                    nested = _decode_fields(raw, inner_map)
                    result.update(nested)
            else:
                if wt == 0:
                    _, pos = _read_varint(payload, pos)
                elif wt == 2:
                    _, pos = _read_ld(payload, pos)
                elif wt == 5:
                    pos += 4
        elif fn == 2 and wt == 2:
            # Field 2 is the message body — decode nested fields inside it
            raw, pos = _read_ld(payload, pos)
            if raw:
                nested = _decode_fields(raw, lookup)
                result.update(nested)
        else:
            # skip
            if wt == 0:
                _, pos = _read_varint(payload, pos)
            elif wt == 2:
                _, pos = _read_ld(payload, pos)
            elif wt == 5:
                pos += 4

    return result


def _extract_score_and_result(payload):
    """从 BattleFinishMethod payload 提取 f4.f19 (score) 和 f9 (result_text)。"""
    score = 0
    result_text = ''
    p = 0
    while p < len(payload):
        tag, p = _read_varint(payload, p)
        if tag is None:
            break
        fn = tag >> 3
        wt = tag & 0x7

        if fn == 9 and wt == 2:
            raw, p = _read_ld(payload, p)
            if raw:
                try:
                    s = raw.decode('utf-8')
                    if s.isprintable():
                        result_text = s
                except Exception:
                    pass
        elif fn == 4 and wt == 2:
            raw, p = _read_ld(payload, p)
            if raw:
                # Walk inside f4 looking for f19 varint
                ip = 0
                while ip < len(raw):
                    itag, ip = _read_varint(raw, ip)
                    if itag is None:
                        break
                    ifn = itag >> 3
                    iwt = itag & 0x7
                    if ifn == 19 and iwt == 0:
                        val, ip = _read_varint(raw, ip)
                        if val is not None:
                            score = val
                        break
                    elif iwt == 0:
                        _, ip = _read_varint(raw, ip)
                    elif iwt == 2:
                        _, ip = _read_ld(raw, ip)
                    elif iwt == 5:
                        ip += 4
                    else:
                        break
        elif wt == 0:
            _, p = _read_varint(payload, p)
        elif wt == 2:
            _, p = _read_ld(payload, p)
        elif wt == 5:
            p += 4
        else:
            break
    return score, result_text


def _track_pk_round(method_label, payload, time_str=None):
    """追踪 PK 回合：BattleMethod 开始 → BattleFinishMethod 结束 → 输出 pk_round。

    仅由 _make_pk_handler 内部调用。
    返回: 一个 pk_round 数据 dict，或 None（回合未完成时）。
    """
    global _pk_current

    if time_str is None:
        time_str = time.strftime('%H:%M:%S')

    if method_label == 'LinkmicBattle':
        participants = _extract_participants(payload)
        play_mode, team_mode = _extract_mode_json(payload)
        _pk_current = {
            'start_time': time_str,
            'participants': participants,
            'play_mode': play_mode,
            'team_mode': team_mode,
        }
        return None

    elif method_label == 'LinkmicBattleFinish':
        score, result_text = _extract_score_and_result(payload)
        play_mode, team_mode = _extract_mode_json(payload)

        if _pk_current is None:
            return None

        start = _pk_current
        _pk_current = None

        # 用时粗略估计（基于系统时间差）
        try:
            t1_parts = start['start_time'].split(':')
            t2_parts = time_str.split(':')
            t1_secs = int(t1_parts[0])*3600 + int(t1_parts[1])*60 + int(t1_parts[2])
            t2_secs = int(t2_parts[0])*3600 + int(t2_parts[1])*60 + int(t2_parts[2])
            dur = max(0, t2_secs - t1_secs)
        except Exception:
            dur = 0

        pm = play_mode or start['play_mode']
        tm = team_mode or start['team_mode']

        return {
            'start_time': start['start_time'],
            'end_time': time_str,
            'duration_sec': dur,
            'mode': _mode_label(pm),
            'participants': ','.join(start['participants']),
            'participant_count': len(start['participants']),
            'opponent_id': '',
            'self_score': score,
            'result': result_text,
        }

    elif method_label == 'BattleEndPunish':
        # opponent_id 已在 pk_event.csv 中有记录。
        # BattleEndPunish 晚于 BattleFinishMethod 到达，流式输出难以回填。
        # 如需合并，可按时间窗口在后期匹配。
        return None

    return None


# 各 PK 消息类型需要提取的字段映射
# 格式: [(field_number, label, field_type, inner_map_or_None), ...]
# field_type: 'varint' | 'string' | 'nested_s'
_PK_FIELD_MAP = {
    'BattleEndPunish': [
        (4,  'duration_sec', 'varint', None),
        (18, 'status_text',  'nested_s', {2: ('status_text', 'string')}),
        (34, 'opponent_id',  'varint', None),
        (40, 'mode',         'varint', None),
    ],
    'BattleAuxiliary': [
        (2, 'aux_type', 'varint', None),
    ],
    'LinkmicPlayModeUpdateScore': [
        (5,  'opponent_id', 'varint', None),
        (6,  'score_self',  'varint', None),
        (10, 'linker_id',   'string', None),
    ],
    'LinkerContribute': [
        (2, 'opponent_id',  'varint', None),
        (7, 'total_display', 'string', None),
    ],
    'BattlePowerContainer': [
        (2, 'user_id_a', 'varint', None),
        (3, 'user_id_b', 'varint', None),
    ],
    'LinkmicBattle': [],
    'LinkmicBattleFinish': [],
    'LinkmicArmies': [],
}

def _make_pk_handler(proto_cls, method_label):
    """创建 PK 消息处理器（闭包捕获 proto 类和标签）。

    对于 BattleEndPunish / UpdateScore / LinkerContribute 等关键消息，
    自动提取 opponent_id、pk_type、duration、score 等字段。
    """
    field_map = _PK_FIELD_MAP.get(method_label, {})

    def handler(payload, enable_outputs=None):
        results = []

        # 1) pk_event 输出（原有逻辑，不动）
        if enable_outputs.get('pk_event', True):
            msg = parse_proto(proto_cls, payload)
            common = msg.common
            data = {
                'time': time.strftime('%H:%M:%S'),
                'method': method_label,
                'payload_len': len(payload),
            }
            if field_map:
                extracted = _extract_pk_fields(payload, field_map)
                data.update(extracted)
            result = {
                'type': 'pk_event',
                'msg': f'[{method_label}]',
                'data': data,
            }
            if len(payload) > 100:
                result['_dump_raw'] = payload
                result['_dump_dir'] = 'pk_dumps'
                result['_dump_name'] = f"{result['data']['time'].replace(':', '')}_{method_label}_{len(payload)}.bin"
            results.append(result)

        # 2) pk_round 追踪（新增，不影响 pk_event）
        if enable_outputs.get('pk_round', True) and method_label in (
            'LinkmicBattle', 'LinkmicBattleFinish', 'BattleEndPunish'
        ):
            now_str = time.strftime('%H:%M:%S')
            round_data = _track_pk_round(method_label, payload, now_str)
            if round_data:
                results.append({
                    'type': 'pk_round',
                    'msg': f'[PK] {round_data.get("start_time", "")}→{round_data.get("end_time", "")}',
                    'data': round_data,
                })

        return results
    return handler


# ── 分发表 ──────────────────────────────────────

HANDLERS = {
    'WebcastChatMessage':                 parse_chat_msg,
    'WebcastGiftMessage':                 parse_gift_msg,
    'WebcastLightGiftMessage':            parse_light_gift_msg,
    'WebcastLikeMessage':                 parse_like_msg,
    'WebcastMemberMessage':               parse_member_msg,
    'WebcastSocialMessage':               parse_social_msg,
    'WebcastRoomUserSeqMessage':          parse_room_user_seq_msg,
    'WebcastFansclubMessage':             parse_fansclub_msg,
    'WebcastControlMessage':              parse_control_msg,
    'WebcastEmojiChatMessage':            parse_emoji_chat_msg,
    'WebcastRoomStatsMessage':            parse_room_stats_msg,
    'WebcastRoomMessage':                 parse_room_msg,
    'WebcastRoomRankMessage':             parse_rank_msg,
    'WebcastRoomStreamAdaptationMessage': parse_room_stream_adaptation_msg,

    # PK/连麦消息
    'WebcastLinkmicBattleMethod':              _make_pk_handler(LinkmicBattleMessage, 'LinkmicBattle'),
    'WebcastLinkmicBattleMethodMessage':       _make_pk_handler(LinkmicBattleMessage, 'LinkmicBattle'),
    'WebcastLinkMicBattleMethod':              _make_pk_handler(LinkmicBattleMessage, 'LinkmicBattle'),
    'WebcastLinkMicBattleMethodMessage':       _make_pk_handler(LinkmicBattleMessage, 'LinkmicBattle'),
    'WebcastLinkmicBattleFinishMethod':        _make_pk_handler(LinkmicBattleFinishMessage, 'LinkmicBattleFinish'),
    'WebcastLinkmicBattleFinishMethodMessage': _make_pk_handler(LinkmicBattleFinishMessage, 'LinkmicBattleFinish'),
    'WebcastLinkMicBattleFinishMethod':        _make_pk_handler(LinkmicBattleFinishMessage, 'LinkmicBattleFinish'),
    'WebcastLinkMicBattleFinishMethodMessage': _make_pk_handler(LinkmicBattleFinishMessage, 'LinkmicBattleFinish'),
    'WebcastLinkmicArmiesMethod':              _make_pk_handler(LinkmicArmiesMessage, 'LinkmicArmies'),
    'WebcastLinkmicArmiesMethodMessage':       _make_pk_handler(LinkmicArmiesMessage, 'LinkmicArmies'),
    'WebcastLinkmicPlayModeUpdateScoreMessage': _make_pk_handler(LinkmicPlayModeUpdateScoreMessage, 'LinkmicPlayModeUpdateScore'),
    'WebcastLinkerContributeMessage':          _make_pk_handler(LinkerContributeMessage, 'LinkerContribute'),
    'WebcastBattleAuxiliaryMessage':           _make_pk_handler(BattleAuxiliaryMessage, 'BattleAuxiliary'),
    'WebcastBattleEndPunishMessage':           _make_pk_handler(BattleEndPunishMessage, 'BattleEndPunish'),
    'WebcastBattlePowerContainerMessage':      _make_pk_handler(BattlePowerContainerMessage, 'BattlePowerContainer'),
}

# ── SQLite 写入 ──────────────────────────────────

DB_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
DB_PATH = os.path.join(DB_DIR, 'douyin_barrage.db')
_local = threading.local()
_conn_registry = set()  # 跟踪所有 SQLite 连接，退出时关闭
_conn_lock = threading.Lock()


def _close_all_connections():
    """进程退出时关闭所有 SQLite 连接，防止 fd 泄漏。"""
    with _conn_lock:
        for c in list(_conn_registry):
            try:
                c.close()
            except Exception:
                pass
        _conn_registry.clear()


import atexit
atexit.register(_close_all_connections)

# ── 启动时自动迁移旧表结构 ──
try:
    os.makedirs(DB_DIR, exist_ok=True)
    _migrate_conn = sqlite3.connect(DB_PATH)
    _migrate_conn.execute('PRAGMA journal_mode=WAL')
    try:
        _migrate_conn.execute('ALTER TABLE users ADD COLUMN grade TEXT DEFAULT ""')
        _migrate_conn.commit()
    except sqlite3.OperationalError:
        pass
    try:
        _migrate_conn.execute('ALTER TABLE gift_logs ADD COLUMN grade TEXT DEFAULT ""')
        _migrate_conn.execute('ALTER TABLE gift_logs ADD COLUMN fans_club TEXT DEFAULT ""')
        _migrate_conn.commit()
    except sqlite3.OperationalError:
        pass
    try:
        _migrate_conn.execute('ALTER TABLE gift_logs ADD COLUMN group_id TEXT DEFAULT ""')
        _migrate_conn.commit()
    except sqlite3.OperationalError:
        pass
    try:
        _migrate_conn.execute('ALTER TABLE users ADD COLUMN sec_uid TEXT DEFAULT ""')
        _migrate_conn.commit()
    except sqlite3.OperationalError:
        pass
    try:
        _migrate_conn.execute('ALTER TABLE users ADD COLUMN avatar_url TEXT DEFAULT ""')
        _migrate_conn.commit()
    except sqlite3.OperationalError:
        pass
    try:
        _migrate_conn.execute('ALTER TABLE users ADD COLUMN notes TEXT DEFAULT ""')
        _migrate_conn.commit()
    except sqlite3.OperationalError:
        pass
    try:
        _migrate_conn.execute('ALTER TABLE users ADD COLUMN tags TEXT DEFAULT ""')
        _migrate_conn.commit()
    except sqlite3.OperationalError:
        pass
    # 升级礼物去重索引：同一秒内同用户同礼物去重，不同秒的保留（防止 websocket 重放但不误伤连刷）
    try:
        _migrate_conn.execute('DROP INDEX IF EXISTS idx_gift_dedup')
        _migrate_conn.execute('DROP INDEX IF EXISTS idx_gift_logs_user')
        _migrate_conn.commit()
    except sqlite3.OperationalError:
        pass
    try:
        _migrate_conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_gift_dedup_ts ON gift_logs(session_id, user_id, gift_name, diamond_total, gift_count, created_at)')
        _migrate_conn.commit()
    except sqlite3.OperationalError:
        pass
    _migrate_conn.close()
except Exception:
    pass




_db_schema_inited = False
_db_schema_lock = threading.Lock()

def _get_conn():
    global _db_schema_inited
    if not hasattr(_local, 'conn') or _local.conn is None:
        os.makedirs(DB_DIR, exist_ok=True)
        _local.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        with _conn_lock:
            _conn_registry.add(_local.conn)
        _local.conn.execute('PRAGMA journal_mode=WAL')
        _local.conn.execute('PRAGMA synchronous=NORMAL')
        _local.conn.execute('PRAGMA busy_timeout=30000')
        _local.conn.execute('PRAGMA cache_size=-16000')
        _local.conn.execute('PRAGMA mmap_size=268435456')
        _local.conn.execute('PRAGMA temp_store=MEMORY')
        _local.conn.execute('PRAGMA foreign_keys=ON')
        _local.conn.row_factory = sqlite3.Row
        if not _db_schema_inited:
            with _db_schema_lock:
                if not _db_schema_inited:
                    init_db()
                    init_gift_prices_table()
                    _db_schema_inited = True
    return _local.conn


def init_db():
    conn = _get_conn()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            room_id TEXT NOT NULL,
            anchor_name TEXT DEFAULT '',
            start_time DATETIME DEFAULT (datetime('now', '+8 hours')),
            end_time DATETIME,
            status TEXT DEFAULT 'live'
        );
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT UNIQUE NOT NULL,
            user_name TEXT NOT NULL,
            fans_club TEXT DEFAULT '',
            grade TEXT DEFAULT '',
            sec_uid TEXT DEFAULT '',
            avatar_url TEXT DEFAULT '',
            notes TEXT DEFAULT '',
            tags TEXT DEFAULT '',
            is_anonymous INTEGER DEFAULT 0,
            anonymous_label TEXT DEFAULT '',
            first_seen DATETIME DEFAULT (datetime('now', '+8 hours')),
            last_seen DATETIME DEFAULT (datetime('now', '+8 hours'))
        );
        CREATE TABLE IF NOT EXISTS contributions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL REFERENCES sessions(id),
            user_id TEXT NOT NULL,
            user_name TEXT NOT NULL,
            consume INTEGER DEFAULT 0,
            rank INTEGER DEFAULT 0,
            fans_club TEXT DEFAULT '',
            source TEXT DEFAULT 'websocket',
            qualified_1000 INTEGER DEFAULT 0,
            qualified_3000 INTEGER DEFAULT 0,
            qualified_10000 INTEGER DEFAULT 0,
            qualified_100000 INTEGER DEFAULT 0,
            recorded_at DATETIME DEFAULT (datetime('now', '+8 hours')),
            UNIQUE(session_id, user_id)
        );
        CREATE TABLE IF NOT EXISTS chat_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER REFERENCES sessions(id),
            user_id TEXT NOT NULL,
            user_name TEXT NOT NULL,
            content TEXT NOT NULL,
            grade TEXT DEFAULT '',
            fans_club TEXT DEFAULT '',
            created_at DATETIME DEFAULT (datetime('now', '+8 hours'))
        );
        CREATE TABLE IF NOT EXISTS upgrade_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER DEFAULT 0,
            user_id TEXT NOT NULL,
            user_name TEXT NOT NULL,
            upgrade_type TEXT NOT NULL,
            from_level INTEGER DEFAULT 0,
            to_level INTEGER DEFAULT 0,
            anchor_name TEXT DEFAULT '',
            created_at DATETIME DEFAULT (datetime('now', '+8 hours'))
        );
        CREATE TABLE IF NOT EXISTS gift_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER REFERENCES sessions(id),
            user_id TEXT NOT NULL,
            user_name TEXT NOT NULL,
            gift_name TEXT NOT NULL,
            gift_count INTEGER DEFAULT 1,
            diamond_total INTEGER DEFAULT 0,
            group_id TEXT DEFAULT '',
            created_at DATETIME DEFAULT (datetime('now', '+8 hours'))
        );
        CREATE TABLE IF NOT EXISTS daily_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            user_name TEXT NOT NULL,
            date TEXT NOT NULL,
            total_consume INTEGER DEFAULT 0,
            sessions_1000 INTEGER DEFAULT 0,
            sessions_3000 INTEGER DEFAULT 0,
            sessions_10000 INTEGER DEFAULT 0,
            sessions_100000 INTEGER DEFAULT 0,
            gift_count INTEGER DEFAULT 0,
            chat_count INTEGER DEFAULT 0,
            UNIQUE(user_id, date)
        );
        CREATE TABLE IF NOT EXISTS monthly_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            user_name TEXT NOT NULL,
            year_month TEXT NOT NULL,
            total_consume INTEGER DEFAULT 0,
            sessions_1000 INTEGER DEFAULT 0,
            sessions_3000 INTEGER DEFAULT 0,
            sessions_10000 INTEGER DEFAULT 0,
            sessions_100000 INTEGER DEFAULT 0,
            days_active INTEGER DEFAULT 0,
            max_rank INTEGER DEFAULT 0,
            UNIQUE(user_id, year_month)
        );
        CREATE TABLE IF NOT EXISTS streamer_config (
            live_id TEXT PRIMARY KEY,
            anchor_name TEXT DEFAULT '',
            enabled INTEGER DEFAULT 0,
            added_at DATETIME DEFAULT (datetime('now', '+8 hours'))
        );
        CREATE INDEX IF NOT EXISTS idx_contributions_session ON contributions(session_id);
        CREATE INDEX IF NOT EXISTS idx_contributions_user ON contributions(user_id);
        CREATE INDEX IF NOT EXISTS idx_chat_logs_user ON chat_logs(user_id);
        CREATE INDEX IF NOT EXISTS idx_monthly_stats ON monthly_stats(year_month, sessions_1000 DESC);
        CREATE INDEX IF NOT EXISTS idx_daily_stats ON daily_stats(date, sessions_1000 DESC);
        CREATE INDEX IF NOT EXISTS idx_contributions_qualified ON contributions(qualified_1000);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_gift_dedup_ts ON gift_logs(session_id, user_id, gift_name, diamond_total, gift_count, created_at);
        CREATE INDEX IF NOT EXISTS idx_upgrade_logs_session ON upgrade_logs(session_id);
        CREATE INDEX IF NOT EXISTS idx_upgrade_logs_type ON upgrade_logs(upgrade_type);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_upgrade_logs_dedup ON upgrade_logs(session_id, user_id, upgrade_type, from_level, to_level);
        CREATE TABLE IF NOT EXISTS gift_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            gift_name TEXT NOT NULL UNIQUE,
            gift_id INTEGER DEFAULT 0,
            diamond_count INTEGER NOT NULL DEFAULT 0,
            source TEXT NOT NULL DEFAULT 'auto',
            is_limited_skin INTEGER NOT NULL DEFAULT 0,
            base_gift_name TEXT NOT NULL DEFAULT '',
            notes TEXT NOT NULL DEFAULT '',
            created_at DATETIME DEFAULT (datetime('now', '+8 hours')),
            updated_at DATETIME DEFAULT (datetime('now', '+8 hours'))
        );
        CREATE TABLE IF NOT EXISTS price_change_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            gift_name TEXT NOT NULL,
            old_price INTEGER NOT NULL,
            new_price INTEGER NOT NULL,
            affected_rows INTEGER DEFAULT 0,
            affected_sessions INTEGER DEFAULT 0,
            notes TEXT DEFAULT '',
            changed_by TEXT DEFAULT 'web_ui',
            created_at DATETIME DEFAULT (datetime('now', '+8 hours'))
        );
    ''')
    # 兼容旧表：给 users 表补充 grade 字段
    try:
        conn.execute('ALTER TABLE users ADD COLUMN grade TEXT DEFAULT ""')
    except Exception:
        pass
    # 清理僵尸场次：结束标记为"直播中"但开始时间超过 12 小时前的场次
    conn.execute("""
        UPDATE sessions SET end_time = start_time, status = 'ended'
        WHERE status = 'live' AND start_time < datetime('now', '+8 hours', '-12 hours')
    """)
    # 数据库完整性检查
    try:
        integrity = conn.execute('PRAGMA integrity_check').fetchone()[0]
        if integrity != 'ok':
            logger.error(f"[DB] 完整性检查失败: {integrity}")
    except Exception:
        pass
    # 开启自动增量 VACUUM，防止文件无限膨胀
    try:
        conn.execute('PRAGMA auto_vacuum=INCREMENTAL')
        conn.execute('PRAGMA incremental_vacuum(0)')
    except Exception:
        pass
    conn.commit()
    logger.info(f"[DB] 已初始化: {DB_PATH}")
    return True


def init_gift_prices_table():
    """Populate gift_prices table from all available sources.

    Priority (higher = never overwritten by lower):
        1. _GIFT_PRICE_OVERRIDE  (manually verified, source='override')
        2. _GIFT_FALLBACK        (manually verified, source='override')
        3. gift_registry.json    (official registry, source='registry')
        4. Auto-detected from gift_logs (consensus price, source='auto')

    Auto-detected entries are refreshed on each startup.
    Authoritative entries (override/registry) are INSERT OR IGNORE only.
    """
    conn = _get_conn()

    # Source 1: _GIFT_PRICE_OVERRIDE
    for name, price in _GIFT_PRICE_OVERRIDE.items():
        conn.execute('''
            INSERT OR IGNORE INTO gift_prices (gift_name, diamond_count, source, is_limited_skin, notes)
            VALUES (?, ?, 'override', 1, 'Limited-edition skin price override')
        ''', (name, price))

    # Source 2: _GIFT_FALLBACK (gift_id-based)
    for gid, info in _GIFT_FALLBACK.items():
        conn.execute('''
            INSERT OR IGNORE INTO gift_prices (gift_name, gift_id, diamond_count, source)
            VALUES (?, ?, ?, 'override')
        ''', (info['name'], gid, info['diamond_count']))

    # Source 3: gift_registry.json (if exists)
    import json, os as _os
    _reg_path = _os.path.join(_os.path.dirname(_os.path.dirname(__file__)), 'data', 'gift_registry.json')
    if _os.path.exists(_reg_path):
        try:
            with open(_reg_path, 'r', encoding='utf-8') as _f:
                _registry = json.load(_f)
            for _name, _info in _registry.items():
                _gid = _info.get('id')
                _price = _info.get('diamond_count', 0)
                if _gid and _price:
                    conn.execute('''
                        INSERT OR IGNORE INTO gift_prices (gift_name, gift_id, diamond_count, source)
                        VALUES (?, ?, ?, 'registry')
                    ''', (_name, _gid, _price))
        except Exception:
            pass  # registry is optional

    # Source 4: Auto-detect from gift_logs (refresh on every startup)
    # First, collect consensus prices for each gift_name
    auto_rows = conn.execute('''
        SELECT gift_name, diamond_total / MAX(gift_count, 1) AS unit_price,
               COUNT(*) AS occurrences
        FROM gift_logs
        WHERE gift_count > 0
        GROUP BY gift_name, unit_price
        ORDER BY gift_name, occurrences DESC
    ''').fetchall()

    # Group by gift_name: pick most common price, flag conflicts
    from collections import defaultdict
    gift_prices_map = {}      # gift_name -> (unit_price, occurrences, has_conflict)
    gift_conflicts = defaultdict(list)  # gift_name -> [(price, count), ...]
    for row in auto_rows:
        name = row['gift_name']
        price = row['unit_price']
        cnt = row['occurrences']
        if name not in gift_prices_map:
            gift_prices_map[name] = (price, cnt, False)
        else:
            existing_price, existing_cnt, _ = gift_prices_map[name]
            if price != existing_price:
                gift_conflicts[name].append((price, cnt))
                if cnt > existing_cnt:
                    gift_prices_map[name] = (price, cnt, True)
                else:
                    gift_prices_map[name] = (existing_price, existing_cnt, True)

    # Upsert auto-detected prices (skip if authoritative source exists)
    for name, (price, cnt, has_conflict) in gift_prices_map.items():
        existing = conn.execute(
            'SELECT source FROM gift_prices WHERE gift_name = ?', (name,)
        ).fetchone()
        if existing:
            # Only update auto-detected entries; skip authoritative ones
            if existing['source'] == 'auto':
                notes = ''
                if has_conflict:
                    conflict_str = '; '.join(
                        f'{p} dia ({c}x)' for p, c in gift_conflicts[name]
                    )
                    notes = f'Conflict: other prices {conflict_str}'
                conn.execute('''
                    UPDATE gift_prices
                    SET diamond_count = ?, notes = ?, updated_at = datetime("now", "+8 hours")
                    WHERE gift_name = ? AND source = 'auto'
                ''', (price, notes, name))
        else:
            # New entry — insert
            notes = ''
            if has_conflict:
                conflict_str = '; '.join(
                    f'{p} dia ({c}x)' for p, c in gift_conflicts[name]
                )
                notes = f'Conflict: other prices {conflict_str}'
            conn.execute('''
                INSERT INTO gift_prices (gift_name, diamond_count, source, notes)
                VALUES (?, ?, 'auto', ?)
            ''', (name, price, notes))

    conn.commit()


def recalculate_gift_price(gift_name, new_price, old_price, notes=''):
    """Recalculate gift_logs.diamond_total for a price change and cascade to derived tables.

    This is called AFTER the gift_prices entry has been updated.
    Uses the existing single-writer queue pattern for safe writes.

    Args:
        gift_name: The gift name that changed.
        new_price: New diamond_count value.
        old_price: Previous diamond_count value.
        notes: Optional change notes.

    Returns:
        dict with affected_rows, affected_sessions, diamond_diff.
    """
    conn = _get_conn()

    # Step A: Find affected gift_logs rows
    affected = conn.execute('''
        SELECT id, gift_count, diamond_total
        FROM gift_logs
        WHERE gift_name = ? AND diamond_total != ? * gift_count
    ''', (gift_name, new_price)).fetchall()

    if not affected:
        return {'affected_rows': 0, 'affected_sessions': 0, 'diamond_diff': 0}

    affected_rows = len(affected)

    # Compute diamond difference
    old_total = sum(r['diamond_total'] for r in affected)
    new_total = sum(new_price * r['gift_count'] for r in affected)
    diamond_diff = new_total - old_total

    # Get affected session IDs
    session_ids = [r['session_id'] for r in conn.execute(
        'SELECT DISTINCT session_id FROM gift_logs WHERE gift_name = ? AND diamond_total != ? * gift_count',
        (gift_name, new_price)
    ).fetchall()]
    affected_sessions = len(session_ids)

    # Step B: Update gift_logs
    conn.execute(
        'UPDATE gift_logs SET diamond_total = ? * gift_count WHERE gift_name = ? AND diamond_total != ? * gift_count',
        (new_price, gift_name, new_price)
    )

    # Step C: Recalculate contributions.consume for affected sessions
    for sid in session_ids:
        conn.execute('''
            UPDATE contributions
            SET consume = (
                SELECT COALESCE(SUM(g.diamond_total), 0)
                FROM gift_logs g
                WHERE g.session_id = contributions.session_id
                  AND g.user_id = contributions.user_id
            )
            WHERE session_id = ?
        ''', (sid,))

    # Step D: Recalculate daily_stats for affected users
    affected_users = [r['user_id'] for r in conn.execute(
        'SELECT DISTINCT user_id FROM gift_logs WHERE gift_name = ?', (gift_name,)
    ).fetchall()]

    for uid in affected_users:
        conn.execute('''
            UPDATE daily_stats
            SET total_consume = (
                SELECT COALESCE(SUM(g.diamond_total), 0)
                FROM gift_logs g
                WHERE g.user_id = daily_stats.user_id
                  AND date(g.created_at) = daily_stats.date
            )
            WHERE user_id = ?
        ''', (uid,))

        conn.execute('''
            UPDATE monthly_stats
            SET total_consume = (
                SELECT COALESCE(SUM(g.diamond_total), 0)
                FROM gift_logs g
                WHERE g.user_id = monthly_stats.user_id
                  AND strftime('%Y-%m', g.created_at) = monthly_stats.year_month
            )
            WHERE user_id = ?
        ''', (uid,))

    # Step E: Log the change
    conn.execute('''
        INSERT INTO price_change_log (gift_name, old_price, new_price, affected_rows, affected_sessions, notes)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (gift_name, old_price, new_price, affected_rows, affected_sessions, notes or ''))

    conn.commit()

    logger.info(f"[礼物定价] 已重算 {gift_name}: {old_price}→{new_price}, "
                f"影响 {affected_rows} 行, {affected_sessions} 场次, 差额 {diamond_diff:+d} 钻石")

    return {
        'affected_rows': affected_rows,
        'affected_sessions': affected_sessions,
        'diamond_diff': diamond_diff,
    }


def get_price_change_history(limit=50):
    """Return recent price change log entries."""
    conn = _get_conn()
    rows = conn.execute('''
        SELECT * FROM price_change_log
        ORDER BY id DESC LIMIT ?
    ''', (limit,)).fetchall()
    return [dict(r) for r in rows]


def create_session(room_id, anchor_name=''):
    conn = _get_conn()
    # 结束同一房间下仍标记为"直播中"的旧场次，避免累积僵尸场次
    old = conn.execute('SELECT id FROM sessions WHERE room_id = ? AND status = "live"', (room_id,)).fetchall()
    for row in old:
        conn.execute('UPDATE sessions SET end_time = datetime("now", "+8 hours"), status = "ended" WHERE id = ?', (row['id'],))
        logger.info(f"[DB] 自动结束旧场次 #{row['id']} (新场次创建)")
    cur = conn.execute('INSERT INTO sessions (room_id, anchor_name) VALUES (?, ?)', (room_id, anchor_name))
    conn.commit()
    sid = cur.lastrowid
    logger.info(f"[DB] 新场次 #{sid}: {anchor_name} ({room_id})")
    return sid


def end_session(session_id):
    conn = _get_conn()
    conn.execute('UPDATE sessions SET end_time = datetime("now", "+8 hours"), status = "ended" WHERE id = ?', (session_id,))
    conn.commit()
    logger.info(f"[DB] 场次 #{session_id} 已结束")


def delete_session(session_id):
    """删除场次及其所有关联数据（礼物、弹幕、贡献记录）。

    Args:
        session_id: 场次 ID。

    Returns:
        dict: {'deleted': True, 'gifts': N, 'chats': N, 'contributions': N}
    """
    conn = _get_conn()
    # 统计各表删除数量
    gift_count = conn.execute('SELECT COUNT(*) FROM gift_logs WHERE session_id = ?', (session_id,)).fetchone()[0]
    chat_count = conn.execute('SELECT COUNT(*) FROM chat_logs WHERE session_id = ?', (session_id,)).fetchone()[0]
    contrib_count = conn.execute('SELECT COUNT(*) FROM contributions WHERE session_id = ?', (session_id,)).fetchone()[0]

    conn.execute('DELETE FROM gift_logs WHERE session_id = ?', (session_id,))
    conn.execute('DELETE FROM chat_logs WHERE session_id = ?', (session_id,))
    conn.execute('DELETE FROM contributions WHERE session_id = ?', (session_id,))
    conn.execute('DELETE FROM sessions WHERE id = ?', (session_id,))
    conn.commit()

    logger.info(f"[DB] 场次 #{session_id} 已删除（礼物:{gift_count} 弹幕:{chat_count} 贡献:{contrib_count}）")
    return {'deleted': True, 'gifts': gift_count, 'chats': chat_count, 'contributions': contrib_count}


def _detect_anonymous(user_name):
    """检测是否为抖音匿名用户（神秘人或 dou 开头的设备 ID 类名称）。

    匿名特征：
    - 以"神秘人"开头，后面不含其他中文字符（系统分配的匿名显示名）
    - 以"dou"开头（大小写不敏感），可能是设备 ID 或系统生成名

    返回 (is_anonymous, anonymous_label)。
    """
    if not user_name:
        return False, ''
    if user_name.startswith('神秘人'):
        # 神秘人后面是数字/字母/空白，不含其他 CJK 字符
        suffix = user_name[3:]
        if not re.search(r'[一-鿿]', suffix):
            return True, user_name
    # dou + 纯数字（如 dou9495919），系统生成的设备 ID 类匿名名
    if re.match(r'dou\d+$', user_name, re.IGNORECASE):
        return True, user_name
    # 损坏的用户名：以 2@ 开头或含有控制字符（protobuf 解析污染）
    if user_name.startswith('2@') or any(ord(c) < 32 for c in user_name):
        return True, user_name
    return False, ''


def _parse_fans_club_string(s):
    """解析粉丝团字符串为 {club_name: level} 字典。

    "[粉丝团:迅猛龙 Lv20] [粉丝团:孙恩盛 Lv5]" → {"迅猛龙": 20, "孙恩盛": 5}
    """
    if not s:
        return {}
    import re as _re
    clubs = {}
    for m in _re.finditer(r'\[粉丝团:([^\]]+?) Lv(\d+)\]', s):
        name, lv = m.group(1), int(m.group(2))
        clubs[name] = max(clubs.get(name, 0), lv)
    return clubs


def _merge_fans_club_strings(existing, new_val):
    """合并新旧粉丝团字符串，同名称保留最高等级。"""
    if not new_val:
        return existing or ''
    if not existing:
        return new_val
    merged = _parse_fans_club_string(existing)
    for name, lv in _parse_fans_club_string(new_val).items():
        merged[name] = max(merged.get(name, 0), lv)
    # 按等级降序排列输出
    sorted_names = sorted(merged.keys(), key=lambda n: merged[n], reverse=True)
    return ' '.join(f'[粉丝团:{n} Lv{merged[n]}]' for n in sorted_names)


def upsert_user(user_id, user_name, grade='', fans_club='', sec_uid='', avatar_url=''):
    """更新或插入用户信息（财富等级、粉丝团、sec_uid、头像URL等）。通过写者队列串行化。"""
    if not user_id:
        return
    try:
        _write_queue.put_nowait(('upsert', user_id, user_name, grade, fans_club, sec_uid, avatar_url))
    except queue.Full:
        logger.warning(f"[DB] upsert queue full: uid={user_id}")

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

    with _flush_lock:
        for r in rows:
            uid = r['user_id']
            nick = r['user_name']
            consume = r['consume']

            # 获取粉丝团和财富等级
            info = conn.execute(
                "SELECT fans_club, grade FROM chat_logs WHERE user_id = ? AND (fans_club != '' OR grade != '') ORDER BY id DESC LIMIT 1",
                (uid,)
            ).fetchone()
            fans_club = info['fans_club'] if info else ''
            grade = info['grade'] if info else ''
    
            # 同步 users 表
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
    
            # 每日/每月达标次数（仅在首次达标时 +1）
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
    
            # 每日/每月总消费：从 contributions 重新计算（始终为实际总和）
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


# ── 单写者线程队列（所有线程推送写入操作，写者单线程处理，消除多线程锁争抢）──
_write_queue = queue.Queue(maxsize=50000)
_WRITER_BATCH_SIZE = 50
_WRITER_FLUSH_INTERVAL = 0.5
_flush_lock = threading.Lock()  # 用于 flush_to_sqlite 等直接写操作


def _writer_loop():
    """后台写者线程：从队列消费写入操作，批量提交。"""
    conn = _get_conn()
    buf = []
    last_flush = time.time()
    while True:
        try:
            item = _write_queue.get(timeout=0.2)
            if item is None:
                _flush_write_batch(conn, buf) if buf else None
                conn.commit()
                break
            buf.append(item)
            now = time.time()
            if len(buf) >= _WRITER_BATCH_SIZE or now - last_flush > _WRITER_FLUSH_INTERVAL:
                _flush_write_batch(conn, buf)
                for _ in buf:
                    _write_queue.task_done()
                buf = []
                last_flush = now
        except queue.Empty:
            if buf and time.time() - last_flush > _WRITER_FLUSH_INTERVAL:
                _flush_write_batch(conn, buf)
                for _ in buf:
                    _write_queue.task_done()
                buf = []
                last_flush = time.time()
        except Exception as _we:
            logger.error(f"[DB] 写者线程异常: {_we}")


def _flush_write_batch(conn, batch):
    """执行一批写入操作。"""
    global _chat_written, _gift_written
    for item in batch:
        op = item[0]
        try:
            if op == 'chat':
                _, sid, uid, uname, content, grade, club = item
                r = conn.execute('INSERT OR IGNORE INTO chat_logs (session_id, user_id, user_name, content, grade, fans_club) VALUES (?, ?, ?, ?, ?, ?)',
                                 (sid, uid, uname, content, grade, club))
                _chat_written[sid] = _chat_written.get(sid, 0) + r.rowcount
            elif op == 'gift':
                if len(item) >= 10:
                    _, sid, uid, uname, gname, cnt, dia, grade, club, gid = item
                    r = conn.execute('INSERT OR IGNORE INTO gift_logs (session_id, user_id, user_name, gift_name, gift_count, diamond_total, grade, fans_club, group_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                                     (sid, uid, uname, gname, cnt, dia, grade, club, gid))
                else:
                    _, sid, uid, uname, gname, cnt, dia, grade, club = item
                    r = conn.execute('INSERT OR IGNORE INTO gift_logs (session_id, user_id, user_name, gift_name, gift_count, diamond_total, grade, fans_club) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                                     (sid, uid, uname, gname, cnt, dia, grade, club))
                _gift_written[sid] = _gift_written.get(sid, 0) + r.rowcount
            elif op == 'upsert':
                _, uid, uname, grade, club, sec, av = item
                is_anon, anon_label = _detect_anonymous(uname)
                # 合并粉丝团（保留最高等级）
                if club:
                    existing = conn.execute('SELECT fans_club FROM users WHERE user_id = ?', (uid,)).fetchone()
                    if existing and existing['fans_club']:
                        club = _merge_fans_club_strings(existing['fans_club'], club)
                conn.execute('''
                    INSERT INTO users (user_id, user_name, grade, fans_club, sec_uid, avatar_url, is_anonymous, anonymous_label, last_seen)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime("now", "+8 hours"))
                    ON CONFLICT(user_id) DO UPDATE SET
                        user_name = CASE WHEN ? != '' THEN ? ELSE user_name END,
                        grade = CASE WHEN ? != '' THEN ? ELSE grade END,
                        fans_club = CASE WHEN ? != '' THEN ? ELSE fans_club END,
                        sec_uid = CASE WHEN ? != '' THEN ? ELSE sec_uid END,
                        avatar_url = CASE WHEN ? != '' THEN ? ELSE avatar_url END,
                        is_anonymous = CASE WHEN ? = 1 THEN 1 ELSE is_anonymous END,
                        anonymous_label = CASE WHEN ? != '' THEN ? ELSE anonymous_label END,
                        last_seen = datetime("now", "+8 hours")
                ''', (uid, uname, grade, club, sec, av, is_anon, anon_label,
                      uname, uname, grade, grade, club, club, sec, sec, av, av,
                      is_anon, anon_label, anon_label))
            elif op == 'backfill_uid':
                _, sid, uname, ruid = item
                r = conn.execute(
                    "UPDATE gift_logs SET user_id = ? WHERE session_id = ? AND (user_id = '' OR user_id IS NULL) AND user_name = ?",
                    (ruid, sid, uname)
                )
                if r.rowcount:
                    logger.info(f"[DB] 补填 {r.rowcount} 条订阅记录: {uname} -> {ruid}")
        except Exception as e:
            logger.warning(f"[DB] writer batch op failed: {op} {e}")
    conn.commit()


# 启动写者线程（模块导入时自动启动）
_writer_thread = threading.Thread(target=_writer_loop, daemon=True, name='db-writer')
_writer_thread.start()


def flush_writes():
    """等待写者线程处理完当前队列（最多等 2 秒）。"""
    try:
        _write_queue.join()
    except Exception:
        pass


def get_flow_counters(session_id=None):
    """返回消息流计数器快照（可按 session_id 过滤）。
    Returns:
        dict: {chat_enqueued, gift_enqueued, chat_written, gift_written, combo_progress}
    """
    def _sid(d):
        """按 session_id 取值，无 session_id 时返回总和。"""
        if session_id is None:
            return sum(d.values()) if d else 0
        return d.get(session_id, 0)
    return {
        'chat_enqueued': _sid(_chat_enqueued),
        'gift_enqueued': _sid(_gift_enqueued),
        'chat_written': _sid(_chat_written),
        'gift_written': _sid(_gift_written),
        'combo_progress': _combo_progress_count,
    }


def record_chat(session_id, user_id, user_name, content, grade='', fans_club=''):
    global _chat_enqueued
    _chat_enqueued[session_id] = _chat_enqueued.get(session_id, 0) + 1
    try:
        _write_queue.put_nowait(('chat', session_id, user_id, user_name, content, grade, fans_club))
    except queue.Full:
        logger.warning(f"[DB] 写入队列已满，丢弃聊天: uid={user_id}")


def request_backfill_uid(session_id, user_name, real_uid):
    """通过写者队列补填订阅记录的 user_id，避免跨线程写冲突。"""
    if not session_id or not user_name or not real_uid:
        return
    try:
        _write_queue.put_nowait(('backfill_uid', session_id, user_name, real_uid))
    except queue.Full:
        logger.warning(f"[DB] 写入队列已满，丢弃 backfill: {user_name} -> {real_uid}")


_gift_dedup_cache = {}  # (user_id, gift_name) → timestamp
_GIFT_DEDUP_WINDOW = 10.0  # same user+gift within 10s = dedup (catches timer race + single dupes)


def _prune_gift_dedup_cache():
    now = time.time()
    stale = [k for k, ts in list(_gift_dedup_cache.items()) if now - ts > _GIFT_DEDUP_WINDOW * 2]
    for k in stale:
        _gift_dedup_cache.pop(k, None)


def record_gift(session_id, user_id, user_name, gift_name, gift_count, diamond_total, grade='', fans_club='', group_id=''):
    """记录礼物（内存时间窗 + 写者队列双重去重）。

    去重 key 使用 (user_id, gift_name, group_id) 三元组。
    group_id 为空时回退到 (user_id, gift_name) 二元组。
    窗口 10s，同一 group_id 的 timer 竞态会被捕获。
    """
    if user_id and gift_name:
        dk = (user_id, gift_name, group_id) if group_id else (user_id, gift_name)
        now = time.time()
        last_ts = _gift_dedup_cache.get(dk)
        if last_ts and now - last_ts < _GIFT_DEDUP_WINDOW:
            logger.debug(f"[DB] record_gift mem-dedup: sid={session_id} uid={user_id} gift={gift_name} x{gift_count} dia={diamond_total}")
            return
        _gift_dedup_cache[dk] = now
        if len(_gift_dedup_cache) > 5000:
            _prune_gift_dedup_cache()
    try:
        global _gift_enqueued; _gift_enqueued[session_id] = _gift_enqueued.get(session_id, 0) + 1
        _write_queue.put_nowait(('gift', session_id, user_id, user_name, gift_name, gift_count, diamond_total, grade, fans_club, group_id))
    except queue.Full:
        logger.warning(f"[DB] 写入队列已满，丢弃礼物: {gift_name} uid={user_id}")
_VALID_TIERS = {1000, 3000, 10000, 100000}


def record_upgrade(session_id, user_id, user_name, upgrade_type, from_level, to_level, anchor_name=''):
    """记录用户升级事件（财富等级/粉丝团），使用 INSERT OR IGNORE 防重复。
    仅记录 from_level > 0 的真实升级（首次检测到的高等级不计为升级事件）。"""
    if not user_id or not upgrade_type or to_level <= 0 or from_level <= 0:
        return
    try:
        conn = _get_conn()
        conn.execute(
            'INSERT OR IGNORE INTO upgrade_logs (session_id, user_id, user_name, upgrade_type, from_level, to_level, anchor_name) VALUES (?, ?, ?, ?, ?, ?, ?)',
            (session_id or 0, user_id, user_name, upgrade_type, from_level, to_level, anchor_name)
        )
        conn.commit()
    except Exception as e:
        logger.debug(f"[DB] record_upgrade failed: {e}")


def query_upgrades(upgrade_type='', session_id=None, min_level=0, anchor_name='', page=1, size=100):
    """查询升级记录。"""
    conn = _get_conn()
    where = []
    params = []
    if upgrade_type:
        where.append('u.upgrade_type = ?')
        params.append(upgrade_type)
    if session_id is not None:
        where.append('u.session_id = ?')
        params.append(session_id)
    if min_level > 0:
        where.append('u.to_level >= ?')
        params.append(min_level)
    if anchor_name:
        where.append('u.anchor_name = ?')
        params.append(anchor_name)

    w = ' AND '.join(where) if where else '1=1'
    total = conn.execute(f'SELECT COUNT(*) FROM upgrade_logs u WHERE {w}', params).fetchone()[0]
    offset = (page - 1) * size
    rows = conn.execute(f'''
        SELECT u.*, COALESCE(usr.avatar_url, '') as avatar_url
        FROM upgrade_logs u
        LEFT JOIN users usr ON usr.user_id = u.user_id
        WHERE {w}
        ORDER BY u.id DESC LIMIT ? OFFSET ?
    ''', params + [size, offset]).fetchall()
    return {'upgrades': [dict(r) for r in rows], 'total': total, 'page': page}


def _resolve_tier(threshold):
    """将 threshold 映射到已知 tier，若为 0 或非标准值则返回 None。"""
    if threshold in _VALID_TIERS:
        return threshold
    return None


def query_leaderboard(threshold=1000, period='session', page=1, size=100, session_id=None, year_month='', min_consume=0, anchor_name='', room_id=''):
    conn = _get_conn()
    offset = (page - 1) * size
    tier = _resolve_tier(threshold)

    # 主播筛选（session 级别已锁定单场，无需重复过滤）
    # 优先按 room_id 过滤（稳定数字 ID，避免 anchor_name 字符串编码问题）
    anchor_filter = ''
    anchor_params = ()
    if room_id and period != 'session':
        anchor_filter = 'AND s.room_id = ?'
        anchor_params = (room_id,)
    elif anchor_name and period != 'session':
        anchor_filter = 'AND TRIM(s.anchor_name) = ?'
        anchor_params = (anchor_name.strip(),)

    if period == 'session' and session_id:
        if tier is not None:
            col = f'qualified_{tier}'
            where_extra = f'AND c.{col} = 1'
            total = conn.execute(f'SELECT COUNT(*) FROM contributions WHERE session_id = ? AND {col} = 1', (session_id,)).fetchone()[0]
        else:
            where_extra = ''
            if threshold > 0:
                where_extra = f'AND c.consume >= {int(threshold)}'
            total = conn.execute(f'SELECT COUNT(*) FROM contributions WHERE session_id = ? AND consume > 0', (session_id,)).fetchone()[0]
        rows = conn.execute(f'''
            SELECT c.user_id, COALESCE(NULLIF(u.user_name, ''), c.user_name) AS user_name, c.consume,
                   COALESCE(NULLIF(u.fans_club, ''), CASE WHEN s.anchor_name != '' THEN '[粉丝团:' || s.anchor_name || ']' ELSE '' END) AS fans_club,
                   COALESCE(
                       u.grade,
                       (SELECT grade FROM chat_logs WHERE user_id = c.user_id AND grade != '' ORDER BY id DESC LIMIT 1),
                       ''
                   ) AS grade,
                   u.sec_uid, u.avatar_url, u.notes, u.tags,
                   c.qualified_1000, c.qualified_3000, c.qualified_10000, c.qualified_100000,
                   1 AS sessions_count
            FROM contributions c
            LEFT JOIN users u ON u.user_id = c.user_id
            JOIN sessions s ON s.id = c.session_id
            WHERE c.session_id = ? AND c.consume > 0 {where_extra}
            ORDER BY c.consume DESC LIMIT ? OFFSET ?
        ''', (session_id, size, offset)).fetchall()
        users_list = []
        for i, r in enumerate(rows):
            d = dict(r)
            d['rank'] = offset + i + 1
            users_list.append(d)
        return {'users': users_list, 'total': total, 'page': page}
    elif period == 'today':
        today = datetime.now().strftime('%Y-%m-%d')
        having = 'HAVING SUM(c.consume) >= ?' if min_consume > 0 else ''
        sessions_col_today = f'SUM(CASE WHEN c.consume >= {int(min_consume)} THEN 1 ELSE 0 END)' if min_consume > 0 else 'COUNT(DISTINCT c.session_id)'
        params_today = [today]
        total_params = (today,)
        if anchor_params:
            params_today.extend(anchor_params)
            total_params = total_params + anchor_params
        if min_consume > 0:
            params_today.append(min_consume)
            total_params = total_params + (min_consume,)
        params_today.extend([size, offset])
        rows = conn.execute(f'''
            SELECT c.user_id, COALESCE(NULLIF(u.user_name, ''), c.user_name) AS user_name, SUM(c.consume) AS consume,
                   COALESCE(u.fans_club, '') AS fans_club,
                   COALESCE(u.grade, '') AS grade,
                   u.sec_uid, u.avatar_url, u.notes, u.tags,
                   {sessions_col_today} AS sessions_count
            FROM contributions c
            JOIN sessions s ON c.session_id = s.id AND date(s.start_time) = ?
            LEFT JOIN users u ON u.user_id = c.user_id
            WHERE c.consume > 0 {anchor_filter}
            GROUP BY c.user_id {having}
            ORDER BY consume DESC LIMIT ? OFFSET ?
        ''', params_today).fetchall()
        total = conn.execute(f'''
            SELECT COUNT(*) FROM (
                SELECT c.user_id FROM contributions c
                JOIN sessions s ON c.session_id = s.id AND date(s.start_time) = ?
                WHERE c.consume > 0 {anchor_filter}
                GROUP BY c.user_id {having}
            )
        ''', total_params).fetchone()[0]
    elif period == 'month':
        month = year_month or datetime.now().strftime('%Y-%m')
        having = 'HAVING SUM(c.consume) >= ?' if min_consume > 0 else ''
        sessions_col_month = f'SUM(CASE WHEN c.consume >= {int(min_consume)} THEN 1 ELSE 0 END)' if min_consume > 0 else 'COUNT(DISTINCT c.session_id)'
        params = [month]
        total_params = (month,)
        if anchor_params:
            params.extend(anchor_params)
            total_params = total_params + anchor_params
        if min_consume > 0:
            params.append(min_consume)
            total_params = total_params + (min_consume,)
        params.extend([size, offset])
        rows = conn.execute(f'''
            SELECT c.user_id, COALESCE(NULLIF(u.user_name, ''), c.user_name) AS user_name, SUM(c.consume) AS consume,
                   COALESCE(u.fans_club, '') AS fans_club,
                   COALESCE(u.grade, '') AS grade,
                   u.sec_uid, u.avatar_url, u.notes, u.tags,
                   {sessions_col_month} AS sessions_count
            FROM contributions c
            JOIN sessions s ON c.session_id = s.id AND strftime('%Y-%m', s.start_time) = ?
            LEFT JOIN users u ON u.user_id = c.user_id
            WHERE c.consume > 0 {anchor_filter}
            GROUP BY c.user_id {having}
            ORDER BY consume DESC LIMIT ? OFFSET ?
        ''', params).fetchall()
        total = conn.execute(f'''
            SELECT COUNT(*) FROM (
                SELECT c.user_id FROM contributions c
                JOIN sessions s ON c.session_id = s.id AND strftime('%Y-%m', s.start_time) = ?
                WHERE c.consume > 0 {anchor_filter}
                GROUP BY c.user_id {having}
            )
        ''', total_params).fetchone()[0]
    elif period == '30d':
        # 滚动 30 天：从 contributions + sessions 按时间窗口聚合
        # 上榜次数：按最低消费计算符合条件的场次数
        if min_consume > 0:
            sessions_col = f'SUM(CASE WHEN c.consume >= {int(min_consume)} THEN 1 ELSE 0 END)'
        else:
            sessions_col = 'COUNT(DISTINCT c.session_id)'

        where_extra_30 = ''
        total_extra_30 = ''
        params_30 = []
        total_params_30 = []
        if anchor_params:
            params_30.extend(anchor_params)
            total_params_30.extend(anchor_params)
        if min_consume > 0:
            where_extra_30 = 'AND SUM(c.consume) >= ?'
            total_extra_30 = 'HAVING SUM(c.consume) >= ?'
            params_30.append(min_consume)
            total_params_30.append(min_consume)
        params_30.extend([size, offset])
        rows = conn.execute(f'''
            SELECT c.user_id, COALESCE(NULLIF(u.user_name, ''), c.user_name) AS user_name, SUM(c.consume) AS consume,
                   COALESCE(
                       NULLIF(u.fans_club, ''),
                       (SELECT fans_club FROM chat_logs WHERE user_id = c.user_id AND fans_club != '' ORDER BY id DESC LIMIT 1),
                       ''
                   ) AS fans_club,
                   COALESCE(
                       u.grade,
                       (SELECT grade FROM chat_logs WHERE user_id = c.user_id AND grade != '' ORDER BY id DESC LIMIT 1),
                       ''
                   ) AS grade,
                   u.sec_uid, u.avatar_url, u.notes, u.tags,
                   {sessions_col} AS sessions_count
            FROM contributions c
            JOIN sessions s ON c.session_id = s.id
            LEFT JOIN users u ON u.user_id = c.user_id
            WHERE s.start_time >= datetime('now', '-30 days')
              AND c.consume > 0 {anchor_filter}
            GROUP BY c.user_id
            HAVING SUM(c.consume) > 0 {where_extra_30}
            ORDER BY consume DESC LIMIT ? OFFSET ?
        ''', params_30).fetchall()
        total = conn.execute(f'''
            SELECT COUNT(*) FROM (
                SELECT c.user_id, SUM(c.consume) AS total_consume
                FROM contributions c
                JOIN sessions s ON c.session_id = s.id
                WHERE s.start_time >= datetime('now', '-30 days') AND c.consume > 0 {anchor_filter}
                GROUP BY c.user_id
                {total_extra_30}
            )
        ''', total_params_30).fetchone()[0]
    else:
        # 全部 / 指定月份：从 contributions 聚合
        having = 'HAVING SUM(c.consume) >= ?' if min_consume > 0 else ''
        month_filter = year_month if year_month else ''
        sessions_col_all = f'SUM(CASE WHEN c.consume >= {int(min_consume)} THEN 1 ELSE 0 END)' if min_consume > 0 else 'COUNT(DISTINCT c.session_id)'

        if month_filter:
            params_all = [month_filter]
            if anchor_params:
                params_all.extend(anchor_params)
            if min_consume > 0:
                params_all.append(min_consume)
            params_all.extend([size, offset])
            total_params_list = [month_filter]
            if anchor_params:
                total_params_list.extend(anchor_params)
            if min_consume > 0:
                total_params_list.append(min_consume)
            total_params = tuple(total_params_list)
            rows = conn.execute(f'''
                SELECT c.user_id, COALESCE(NULLIF(u.user_name, ''), c.user_name) AS user_name, SUM(c.consume) AS consume,
                       COALESCE(u.fans_club, '') AS fans_club,
                       COALESCE(u.grade, '') AS grade,
                       u.sec_uid, u.avatar_url, u.notes, u.tags,
                       {sessions_col_all} AS sessions_count
                FROM contributions c
                JOIN sessions s ON c.session_id = s.id AND strftime('%Y-%m', s.start_time) = ?
                LEFT JOIN users u ON u.user_id = c.user_id
                WHERE c.consume > 0 {anchor_filter}
                GROUP BY c.user_id {having}
                ORDER BY consume DESC LIMIT ? OFFSET ?
            ''', params_all).fetchall()
            total = conn.execute(f'''
                SELECT COUNT(*) FROM (
                    SELECT c.user_id FROM contributions c
                    JOIN sessions s ON c.session_id = s.id AND strftime('%Y-%m', s.start_time) = ?
                    WHERE c.consume > 0 {anchor_filter}
                    GROUP BY c.user_id {having}
                )
            ''', total_params).fetchone()[0]
        else:
            params_all = []
            total_params_list = []
            if anchor_params:
                params_all.extend(anchor_params)
                total_params_list.extend(anchor_params)
            if min_consume > 0:
                params_all.append(min_consume)
                total_params_list.append(min_consume)
            params_all.extend([size, offset])
            total_params = tuple(total_params_list)
            sessions_join_all = 'JOIN sessions s ON c.session_id = s.id' if anchor_params else ''
            rows = conn.execute(f'''
                SELECT c.user_id, COALESCE(NULLIF(u.user_name, ''), c.user_name) AS user_name, SUM(c.consume) AS consume,
                       COALESCE(u.fans_club, '') AS fans_club,
                       COALESCE(u.grade, '') AS grade,
                       u.sec_uid, u.avatar_url, u.notes, u.tags,
                       {sessions_col_all} AS sessions_count
                FROM contributions c
                {sessions_join_all}
                LEFT JOIN users u ON u.user_id = c.user_id
                WHERE c.consume > 0 {anchor_filter}
                GROUP BY c.user_id {having}
                ORDER BY consume DESC LIMIT ? OFFSET ?
            ''', params_all).fetchall()
            total = conn.execute(f'''
                SELECT COUNT(*) FROM (
                    SELECT c.user_id FROM contributions c
                    {sessions_join_all}
                    WHERE c.consume > 0 {anchor_filter}
                    GROUP BY c.user_id {having}
                )
            ''', total_params).fetchone()[0]

    users_list = []
    for i, r in enumerate(rows):
        d = dict(r)
        d['rank'] = offset + i + 1
        d['sessions_count'] = d.pop('sessions_count', 0)
        users_list.append(d)
    return {'users': users_list, 'total': total, 'page': page}


def query_user(user_id):
    conn = _get_conn()
    user = conn.execute('SELECT * FROM users WHERE user_id = ?', (user_id,)).fetchone()
    if not user:
        user = conn.execute('SELECT user_id, user_name, fans_club, "" as grade FROM contributions WHERE user_id = ? LIMIT 1', (user_id,)).fetchone()
    if not user:
        user = conn.execute('SELECT DISTINCT user_id, user_name, grade, fans_club FROM chat_logs WHERE user_id = ? LIMIT 1', (user_id,)).fetchone()
    if not user:
        return None

    monthly = conn.execute('''
        SELECT year_month, total_consume, sessions_1000, sessions_3000, sessions_10000, sessions_100000, days_active
        FROM monthly_stats WHERE user_id = ? ORDER BY year_month DESC
    ''', (user_id,)).fetchall()

    total_consume = conn.execute('SELECT SUM(consume) FROM contributions WHERE user_id = ?', (user_id,)).fetchone()[0] or 0
    sessions_all = conn.execute('SELECT COUNT(*) FROM contributions WHERE user_id = ? AND qualified_1000 = 1', (user_id,)).fetchone()[0]

    # 从最新弹幕中获取财富等级
    latest_chat = conn.execute(
        'SELECT grade, fans_club FROM chat_logs WHERE user_id = ? AND (grade != "" OR fans_club != "") ORDER BY id DESC LIMIT 1',
        (user_id,)
    ).fetchone()

    result = dict(user)
    if latest_chat:
        result['grade'] = latest_chat['grade'] or result.get('grade', '')
        if latest_chat['fans_club']:
            result['fans_club'] = latest_chat['fans_club']
    elif not result.get('grade'):
        result['grade'] = ''
    result['total_consume'] = total_consume
    result['total_sessions_1000'] = sessions_all
    result['monthly'] = [dict(r) for r in monthly]
    return result


def query_user_detail(user_id):
    """查询用户深度记录：按场次分组，含每场音浪/礼物/发言/时间线。"""
    conn = _get_conn()
    user = conn.execute('SELECT * FROM users WHERE user_id = ?', (user_id,)).fetchone()
    if not user:
        user = conn.execute('SELECT user_id, user_name, fans_club, "" as grade FROM contributions WHERE user_id = ? LIMIT 1', (user_id,)).fetchone()
    if not user:
        user = conn.execute('SELECT DISTINCT user_id, user_name, grade, fans_club FROM chat_logs WHERE user_id = ? LIMIT 1', (user_id,)).fetchone()
    if not user:
        return None

    total_consume = conn.execute('SELECT SUM(consume) FROM contributions WHERE user_id = ?', (user_id,)).fetchone()[0] or 0
    total_gifts = conn.execute('SELECT COUNT(*) FROM gift_logs WHERE user_id = ?', (user_id,)).fetchone()[0]
    total_chats = conn.execute('SELECT COUNT(*) FROM chat_logs WHERE user_id = ?', (user_id,)).fetchone()[0]

    # 从最新弹幕中获取财富等级和粉丝团信息
    latest_chat = conn.execute(
        'SELECT grade, fans_club FROM chat_logs WHERE user_id = ? AND (grade != "" OR fans_club != "") ORDER BY id DESC LIMIT 1',
        (user_id,)
    ).fetchone()

    rows = conn.execute("""
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
    """, (user_id, user_id, user_id, user_id, user_id)).fetchall()

    result = dict(user)
    result['total_consume'] = total_consume
    result['total_gifts'] = total_gifts
    result['total_chats'] = total_chats
    result['sessions'] = []

    # 补充财富等级和粉丝团（优先取 users 表，始终为最新合并值）
    if latest_chat and latest_chat['grade']:
        result['grade'] = latest_chat['grade']
    elif not result.get('grade'):
        result['grade'] = ''
    # fans_club 以 users 表为准（已合并各消息类型并保留最高等级），不再用 chat_logs 覆盖

    for s in rows:
        sd = dict(s)
        # 查询该场次的最后财富等级和粉丝团
        sess_info = conn.execute(
            'SELECT grade, fans_club FROM chat_logs WHERE session_id = ? AND user_id = ? AND (grade != "" OR fans_club != "") ORDER BY id DESC LIMIT 1',
            (s['id'], user_id)
        ).fetchone()
        if sess_info and sess_info['grade']:
            sd['grade'] = sess_info['grade']
        if sess_info and sess_info['fans_club']:
            sd['fans_club'] = sess_info['fans_club']
        # 如果 chat_logs 没有，从 users 表获取
        if not sd.get('grade') or not sd.get('fans_club'):
            u_info = conn.execute('SELECT grade, fans_club FROM users WHERE user_id = ?', (user_id,)).fetchone()
            if u_info:
                if not sd.get('grade') and u_info['grade']:
                    sd['grade'] = u_info['grade']
                if not sd.get('fans_club') and u_info['fans_club']:
                    sd['fans_club'] = u_info['fans_club']
        tl = []
        for row in conn.execute("SELECT created_at as time, 'chat' as type, content, '' as amount FROM chat_logs WHERE session_id = ? AND user_id = ? ORDER BY created_at DESC LIMIT 500", (s['id'], user_id)).fetchall():
            tl.append(dict(row))
        for row in conn.execute("SELECT created_at as time, 'gift' as type, gift_name || ' x ' || gift_count as content, diamond_total as amount FROM gift_logs WHERE session_id = ? AND user_id = ? ORDER BY created_at DESC LIMIT 500", (s['id'], user_id)).fetchall():
            tl.append(dict(row))
        tl.sort(key=lambda x: str(x.get('time', '')), reverse=True)
        sd['timeline'] = tl[:500]
        result['sessions'].append(sd)

    return result


def query_user_timeline(user_id, type_filter='all', keyword='', page=1, size=50):
    conn = _get_conn()
    offset = (page - 1) * size
    results = []

    if type_filter in ('all', 'chat'):
        sql = '''SELECT c.created_at as time, "chat" as type, c.content, "" as amount, c.grade,
                        COALESCE(s.anchor_name, '') as anchor_name
                 FROM chat_logs c
                 LEFT JOIN sessions s ON s.id = c.session_id
                 WHERE c.user_id = ?'''
        params = [user_id]
        if keyword:
            sql += ' AND c.content LIKE ?'
            params.append(f'%{keyword}%')
        sql += ' ORDER BY c.created_at DESC LIMIT ? OFFSET ?'
        for row in conn.execute(sql, params + [size, offset]):
            results.append(dict(row))

    if type_filter in ('all', 'gift'):
        sql = '''SELECT g.created_at as time, "gift" as type, g.gift_name || " x " || g.gift_count as content,
                        g.diamond_total as amount,
                        COALESCE(s.anchor_name, '') as anchor_name
                 FROM gift_logs g
                 LEFT JOIN sessions s ON s.id = g.session_id
                 WHERE g.user_id = ?'''
        params = [user_id]
        if keyword:
            sql += ' AND (g.gift_name LIKE ?)'
            params.append(f'%{keyword}%')
        sql += ' ORDER BY g.created_at DESC LIMIT ? OFFSET ?'
        for row in conn.execute(sql, params + [size, offset]):
            results.append(dict(row))

    results.sort(key=lambda x: str(x.get('time', '')), reverse=True)
    results = results[:size]

    total = conn.execute('SELECT COUNT(*) FROM chat_logs WHERE user_id = ?', (user_id,)).fetchone()[0] + \
            conn.execute('SELECT COUNT(*) FROM gift_logs WHERE user_id = ?', (user_id,)).fetchone()[0]
    return {'timeline': results, 'total': total, 'page': page}


def query_chat(user_id='', keyword='', page=1, size=50):
    conn = _get_conn()
    offset = (page - 1) * size
    where = []
    params = []
    if user_id:
        where.append('c.user_id = ?')
        params.append(user_id)
    if keyword:
        where.append('c.content LIKE ?')
        params.append(f'%{keyword}%')
    w = ' AND '.join(where) if where else '1=1'
    total = conn.execute(f'SELECT COUNT(*) FROM chat_logs c WHERE {w}', params).fetchone()[0]
    rows = conn.execute(f'''SELECT c.created_at as time, c.user_id, c.user_name, c.content, c.grade, c.fans_club,
                                   COALESCE(s.anchor_name, '') as anchor_name
                            FROM chat_logs c
                            LEFT JOIN sessions s ON s.id = c.session_id
                            WHERE {w}
                            ORDER BY c.created_at DESC LIMIT ? OFFSET ?''',
                        params + [size, offset]).fetchall()
    return {'chats': [dict(r) for r in rows], 'total': total, 'page': page}


def query_anonymous(page=1, size=50, search=''):
    conn = _get_conn()
    offset = (page - 1) * size
    where = "COALESCE(u.anonymous_label, '') != '' AND COALESCE(u.anonymous_label, '') != 'fake'"
    params = []
    if search:
        where += ' AND (u.user_name LIKE ? OR u.user_id LIKE ?)'
        params.extend([f'%{search}%', f'%{search}%'])
    total = conn.execute(f'SELECT COUNT(*) FROM users u WHERE {where}', params).fetchone()[0]
    rows = conn.execute(f'''
        SELECT u.user_id AS real_user_id, u.user_name,
               u.anonymous_label, u.grade, u.fans_club,
               COALESCE(SUM(c.consume), 0) AS consume,
               COUNT(c.id) AS sessions_count, MAX(c.recorded_at) AS last_seen
        FROM users u LEFT JOIN contributions c ON c.user_id = u.user_id
        WHERE {where}
        GROUP BY u.user_id ORDER BY consume DESC LIMIT ? OFFSET ?
    ''', params + [size, offset]).fetchall()
    return {'users': [dict(r) for r in rows], 'total': total, 'page': page}


def query_million(year_month='', page=1, size=100):
    conn = _get_conn()
    if not year_month:
        year_month = datetime.now().strftime('%Y-%m')
    offset = (page - 1) * size
    rows = conn.execute('''
        SELECT m.user_id, m.user_name, m.total_consume, m.days_active,
               m.sessions_1000, m.sessions_3000, m.sessions_10000, m.sessions_100000,
               COALESCE(u.grade, '') as grade,
               COALESCE(u.fans_club, '') as fans_club
        FROM monthly_stats m
        LEFT JOIN users u ON u.user_id = m.user_id
        WHERE m.year_month = ? AND m.total_consume >= 1000000
        ORDER BY m.total_consume DESC LIMIT ? OFFSET ?
    ''', (year_month, size, offset)).fetchall()
    total = conn.execute('SELECT COUNT(*) FROM monthly_stats WHERE year_month = ? AND total_consume >= 1000000', (year_month,)).fetchone()[0]
    users = []
    for i, r in enumerate(rows):
        d = dict(r)
        d['rank'] = offset + i + 1
        users.append(d)
    return {'users': users, 'total': total, 'page': page}


def query_sessions(limit=20, anchor=''):
    conn = _get_conn()
    if anchor:
        rows = conn.execute('''
            SELECT s.*,
                (SELECT COUNT(*) FROM contributions WHERE session_id = s.id AND qualified_1000 = 1) as user_count,
                (SELECT COALESCE(SUM(diamond_total), 0) FROM gift_logs WHERE session_id = s.id) as total_diamonds,
                (SELECT COUNT(*) FROM gift_logs WHERE session_id = s.id) as total_gifts,
                (SELECT COUNT(*) FROM chat_logs WHERE session_id = s.id) as total_chats
            FROM sessions s WHERE s.anchor_name LIKE ? ORDER BY id DESC LIMIT ?
        ''', (f'%{anchor}%', limit)).fetchall()
    else:
        rows = conn.execute('''
            SELECT s.*,
                (SELECT COUNT(*) FROM contributions WHERE session_id = s.id AND qualified_1000 = 1) as user_count,
                (SELECT COALESCE(SUM(diamond_total), 0) FROM gift_logs WHERE session_id = s.id) as total_diamonds,
                (SELECT COUNT(*) FROM gift_logs WHERE session_id = s.id) as total_gifts,
                (SELECT COUNT(*) FROM chat_logs WHERE session_id = s.id) as total_chats
            FROM sessions s ORDER BY id DESC LIMIT ?
        ''', (limit,)).fetchall()
    return [dict(r) for r in rows]


def query_session_detail(session_id, top_n=50):
    """查询单场次的详细信息：基础信息 + 贡献用户分层 + Top贡献用户 + 礼物/弹幕统计。"""
    conn = _get_conn()
    session = conn.execute('SELECT * FROM sessions WHERE id = ?', (session_id,)).fetchone()
    if not session:
        return None
    result = dict(session)

    # 礼物统计
    gift_stats = conn.execute('''
        SELECT COUNT(*) as total_gifts, COALESCE(SUM(diamond_total), 0) as total_diamonds,
               COUNT(DISTINCT user_id) as gift_users
        FROM gift_logs WHERE session_id = ?
    ''', (session_id,)).fetchone()
    result['gift_stats'] = dict(gift_stats) if gift_stats else {}

    # 弹幕统计
    chat_stats = conn.execute('''
        SELECT COUNT(*) as total_chats, COUNT(DISTINCT user_id) as chat_users
        FROM chat_logs WHERE session_id = ?
    ''', (session_id,)).fetchone()
    result['chat_stats'] = dict(chat_stats) if chat_stats else {}

    # 达标用户层次统计
    tier_counts = conn.execute('''
        SELECT
            COUNT(*) as total_contributors,
            SUM(CASE WHEN qualified_1000 = 1 THEN 1 ELSE 0 END) as tier_1000,
            SUM(CASE WHEN qualified_3000 = 1 THEN 1 ELSE 0 END) as tier_3000,
            SUM(CASE WHEN qualified_10000 = 1 THEN 1 ELSE 0 END) as tier_10000,
            SUM(CASE WHEN qualified_100000 = 1 THEN 1 ELSE 0 END) as tier_100000
        FROM contributions WHERE session_id = ?
    ''', (session_id,)).fetchone()
    result['tier_counts'] = dict(tier_counts) if tier_counts else {}

    # 贡献用户 Top N（含粉丝团、财富等级、达标层级）
    top = conn.execute('''
        SELECT c.user_id, COALESCE(NULLIF(u.user_name, ''), c.user_name) AS user_name, c.consume,
               COALESCE(NULLIF(u.fans_club, ''), CASE WHEN s.anchor_name != '' THEN '[粉丝团:' || s.anchor_name || ']' ELSE '' END) AS fans_club,
               COALESCE(
                   u.grade,
                   (SELECT grade FROM chat_logs WHERE user_id = c.user_id AND grade != '' ORDER BY id DESC LIMIT 1),
                   ''
               ) AS grade,
               u.sec_uid, u.avatar_url, u.notes, u.tags,
               c.qualified_1000, c.qualified_3000, c.qualified_10000, c.qualified_100000,
               (SELECT COUNT(*) FROM gift_logs WHERE session_id = c.session_id AND user_id = c.user_id) as gift_count,
               (SELECT COUNT(*) FROM chat_logs WHERE session_id = c.session_id AND user_id = c.user_id) as chat_count
        FROM contributions c
        LEFT JOIN users u ON u.user_id = c.user_id
        JOIN sessions s ON s.id = c.session_id
        WHERE c.session_id = ? AND c.consume > 0
        ORDER BY c.consume DESC LIMIT ?
    ''', (session_id, top_n)).fetchall()
    result['top_users'] = [dict(r) for r in top]

    # 礼物类型分布 Top N
    gifts = conn.execute('''
        SELECT gift_name, COUNT(*) as times, SUM(gift_count) as total_count, SUM(diamond_total) as total_diamonds
        FROM gift_logs WHERE session_id = ?
        GROUP BY gift_name ORDER BY total_diamonds DESC LIMIT ?
    ''', (session_id, top_n)).fetchall()
    result['top_gifts'] = [dict(r) for r in gifts]

    return result


def query_search(q, page=1, size=20):
    conn = _get_conn()
    offset = (page - 1) * size
    rows = conn.execute('''
        SELECT DISTINCT c.user_id, COALESCE(NULLIF(u.user_name, ''), c.user_name) AS user_name,
               COALESCE(m.total_consume, 0) as total_consume,
               COALESCE(m.sessions_1000, 0) as sessions_1000, c.fans_club,
               u.sec_uid, u.avatar_url
        FROM contributions c
        LEFT JOIN monthly_stats m ON m.user_id = c.user_id AND m.year_month = strftime('%Y-%m', 'now')
        LEFT JOIN users u ON u.user_id = c.user_id
        WHERE c.user_id = ?
        ORDER BY c.consume DESC LIMIT ? OFFSET ?
    ''', (q, size, offset)).fetchall()
    if not rows:
        rows = conn.execute('''
            SELECT DISTINCT c.user_id, COALESCE(NULLIF(u.user_name, ''), c.user_name) AS user_name,
                   COALESCE(m.total_consume, 0) as total_consume,
                   COALESCE(m.sessions_1000, 0) as sessions_1000, c.fans_club,
                   u.sec_uid, u.avatar_url
            FROM contributions c
            LEFT JOIN monthly_stats m ON m.user_id = c.user_id AND m.year_month = strftime('%Y-%m', 'now')
            LEFT JOIN users u ON u.user_id = c.user_id
            WHERE c.user_name LIKE ?
            ORDER BY c.consume DESC LIMIT ? OFFSET ?
        ''', (f'%{q}%', size, offset)).fetchall()
    return {'users': [dict(r) for r in rows], 'total': len(rows), 'page': page}


def query_audit():
    """审计诊断：去重健康、时间间隙、场次概览、损坏用户名检测。"""
    conn = _get_conn()
    result = {}

    # ── 1. 总体概览 ──
    overview = conn.execute('''
        SELECT
            (SELECT COUNT(*) FROM gift_logs) AS total_gifts,
            (SELECT COALESCE(SUM(diamond_total), 0) FROM gift_logs) AS total_diamond,
            (SELECT COUNT(*) FROM chat_logs) AS total_chats,
            (SELECT COUNT(*) FROM sessions) AS total_sessions,
            (SELECT COUNT(DISTINCT user_id) FROM gift_logs) AS gift_users
    ''').fetchone()
    result['overview'] = dict(overview)

    # ── 2. 去重健康状态 ──
    dd = get_dedup_stats()
    dd['reject_rate'] = round(dd['rejected'] / dd['raw'] * 100, 2) if dd['raw'] else 0
    result['dedup'] = dd

    # ── 3. 时间间隙检测（最近 10 场） ──
    recent = conn.execute(
        'SELECT id FROM gift_logs GROUP BY session_id ORDER BY MAX(created_at) DESC LIMIT 10'
    ).fetchall()
    all_gaps = []
    max_gap = 0
    total_lost = 0
    gap_details = []
    for row in recent:
        times = conn.execute(
            'SELECT created_at FROM gift_logs WHERE session_id = ? ORDER BY created_at',
            (row['id'],)
        ).fetchall()
        for i in range(1, len(times)):
            try:
                prev = times[i-1]['created_at']
                curr = times[i]['created_at']
                if prev and curr:
                    fmt = '%Y-%m-%d %H:%M:%S'
                    ps = datetime.strptime(prev[:19], fmt)
                    cs = datetime.strptime(curr[:19], fmt)
                    gap = (cs - ps).total_seconds()
                    if gap >= 5:
                        all_gaps.append(gap)
                        if gap > max_gap:
                            max_gap = gap
                        total_lost += gap
                        if len(gap_details) < 10:
                            gap_details.append({'from': prev[11:19], 'to': curr[11:19], 'gap': int(gap)})
            except (ValueError, TypeError, KeyError):
                pass
    result['gaps'] = {
        'total_gaps': len(all_gaps),
        'max_gap_sec': int(max_gap),
        'total_lost_sec': int(total_lost),
        'details': gap_details,
    }

    # ── 4. 场次详情（最近 50 场） ──
    sessions = conn.execute('''
        SELECT s.id, s.anchor_name, s.start_time, s.end_time, s.status,
               COALESCE(g.gift_cnt, 0) AS gift_cnt,
               COALESCE(g.diamond_sum, 0) AS diamond_sum,
               COALESCE(g.user_cnt, 0) AS user_cnt,
               COALESCE(c.chat_cnt, 0) AS chat_cnt
        FROM sessions s
        LEFT JOIN (
            SELECT session_id,
                   COUNT(*) AS gift_cnt,
                   SUM(diamond_total) AS diamond_sum,
                   COUNT(DISTINCT user_id) AS user_cnt
            FROM gift_logs GROUP BY session_id
        ) g ON g.session_id = s.id
        LEFT JOIN (
            SELECT session_id, COUNT(*) AS chat_cnt
            FROM chat_logs GROUP BY session_id
        ) c ON c.session_id = s.id
        ORDER BY s.id DESC LIMIT 50
    ''').fetchall()
    session_list = []
    for r in sessions:
        d = dict(r)
        dur = 0
        try:
            if d['start_time'] and d['end_time']:
                sd = datetime.strptime(d['start_time'][:19], '%Y-%m-%d %H:%M:%S')
                ed = datetime.strptime(d['end_time'][:19], '%Y-%m-%d %H:%M:%S')
                dur = max(1, (ed - sd).total_seconds() / 3600)
        except (ValueError, TypeError):
            dur = 1
        d['gifts_per_hour'] = round(d['gift_cnt'] / dur) if dur > 0 else 0
        session_list.append(d)
    result['sessions'] = session_list

    # ── 5. 异常用户名检测 ──
    bad = conn.execute('''
        SELECT user_id, user_name FROM users
        WHERE user_name LIKE '2@%'
           OR instr(user_name, x'02') > 0
           OR instr(user_name, x'03') > 0
        LIMIT 50
    ''').fetchall()
    result['bad_usernames'] = [dict(r) for r in bad]

    # ── 6. 匿名用户解析率 ──
    anon_total = conn.execute('''
        SELECT COUNT(*) as total FROM users WHERE is_anonymous = 1
    ''').fetchone()['total']
    anon_resolved = conn.execute('''
        SELECT COUNT(*) as total FROM users
        WHERE is_anonymous = 1 AND user_name != anonymous_label AND anonymous_label != ''
    ''').fetchone()['total']
    anon_by_type = conn.execute('''
        SELECT
            SUM(CASE WHEN anonymous_label LIKE '神秘人%' THEN 1 ELSE 0 END) AS mystic,
            SUM(CASE WHEN anonymous_label LIKE 'dou%' THEN 1 ELSE 0 END) AS dou_prefix
        FROM users WHERE is_anonymous = 1
    ''').fetchone()
    result['anonymous'] = {
        'total': anon_total,
        'resolved': anon_resolved,
        'unresolved': anon_total - anon_resolved,
        'resolve_rate': round(anon_resolved / anon_total * 100, 1) if anon_total else 0,
        'mystic': anon_by_type['mystic'] if anon_by_type else 0,
        'dou_prefix': anon_by_type['dou_prefix'] if anon_by_type else 0,
    }

    # ── 7. 每场去重分析（最近 20 场） ──
    dedup_sessions = conn.execute('''
        SELECT
            g.session_id,
            s.anchor_name,
            COUNT(*) AS raw_rows,
            COUNT(DISTINCT g.user_id || ':' || g.gift_name || ':' || g.diamond_total) AS unique_events,
            SUM(g.diamond_total) AS diamond_sum,
            COUNT(DISTINCT g.user_id) AS user_cnt
        FROM gift_logs g
        JOIN sessions s ON s.id = g.session_id
        GROUP BY g.session_id
        ORDER BY g.session_id DESC LIMIT 20
    ''').fetchall()
    dedup_list = []
    for r in dedup_sessions:
        d = dict(r)
        raw = max(d['raw_rows'], 1)
        d['dup_rate'] = round((1 - d['unique_events'] / raw) * 100, 1)
        dedup_list.append(d)
    result['dedup_sessions'] = dedup_list

    return result

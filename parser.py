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

import csv
import logging
import os
import re
import sqlite3
import threading
import time
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
    get_user_id, get_user_sec_uid, get_user_avatar_url, get_user_name,
    fmt_grade, fmt_fans_club,
)

logger = logging.getLogger(__name__)

# ── SSE event callback (set by app.py to avoid circular imports) ──
_sse_callback = None

def set_sse_callback(cb):
    """Register callback for SSE events. Called as cb(event_type, data_dict)."""
    global _sse_callback
    _sse_callback = cb


# ── 礼物 Combo 缓冲器（替代 delta 法）────────────────
# 连击礼物（combo_count > 0）不通过 delta 法实时写入，而是缓冲到
# combo 结束（repeat_end=1）或超时（默认 3s）后一次性写入最终 count。
# 这消除了 delta_zero / out_of_order / combo_block 等误杀。
#
# key: (group_id, gift_id, user_id)
# value: {
#   'cnt': int (取 MAX),
#   'unit_price': int,
#   'display_name': str,
#   'user_name': str, 'user_id': str, 'grade': str, 'fans_club': str,
#   'sec_uid': str, 'avatar_url': str, 'display_id': str,
#   'timer': threading.Timer or None,
#   'repeat_end_seen': bool,
#   'last_update': float,
# }

_gift_combo_buffer = {}
_gift_combo_lock = threading.Lock()
_gift_combo_timeout = 3.0  # combo idle timeout (seconds)
_gift_finalize_callback = None  # (session_id, data) -> None

def set_gift_finalize_callback(cb):
    """Register callback for combo finalization. Called as cb(session_id, data_dict).
    Data dict is the record_gift-compatible fields + session_id."""
    global _gift_finalize_callback
    _gift_finalize_callback = cb


def _combo_finalize(key, update_counter=True):
    """Finalize a buffered combo: compute final total, write via callback.

    Called by repeat_end or timer expiry.
    """
    with _gift_combo_lock:
        buf = _gift_combo_buffer.pop(key, None)
    if not buf:
        return
    if update_counter:
        _dedup_diag['passed'] += 1
    unit_price = buf['unit_price']
    cnt = buf['cnt']
    total_value = unit_price * cnt
    diamond_info = f" ({total_value}钻石)" if total_value > 0 else ""
    display_name = buf['display_name']

    # 通过回调写入 SQLite (fetcher 注册了此回调，包含 session_id)
    if _gift_finalize_callback and cnt > 0:
        data = {
            'time': time.strftime('%H:%M:%S'),
            'user_id': buf['user_id'],
            'douyin_id': buf['display_id'],
            'user_name': buf['user_name'],
            'gift_name': display_name,
            'gift_count': cnt,
            'diamond_total': total_value,
            'grade': buf.get('grade', ''),
            'fans_club': buf.get('fans_club', ''),
            'sec_uid': buf.get('sec_uid', ''),
            'avatar_url': buf.get('avatar_url', ''),
            'group_id': key[0],
            'raw_repeat_count': cnt,
            'raw_repeat_end': 1,
            'raw_combo_count': cnt,
            'raw_total_count': cnt,
        }
        _gift_finalize_callback(data)

    # SSE push for finalized combo
    if _sse_callback and buf.get('user_id'):
        _sse_callback('gift', {
            'user_id': buf['user_id'],
            'user_name': buf['user_name'],
            'gift_name': display_name,
            'gift_count': cnt,
            'diamond_total': total_value,
            'group_id': key[0],
        })

    msg = f"[礼物] {buf['user_name']}[{buf['user_id']}] 礼物:{display_name} x{cnt}{diamond_info}"
    logger.log(BARRAGE, msg)

    return {
        'type': 'gift',
        'msg': msg,
        'data': data,
    }


def flush_combo_buffer():
    """Force-finalize all pending combos (on shutdown)."""
    with _gift_combo_lock:
        keys = list(_gift_combo_buffer.keys())
        _gift_combo_buffer.clear()
    for key in keys:
        _combo_finalize(key, update_counter=False)


# ── 礼物去重状态（delta法）────────────────────────
# 仅用于非连击礼物的 fallback（trace_id 不可用时）
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

# ── 去重诊断计数器 ────────────────────────────────
_dedup_diag = {
    'raw': 0, 'passed': 0, 'rejected': 0,
    'repeat_zero': 0, 'combo_block': 0,
    'counter_reset': 0, 'delta_zero': 0, 'out_of_order': 0,
    'rc1_dup': 0, 'bulk_dup': 0,
}
_dedup_csv_path = None
_dedup_csv_lock = threading.Lock()


def set_dedup_csv_dir(data_dir):
    """设置去重诊断 CSV 的输出目录。由 fetcher 在 DataRecorder.open() 后调用。"""
    global _dedup_csv_path, _rejected_csv_path, _trace_shadow_csv_path
    import os
    _dedup_csv_path = os.path.join(data_dir, 'dedup_stats.csv')
    with open(_dedup_csv_path, 'w', encoding='utf-8') as f:
        f.write('time,raw,passed,rejected,repeat_zero,combo_block,counter_reset,out_of_order,delta_zero,rc1_dup,bulk_dup\n')
    _rejected_csv_path = os.path.join(data_dir, 'rejected_gifts.csv')
    with open(_rejected_csv_path, 'w', encoding='utf-8') as f:
        f.write('time,user_id,user_name,gift_name,gift_id,diamond_count,repeat_count,group_id,reason,log_id,trace_id,send_time,priority_score,fold_type,anchor_fold_type,msg_filter_k,msg_filter_v,is_dispatch,queue_priority\n')
    _trace_shadow_csv_path = os.path.join(data_dir, 'trace_shadow.csv')
    with open(_trace_shadow_csv_path, 'w', encoding='utf-8') as f:
        f.write('time,user_id,user_name,gift_name,diamond_count,repeat_count,trace_id,current_decision,trace_decision\n')
    with _trace_shadow_lock:
        _seen_trace_ids.clear()

_rejected_csv_path = None
_rejected_csv_lock = threading.Lock()

_trace_shadow_csv_path = None
_trace_shadow_csv_lock = threading.Lock()


def _write_rejected_gift(group_id, gift_id, gift_name, diamond_count, user_id, user_name, repeat_count, reason,
                         log_id='', trace_id='', send_time='', priority_score=0, fold_type=0, anchor_fold_type=0,
                         msg_filter_k='', msg_filter_v='', is_dispatch=0, queue_priority=0):
    global _rejected_csv_path
    if not _rejected_csv_path:
        return
    with _rejected_csv_lock:
        ts = time.strftime('%H:%M:%S')
        try:
            with open(_rejected_csv_path, 'a', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([ts, user_id, user_name, gift_name, gift_id, diamond_count, repeat_count, group_id, reason,
                                 log_id, trace_id, send_time, priority_score, fold_type, anchor_fold_type, msg_filter_k, msg_filter_v, is_dispatch, queue_priority])
        except Exception:
            pass


def _write_trace_shadow(user_id, user_name, gift_name, diamond_count, repeat_count, trace_id, current_decision, trace_decision):
    """记录 trace_id 影子去重与当前逻辑的决策对比。"""
    global _trace_shadow_csv_path
    if not _trace_shadow_csv_path:
        return
    with _trace_shadow_csv_lock:
        ts = time.strftime('%H:%M:%S')
        try:
            with open(_trace_shadow_csv_path, 'a', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([ts, user_id, user_name, gift_name, diamond_count, repeat_count, trace_id, current_decision, trace_decision])
        except Exception:
            pass


def _flush_dedup_stats():
    """将当前 _dedup_diag 累计值追加写入 dedup_stats.csv。不重置计数器。"""
    global _dedup_csv_path
    if not _dedup_csv_path:
        return
    with _dedup_csv_lock:
        import os as _os
        if not _os.path.exists(_dedup_csv_path):
            return
        stats = dict(_dedup_diag)
        ts = time.strftime('%H:%M:%S')
        try:
            with open(_dedup_csv_path, 'a', encoding='utf-8') as f:
                f.write(f"{ts},{stats['raw']},{stats['passed']},{stats['rejected']},{stats['repeat_zero']},{stats['combo_block']},{stats['counter_reset']},{stats['out_of_order']},{stats['delta_zero']},{stats['rc1_dup']},{stats['bulk_dup']}\n")
        except Exception:
            pass


def flush_dedup_stats():
    """公开接口：立即写入当前去重诊断快照（不清零计数器）。"""
    _flush_dedup_stats()


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
    """清理超过 max_age 秒未更新的去重状态。"""
    now = time.time()
    with _dedup_lock:
        stale = [k for k, (_, t, _) in _gift_dedup.items() if now - t > max_age]
        for k in stale:
            del _gift_dedup[k]
        rc1_stale = [k for k, t in _rc1_dedup.items() if now - t > max_age]
        for k in rc1_stale:
            del _rc1_dedup[k]
    # 定期清理 trace_id 集合（每 10 分钟清空，防止无限增长）
    with _trace_shadow_lock:
        if len(_seen_trace_ids) > 100_000:
            _seen_trace_ids.clear()


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
    common = {
        'time': time.strftime('%H:%M:%S'),
        'user_id': uid,
        'douyin_id': user.display_id,
        'user_name': get_user_name(user),
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
                'msg': f"[福袋口令] {get_user_name(user)}[{uid}] 内容:{msg.content}",
                'data': {**common, 'content': msg.content},
            })
    else:  # 普通聊天
        if enable_outputs.get('chat', True):
            results.append({
                'type': 'chat',
                'msg': f"[聊天] {get_user_name(user)}[{uid}] 内容:{msg.content}",
                'data': {**common, 'content': msg.content},
            })
    return results


def parse_gift_msg(payload, enable_outputs=None):
    """解析礼物消息，使用 delta 法去重。

    同组同礼物的 repeat_count 是累计值，每条消息只统计增量，
    避免将累计值当作单次数量导致超采。
    """
    if not enable_outputs.get('gift', True):
        return []
    _dedup_diag['raw'] += 1
    msg = parse_proto(GiftMessage, payload)
    user = msg.user
    uid = get_user_id(user)
    gift = msg.gift

    # ── 混合去重策略 ──
    # 连击礼物（combo_count > 0）：缓冲到 combo 结束，取 MAX count 一次性写入。
    # 非连击礼物（combo_count == 0）：用 trace_id 去重，即时写入。
    combo_cnt = msg.combo_count or 0
    rc = combo_cnt if combo_cnt > 0 else (msg.repeat_count or 0)
    gid = str(msg.group_id) if msg.group_id else '0'
    gft_id = gift.id or 0
    log_id = msg.log_id or ''
    trace_id = msg.trace_id or ''
    send_time = msg.send_time or 0
    is_combo = combo_cnt > 0
    now = time.time()

    # ── 计算礼物单价（供 combo buffer 和非连击路径共用）──
    composite_price = 0
    composite_name = ''
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
        unit_price = composite_price
        display_name = composite_name or gift.name
    else:
        unit_price = _GIFT_PRICE_OVERRIDE.get(gift.name, gift.diamond_count)
        display_name = gift.name

    # ── 路径 A：连击礼物 → Combo 缓冲器 ──
    if is_combo:
        key = (gid, str(gft_id), uid)
        with _gift_combo_lock:
            entry = _gift_combo_buffer.get(key)
            if entry is None:
                # 新连击：创建缓冲
                entry = {
                    'cnt': rc,
                    'unit_price': unit_price,
                    'display_name': display_name,
                    'user_name': get_user_name(user) or str(uid),
                    'user_id': uid,
                    'grade': fmt_grade(user),
                    'fans_club': fmt_fans_club(user),
                    'sec_uid': get_user_sec_uid(user),
                    'avatar_url': get_user_avatar_url(user),
                    'display_id': user.display_id or '',
                    'group_id': gid,
                    'timer': None,
                    'repeat_end_seen': bool(msg.repeat_end or 0),
                    'last_update': now,
                }
                _gift_combo_buffer[key] = entry
            else:
                # 已有缓冲：取 MAX count
                entry['cnt'] = max(entry['cnt'], rc)
                entry['last_update'] = now
                if msg.repeat_end:
                    entry['repeat_end_seen'] = True
                # 取消旧计时器
                if entry.get('timer'):
                    try:
                        entry['timer'].cancel()
                    except Exception:
                        pass
                    entry['timer'] = None

            # 如果 repeat_end 到达 → 立即结束
            if entry['repeat_end_seen']:
                _combo_finalize(key)
                _dedup_diag['passed'] += 1
                # 返回 BARRAGE 日志
                unit_price = entry['unit_price'] or _GIFT_PRICE_OVERRIDE.get(gift.name, gift.diamond_count)
                total_value = unit_price * entry['cnt']
                msg_text = f"[礼物] {entry['user_name']}[{uid}] 礼物:{gift.name} x{entry['cnt']} ({total_value}钻石)" if total_value > 0 else f"[礼物] {entry['user_name']}[{uid}] 礼物:{gift.name} x{entry['cnt']}"
                return [{
                    'type': 'gift',
                    'msg': msg_text,
                    'data': {'_combo_progress': True},
                }]
            else:
                # 未结束：设定时器（防止丢包导致 repeat_end 永远不达）
                def _on_timeout(k=key):
                    return _combo_finalize(k)
                timer = threading.Timer(_gift_combo_timeout, _on_timeout)
                timer.daemon = True
                timer.start()
                entry['timer'] = timer
                # 返回 BARRAGE 日志（不写入 DB）
                unit_price = entry['unit_price'] or _GIFT_PRICE_OVERRIDE.get(gift.name, gift.diamond_count)
                total_value = unit_price * entry['cnt']
                msg_text = f"[礼物] {entry['user_name']}[{uid}] 礼物:{gift.name} x{entry['cnt']}"
                return [{
                    'type': 'gift',
                    'msg': msg_text,
                    'data': {'_combo_progress': True},
                }]

    # ── 路径 B：非连击礼物 → 即时写入（trace_id 去重）──
    # 单次送礼（rc=1）：每条消息是独立事件。trace_id 去重，无 trace_id 时直接接受。
    if rc == 1:
        if trace_id:
            with _trace_shadow_lock:
                if trace_id in _seen_trace_ids:
                    _dedup_diag['rc1_dup'] += 1
                    cnt, reason = 0, 'rc1_dup'
                else:
                    _seen_trace_ids.add(trace_id)
                    cnt, reason = 1, ''
        else:
            cnt, reason = 1, ''
    elif rc > 1 and trace_id:
        # 非连击批量礼物（人气票 x999）：atomic 批次，trace_id 去重
        with _trace_shadow_lock:
            if trace_id in _seen_trace_ids:
                _dedup_diag['bulk_dup'] += 1
                cnt, reason = 0, 'bulk_dup'
            else:
                _seen_trace_ids.add(trace_id)
                cnt, reason = rc, ''
    else:
        # 无 trace_id 的兜底：用 delta 法
        cnt, reason = _compute_gift_count(gid, gft_id, uid, rc, repeat_end=msg.repeat_end or 0)

    # 定期清理
    if rc > 0 and (rc % 100) == 0:
        _prune_dedup_state()
    if _dedup_diag['raw'] % 500 == 0:
        _flush_dedup_stats()

    # 影子去重
    trace_seen = False
    with _trace_shadow_lock:
        if trace_id:
            if trace_id in _seen_trace_ids:
                trace_seen = True
            else:
                _seen_trace_ids.add(trace_id)

    if cnt <= 0:
        _dedup_diag['rejected'] += 1
        _write_rejected_gift(gid, gft_id, gift.name, gift.diamond_count, uid, get_user_name(user), rc, reason,
                             log_id=log_id, trace_id=trace_id, send_time=send_time,
                             priority_score=msg.common.priority_score if msg.common else 0,
                             fold_type=msg.common.fold_type if msg.common else 0,
                             anchor_fold_type=msg.common.anchor_fold_type if msg.common else 0,
                             msg_filter_k=msg.common.msg_process_filter_k if msg.common else '',
                             msg_filter_v=msg.common.msg_process_filter_v if msg.common else '',
                             is_dispatch=1 if (msg.common and msg.common.is_dispatch) else 0,
                             queue_priority=msg.priority.priority if msg.priority else 0)
        # 影子记录：当前拒绝，trace 判断
        cur_dec = f'reject_{reason}' if reason else 'reject'
        trace_dec = 'reject_dup' if trace_seen else 'accept'
        _write_trace_shadow(uid, get_user_name(user), gift.name, gift.diamond_count, rc, trace_id, cur_dec, trace_dec)
        return []  # 重复消息或增量为零，跳过
    _dedup_diag['passed'] += 1

    # SSE push for accepted gifts
    if _sse_callback and uid:
        gid_str = str(gft_id) if gft_id else '0'
        _sse_callback('gift', {
            'user_id': uid,
            'user_name': get_user_name(user),
            'gift_name': display_name,
            'gift_count': cnt,
            'diamond_total': unit_price * cnt,
            'group_id': gid_str,
        })

    # 影子记录：当前接受，trace 判断
    cur_dec = 'accept'
    trace_dec = 'reject_dup' if trace_seen else 'accept'
    _write_trace_shadow(uid, get_user_name(user), gift.name, gift.diamond_count, rc, trace_id, cur_dec, trace_dec)
    total_value = unit_price * cnt
    diamond_info = f" ({total_value}钻石)" if total_value > 0 else ""
    return [{
        'type': 'gift',
        'msg': f"[礼物] {get_user_name(user)}[{uid}] 礼物:{display_name} x{cnt}{diamond_info}",
        'data': {
            'time': time.strftime('%H:%M:%S'),
            'user_id': uid,
            'douyin_id': user.display_id,
            'user_name': get_user_name(user),
            'gender': {1: "男", 2: "女"}.get(user.gender, "未知"),
            'gift_name': display_name,
            'gift_count': cnt,
            'diamond_total': total_value,
            'grade': fmt_grade(user),
            'fans_club': fmt_fans_club(user),
            'sec_uid': get_user_sec_uid(user),
            'avatar_url': get_user_avatar_url(user),
            'group_id': gid,
            'raw_repeat_count': msg.repeat_count,
            'raw_repeat_end': msg.repeat_end,
            'raw_combo_count': msg.combo_count,
            'raw_total_count': msg.total_count,
            'log_id': log_id,
            'trace_id': trace_id,
            'send_time': send_time,
            # protobuf 过滤字段 (Kimi 建议：区分折叠 vs 过滤)
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
    return [{
        'type': 'like',
        'msg': f"[点赞] {get_user_name(user)}[{uid}] 点赞:{msg.count}个, 累计{msg.total}赞",
        'data': {
            'time': time.strftime('%H:%M:%S'),
            'user_id': uid, 'user_name': get_user_name(user),
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
    gender = {0: "未知", 1: "男", 2: "女"}.get(user.gender, "未知")
    extras = f" (直播间人数:{msg.member_count})" if msg.member_count else ""
    return [{
        'type': 'member',
        'msg': f"[进场] {get_user_name(user)}[{uid}][{gender}] 进入了直播间{extras}",
        'data': {
            'time': time.strftime('%H:%M:%S'),
            'user_id': uid, 'user_name': get_user_name(user), 'gender': gender,
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
    action = {1: "关注了主播", 2: "分享了直播间"}.get(msg.action, "互动")
    follow = f"(第{msg.follow_count}个关注)" if msg.follow_count else ""
    return [{
        'type': 'social',
        'msg': f"[关注/分享] {get_user_name(user)}[{uid}] {action} {follow}",
        'data': {
            'time': time.strftime('%H:%M:%S'),
            'user_id': uid, 'user_name': get_user_name(user), 'action': action,
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
    t = {1: "升级", 2: "加入"}.get(msg.type, "变动")
    return [{
        'type': 'fansclub',
        'msg': f"[粉丝团] {get_user_name(user)}[{uid}] {t}: {msg.content}",
        'data': {
            'time': time.strftime('%H:%M:%S'),
            'user_id': uid, 'user_name': get_user_name(user),
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
    content = msg.default_content or f"[表情{msg.emoji_id}]"
    return [{
        'type': 'emoji',
        'msg': f"[表情] {get_user_name(user)}[{uid}]: {content}",
        'data': {
            'time': time.strftime('%H:%M:%S'),
            'user_id': uid, 'user_name': get_user_name(user),
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

    # 提取 douyin_id
    douyin_id = _extract_douyin_id(payload)

    # ── 辅助：从字符串列表中找用户名 ──
    def _find_username(candidates):
        """从候选字符串中提取可能的用户名（排除已知关键词）。"""
        known_words = {'开通', '续费', '月度', '季度', '年度', '会员', '星守护',
                       'subscribe_anchor_mvp_v2', 'webcast', 'douyin', 'room'}
        for s in candidates:
            if not s or len(s) > 60:
                continue
            # 跳过模板占位符
            if s.startswith('{') and s.endswith('}'):
                continue
            # 跳过纯数字（可能是 ID）
            if s.isdigit():
                continue
            # 跳过已知关键词（精确匹配或包含）
            if s in known_words:
                continue
            # 清理引号
            clean = s.lstrip('"').rstrip('" ').strip()
            if clean and len(clean) <= 50:
                return clean
        return ''

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
                    'user': user, 'douyin_id': douyin_id}

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
                    'user': user, 'douyin_id': douyin_id}

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

        results.append({
            'type': 'subscribe',
            'msg': f'[订阅] {user} {action}{sub_type}{event} ({price}钻石)',
            'data': {
                'time': time.strftime('%H:%M:%S'),
                'user_name': user,
                'douyin_id': douyin_id,
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
        results.append({
            'type': 'rank',
            'msg': f"[排行榜] 第{i}名: {get_user_name(r.user)} 积分:{r.score_str}",
            'data': {
                'time': time.strftime('%H:%M:%S'),
                'rank_pos': i,
                'user_id': uid,
                'douyin_id': r.user.display_id,
                'user_name': get_user_name(r.user),
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
    try:
        _migrate_conn.execute('ALTER TABLE users ADD COLUMN display_id TEXT DEFAULT ""')
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
    try:
        _migrate_conn.execute('''CREATE TABLE IF NOT EXISTS pk_rounds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER REFERENCES sessions(id),
            start_time TEXT NOT NULL,
            end_time TEXT,
            duration_sec INTEGER DEFAULT 0,
            mode TEXT DEFAULT '',
            participants TEXT DEFAULT '',
            participant_count INTEGER DEFAULT 0,
            self_score INTEGER DEFAULT 0,
            opponent_score INTEGER DEFAULT 0,
            result TEXT DEFAULT '',
            created_at DATETIME DEFAULT (datetime('now', '+8 hours'))
        )''')
        _migrate_conn.commit()
    except sqlite3.OperationalError:
        pass
    _migrate_conn.close()
except Exception:
    pass




def _get_conn():
    if not hasattr(_local, 'conn') or _local.conn is None:
        os.makedirs(DB_DIR, exist_ok=True)
        _local.conn = sqlite3.connect(DB_PATH)
        _local.conn.execute('PRAGMA journal_mode=WAL')
        _local.conn.execute('PRAGMA synchronous=NORMAL')
        _local.conn.execute('PRAGMA wal_autocheckpoint=100')
        _local.conn.execute('PRAGMA journal_size_limit=67108864')
        _local.conn.execute('PRAGMA busy_timeout=2000')
        _local.conn.row_factory = sqlite3.Row
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
        CREATE TABLE IF NOT EXISTS gift_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER REFERENCES sessions(id),
            user_id TEXT NOT NULL,
            user_name TEXT NOT NULL,
            gift_name TEXT NOT NULL,
            gift_count INTEGER DEFAULT 1,
            diamond_total INTEGER DEFAULT 0,
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
        CREATE TABLE IF NOT EXISTS pk_rounds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER REFERENCES sessions(id),
            start_time TEXT NOT NULL,
            end_time TEXT,
            duration_sec INTEGER DEFAULT 0,
            mode TEXT DEFAULT '',
            participants TEXT DEFAULT '',
            participant_count INTEGER DEFAULT 0,
            self_score INTEGER DEFAULT 0,
            opponent_score INTEGER DEFAULT 0,
            result TEXT DEFAULT '',
            created_at DATETIME DEFAULT (datetime('now', '+8 hours'))
        );
        CREATE INDEX IF NOT EXISTS idx_contributions_session ON contributions(session_id);
        CREATE INDEX IF NOT EXISTS idx_contributions_user ON contributions(user_id);
        CREATE INDEX IF NOT EXISTS idx_chat_logs_user ON chat_logs(user_id);
        CREATE INDEX IF NOT EXISTS idx_monthly_stats ON monthly_stats(year_month, sessions_1000 DESC);
        CREATE INDEX IF NOT EXISTS idx_daily_stats ON daily_stats(date, sessions_1000 DESC);
        CREATE INDEX IF NOT EXISTS idx_contributions_qualified ON contributions(qualified_1000);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_gift_dedup_ts ON gift_logs(session_id, user_id, gift_name, diamond_total, gift_count, created_at);
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
    conn.commit()
    logger.info(f"[DB] 已初始化: {DB_PATH}")
    return True


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
    """检测是否为抖音神秘人（匿名用户）。

    神秘人由抖音分配匿名显示名（如"神秘人七阶"、"神秘人523150"），
    特征：以"神秘人"开头，后面不含其他中文字符。
    """
    if not user_name:
        return False, ''
    if user_name.startswith('神秘人'):
        # 神秘人后面是数字/字母/空白，不含其他 CJK 字符
        suffix = user_name[3:]
        if not re.search(r'[一-鿿]', suffix):
            return True, user_name
    return False, ''


def upsert_user(user_id, user_name, grade='', fans_club='', sec_uid='', avatar_url='', display_id=''):
    """更新或插入用户信息（财富等级、粉丝团、sec_uid、头像URL等）。线程安全。

    Args:
        user_id: 抖音用户数字 ID。
        user_name: 昵称。
        grade: 消费等级标签。
        fans_club: 粉丝团标签。
        sec_uid: 抖音永久用户标识（~50位字符串），仅在非空时更新。
        avatar_url: 用户头像 URL，仅在非空时更新。
        display_id: 抖音显示 ID（@ 号），仅在非空时更新。
    """
    if not user_id:
        return
    is_anon, anon_label = _detect_anonymous(user_name)
    conn = _get_conn()

    # 两步 API 解析：user_id → sec_uid → 真实昵称+头像
    # 仅对可疑名称执行（神秘人、dou开头、含控制字符）
    need_resolve = is_anon or (user_name and (user_name.lower().startswith('dou') or any(ord(c) < 32 for c in user_name)))
    if need_resolve and user_id:
        try:
            from service.network import resolve_user_info
            resolved = resolve_user_info(user_id)
            if resolved and resolved.get('nickname'):
                nick = resolved['nickname'].strip()
                if nick and len(nick) >= 2 and not nick.startswith('神秘人'):
                    user_name = nick
                    # 同时更新头像和 sec_uid（API 返回的值更准确）
                    if resolved.get('avatar_url'):
                        avatar_url = resolved['avatar_url']
                    if resolved.get('sec_uid'):
                        sec_uid = resolved['sec_uid']
        except Exception:
            pass

    conn.execute('''
        INSERT INTO users (user_id, user_name, grade, fans_club, sec_uid, avatar_url, display_id, is_anonymous, anonymous_label, last_seen)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime("now", "+8 hours"))
        ON CONFLICT(user_id) DO UPDATE SET
            user_name = CASE WHEN ? != '' THEN ? ELSE user_name END,
            grade = CASE WHEN ? != '' THEN ? ELSE grade END,
            fans_club = CASE WHEN ? != '' THEN ? ELSE fans_club END,
            sec_uid = CASE WHEN ? != '' THEN ? ELSE sec_uid END,
            avatar_url = CASE WHEN ? != '' THEN ? ELSE avatar_url END,
            display_id = CASE WHEN ? != '' THEN ? ELSE display_id END,
            is_anonymous = CASE WHEN ? = 1 THEN 1 ELSE is_anonymous END,
            anonymous_label = CASE WHEN ? != '' THEN ? ELSE anonymous_label END,
            last_seen = datetime("now", "+8 hours")
    ''', (user_id, user_name, grade, fans_club, sec_uid, avatar_url, display_id, is_anon, anon_label,
          user_name, user_name,
          grade, grade,
          fans_club, fans_club,
          sec_uid, sec_uid,
          avatar_url, avatar_url,
          display_id, display_id,
          is_anon, anon_label, anon_label))
    conn.commit()


def flush_to_sqlite(session_id):
    """从 gift_logs 聚合贡献数据写入 contributions / daily_stats / monthly_stats。"""
    conn = _get_conn()
    today = datetime.now().strftime('%Y-%m-%d')  # uses local system time (set to UTC+8 for Singapore)
    month = datetime.now().strftime('%Y-%m')

    # 从 gift_logs 聚合每个用户的消费
    rows = conn.execute('''
        SELECT user_id, user_name, SUM(diamond_total) as consume
        FROM gift_logs WHERE session_id = ?
        GROUP BY user_id
    ''', (session_id,)).fetchall()

    if not rows:
        return

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


# ── 批量写入缓冲区（线程隔离）──
# 单条 commit 在高负载下（200+ msg/s）会导致 SQLite 写锁饥荒。
# 改为缓冲批量提交：每 100 条或 500ms 刷新一次，减少 50x fsync。
_local._gift_buf = []
_local._chat_buf = []
_local._buf_last_flush = 0

def _flush_buffers(force=False):
    """将当前线程的礼物/弹幕缓冲区批量写入 SQLite。"""
    conn = _get_conn()
    now = time.time()
    last = getattr(_local, '_buf_last_flush', 0)
    gifts = getattr(_local, '_gift_buf', [])
    chats = getattr(_local, '_chat_buf', [])
    # 触发条件：强制、超 500ms、或任一队列超 100 条
    if not force and (now - last < 0.5) and len(gifts) < 100 and len(chats) < 100:
        return
    if not gifts and not chats:
        return
    try:
        conn.execute('BEGIN IMMEDIATE')
        if gifts:
            conn.executemany(
                'INSERT OR IGNORE INTO gift_logs (session_id, user_id, user_name, gift_name, gift_count, diamond_total, grade, fans_club) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                gifts)
            gifts.clear()
        if chats:
            conn.executemany(
                'INSERT OR IGNORE INTO chat_logs (session_id, user_id, user_name, content, grade, fans_club) VALUES (?, ?, ?, ?, ?, ?)',
                chats)
            chats.clear()
        conn.commit()
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        # 恢复数据到缓冲区（避免丢失）
        if gifts:
            _local._gift_buf = gifts + _local._gift_buf
        if chats:
            _local._chat_buf = chats + _local._chat_buf
        logger.error(f"[DB] batch flush failed: {e} — data retained in buffer")
    _local._buf_last_flush = time.time()


def record_chat(session_id, user_id, user_name, content, grade='', fans_club=''):
    buf = getattr(_local, '_chat_buf', None)
    if buf is None:
        _local._chat_buf = []
        buf = _local._chat_buf
    buf.append((session_id, user_id, user_name, content, grade, fans_club))
    _flush_buffers()


def record_gift(session_id, user_id, user_name, gift_name, gift_count, diamond_total, grade='', fans_club=''):
    """记录礼物（缓冲批量写入，减少 SQLite 写锁竞争）。"""
    buf = getattr(_local, '_gift_buf', None)
    if buf is None:
        _local._gift_buf = []
        buf = _local._gift_buf
    buf.append((session_id, user_id, user_name, gift_name, gift_count, diamond_total, grade, fans_club))
    _flush_buffers()


def flush_all_buffers():
    """强制刷新当前线程的所有缓冲区（在 collector stop 时调用）。"""
    try:
        _flush_buffers(force=True)
    except Exception:
        pass
    # 强制结束所有待定 combo（在 collector stop 时调用）
    try:
        flush_combo_buffer()
    except Exception:
        pass
_VALID_TIERS = {1000, 3000, 10000, 100000}

def _resolve_tier(threshold):
    """将 threshold 映射到已知 tier，若为 0 或非标准值则返回 None。"""
    if threshold in _VALID_TIERS:
        return threshold
    return None


def query_leaderboard(threshold=1000, period='session', page=1, size=100, session_id=None, year_month='', min_consume=0, room_id=''):
    conn = _get_conn()
    offset = (page - 1) * size
    tier = _resolve_tier(threshold)

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
            SELECT c.user_id, c.user_name, c.consume,
                   COALESCE(
                       NULLIF(u.fans_club, ''),
                       NULLIF(c.fans_club, ''),
                       (SELECT fans_club FROM chat_logs WHERE user_id = c.user_id AND fans_club != '' ORDER BY id DESC LIMIT 1),
                       ''
                   ) AS fans_club,
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
        room_where = 'AND s.room_id = ?' if room_id else ''
        # threshold 控制上榜门槛：各session消费需 >= threshold 才算上榜一次
        t = max(int(threshold), max(int(min_consume), 0))

        params_today = [today]
        if room_id:
            params_today.append(room_id)
        params_today.extend([t, size, offset])

        total_params = tuple([today] + ([room_id] if room_id else []) + [t])

        rows = conn.execute(f'''
            SELECT c.user_id, c.user_name, SUM(c.consume) AS consume,
                   COALESCE(u.fans_club, '') AS fans_club,
                   COALESCE(u.grade, '') AS grade,
                   u.sec_uid, u.avatar_url, u.notes, u.tags,
                   (SELECT COUNT(DISTINCT c2.session_id) FROM contributions c2
                    JOIN sessions s2 ON c2.session_id = s2.id AND date(s2.start_time) = ?
                    WHERE c2.user_id = c.user_id AND c2.consume >= ?) AS sessions_count
            FROM contributions c
            JOIN sessions s ON c.session_id = s.id AND date(s.start_time) = ?
            LEFT JOIN users u ON u.user_id = c.user_id
            WHERE c.consume > 0 {room_where}
            GROUP BY c.user_id
            HAVING SUM(c.consume) >= ?
            ORDER BY consume DESC LIMIT ? OFFSET ?
        ''', (today, t, today, *params_today[1:])).fetchall()
        total = conn.execute(f'''
            SELECT COUNT(*) FROM (
                SELECT c.user_id FROM contributions c
                JOIN sessions s ON c.session_id = s.id AND date(s.start_time) = ?
                WHERE c.consume > 0 {room_where} GROUP BY c.user_id HAVING SUM(c.consume) >= ?
            )
        ''', total_params).fetchone()[0]
    elif period == 'month':
        month = year_month or datetime.now().strftime('%Y-%m')
        room_where = 'AND s.room_id = ?' if room_id else ''
        t = max(int(threshold), max(int(min_consume), 0))

        params = [month]
        if room_id:
            params.append(room_id)
        params.extend([t, size, offset])

        total_params = tuple([month] + ([room_id] if room_id else []) + [t])

        rows = conn.execute(f'''
            SELECT c.user_id, c.user_name, SUM(c.consume) AS consume,
                   COALESCE(u.fans_club, '') AS fans_club,
                   COALESCE(u.grade, '') AS grade,
                   u.sec_uid, u.avatar_url, u.notes, u.tags,
                   (SELECT COUNT(DISTINCT c2.session_id) FROM contributions c2
                    JOIN sessions s2 ON c2.session_id = s2.id AND strftime('%Y-%m', s2.start_time) = ?
                    WHERE c2.user_id = c.user_id AND c2.consume >= ?) AS sessions_count
            FROM contributions c
            JOIN sessions s ON c.session_id = s.id AND strftime('%Y-%m', s.start_time) = ?
            LEFT JOIN users u ON u.user_id = c.user_id
            WHERE c.consume > 0 {room_where}
            GROUP BY c.user_id
            HAVING SUM(c.consume) >= ?
            ORDER BY consume DESC LIMIT ? OFFSET ?
        ''', (month, t, month, *params[1:])).fetchall()
        total = conn.execute(f'''
            SELECT COUNT(*) FROM (
                SELECT c.user_id FROM contributions c
                JOIN sessions s ON c.session_id = s.id AND strftime('%Y-%m', s.start_time) = ?
                WHERE c.consume > 0 {room_where} GROUP BY c.user_id HAVING SUM(c.consume) >= ?
            )
        ''', total_params).fetchone()[0]
    elif period == '30d':
        # 滚动 30 天：从 contributions + sessions 按时间窗口聚合
        # 上榜次数：按最低消费计算符合条件的场次数
        t = max(int(threshold), max(int(min_consume), 0))
        sessions_col = f'SUM(CASE WHEN c.consume >= {int(t)} THEN 1 ELSE 0 END)'

        room_where = 'AND s.room_id = ?' if room_id else ''

        params_30 = []
        if room_id:
            params_30.append(room_id)
        params_30.extend([size, offset])

        where_extra_30 = f'AND SUM(c.consume) >= {int(t)}' if t > 0 else ''

        rows = conn.execute(f'''
            SELECT c.user_id, c.user_name, SUM(c.consume) AS consume,
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
              AND c.consume > 0 {room_where}
            GROUP BY c.user_id
            HAVING SUM(c.consume) > 0 {where_extra_30}
            ORDER BY consume DESC LIMIT ? OFFSET ?
        ''', params_30).fetchall()

        total_params_30 = []
        if room_id:
            total_params_30.append(room_id)
        total_extra_30 = ''
        if min_consume > 0:
            total_extra_30 = 'HAVING SUM(c.consume) >= ?'
            total_params_30.append(min_consume)
        total = conn.execute(f'''
            SELECT COUNT(*) FROM (
                SELECT c.user_id, SUM(c.consume) AS total_consume
                FROM contributions c
                JOIN sessions s ON c.session_id = s.id
                WHERE s.start_time >= datetime('now', '-30 days') AND c.consume > 0 {room_where}
                GROUP BY c.user_id
                {total_extra_30}
            )
        ''', total_params_30).fetchone()[0]
    else:
        # 全部 / 指定月份：从 contributions 聚合
        room_where = 'AND s.room_id = ?' if room_id else ''
        month_filter = year_month if year_month else ''
        t = max(int(threshold), max(int(min_consume), 0))

        if month_filter:
            params_all = [month_filter]
            if room_id:
                params_all.append(room_id)
            params_all.extend([t, size, offset])
            total_params = tuple(params_all[:3])

            rows = conn.execute(f'''
                SELECT c.user_id, c.user_name, SUM(c.consume) AS consume,
                       COALESCE(u.fans_club, '') AS fans_club,
                       COALESCE(u.grade, '') AS grade,
                       u.sec_uid, u.avatar_url, u.notes, u.tags,
                       (SELECT COUNT(DISTINCT c2.session_id) FROM contributions c2
                        JOIN sessions s2 ON c2.session_id = s2.id AND strftime('%Y-%m', s2.start_time) = ?
                        WHERE c2.user_id = c.user_id AND c2.consume >= ?) AS sessions_count
                FROM contributions c
                JOIN sessions s ON c.session_id = s.id AND strftime('%Y-%m', s.start_time) = ?
                LEFT JOIN users u ON u.user_id = c.user_id
                WHERE c.consume > 0 {room_where}
                GROUP BY c.user_id HAVING SUM(c.consume) >= ?
                ORDER BY consume DESC LIMIT ? OFFSET ?
            ''', (month_filter, t, month_filter, *params_all[1:])).fetchall()
            total = conn.execute(f'''
                SELECT COUNT(*) FROM (
                    SELECT c.user_id FROM contributions c
                    JOIN sessions s ON c.session_id = s.id AND strftime('%Y-%m', s.start_time) = ?
                    WHERE c.consume > 0 {room_where} GROUP BY c.user_id HAVING SUM(c.consume) >= ?
                )
            ''', total_params).fetchone()[0]
        else:
            params_all = []
            if room_id:
                params_all.append(room_id)
            params_all.extend([t, size, offset])
            total_params = tuple(params_all[:2])

            room_join = 'JOIN sessions s ON c.session_id = s.id' if room_id else ''

            rows = conn.execute(f'''
                SELECT c.user_id, c.user_name, SUM(c.consume) AS consume,
                       COALESCE(u.fans_club, '') AS fans_club,
                       COALESCE(u.grade, '') AS grade,
                       u.sec_uid, u.avatar_url, u.notes, u.tags,
                       (SELECT COUNT(DISTINCT c2.session_id) FROM contributions c2
                        WHERE c2.user_id = c.user_id AND c2.consume >= ?) AS sessions_count
                FROM contributions c
                {room_join}
                LEFT JOIN users u ON u.user_id = c.user_id
                WHERE c.consume > 0 {room_where}
                GROUP BY c.user_id HAVING SUM(c.consume) >= ?
                ORDER BY consume DESC LIMIT ? OFFSET ?
            ''', (t, *params_all)).fetchall()
            total = conn.execute(f'''
                SELECT COUNT(*) FROM (
                    SELECT c.user_id FROM contributions c
                    {room_join}
                    WHERE c.consume > 0 {room_where} GROUP BY c.user_id HAVING SUM(c.consume) >= ?
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

    # 补充财富等级和粉丝团（优先取最新弹幕中的值）
    if latest_chat:
        result['grade'] = latest_chat['grade'] or result.get('grade', '')
        if latest_chat['fans_club']:
            result['fans_club'] = latest_chat['fans_club']
    elif not result.get('grade'):
        result['grade'] = ''

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
    where = 'u.is_anonymous = 1'
    params = []
    if search:
        where += ' AND (u.user_name LIKE ? OR u.user_id LIKE ?)'
        params.extend([f'%{search}%', f'%{search}%'])
    total = conn.execute(f'SELECT COUNT(*) FROM users u WHERE {where}', params).fetchone()[0]
    rows = conn.execute(f'''
        SELECT u.user_id AS real_user_id, u.user_name,
               u.anonymous_label, u.grade, u.fans_club,
               COALESCE(SUM(c.consume), 0) AS consume,
               COALESCE(SUM(CASE WHEN c.qualified_1000 = 1 THEN 1 ELSE 0 END), 0) AS sessions_count,
               MAX(c.recorded_at) AS last_seen
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
        SELECT c.user_id, c.user_name, c.consume,
               COALESCE(
                   NULLIF(u.fans_club, ''),
                   NULLIF(c.fans_club, ''),
                   (SELECT fans_club FROM chat_logs WHERE user_id = c.user_id AND fans_club != '' ORDER BY id DESC LIMIT 1),
                   ''
               ) AS fans_club,
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


# ── PK 回合 ──────────────────────────────────────

def record_pk_round(session_id, data):
    """记录 PK 回合到数据库。

    Args:
        session_id: 场次 ID。
        data: PK 回合数据 dict（来自 _track_pk_round）。
    """
    if not session_id or not data:
        return
    try:
        conn = _get_conn()
        conn.execute('''
            INSERT INTO pk_rounds (session_id, start_time, end_time, duration_sec, mode,
                                   participants, participant_count, self_score, result)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            session_id,
            data.get('start_time', ''),
            data.get('end_time', ''),
            data.get('duration_sec', 0),
            data.get('mode', ''),
            str(data.get('participants', '')),
            data.get('participant_count', 0),
            data.get('self_score', 0),
            data.get('result', ''),
        ))
        conn.commit()
    except Exception as e:
        logger.warning(f"[DB] record_pk_round failed: {e}")


def query_pk_rounds(session_id=None, limit=50):
    """查询 PK 回合列表（含主播名称）。

    Args:
        session_id: 可选，按场次筛选。
        limit: 返回条数。

    Returns:
        list[dict]: PK 回合列表。
    """
    conn = _get_conn()
    if session_id:
        rows = conn.execute('''
            SELECT p.*, COALESCE(s.anchor_name, '') as anchor_name,
                   COALESCE(s.room_id, '') as room_id
            FROM pk_rounds p
            LEFT JOIN sessions s ON s.id = p.session_id
            WHERE p.session_id = ? ORDER BY p.id DESC LIMIT ?
        ''', (session_id, limit)).fetchall()
    else:
        rows = conn.execute('''
            SELECT p.*, COALESCE(s.anchor_name, '') as anchor_name,
                   COALESCE(s.room_id, '') as room_id
            FROM pk_rounds p
            LEFT JOIN sessions s ON s.id = p.session_id
            ORDER BY p.id DESC LIMIT ?
        ''', (limit,)).fetchall()
    return [dict(r) for r in rows]


def query_pk_contributors(pk_start, pk_end, session_id=None):
    """查询 PK 期间的贡献用户（从 gift_logs 按时间窗口匹配）。

    Args:
        pk_start: PK 开始时间字符串（如 "12:30:00"）。
        pk_end: PK 结束时间字符串。
        session_id: 可选，按场次筛选。

    Returns:
        list[dict]: 按贡献降序排列的用户列表。
    """
    conn = _get_conn()
    if session_id:
        rows = conn.execute('''
            SELECT user_id, user_name, COUNT(*) as gift_count,
                   SUM(gift_count) as total_count, SUM(diamond_total) as total_diamonds
            FROM gift_logs
            WHERE session_id = ? AND created_at >= ? AND created_at <= ?
            GROUP BY user_id ORDER BY total_diamonds DESC LIMIT 50
        ''', (session_id, pk_start, pk_end)).fetchall()
    else:
        rows = conn.execute('''
            SELECT user_id, user_name, COUNT(*) as gift_count,
                   SUM(gift_count) as total_count, SUM(diamond_total) as total_diamonds
            FROM gift_logs
            WHERE (created_at >= ? AND created_at <= ?)
            GROUP BY user_id ORDER BY total_diamonds DESC LIMIT 50
        ''', (pk_start, pk_end)).fetchall()
    return [dict(r) for r in rows]


def query_pk_detail(pk_id):
    """查询单个 PK 回合详情（含贡献榜）。

    Args:
        pk_id: PK 回合 ID。

    Returns:
        dict: PK 回合详情 + contributors 列表，或 None。
    """
    conn = _get_conn()
    row = conn.execute('''
        SELECT p.*, COALESCE(s.anchor_name, '') as anchor_name,
               COALESCE(s.room_id, '') as room_id
        FROM pk_rounds p
        LEFT JOIN sessions s ON s.id = p.session_id
        WHERE p.id = ?
    ''', (pk_id,)).fetchone()
    if not row:
        return None
    result = dict(row)
    # 查询贡献榜
    if result.get('start_time') and result.get('end_time'):
        sid = result.get('session_id')
        start = result['start_time']
        end = result['end_time']
        # 需要将 start/end 拼成完整 datetime
        # pk_rounds 存的是 "HH:MM:SS" 格式，需要从 session 获取日期
        s_row = conn.execute(
            "SELECT start_time FROM sessions WHERE id = ?", (sid,)
        ).fetchone()
        if s_row:
            s_date = str(s_row['start_time'])[:10]  # "2026-06-26"
            full_start = f"{s_date} {start}"
            full_end = f"{s_date} {end}"
            result['contributors'] = query_pk_contributors(full_start, full_end, sid)
        else:
            result['contributors'] = []
    else:
        result['contributors'] = []
    return result


def query_search(q, page=1, size=20):
    conn = _get_conn()
    offset = (page - 1) * size
    rows = conn.execute('''
        SELECT DISTINCT c.user_id, c.user_name,
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
            SELECT DISTINCT c.user_id, c.user_name,
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


# ── 消息完整性审计 ──────────────────────────────────

def query_audit():
    """分析数据库中的消息完整性，检测数据丢失、时间间隙等。

    Returns:
        dict: 审计结果，包含总体统计、场次分析、间隙检测等。
    """
    conn = _get_conn()
    now = time.time()
    result = {}

    # 1. 总体统计
    total_gifts = conn.execute('SELECT COUNT(*) FROM gift_logs').fetchone()[0]
    total_chats = conn.execute('SELECT COUNT(*) FROM chat_logs').fetchone()[0]
    total_sessions = conn.execute('SELECT COUNT(*) FROM sessions').fetchone()[0]
    total_users = conn.execute('SELECT COUNT(DISTINCT user_id) FROM gift_logs').fetchone()[0]
    total_diamonds = conn.execute('SELECT COALESCE(SUM(diamond_total), 0) FROM gift_logs').fetchone()[0]
    result['overview'] = {
        'total_gifts': total_gifts,
        'total_chats': total_chats,
        'total_sessions': total_sessions,
        'total_users': total_users,
        'total_diamonds': total_diamonds,
    }

    # 2. 场次列表（含礼物数、弹幕数、流失估计）
    sessions = conn.execute('''
        SELECT s.id, s.anchor_name, s.start_time, s.end_time, s.status,
               (SELECT COUNT(*) FROM gift_logs WHERE session_id = s.id) as gift_count,
               (SELECT COUNT(*) FROM chat_logs WHERE session_id = s.id) as chat_count,
               (SELECT COUNT(DISTINCT user_id) FROM contributions WHERE session_id = s.id) as user_count
        FROM sessions s ORDER BY s.id DESC LIMIT 50
    ''').fetchall()
    session_list = []
    for s in sessions:
        sd = dict(s)
        # 计算采集时长（秒）
        if sd['start_time']:
            try:
                st = datetime.strptime(str(sd['start_time'])[:19], '%Y-%m-%d %H:%M:%S')
                if sd['end_time']:
                    et = datetime.strptime(str(sd['end_time'])[:19], '%Y-%m-%d %H:%M:%S')
                    sd['duration_sec'] = int((et - st).total_seconds())
                else:
                    sd['duration_sec'] = int((datetime.now() - st).total_seconds())
            except Exception:
                sd['duration_sec'] = 0
        else:
            sd['duration_sec'] = 0
        sd['gifts_per_hour'] = round(sd['gift_count'] / max(sd['duration_sec'] / 3600, 0.01), 1)
        session_list.append(sd)
    result['sessions'] = session_list

    # 3. 时间间隙检测：检查每场次的消息间隔
    gaps = []
    gap_sessions = conn.execute('''
        SELECT id, anchor_name, start_time, end_time FROM sessions ORDER BY id DESC LIMIT 10
    ''').fetchall()
    for s in gap_sessions:
        sid = s['id']
        times = conn.execute('''
            SELECT created_at FROM gift_logs WHERE session_id = ? ORDER BY id ASC
        ''', (sid,)).fetchall()
        if len(times) < 2:
            continue
        prev = None
        session_gaps = []
        for t in times:
            curr = str(t['created_at'])
            if prev:
                try:
                    pt = datetime.strptime(str(prev)[:19], '%Y-%m-%d %H:%M:%S')
                    ct = datetime.strptime(str(curr)[:19], '%Y-%m-%d %H:%M:%S')
                    gap_sec = int((ct - pt).total_seconds())
                    if gap_sec >= 5:
                        session_gaps.append({'from': str(prev)[11:19], 'to': str(curr)[11:19], 'gap': gap_sec})
                except Exception:
                    pass
            prev = curr
        if session_gaps:
            gaps.append({
                'session_id': sid,
                'anchor': s['anchor_name'] or str(sid),
                'total_gaps': len(session_gaps),
                'max_gap': max(g['gap'] for g in session_gaps),
                'total_gap_seconds': sum(g['gap'] for g in session_gaps),
                'gaps': session_gaps[:10],  # 只展示前10个间隙
            })
    result['gaps'] = gaps

    # 4. 去重健康状态
    dd = get_dedup_stats()
    reject_rate = round(dd['rejected'] / max(dd['raw'], 1) * 100, 2)
    result['dedup_health'] = {
        'raw': dd['raw'],
        'passed': dd['passed'],
        'rejected': dd['rejected'],
        'reject_rate': reject_rate,
        'delta_zero': dd['delta_zero'],
        'out_of_order': dd['out_of_order'],
        'combo_block': dd['combo_block'],
        'rc1_dup': dd['rc1_dup'],
        'counter_reset': dd['counter_reset'],
    }

    # 5. 异常检测：可疑的用户名（含控制字符）
    bad_names = conn.execute('''
        SELECT user_id, user_name FROM users
        WHERE user_name LIKE '2@%' OR instr(user_name, x'02') > 0 OR instr(user_name, x'03') > 0
        LIMIT 20
    ''').fetchall()
    result['corrupted_names'] = [dict(r) for r in bad_names]

    return result

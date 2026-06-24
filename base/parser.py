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
import threading
import time

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
    get_user_id, fmt_grade, fmt_fans_club,
)

logger = logging.getLogger(__name__)

# ── 贡献用户追踪（1000贡献用户列表）────────────────
# 从所有消息的 public_area_common.user_consume_in_room 提取
# 该字段记录了用户在当前房间的累计消费钻石数
_contribution_cache = {}        # user_id -> {'nick': str, 'consume': int, 'time': str}
_contribution_lock = threading.Lock()
_flushed_users = set()          # 已写入 CSV 的 user_id（防止重复）


# ── 贡献用户追踪 ────────────────────────────────

def track_contribution(user_id, user_name, consume_in_room, msg_time=None, fans_club='', grade=''):
    """更新用户的房间贡献值（累计消费钻石）。

    从 public_area_common.user_consume_in_room 字段获取，
    该字段是服务端计算的用户在房间的总消费，比累加礼物更准确。

    Args:
        user_id: 用户 ID 字符串。
        user_name: 用户昵称。
        consume_in_room: 用户在房间的累计消费钻石数。
        msg_time: 消息时间戳。
        fans_club: 粉丝团名称/等级。
        grade: 财富等级。
    """
    if not user_id or not consume_in_room:
        return
    with _contribution_lock:
        prev = _contribution_cache.get(user_id, {})
        if consume_in_room > prev.get('consume', 0):
            _contribution_cache[user_id] = {
                'user_id': user_id,
                'nick': user_name or prev.get('nick', ''),
                'consume': consume_in_room,
                'time': msg_time or time.strftime('%H:%M:%S'),
                'fans_club': fans_club or prev.get('fans_club', ''),
                'grade': grade or prev.get('grade', ''),
            }


def get_contribution_users():
    """获取当前已知的贡献用户列表。

    Returns:
        list[dict]: 按 consume 降序排列的用户列表。
    """
    with _contribution_lock:
        users = list(_contribution_cache.values())
    users.sort(key=lambda x: -x['consume'])
    return users


def get_contribution_count(threshold=1000):
    """获取贡献值超过阈值的用户数。

    Args:
        threshold: 阈值，默认 1000（1000贡献用户）。

    Returns:
        int: 符合条件的用户数。
    """
    with _contribution_lock:
        return sum(1 for u in _contribution_cache.values() if u['consume'] >= threshold)


def flush_contribution_csv(csv_path=None, threshold=1000):
    """将贡献用户数据写入 CSV（仅写入超过阈值的用户，自动去重）。

    每个 user_id 只写入一次，后续更新不再重复写入。
    Args:
        csv_path: CSV 文件路径。为 None 时使用默认路径。
        threshold: 贡献值阈值，默认 1000（1000贡献用户）。
    """
    if not csv_path:
        return
    import csv as _csv
    import os as _os
    users = get_contribution_users()
    if not users:
        return
    fields = ['time', 'user_name', 'user_id', 'rank', 'fans_club', 'consume', 'source']

    # 首次运行：读取已存在的 CSV，初始化 _flushed_users
    global _flushed_users
    if not _flushed_users and _os.path.exists(csv_path):
        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                for row in _csv.DictReader(f):
                    uid = row.get('user_id', '') or row.get('user_name', '')
                    if uid:
                        _flushed_users.add(uid)
        except Exception:
            _flushed_users.clear()

    filtered = [u for u in users if u['consume'] >= threshold]
    # 去重：已写入的 user_id 不再写入；nickname 仅在没有 user_id 时去重
    new_users = []
    for u in filtered:
        uid = u.get('user_id', '')
        nick = u.get('nick', '')
        if uid:
            if uid in _flushed_users:
                continue  # 已写入过 ID，跳过
            # 如果有 user_id，移除旧的 nick-only 记录
            if nick and nick in _flushed_users:
                _flushed_users.discard(nick)
        else:
            if nick and nick in _flushed_users:
                continue  # 仅昵称且已写入过，跳过
        new_users.append(u)
        _flushed_users.add(uid or nick)
    if not new_users:
        return

    try:
        is_new = not _os.path.exists(csv_path)
        with open(csv_path, 'a', encoding='utf-8-sig', newline='') as f:
            writer = _csv.DictWriter(f, fieldnames=fields)
            if is_new:
                writer.writeheader()
            for rank, u in enumerate(new_users, 1):
                uid = u.get('user_id', '')
                writer.writerow({
                    'time': u.get('time', ''),
                    'user_name': u.get('nick', ''),
                    'user_id': uid,
                    'rank': rank,
                    'fans_club': u.get('fans_club', ''),
                    'consume': u.get('consume', 0),
                    'source': 'websocket',
                })
    except Exception:
        pass


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
    'rc1_dup': 0,
}
_dedup_csv_path = None
_dedup_csv_lock = threading.Lock()


def set_dedup_csv_dir(data_dir):
    """设置去重诊断 CSV 的输出目录。由 fetcher 在 DataRecorder.open() 后调用。"""
    global _dedup_csv_path, _rejected_csv_path, _trace_shadow_csv_path
    import os
    _dedup_csv_path = os.path.join(data_dir, 'dedup_stats.csv')
    with open(_dedup_csv_path, 'w', encoding='utf-8') as f:
        f.write('time,raw,passed,rejected,repeat_zero,combo_block,counter_reset,out_of_order,delta_zero,rc1_dup\n')
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
                f.write(f"{ts},{stats['raw']},{stats['passed']},{stats['rejected']},{stats['repeat_zero']},{stats['combo_block']},{stats['counter_reset']},{stats['out_of_order']},{stats['delta_zero']},{stats['rc1_dup']}\n")
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
    # 追踪房间贡献值
    if msg.public_area_common:
        track_contribution(uid, user.nick_name, msg.public_area_common.user_consume_in_room,
                          fans_club=fmt_fans_club(user), grade=fmt_grade(user))
    common = {
        'time': time.strftime('%H:%M:%S'),
        'user_id': uid,
        'douyin_id': user.display_id,
        'user_name': user.nick_name,
        'gender': {1: "男", 2: "女"}.get(user.gender, "未知"),
        'grade': fmt_grade(user),
        'fans_club': fmt_fans_club(user),
    }

    results = []
    if msg.chat_by == 9:  # 福袋口令
        if enable_outputs.get('lucky_bag', True):
            results.append({
                'type': 'lucky_bag',
                'msg': f"[福袋口令] {user.nick_name}[{uid}] 内容:{msg.content}",
                'data': {**common, 'content': msg.content},
            })
    else:  # 普通聊天
        if enable_outputs.get('chat', True):
            results.append({
                'type': 'chat',
                'msg': f"[聊天] {user.nick_name}[{uid}] 内容:{msg.content}",
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

    # delta 法去重：只统计本次消息新增的礼物数量
    rc = msg.repeat_count or 0
    gid = str(msg.group_id) if msg.group_id else '0'
    gft_id = gift.id or 0
    log_id = msg.log_id or ''
    trace_id = msg.trace_id or ''
    send_time = msg.send_time or 0
    cnt, reason = _compute_gift_count(gid, gft_id, uid, rc, repeat_end=msg.repeat_end or 0)

    # 追踪贡献值：优先用 user_consume_in_room（服务端累计值），否则累加本次礼物钻石 × delta
    if cnt > 0:
        _consume = 0
        if msg.public_area_common:
            _consume = msg.public_area_common.user_consume_in_room or 0
        if not _consume:
            dia = gift.diamond_count or 0
            _consume = dia * cnt  # 使用 cnt(delta) 而非 raw repeat_count
        if _consume:
            track_contribution(uid, user.nick_name, _consume,
                              fans_club=fmt_fans_club(user), grade=fmt_grade(user))

    # 定期清理过期状态（长连击时，约每 5 分钟一次）
    if rc > 0 and (rc % 100) == 0:
        _prune_dedup_state()
    # 每 500 条原始礼物消息写入诊断快照（不依赖 combo 长度）
    if _dedup_diag['raw'] % 500 == 0:
        _flush_dedup_stats()

    # ── trace_id 影子去重：记录"如果只用 trace_id 会怎样" ──
    trace_seen = False
    with _trace_shadow_lock:
        if trace_id:
            if trace_id in _seen_trace_ids:
                trace_seen = True
            else:
                _seen_trace_ids.add(trace_id)

    if cnt <= 0:
        _dedup_diag['rejected'] += 1
        _write_rejected_gift(gid, gft_id, gift.name, gift.diamond_count, uid, user.nick_name, rc, reason,
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
        _write_trace_shadow(uid, user.nick_name, gift.name, gift.diamond_count, rc, trace_id, cur_dec, trace_dec)
        return []  # 重复消息或增量为零，跳过
    _dedup_diag['passed'] += 1

    # 影子记录：当前接受，trace 判断
    cur_dec = 'accept'
    trace_dec = 'reject_dup' if trace_seen else 'accept'
    _write_trace_shadow(uid, user.nick_name, gift.name, gift.diamond_count, rc, trace_id, cur_dec, trace_dec)

    # ── 甄选礼盒/复合礼物：从 diy_item_info 提取真实礼物 ID ──
    # 甄选AI分身礼物邮箱等复合礼物，外层 gift.diamond_count 只有容器价(99)。
    # 真实礼物在 field 27 (diy_item_info) JSON 中，格式：
    # [{"diy_item_id":10002,...}, {"diy_item_id":10003,"values":{"gift_id":"789",...}}]
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

    # 定价优先级：复合礼物 gift_id 反查 > 静态覆盖表 > protobuf diamond_count
    if composite_price > 0:
        unit_price = composite_price
        display_name = composite_name or gift.name  # 用真实礼物名替换容器名
    else:
        unit_price = _GIFT_PRICE_OVERRIDE.get(gift.name, gift.diamond_count)
        display_name = gift.name
    total_value = unit_price * cnt
    diamond_info = f" ({total_value}钻石)" if total_value > 0 else ""
    return [{
        'type': 'gift',
        'msg': f"[礼物] {user.nick_name}[{uid}] 礼物:{display_name} x{cnt}{diamond_info}",
        'data': {
            'time': time.strftime('%H:%M:%S'),
            'user_id': uid,
            'douyin_id': user.display_id,
            'user_name': user.nick_name,
            'gender': {1: "男", 2: "女"}.get(user.gender, "未知"),
            'gift_name': display_name,
            'gift_count': cnt,
            'diamond_total': total_value,
            'grade': fmt_grade(user),
            'fans_club': fmt_fans_club(user),
            'group_id': gid,
            'raw_repeat_count': msg.repeat_count,
            'raw_repeat_end': msg.repeat_end,
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

    # 轻礼物也计入贡献（虽然没有 user_name，但有 user_id）
    if total_value > 0:
        track_contribution(uid, '', total_value)

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
        'msg': f"[点赞] {user.nick_name}[{uid}] 点赞:{msg.count}个, 累计{msg.total}赞",
        'data': {
            'time': time.strftime('%H:%M:%S'),
            'user_id': uid, 'user_name': user.nick_name,
            'count': msg.count, 'total': msg.total,
            'grade': fmt_grade(user), 'fans_club': fmt_fans_club(user),
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
    # 追踪房间贡献值
    if msg.public_area_common:
        track_contribution(uid, user.nick_name, msg.public_area_common.user_consume_in_room,
                          fans_club=fmt_fans_club(user), grade=fmt_grade(user))
    gender = {0: "未知", 1: "男", 2: "女"}.get(user.gender, "未知")
    extras = f" (直播间人数:{msg.member_count})" if msg.member_count else ""
    return [{
        'type': 'member',
        'msg': f"[进场] {user.nick_name}[{uid}][{gender}] 进入了直播间{extras}",
        'data': {
            'time': time.strftime('%H:%M:%S'),
            'user_id': uid, 'user_name': user.nick_name, 'gender': gender,
            'grade': fmt_grade(user), 'fans_club': fmt_fans_club(user),
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
    # 追踪房间贡献值
    if msg.public_area_common:
        track_contribution(uid, user.nick_name, msg.public_area_common.user_consume_in_room,
                          fans_club=fmt_fans_club(user), grade=fmt_grade(user))
    action = {1: "关注了主播", 2: "分享了直播间"}.get(msg.action, "互动")
    follow = f"(第{msg.follow_count}个关注)" if msg.follow_count else ""
    return [{
        'type': 'social',
        'msg': f"[关注/分享] {user.nick_name}[{uid}] {action} {follow}",
        'data': {
            'time': time.strftime('%H:%M:%S'),
            'user_id': uid, 'user_name': user.nick_name, 'action': action,
            'follow_count': msg.follow_count or '',
            'grade': fmt_grade(user), 'fans_club': fmt_fans_club(user),
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

    # ── 贡献用户排行（1000贡献用户列表 Top N） ──
    if enable_outputs.get('contribution', True):
        all_contributors = list(msg.ranks_list or []) + list(msg.seats_list or [])
        if all_contributors:
            seen_uids = set()
            for c in all_contributors:
                uid = get_user_id(c.user) if c.user else ''
                if uid in seen_uids:
                    continue
                seen_uids.add(uid)
                nick = c.user.nick_name if c.user else ''
                rank_pos = c.rank or 0
                # 同时加入贡献缓存（确保 user_id 被记录）
                _g = fmt_grade(c.user) if c.user else ''
                _f = fmt_fans_club(c.user) if c.user else ''
                track_contribution(uid, nick, c.score or 0, fans_club=_f, grade=_g)
                results.append({
                    'type': 'contribution',
                    'msg': f"[贡献] #{rank_pos} {nick}[{uid}]",
                    'data': {
                        'time': time.strftime('%H:%M:%S'),
                        'rank_pos': rank_pos,
                        'user_id': uid,
                        'user_name': nick,
                        'score': c.score or 0,
                        'grade': _g,
                        'fans_club': _f,
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
        'msg': f"[粉丝团] {user.nick_name}[{uid}] {t}: {msg.content}",
        'data': {
            'time': time.strftime('%H:%M:%S'),
            'user_id': uid, 'user_name': user.nick_name,
            'type': t, 'content': msg.content,
            'grade': fmt_grade(user), 'fans_club': fmt_fans_club(user),
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
        'msg': f"[表情] {user.nick_name}[{uid}]: {content}",
        'data': {
            'time': time.strftime('%H:%M:%S'),
            'user_id': uid, 'user_name': user.nick_name,
            'emoji_id': msg.emoji_id, 'content': content,
            'grade': fmt_grade(user), 'fans_club': fmt_fans_club(user),
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
    strings = _extract_proto_strings(payload)

    # 过滤噪音
    noise_patterns = ('勋章', '粉丝团', 'http', 'png', 'douyin', 'webcast', 'img/')
    strings = [s for s in strings
               if not any(p in s for p in noise_patterns)
               and s not in ('小葵花', '送{0}', '')]

    # 提取 douyin_id
    douyin_id = _extract_douyin_id(payload)

    # ── 会员订阅: "{0:user} {1:string}了{2:string}会员" ──
    if '{0:user} {1:string}了{2:string}会员' in strings:
        user = ''
        action = ''
        sub_type = ''
        for s in strings:
            if s in ('{0:user} {1:string}了{2:string}会员', 'subscribe_anchor_mvp_v2', '{0:user} 送出{1:string}{2:image} {3:string}'):
                continue
            if s in ('开通', '续费'):
                action = s
            elif s in ('月度', '季度', '年度'):
                sub_type = s
            elif s and any(ord(c) > 127 for c in s) and not s.startswith('{') and len(s) <= 30:
                user = s.lstrip('"').rstrip('" ')
        if action and sub_type:
            return {'event': '会员', 'action': action, 'type': sub_type, 'user': user, 'douyin_id': douyin_id}

    # ── 星守护: "恭喜 {0:user} 成为星守护" ──
    if '恭喜 {0:user} 成为星守护' in strings:
        user = ''
        for s in strings:
            if s in ('恭喜 {0:user} 成为星守护', 'subscribe_anchor_mvp_v2'):
                continue
            if s and any(ord(c) > 127 for c in s) and not s.startswith('{') and len(s) <= 30:
                user = s.lstrip('"').rstrip('" ')
        if user:
            return {'event': '星守护', 'action': '开通', 'type': '月度', 'user': user, 'douyin_id': douyin_id}

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
            'msg': f"[排行榜] 第{i}名: {r.user.nick_name} 积分:{r.score_str}",
            'data': {
                'time': time.strftime('%H:%M:%S'),
                'rank_pos': i,
                'user_id': uid,
                'douyin_id': r.user.display_id,
                'user_name': r.user.nick_name,
                'score': r.score_str,
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
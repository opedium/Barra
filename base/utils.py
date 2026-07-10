"""基础工具：配置加载、Cookie 解析、常量定义、格式化、ID 生成。

本模块是项目的共享基础层，被 service/ 和 base/ 其他模块共同依赖。
抖音 API 参数（APP_ID、VERSION_CODE 等）集中在此维护，更新时只需改一处。
"""

import os
import random
import re
import threading
import time

import yaml


# ── 常量 ──────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

# ── 签名 & API 共享参数 ───────────────────────────
# 抖音 Web 端参数，签名和 WebSocket URL 共用。
# 抖音版本更新时只需修改这里。

APP_ID = '6383'                  # 抖音 Web 端应用 ID
LIVE_ID = '1'                    # 直播类型标识（1 = 普通直播）
VERSION_CODE = '180800'          # 客户端版本号（对应 18.08.00）
WEBCAST_SDK_VERSION = '1.0.15'   # WebCast SDK 版本，签名和 WS URL 须一致
DID_RULE = '3'                   # 设备 ID 生成规则版本（3 = 当前线上版本）
DEVICE_PLATFORM = 'web'          # 平台标识

# 低频/低价值消息类型，仅计数不解析
LOW_VALUE_TYPES = frozenset({
    'WebcastChatLikeMessage', 'WebcastResidentGuestMessage',
    'WebcastLowPcuGuideMessage', 'WebcastCommonDotMessage',
    'WebcastGiftUpdateMessage', 'WebcastInRoomBannerMessage',
    'WebcastNotifyEffectMessage', 'WebcastHotRoomMessage',
    # 系统推送/低频消息，不解析不报警
    'WebcastActivityEmojiGroupsMessage',
    'WebcastAnchorLinkmicSilenceMessage',
    'WebcastAssetMessage',
    'WebcastAssetEffectUtilMessage',
    'WebcastAudioBgImgMessage',
    'WebcastBackupSeiMessage',
    'WebcastBattleEffectContainerMessage',
    'WebcastBattleRankSeasonMessage',
    'WebcastBattleTeamTaskMessage',
    'WebcastCommonToastMessage',
    'WebcastExhibitionChatMessage',
    'WebcastGiftPlayEventMessage',
    'WebcastGiftSortMessage',
    'WebcastHighlightCommentMessage',
    'WebcastHotChatMessage',
    'WebcastInteractEffectMessage',
    'WebcastLinkMessage',
    'WebcastLinkSettingNotifyMessage',
    'WebcastLotteryDrawResultEventMessage',
    'WebcastLotteryEventNewMessage',
    'WebcastLuckyBoxMessage',
    'WebcastLuckyBoxEndMessage',
    'WebcastLuckyBoxRewardMessage',
    'WebcastLuckyBoxTempStatusMessage',
    'WebcastPrivilegeScreenChatMessage',
    'WebcastPrizeNoticeMessage',
    'WebcastProfitGameStatusMessage',
    'WebcastProfitInteractionScoreMessage',
    'WebcastRanklistAwardMessage',
    'WebcastRanklistHourEntranceMessage',
    'WebcastRoomCommentTopicMessage',
    'WebcastRoomDataSyncMessage',
    'WebcastRoomNotifyMessage',
    'WebcastScreenChatMessage',
    'WebcastToastMessage',
    'WebcastTopEffectMessage',
})

# 交互类消息，用于"等待开播"模式判断直播间是否活跃
INTERACTIVE_TYPES = frozenset({
    'WebcastChatMessage', 'WebcastGiftMessage', 'WebcastLikeMessage',
    'WebcastMemberMessage', 'WebcastSocialMessage', 'WebcastFansclubMessage',
    'WebcastEmojiChatMessage',
})

# 用户真实互动消息类型（用于看门狗检测业务静默）
# 仅追踪弹幕和礼物 — 有任意一条即视为"直播间有用户活跃"
USER_MSG_TYPES = frozenset({
    'WebcastChatMessage', 'WebcastGiftMessage', 'WebcastLightGiftMessage',
})

# WebSocket method → output config key 映射
# strip('Webcast','Message').lower() 后与 config key 不一致的特殊映射
METHOD_TO_CONFIG = {
    'WebcastChatMessage':                 'chat',
    'WebcastGiftMessage':                 'gift',
    'WebcastLightGiftMessage':            'light_gift',
    'WebcastLikeMessage':                 'like',
    'WebcastMemberMessage':               'member',
    'WebcastSocialMessage':               'social',
    'WebcastRoomUserSeqMessage':          'stats',
    'WebcastFansclubMessage':             'fansclub',
    'WebcastControlMessage':              'control',
    'WebcastEmojiChatMessage':            'emoji',
    'WebcastRoomStatsMessage':            'roomstats',
    'WebcastRoomMessage':                 'room',
    'WebcastRoomRankMessage':             'rank',
    'WebcastRoomStreamAdaptationMessage': 'control',  # 无独立 config，归入 control

    # PK/连麦消息
    'WebcastLinkmicBattleMethod':              'pk_event',
    'WebcastLinkmicBattleMethodMessage':       'pk_event',
    'WebcastLinkMicBattleMethod':              'pk_event',
    'WebcastLinkMicBattleMethodMessage':       'pk_event',
    'WebcastLinkmicBattleFinishMethod':        'pk_event',
    'WebcastLinkmicBattleFinishMethodMessage': 'pk_event',
    'WebcastLinkMicBattleFinishMethod':        'pk_event',
    'WebcastLinkMicBattleFinishMethodMessage': 'pk_event',
    'WebcastLinkmicArmiesMethod':              'pk_event',
    'WebcastLinkmicArmiesMethodMessage':       'pk_event',
    'WebcastLinkmicPlayModeUpdateScoreMessage': 'pk_event',
    'WebcastLinkerContributeMessage':          'pk_event',
    'WebcastBattleAuxiliaryMessage':           'pk_event',
    'WebcastBattleEndPunishMessage':           'pk_event',
    'WebcastBattlePowerContainerMessage':      'pk_event',
}

SCRIPT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_MIN_UA_SWITCH_INTERVAL = 8       # UA 切换最小间隔（秒），防止频繁切换触发风控
_ua_switch_lock = threading.Lock()
_last_ua_switch_time = 0.0


# ── 配置加载 ──────────────────────────────────────

def load_config(config_file, default_config):
    """加载 YAML 配置文件，与默认配置做浅合并。

    字典类型的配置项（如 output、network）做一层嵌套合并，
    非字典类型直接覆盖。文件不存在时返回默认配置。

    Args:
        config_file: 配置文件路径（相对路径相对于项目根目录）。
        default_config: 默认配置字典。

    Returns:
        合并后的配置字典。
    """
    if not os.path.isabs(config_file):
        config_file = os.path.join(SCRIPT_DIR, config_file)

    if not os.path.exists(config_file):
        base = os.path.splitext(config_file)[0]
        for ext in ['.yaml', '.yml']:
            alt = base + ext
            if os.path.exists(alt):
                config_file = alt
                break

    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            user_cfg = yaml.safe_load(f.read()) or {}
        cfg = dict(default_config)
        for k, v in user_cfg.items():
            if isinstance(v, dict) and isinstance(cfg.get(k), dict):
                cfg[k] = {**cfg[k], **v}
            else:
                cfg[k] = v
        return cfg
    except (FileNotFoundError, yaml.YAMLError) as e:
        print(f"配置加载失败({e})，使用默认配置")
        return dict(default_config)


def load_cookies(cookie_file, script_dir=''):
    """加载 Cookie 文件，自动识别三种格式。

    支持格式：
    - 浏览器导出：name1=value1; name2=value2
    - 每行一个：name1=value1（多行）
    - Netscape cookie jar：带 tab 分隔的 7 列格式

    Args:
        cookie_file: Cookie 文件路径。
        script_dir: 相对路径的基准目录（为空时使用项目根目录）。

    Returns:
        {cookie_name: cookie_value} 字典，文件不存在时返回空字典。
    """
    if not os.path.isabs(cookie_file):
        cookie_file = os.path.join(script_dir, cookie_file)
    if not os.path.exists(cookie_file):
        return {}

    try:
        with open(cookie_file, 'r', encoding='utf-8') as f:
            content = f.read().strip()
    except Exception:
        return {}
    if not content:
        return {}

    cookies = {}
    lines = content.splitlines()
    is_netscape = any(line.count('\t') >= 6 and not line.startswith('#') for line in lines[:10])

    if is_netscape:
        for line in lines:
            line = line.strip()
            if not line:
                continue
            # Handle #HttpOnly_ prefix (Netscape format for secure cookies)
            if line.startswith('#HttpOnly_'):
                line = line[len('#HttpOnly_'):]
            elif line.startswith('#'):
                continue
            parts = line.split('\t')
            if len(parts) >= 7:
                name, value = parts[5].strip(), parts[6].strip()
                if name:
                    cookies[name] = value
    else:
        content = content.replace('\n', ';').replace('\r', '')
        for item in content.split(';'):
            item = item.strip()
            if not item or '=' not in item:
                continue
            name, value = item.split('=', 1)
            if name.strip():
                cookies[name.strip()] = value.strip()
    return cookies


# ── 配置写回 ──────────────────────────────────────

_config_write_lock = threading.RLock()


def update_room_name_in_config(room_id, anchor_name, rooms_file='rooms.txt'):
    """更新或添加 rooms.txt 中的房间记录。

    线程安全：通过可重入锁防止多房间并发写入。
    - 房间已存在：更新主播名
    - 房间不存在：追加到文件末尾
    - 文件不存在：创建文件并写入

    Args:
        room_id: 直播间 ID。
        anchor_name: 主播昵称。
        rooms_file: 房间文件路径（相对于项目根目录）。
    """
    if not anchor_name:
        return
    if not os.path.isabs(rooms_file):
        rooms_file = os.path.join(SCRIPT_DIR, rooms_file)

    with _config_write_lock:
        try:
            if not os.path.exists(rooms_file):
                with open(rooms_file, 'w', encoding='utf-8') as f:
                    f.write(f'{room_id},{anchor_name}\n')
                return

            with open(rooms_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            updated = False
            found = False
            new_lines = []

            for line in lines:
                stripped = line.strip()
                if not stripped:
                    new_lines.append(line)
                    continue

                prefix = ''
                content = stripped
                if stripped.startswith('#'):
                    prefix = '#'
                    content = stripped[1:].strip()

                if not content:
                    new_lines.append(line)
                    continue

                parts = content.split(',', 1)
                if parts[0].strip() == room_id:
                    indent = re.match(r'^(\s*)', line).group(1) if re.match(r'^(\s*)', line) else ''
                    new_lines.append(f'{indent}{prefix}{room_id},{anchor_name}\n')
                    updated = True
                    found = True
                else:
                    new_lines.append(line)

            if not found:
                if new_lines and not new_lines[-1].endswith('\n'):
                    new_lines.append('\n')
                new_lines.append(f'{room_id},{anchor_name}\n')
                updated = True

            if updated:
                import tempfile
                import shutil

                fd, temp_path = tempfile.mkstemp(suffix='.txt', dir=os.path.dirname(rooms_file))
                try:
                    with os.fdopen(fd, 'w', encoding='utf-8') as f:
                        f.writelines(new_lines)
                    shutil.move(temp_path, rooms_file)
                except Exception:
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                    raise

        except Exception as e:
            try:
                logger = logging.getLogger(__name__)
                logger.error(f"[配置] 更新主播名失败：room_id={room_id}, error={e}")
            except Exception:
                pass


# ── 工具函数 ──────────────────────────────────────

def generate_user_unique_id():
    """生成随机用户唯一 ID，用于 WebSocket 连接标识。

    Returns:
        18~19 位随机数字字符串。
    """
    return str(random.randint(10**18, 10**19 - 1))


def generate_ms_token(length=182):
    """生成随机 msToken 字符串，用于 HTTP 请求参数。

    Args:
        length: token 主体长度（不含末尾 '=_' 后缀）。

    Returns:
        指定长度的随机字符串 + '=_' 后缀。
    """
    charset = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+='
    return ''.join(random.choice(charset) for _ in range(length)) + '=_'


def extract_ua_version(ua: str) -> str:
    """从 User-Agent 字符串中提取 Chrome 版本号。

    Args:
        ua: 完整的 User-Agent 字符串。

    Returns:
        'Chrome/x.x.x.x' 格式的版本字符串，无法匹配时返回默认值。
    """
    m = re.search(r'Chrome/(\d+\.\d+\.\d+\.\d+)', ua)
    return f"Chrome/{m.group(1)}" if m else "Chrome/132.0.0.0"


# ── 主播名称映射（供 fmt_fans_club 使用）──
# 解析线程（msg-processor）可能同时处理多个房间的消息，线程名无法区分。
# 改用 thread-local 变量，由 fetcher 的 _process_item 在处理每条消息前设置。
_thread_fans = threading.local()

def set_current_anchor(name):
    """设置当前处理消息的主播名（由 fetcher 在处理每条消息前调用）。"""
    _thread_fans.anchor = name

def get_current_anchor():
    """获取当前处理消息的主播名。"""
    return getattr(_thread_fans, 'anchor', '')


# 仍然保留 live_id → name 映射，供 set_current_anchor 查询
_anchor_names = {}

def set_anchor_name(live_id, name):
    """注册直播间对应的主播名。由 fetcher 在获取房间信息后调用。"""
    if name and live_id:
        _anchor_names[live_id] = name


def _get_anchor_name():
    """获取当前主播名。优先使用 thread-local，回退到线程名推断。"""
    name = get_current_anchor()
    if name:
        return name
    import threading
    tname = threading.current_thread().name
    if tname.startswith('room-'):
        return _anchor_names.get(tname[5:], '')
    return ''


def fmt_fans_club(user):
    """格式化用户的粉丝团信息为显示字符串，支持多粉丝团。

    Args:
        user: protobuf User 对象。

    Returns:
        粉丝团标签字符串，多个以空格分隔。格式为：
        '[粉丝团:名称 Lv等级]'，当前主播的粉丝团排在首位，
        无粉丝团时返回空字符串。
    """
    try:
        all_clubs = []
        host = _get_anchor_name()

        def _fmt(name, level):
            return f"{name} Lv{level}"

        # 收集所有粉丝团（去重，保留最高等级）
        seen = {}  # club_name → fmt_string
        data = user.fans_club.data
        if data and data.level > 0:
            cn = data.club_name or host or ''
            if cn:
                seen[cn] = seen.get(cn, _fmt(cn, data.level))
        pd = getattr(user.fans_club, 'prefer_data', None)
        if pd:
            try:
                keys = list(pd.keys()) if hasattr(pd, 'keys') else []
                for key in keys:
                    club = pd.get(key) if hasattr(pd, 'get') else pd[key]
                    if club and club.level > 0 and club.club_name:
                        extant = seen.get(club.club_name)
                        if not extant:
                            seen[club.club_name] = _fmt(club.club_name, club.level)
                        else:
                            # 同名称保留最高等级
                            import re as _re
                            m = _re.search(r'Lv(\d+)', extant)
                            if m and int(m.group(1)) < club.level:
                                seen[club.club_name] = _fmt(club.club_name, club.level)
            except (AttributeError, TypeError):
                pass

        # 按顺序输出：当前主播的粉丝团排首位，其余保持稳定
        all_clubs = list(seen.values())
        host_entry = seen.pop(host, None) if host else None
        if host_entry:
            all_clubs = [host_entry] + [v for v in all_clubs if v != host_entry]
        return ' '.join(all_clubs)
    except (AttributeError, TypeError):
        pass
    return ''


def get_fans_club_anchor_id(user):
    """Extract the anchor_id from the user's primary fans club data.

    Returns the streamer's user ID string, or empty string if no fans club.
    """
    try:
        data = user.fans_club.data
        if data and data.anchor_id:
            return str(data.anchor_id)
    except (AttributeError, TypeError):
        pass
    return ''


def fmt_grade(user):
    """格式化用户的消费等级为显示字符串。

    Args:
        user: protobuf User 对象。

    Returns:
        '[等级N]' 格式字符串，等级为 0 或缺失时返回空字符串。
    """
    try:
        if user.pay_grade and user.pay_grade.level > 0:
            return f"[等级{user.pay_grade.level}]"
    except (AttributeError, TypeError):
        pass
def safe_time(ts):
    """安全地将 Unix 时间戳格式化为 'HH:MM:SS'。

    Args:
        ts: Unix 时间戳（秒）。

    Returns:
        'HH:MM:SS' 格式的时间字符串，时间戳无效时返回空字符串。
    """
    try:
        if ts > 0:
            return time.strftime('%H:%M:%S', time.localtime(ts))
    except (OSError, ValueError):
        pass
    return ''


def rotate_ua(current_ua):
    """重连时切换 User-Agent，降低风控风险。

    两次切换间隔不足 _MIN_UA_SWITCH_INTERVAL 秒时跳过，
    避免重连密集期频繁切换反而触发异常检测。

    线程安全：多实例并发时通过锁保护全局切换时间。

    Args:
        current_ua: 当前使用的 User-Agent 字符串。

    Returns:
        (新 UA 字符串, 新 UA 版本字符串) 元组。
    """
    global _last_ua_switch_time
    with _ua_switch_lock:
        now = time.time()
        if now - _last_ua_switch_time < _MIN_UA_SWITCH_INTERVAL:
            return current_ua, extract_ua_version(current_ua)
        candidates = [u for u in USER_AGENTS if u != current_ua]
        if not candidates:
            return current_ua, extract_ua_version(current_ua)
        new_ua = random.choice(candidates)
        _last_ua_switch_time = now
        return new_ua, extract_ua_version(new_ua)


def get_user_id(user):
    """获取用户 ID 字符串，优先使用 id_str（大数精度更高）。

    Args:
        user: protobuf User 对象。

    Returns:
        用户 ID 字符串。
    """
    s = user.id_str
    return s if s else str(user.id)


def get_user_sec_uid(user):
    """从 User protobuf 中提取 sec_uid。

    sec_uid 是抖音用户的永久标识符（~50位字符串），可跨 session 追踪同一用户，
    也是调用抖音公开 API 获取用户信息的必需参数。

    Args:
        user: protobuf User 对象。

    Returns:
        sec_uid 字符串，缺失时返回空字符串。
    """
    try:
        su = user.sec_uid
        return su if su else ''
    except (AttributeError, TypeError):
        return ''


def sanitize_username(raw_name, display_id=''):
    """清理并验证用户昵称。

    部分 protobuf 消息中的 nick_name 可能含控制字符（如 \\x02 标记字节
    泄漏到字符串中），导致多个用户显示为相同的损坏名称（如 "2@\\x02"）。
    此函数会：
    1. 去除 ASCII 控制字符（0x00-0x1f，保留 0x20+ 的可见字符）
    2. 如果清理后为空或明显损坏，回退到 display_id
    3. 去首尾空白

    Args:
        raw_name: protobuf User.nick_name 的原始值。
        display_id: User.display_id 备用字段。

    Returns:
        清理后的用户显示名。
    """
    if not raw_name:
        if display_id:
            return str(display_id)
        return '用户'
    # 过滤控制字符
    cleaned = ''.join(c for c in str(raw_name) if ord(c) >= 0x20 or c in '\n\r\t')
    cleaned = cleaned.strip()
    if not cleaned or len(cleaned) <= 1:
        if display_id:
            return str(display_id)
        return '用户'
    return cleaned


def get_user_name(user):
    """安全获取用户显示名，自动清理 + fallback 链。

    Fallback 链（逐级回退）:
    1. user.nick_name → sanitize_username() 清理控制字符
    2. user.display_id 备用
    3. user.sec_uid 末段（如 '...aBcDeFgH'），至少可区分用户
    4. '用户' 兜底

    Args:
        user: protobuf User 对象。

    Returns:
        清理后的用户显示名。
    """
    try:
        raw = user.nick_name
        disp = getattr(user, 'display_id', '') or ''
        name = sanitize_username(raw, disp)
        if name != '用户':
            return name
        # nick_name 和 display_id 均不可用 → 尝试 sec_uid 末段作为标识
        su = get_user_sec_uid(user)
        if su:
            return f'…{su[-12:]}' if len(su) > 12 else su
        return '用户'
    except (AttributeError, TypeError):
        return '用户'


def get_user_avatar_url(user):
    """从 User protobuf 中提取最佳可用头像 URL。

    优先级：avatar_medium > avatar_thumb > avatar_large
    （avatar_medium 通常是 1080p WebP 最佳；avatar_large 可能过大导致加载慢）

    每个 Image 对象包含 url_list_list（重复 string 字段），
    url_list_list[0] 通常是该分辨率级别中最高质量的 URL。

    Args:
        user: protobuf User 对象。

    Returns:
        头像 URL 字符串，缺失时返回空字符串。
    """
    try:
        # 优先取中等尺寸（兼顾清晰度和加载速度）
        for avatar in (user.avatar_medium, user.avatar_thumb, user.avatar_large):
            try:
                urls = avatar.url_list_list
                if urls and urls[0]:
                    return urls[0]
            except (AttributeError, IndexError, TypeError):
                continue
    except (AttributeError, TypeError):
        pass
    return ''


def get_badge_urls(user):
    """Extract ALL badge image URLs from user's badge_image_list.

    Returns a JSON array string with url, name, level for each badge.
    Covers: pay grade, league, membership V, fans club, and any other badges.
    """
    import json as _json
    badges = []
    try:
        pb_badges = getattr(user, 'badge_image_list', None)
        if pb_badges:
            for b in pb_badges:
                if not b or not b.url_list_list:
                    continue
                entry = {'url': b.url_list_list[0]}
                # Extract content metadata (name, font_color, level, alt text)
                if hasattr(b, 'content') and b.content:
                    c = b.content
                    if getattr(c, 'name', None):
                        entry['name'] = c.name
                    if getattr(c, 'font_color', None):
                        entry['font_color'] = c.font_color
                    if getattr(c, 'level', 0) > 0:
                        entry['level'] = c.level
                    if getattr(c, 'alternative_text', None):
                        entry['alt'] = c.alternative_text
                badges.append(entry)

        # Also extract from pay_grade sub-fields — includes league/recent_consume_badge
        # and new_im_icon_with_level / new_live_icon
        if user.pay_grade:
            pg = user.pay_grade
            for attr in ('new_im_icon_with_level', 'new_live_icon', 'recent_consume_badge'):
                icon = getattr(pg, attr, None)
                if icon and icon.url_list_list and icon.url_list_list[0]:
                    entry = {'url': icon.url_list_list[0]}
                    if hasattr(icon, 'content') and icon.content:
                        c = icon.content
                        if getattr(c, 'alternative_text', None):
                            entry['alt'] = c.alternative_text
                    badges.append(entry)

        # Fans club badge from UserBadge.icons
        if user.fans_club and user.fans_club.data and user.fans_club.data.badge:
            icons = user.fans_club.data.badge.icons
            if hasattr(icons, 'values'):
                for icon in icons.values():
                    if icon and icon.url_list_list and icon.url_list_list[0]:
                        entry = {'url': icon.url_list_list[0]}
                        fc_name = getattr(user.fans_club.data, 'club_name', '')
                        if fc_name:
                            entry['name'] = fc_name
                        if hasattr(icon, 'content') and icon.content:
                            c = icon.content
                            if getattr(c, 'name', None):
                                entry['name'] = c.name
                            if getattr(c, 'font_color', None):
                                entry['font_color'] = c.font_color
                            if getattr(c, 'level', 0) > 0:
                                entry['level'] = c.level
                        badges.append(entry)
    except Exception:
        pass
    return _json.dumps(badges, ensure_ascii=False)


def make_badge_fallback(grade_text="", fans_club_text=""):
    """Construct synthetic badge URLs from grade/fans_club text for historical records
    that didn't capture the full badge_image_list from the websocket protobuf.

    Generates URLs for:
      - 荣誉等级 (pay grade / level badge)   e.g. [等级43]
      - 消费等级图标                          e.g. aweme_pay_grade_2x_40_44
      - 粉丝团 (fans club badge)             e.g. 逸楠💫 Lv20

    This is a best-effort fallback — the real badge_image_list from the protobuf
    is always richer (includes league icons, membership V, super badges, etc).
    """
    import json as _json, re as _re
    badges = []

    # Extract level number from grade like "[等级43]"
    if grade_text:
        m = _re.search(r'等级(\d+)', grade_text)
        if m:
            level = int(m.group(1))
            # Shining level badge (new_shining_level — the main honor badge)
            badges.append({
                'url': f'https://p3-webcast.douyinpic.com/img/webcast/new_shining_level_{level}.webp~tplv-obj.image',
                'level': level,
                'alt': f'荣誉等级{level}级勋章',
            })
            # League icon is ONLY from real Douyin data (pay_grade.recent_consume_badge).
            # For fallback records without websocket data, do NOT generate it — the
            # actual league level per-user is unknown.

            # Membership V badge — monthly by default, yearly if text contains 年度
            is_yearly = grade_text and '年度' in grade_text
            membership_url = 'https://p11-webcast.douyinpic.com/img/webcast/31231321Vnian.png~tplv-obj.image' if is_yearly else 'https://p3-webcast.douyinpic.com/img/webcast/1231241211V.png~tplv-obj.image'
            badges.append({
                'url': membership_url,
                'alt': '年度会员' if is_yearly else '会员',
            })

    # Extract fans club name + level from text like "逸楠💫 Lv20" or "[粉丝团:香奈儿 Lv20]"
    if fans_club_text:
        m = _re.search(r'(?:\[?粉丝团:)?([^\]]+?)\s+Lv(\d+)\]?', fans_club_text)
        if m:
            club_name = m.group(1).strip()
            level = m.group(2)
            # only keep the _xmp overlay badge (represents fansclub visually)
            badges.append({
                'url': f'https://p3-webcast.douyinpic.com/img/webcast/fansclub_new_advanced_badge_{level}_xmp.png~tplv-obj.image',
                'name': club_name,
                'level': int(level),
            })
    return _json.dumps(badges, ensure_ascii=False)


def _merge_badges(existing_json, grade_text='', fans_club_text=''):
    """Augment existing badge_url JSON with missing badges (league, membership V)
    that the websocket protobuf often omits. Returns JSON string or None if no merge needed."""
    import json as _json, re as _re
    try:
        existing = _json.loads(existing_json) if isinstance(existing_json, str) else existing_json
        if not isinstance(existing, list):
            return None
    except Exception:
        return None
    existing_urls = set()
    for b in existing:
        if b.get('url'):
            existing_urls.add(b['url'].split('~tplv')[0])
    added = False
    if grade_text:
        m = _re.search(r'等级(\d+)', grade_text)
        if m:
            level = int(m.group(1))
            # League icon is ONLY from real Douyin data. Skip here.
            pass
    v_url = 'https://p3-webcast.douyinpic.com/img/webcast/1231241211V.png~tplv-obj.image'
    if v_url.split('~tplv')[0] not in existing_urls:
        existing.append({'url': v_url, 'alt': '会员'})
        added = True
    return _json.dumps(existing, ensure_ascii=False) if added else None
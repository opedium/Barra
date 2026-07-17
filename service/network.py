"""网络服务层：HTTP 请求（带重试）、WebSocket URL 构建、房间 API。

职责划分：
    - http_get_with_retry: 通用 HTTP GET，指数退避 + DNS 重试。
    - fetch_ttwid: 获取 ttwid Cookie 并验证登录态。
    - enter_room_api: 调用 /webcast/room/web/enter/ 获取房间信息。
    - build_websocket_url / build_ws_cookie: 构建 WebSocket 连接参数。
"""

import json
import logging
import random
import re
import time
import urllib.parse

import requests

from base.utils import (
    generate_ms_token, APP_ID, LIVE_ID, VERSION_CODE,
    WEBCAST_SDK_VERSION, DID_RULE, DEVICE_PLATFORM,
)

logger = logging.getLogger(__name__)


# ── HTTP 客户端 ──────────────────────────────────

def http_get_with_retry(session, url, max_retries=3, timeout=15, **kwargs):
    """带指数退避 + 超时自增的 HTTP GET 请求。

    DNS 连接失败和超时会自动重试，其他异常直接抛出。
    每次超时后 timeout ×1.5（封顶 60s）。

    Args:
        session: requests.Session 实例。
        url: 请求 URL。
        max_retries: 最大重试次数（含首次）。
        timeout: 初始超时秒数。

    Returns:
        requests.Response 对象。

    Raises:
        requests.RequestException: 所有重试耗尽后抛出最后一次异常。
    """
    last_exc = None
    for attempt in range(max_retries):
        try:
            resp = session.get(url, timeout=timeout, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.exceptions.ConnectionError as e:
            last_exc = e
            wait = min(2 ** attempt + random.uniform(0, 1), 10)
            logger.warning(f"[网络] 连接失败（尝试 {attempt+1}/{max_retries}）: {e}，{wait:.1f}s 后重试")
            time.sleep(wait)
        except requests.exceptions.Timeout as e:
            last_exc = e
            timeout = min(timeout * 1.5, 60)
            wait = min(2 ** attempt + random.uniform(0, 1), 10)
            logger.warning(f"[网络] 请求超时（尝试 {attempt+1}/{max_retries}），下次超时 {timeout:.0f}s，{wait:.1f}s 后重试")
            time.sleep(wait)
        except requests.RequestException as e:
            last_exc = e
            if attempt < max_retries - 1:
                time.sleep(min(2 ** attempt, 10))
            else:
                raise
    raise last_exc


def build_http_headers(ua, ua_version):
    """构建模拟现代 Chrome 浏览器的 HTTP 请求头。

    根据 UA 中是否包含 Chrome/Safari 自动添加 Sec-Ch-Ua 系列头。

    Args:
        ua: User-Agent 字符串。
        ua_version: 'Chrome/x.x.x.x' 格式的版本字符串。

    Returns:
        请求头字典。
    """
    headers = {
        'User-Agent': ua,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Cache-Control': 'no-cache',
        'Referer': 'https://live.douyin.com/',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
    }
    if 'Chrome' in ua and 'Safari' in ua:
        ver = ua_version.split('/')[1].split('.')[0]
        headers.update({
            'Sec-Ch-Ua': f'"Chromium";v="{ver}", "Not_A Brand";v="24", "Google Chrome";v="{ver}"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"' if 'Windows' in ua else '"MacOS"',
        })
    return headers


# ── WebSocket 客户端 ──────────────────────────────

def build_websocket_url(room_id, uid, ua_version, ws_host=None, ws_path=None):
    """构建 WebSocket 长连接 URL，包含所有查询参数。

    cursor 格式为 't-{毫秒时间戳}_r-{随机数}_d-1_u-1_h-{随机数}'，
    internal_ext 包含 room_id、did、时间戳等连接标识。

    Args:
        room_id: 直播间真实 room_id。
        uid: 用户唯一 ID（18~19 位随机数字）。
        ua_version: 'Chrome/x.x.x.x' 格式的版本字符串。
        ws_host: WebSocket 域名，为 None 时使用默认域名。
        ws_path: URL 路径，为 None 时使用默认 /webcast/im/push/v2/。

    Returns:
        完整的 wss:// URL 字符串。
    """
    host = ws_host or 'webcast100-ws-web-hl.douyin.com'
    path = ws_path or '/webcast/im/push/v2/'
    ts = int(time.time() * 1000)
    return (
        f"wss://{host}{path}"
        f"?app_name=douyin_web"
        f"&version_code={VERSION_CODE}"
        f"&webcast_sdk_version={WEBCAST_SDK_VERSION}"
        f"&update_version_code={WEBCAST_SDK_VERSION}"
        f"&compress=gzip"
        f"&device_platform={DEVICE_PLATFORM}"
        f"&cookie_enabled=true"
        f"&screen_width=1920&screen_height=1080"
        f"&browser_language=zh-CN&browser_platform=Win32"
        f"&browser_name=Mozilla"
        f"&browser_version={urllib.parse.quote(ua_version, safe='')}"
        f"&browser_online=true&tz_name=Asia/Shanghai"
        f"&cursor=t-{ts}_r-{random.randint(10**18, 10**19 - 1)}_d-1_u-1_h-{random.randint(10**18, 10**19 - 1)}"
        f"&internal_ext=internal_src:dim|wss_push_room_id:{room_id}"
        f"|wss_push_did:{uid}"
        f"|first_req_ms:{ts}|fetch_time:{ts}"
        f"|seq:1|wss_info:0-{ts}-0-0"
        f"|wrds_v:{random.randint(10**18, 10**19 - 1)}"
        f"&host=https://live.douyin.com"
        f"&aid={APP_ID}&live_id={LIVE_ID}&did_rule={DID_RULE}"
        f"&endpoint=live_pc&support_wrds=1"
        f"&user_unique_id={uid}"
        f"&im_path=/webcast/im/fetch/"
        f"&identity=audience"
        f"&need_persist_msg_count=15"
        f"&insert_task_id=&live_reason="
        f"&room_id={room_id}"
        f"&heartbeatDuration=0"
    )


def build_ws_cookie(ttwid, login_cookies):
    """构建 WebSocket 握手的 Cookie 字符串。

    始终包含 ttwid 和随机 msToken，登录态相关 Cookie 按白名单选取。

    Args:
        ttwid: ttwid Cookie 值。
        login_cookies: {name: value} 字典（来自 load_cookies）。

    Returns:
        'name1=value1; name2=value2' 格式的 Cookie 字符串。
    """
    cookie_parts = [
        f"ttwid={ttwid}",
        f"msToken={generate_ms_token()}",
    ]
    ws_cookie_keys = (
        'sessionid', 'sessionid_ss',
        'sid_tt', 'sid_guard',
        'uid_tt', 'uid_tt_ss',
        'passport_csrf_token',
        'odin_tt',
        'is_staff_user',
    )
    for key in ws_cookie_keys:
        value = login_cookies.get(key)
        if value:
            cookie_parts.append(f"{key}={value}")
    return "; ".join(cookie_parts)


# ── 房间 API ──────────────────────────────────────

def fetch_ttwid(session, live_id, login_cookies, http_timeout=15):
    """获取 ttwid Cookie 并验证登录态。

    优先访问直播间页面获取（服务端 SSR 页面最可靠），
    失败则回退到 cookie.txt 中的 ttwid。

    使用独立最小 headers 请求，确保服务端返回 SSR 版本（内嵌登录信息）。
    完整浏览器头（Sec-Ch-Ua 等）会导致返回 SPA 版本，登录信息不在 HTML 中。

    Args:
        session: 已配置 Cookie 的 requests.Session。
        live_id: 直播间 ID（web_rid）。
        login_cookies: 登录 Cookie 字典。
        http_timeout: HTTP 请求超时秒数。

    Returns:
        (ttwid: str, login_info: dict) 元组。
        login_info 包含 is_login、nickname、uid 三个字段。

    Raises:
        RuntimeError: 无法获取 ttwid 时抛出。
    """
    login_info = {'is_login': False, 'nickname': '', 'uid': ''}
    try:
        room_url = f"https://live.douyin.com/{live_id}"

        # 使用独立 session + 最小 headers，确保拿到 SSR 页面（内嵌 defaultHeaderUserInfo）
        # requests 的 headers 参数是合并而非替换，必须用独立 session 才能去掉 Sec-* 头
        ua = session.headers.get('User-Agent',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36')
        ssr_session = requests.Session()
        try:
            ssr_session.headers.update({
                'User-Agent': ua,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'zh-CN,zh;q=0.9',
                'Referer': 'https://live.douyin.com/',
            })
            # 复制 cookies
            for cookie in session.cookies:
                ssr_session.cookies.set(cookie.name, cookie.value, domain=cookie.domain)

            resp = http_get_with_retry(ssr_session, room_url, timeout=http_timeout)
        finally:
            ssr_session.close()

        # 从 HTML 内嵌数据中提取登录状态
        resp_body = resp.text
        m = re.search(
            r'defaultHeaderUserInfo.*?isLogin.*?(true|false).*?nickname\\?"[,:]\\?"([^"\\]+)',
            resp_body, re.DOTALL
        )
        if m:
            login_info['is_login'] = m.group(1) == 'true'
            login_info['nickname'] = m.group(2)

        # 提取 uid
        m_uid = re.search(r'defaultHeaderUserInfo.*?uid\\?"[,:]\\?"(\d+)', resp_body, re.DOTALL)
        if m_uid:
            login_info['uid'] = m_uid.group(1)

        ttwid = resp.cookies.get('ttwid')
        if ttwid:
            session.cookies.set('ttwid', ttwid, domain='.douyin.com')
            logger.debug(f"[房间] ttwid 自动获取成功: {ttwid[:50]}...")
            return ttwid, login_info
    except Exception as e:
        logger.debug(f"[房间] ttwid 自动获取失败: {e}")

    if login_cookies.get('ttwid'):
        logger.info("[房间] ttwid 自动获取失败，使用 cookie.txt 中的 ttwid")
        return login_cookies['ttwid'], login_info

    raise RuntimeError("无法获取 ttwid Cookie，抖音可能更新了认证流程，请检查网络或更新 cookie.txt")


def download_image(session, url, save_path, timeout=15):
    """下载图片并保存到指定路径。

    Args:
        session: requests.Session 实例。
        url: 图片 URL。
        save_path: 保存路径（完整文件路径）。
        timeout: 请求超时秒数。

    Returns:
        bool: 下载成功返回 True，失败返回 False。
    """
    if not url:
        return False
    try:
        resp = session.get(url, timeout=timeout, stream=True)
        resp.raise_for_status()
        with open(save_path, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        resp.close()
        logger.debug(f"[下载] 图片保存成功: {save_path}")
        return True
    except Exception as e:
        logger.warning(f"[下载] 图片下载失败: {e}")
        return False


def enter_room_api(ttwid, ua, ua_version, live_id, http_timeout=15, session=None):
    """调用 /webcast/room/web/enter/ API 获取房间信息。

    Args:
        ttwid: ttwid Cookie 值。
        ua: User-Agent 字符串。
        ua_version: 'Chrome/x.x.x.x' 格式的版本字符串。
        live_id: 直播间 ID（web_rid）。
        http_timeout: HTTP 请求超时秒数。
        session: 可选的 requests.Session，传入时复用连接池和 headers。
                 未传入时创建临时 Session（向后兼容）。

    Returns:
        dict 包含以下字段:
        - room_id: 直播间真实 room_id
        - status: 状态码（2=直播中，4=未开播）
        - anchor_name: 主播昵称
        - anchor_avatar: 主播头像 URL
        - room_title: 直播间标题
        - room_cover: 直播间封面 URL
        - sec_uid: 主播 sec_uid

    Raises:
        ValueError: API 返回非 JSON 或房间数据为空时抛出。
    """
    logger.debug(f"[房间] ttwid: {ttwid[:50] if ttwid else 'None'}...")

    owns_session = session is None
    if owns_session:
        session = requests.Session()

    original_cookies = {c.name: c.value for c in session.cookies if c.domain == '.douyin.com'}
    session.cookies.set('ttwid', ttwid, domain='.douyin.com')

    browser_ver = ua_version.split('/')[1] if '/' in ua_version else '134.0.0.0'
    params = {
        'aid': APP_ID,
        'app_name': 'douyin_web',
        'live_id': '1',
        'device_platform': 'web',
        'language': 'zh-CN',
        'browser_language': 'zh-CN',
        'browser_platform': 'Win32',
        'browser_name': 'Chrome',
        'browser_version': browser_ver,
        'web_rid': live_id,
        'msToken': '',
    }
    url = f'https://live.douyin.com/webcast/room/web/enter/?{urllib.parse.urlencode(params)}'

    try:
        resp = http_get_with_retry(
            session, url,
            headers={
                'Referer': f'https://live.douyin.com/{live_id}',
                'Accept': 'application/json, text/plain, */*',
                'User-Agent': ua,
            },
            timeout=http_timeout,
        )
    finally:
        if owns_session:
            session.close()
        else:
            session.cookies.clear(domain='.douyin.com')
            for name, value in original_cookies.items():
                session.cookies.set(name, value, domain='.douyin.com')

    logger.debug(f"[网络] API 响应状态码: {resp.status_code}, Content-Type: {resp.headers.get('Content-Type', 'N/A')}")
    logger.debug(f"[网络] API 响应内容长度: {len(resp.content)}")

    resp_text = resp.text

    logger.debug(f"[网络] API 响应内容前 500 字符: {resp_text[:500]}")

    try:
        resp_data = json.loads(resp_text)
        logger.debug(f"[网络] API 返回 JSON 结构: status_code={resp_data.get('status_code', 'N/A')}, "
                     f"data.data length={len(resp_data.get('data', {}).get('data', []))}")
    except (ValueError, json.JSONDecodeError):
        logger.error(f"[网络] API 响应非 JSON，前 200 字符: {resp_text[:200]}")
        raise ValueError(f'API 响应非 JSON (status_code={resp.status_code})')

    room_list = resp_data.get('data', {}).get('data', [])
    if not room_list:
        raise ValueError(f'API 未返回房间数据 (status_code={resp_data.get("status_code", "N/A")})')

    room = room_list[0]
    room_id = str(room.get('room_id_str', '') or room.get('room_id', '') or
                   room.get('id_str', '') or room.get('id', ''))
    status = room.get('status', 0)
    
    user = resp_data.get('data', {}).get('user', {})
    anchor_name = user.get('nickname', '')
    sec_uid = user.get('sec_uid', '')
    anchor_user_id = str(user.get('id_str', '') or user.get('id', '') or user.get('uid', ''))

    avatar_list = user.get('avatar_thumb', {}).get('url_list', [])
    anchor_avatar = avatar_list[0] if avatar_list else ''

    room_title = room.get('title', '')

    cover_list = room.get('cover', {}).get('url_list', [])
    room_cover = cover_list[0] if cover_list else ''

    if not room_id:
        raise ValueError('API 返回的 room_id 为空')

    return {
        'room_id': room_id,
        'status': status,
        'anchor_name': anchor_name,
        'anchor_avatar': anchor_avatar,
        'anchor_user_id': anchor_user_id,
        'room_title': room_title,
        'room_cover': room_cover,
        'sec_uid': sec_uid,
    }


# ── 用户信息 API ──────────────────────────────────

def fetch_user_info_by_sec_uid(sec_uid, ua=None, timeout=15, session=None):
    """通过 sec_uid 调用抖音公开 API 获取用户信息（含头像、昵称等）。

    端点: GET https://www.douyin.com/web/api/v2/user/info/?sec_uid={sec_uid}
    该端点公开可用，无需登录态，但需要合理的浏览器 User-Agent。

    Args:
        sec_uid: 抖音用户永久标识符（~50位字符串）。
        ua: User-Agent 字符串，未传入时使用内置默认值。
        timeout: HTTP 请求超时秒数。
        session: 可选的 requests.Session，传入时复用连接池。

    Returns:
        dict 包含以下字段:
        - sec_uid: 用户 sec_uid（回显）
        - nickname: 昵称
        - avatar_medium: 中等尺寸头像 URL（推荐）
        - avatar_thumb: 小尺寸头像 URL
        - avatar_large: 大尺寸头像 URL
        - avatar_url: 最佳可用头像 URL（medium > thumb > large）
        - unique_id: 抖音号（如 "douyin_xxx"）
        - signature: 个人签名
        - follower_count: 粉丝数
        - following_count: 关注数
        - aweme_count: 作品数
        - total_favorited: 获赞总数
        - ip_location: IP 属地

        请求失败时返回 None。

    Raises:
        ValueError: sec_uid 为空时抛出。
    """
    if not sec_uid:
        raise ValueError('sec_uid 不能为空')

    if ua is None:
        from base.utils import USER_AGENTS
        ua = USER_AGENTS[0]

    url = f'https://www.douyin.com/web/api/v2/user/info/?sec_uid={sec_uid}'

    owns_session = session is None
    if owns_session:
        session = requests.Session()

    try:
        resp = http_get_with_retry(
            session, url,
            headers={
                'User-Agent': ua,
                'Accept': 'application/json',
                'Accept-Language': 'zh-CN,zh;q=0.9',
                'Referer': 'https://www.douyin.com/',
            },
            timeout=timeout,
        )

        data = resp.json()
        if data.get('status_code') != 0:
            logger.warning(f'[用户API] sec_uid={sec_uid[:20]}... 返回非0状态码: {data.get("status_code")}')
            return None

        user_info = data.get('user_info', {})
        if not user_info:
            logger.warning(f'[用户API] sec_uid={sec_uid[:20]}... 返回空 user_info')
            return None

        # 提取各级头像 URL
        def _get_url(image_dict):
            urls = image_dict.get('url_list', [])
            return urls[0] if urls else ''

        avatar_thumb = _get_url(user_info.get('avatar_thumb', {}))
        avatar_medium = _get_url(user_info.get('avatar_medium', {}))
        avatar_large = _get_url(user_info.get('avatar_large', {}))

        # 最佳可用头像 URL（优先中等尺寸）
        best_avatar = avatar_medium or avatar_thumb or avatar_large

        result = {
            'sec_uid': user_info.get('sec_uid', sec_uid),
            'user_id': str(user_info.get('uid', '')),
            'nickname': user_info.get('nickname', ''),
            'avatar_medium': avatar_medium,
            'avatar_thumb': avatar_thumb,
            'avatar_large': avatar_large,
            'avatar_url': best_avatar,
            'unique_id': user_info.get('unique_id', ''),
            'signature': user_info.get('signature', ''),
            'follower_count': user_info.get('follower_count', 0),
            'following_count': user_info.get('following_count', 0),
            'aweme_count': user_info.get('aweme_count', 0),
            'total_favorited': user_info.get('total_favorited', 0),
            'ip_location': user_info.get('ip_location', ''),
        }

        logger.debug(f'[用户API] sec_uid={sec_uid[:20]}... 获取成功: nickname={result["nickname"]}')
        return result

    except Exception as e:
        logger.warning(f'[用户API] sec_uid={sec_uid[:20]}... 请求失败: {e}')
        return None

    finally:
        if owns_session:
            session.close()


def fetch_user_avatar(sec_uid, ua=None, timeout=15, session=None):
    """通过 sec_uid 仅获取用户头像 URL（轻量接口）。

    这是 fetch_user_info_by_sec_uid 的便捷封装，只返回最佳可用头像 URL。

    Args:
        sec_uid: 抖音用户永久标识符。
        ua: User-Agent 字符串，未传入时使用内置默认值。
        timeout: HTTP 请求超时秒数。
        session: 可选的 requests.Session。

    Returns:
        头像 URL 字符串，失败时返回空字符串。
    """
    info = fetch_user_info_by_sec_uid(sec_uid, ua=ua, timeout=timeout, session=session)
    if info and info.get('avatar_url'):
        return info['avatar_url']
    return ''


def fetch_user_info_by_user_id(user_id, ua=None, timeout=15, session=None):
    """通过数字 user_id 调用抖音直播用户 API 获取用户信息。

    端点: GET https://live.douyin.com/webcast/user/
    参数: aid=6383, live_id=1, device_platform=web, target_uid={user_id}

    这是 Douyin 直播 Web 端的内部 API，公开可用，可直接通过数字 UID 查询
    用户昵称、头像、display_id（抖音号）等信息。

    Args:
        user_id: 抖音用户数字 ID（如 "1234567890123456789"）。
        ua: User-Agent 字符串，未传入时使用内置默认值。
        timeout: HTTP 请求超时秒数。
        session: 可选的 requests.Session，传入时复用连接池。

    Returns:
        dict 包含以下字段:
        - user_id: 用户数字 ID（回显）
        - nickname: 昵称
        - display_id: 抖音号（如 "douyin_xxx"）
        - avatar_url: 中等尺寸头像 URL
        - avatar_thumb: 缩略图 URL
        - city: 城市
        - follower_count: 粉丝数
        - following_count: 关注数
        - badge_text: 等级/认证标签文本

        请求失败时返回 None。
    """
    if not user_id:
        raise ValueError('user_id 不能为空')

    if ua is None:
        from base.utils import USER_AGENTS
        ua = USER_AGENTS[0]

    url = (
        f'https://live.douyin.com/webcast/user/'
        f'?aid=6383&live_id=1&device_platform=web'
        f'&language=zh-CN&target_uid={user_id}'
    )

    owns_session = session is None
    if owns_session:
        session = requests.Session()

    try:
        resp = http_get_with_retry(
            session, url,
            headers={
                'User-Agent': ua,
                'Accept': 'application/json',
                'Accept-Language': 'zh-CN,zh;q=0.9',
                'Referer': 'https://live.douyin.com/',
            },
            timeout=timeout,
        )

        data = resp.json()
        user_data = data.get('data', {})
        if not user_data:
            logger.warning(f'[UID API] user_id={user_id} 返回空 data')
            return None

        # 提取头像
        avatar_medium = user_data.get('avatar_medium', {})
        avatar_thumb = user_data.get('avatar_thumb', {})
        avatar_url = ''
        if isinstance(avatar_medium, dict):
            urls = avatar_medium.get('url_list', [])
            avatar_url = urls[0] if urls else ''
        if not avatar_url and isinstance(avatar_thumb, dict):
            urls = avatar_thumb.get('url_list', [])
            avatar_url = urls[0] if urls else ''

        # 粉丝/关注数
        follow_info = user_data.get('follow_info', {}) or {}

        # 等级标签
        badge_text = ''
        badges = user_data.get('badge_image_list', []) or []
        for badge in badges:
            alt = badge.get('content', {}).get('alternative_text', '')
            if alt:
                badge_text = alt
                break

        result = {
            'user_id': str(user_data.get('uid', user_id)),
            'nickname': user_data.get('nickname', ''),
            'display_id': user_data.get('display_id', ''),
            'sec_uid': user_data.get('sec_uid', ''),
            'avatar_url': avatar_url,
            'avatar_thumb': user_data.get('avatar_thumb', {}).get('url_list', [''])[0]
                if isinstance(user_data.get('avatar_thumb'), dict) else '',
            'city': user_data.get('city', ''),
            'follower_count': follow_info.get('follower_count', 0),
            'following_count': follow_info.get('following_count', 0),
            'badge_text': badge_text,
        }

        logger.debug(f'[UID API] user_id={user_id} 获取成功: nickname={result["nickname"]}')
        return result

    except Exception as e:
        logger.warning(f'[UID API] user_id={user_id} 请求失败: {e}')
        return None

    finally:
        if owns_session:
            session.close()


def fetch_user_info_by_unique_id(unique_id, ua=None, timeout=15, session=None):
    """通过抖音号（unique_id / display_id）获取用户信息（含 sec_uid）。

    端点: GET https://www.iesdouyin.com/web/api/v2/user/info/?unique_id={unique_id}

    与 sec_uid 版本的 user/info API 类似，但接受抖音短号（如 "douyin_xxx"），
    返回完整用户信息包括 sec_uid。

    Args:
        unique_id: 抖音号/display_id（如 "douyin_xxx"）。
        ua: User-Agent 字符串。
        timeout: HTTP 请求超时秒数。
        session: 可选的 requests.Session。

    Returns:
        dict 包含 sec_uid, nickname, avatar_url 等字段，失败时返回 None。
    """
    if not unique_id:
        raise ValueError('unique_id 不能为空')

    if ua is None:
        from base.utils import USER_AGENTS
        ua = USER_AGENTS[0]

    url = f'https://www.iesdouyin.com/web/api/v2/user/info/?unique_id={unique_id}'

    owns_session = session is None
    if owns_session:
        session = requests.Session()

    try:
        resp = http_get_with_retry(
            session, url,
            headers={
                'User-Agent': ua,
                'Accept': 'application/json',
                'Accept-Language': 'zh-CN,zh;q=0.9',
                'Referer': 'https://www.douyin.com/',
            },
            timeout=timeout,
        )

        data = resp.json()
        if data.get('status_code') != 0:
            logger.warning(f'[unique_id API] unique_id={unique_id} 返回非0状态码: {data.get("status_code")}')
            return None

        user_info = data.get('user_info', {})
        if not user_info:
            logger.warning(f'[unique_id API] unique_id={unique_id} 返回空 user_info')
            return None

        def _get_url(image_dict):
            urls = image_dict.get('url_list', [])
            return urls[0] if urls else ''

        avatar_medium = _get_url(user_info.get('avatar_medium', {}))
        avatar_thumb = _get_url(user_info.get('avatar_thumb', {}))
        best_avatar = avatar_medium or avatar_thumb

        result = {
            'sec_uid': user_info.get('sec_uid', ''),
            'user_id': str(user_info.get('uid', '')),
            'nickname': user_info.get('nickname', ''),
            'unique_id': user_info.get('unique_id', unique_id),
            'avatar_url': best_avatar,
            'avatar_medium': avatar_medium,
            'avatar_thumb': avatar_thumb,
            'signature': user_info.get('signature', ''),
            'follower_count': user_info.get('follower_count', 0),
            'following_count': user_info.get('following_count', 0),
        }

        logger.debug(f'[unique_id API] unique_id={unique_id} 获取成功: nickname={result["nickname"]}')
        return result

    except Exception as e:
        logger.warning(f'[unique_id API] unique_id={unique_id} 请求失败: {e}')
        return None

    finally:
        if owns_session:
            session.close()


def fetch_user_info(user_id, ua=None, timeout=15, session=None):
    """两步式安全获取用户信息：先用 user_id 获取 sec_uid，再用 sec_uid 获取完整信息。

    流程: user_id → sec_uid → 完整用户信息（昵称、头像等）
    比直接调用单个 API 更安全，因为 sec_uid 端点的数据更完整可靠。

    Args:
        user_id: 抖音用户数字 ID（如 "1234567890123456789"）。
        ua: User-Agent 字符串，未传入时使用内置默认值。
        timeout: HTTP 请求超时秒数。
        session: 可选的 requests.Session，传入时复用连接池。

    Returns:
        dict 包含 nickname, avatar_url, sec_uid 等完整字段，失败时返回 None。
    """
    if not user_id:
        raise ValueError('user_id 不能为空')

    if ua is None:
        from base.utils import USER_AGENTS
        ua = USER_AGENTS[0]

    # Step 1: 用 user_id 获取 sec_uid（及基础信息）
    owns_session = session is None
    if owns_session:
        session = requests.Session()

    try:
        basic = fetch_user_info_by_user_id(user_id, ua=ua, timeout=timeout, session=session)
        if not basic:
            return None

        sec_uid = basic.get('sec_uid', '') or ''
        if not sec_uid:
            logger.debug(f'[用户信息] user_id={user_id} 无 sec_uid，返回基础信息')
            return basic

        # Step 2: 用 sec_uid 获取完整信息（昵称、头像等多字段）
        full = fetch_user_info_by_sec_uid(sec_uid, ua=ua, timeout=timeout, session=session)
        if full:
            return full

        # sec_uid API 失败，回退到基础信息
        logger.debug(f'[用户信息] user_id={user_id} sec_uid API 回退，使用基础信息')
        return basic

    finally:
        if owns_session:
            session.close()

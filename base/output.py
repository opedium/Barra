"""输出模块：异步日志、吞吐统计。

日志通过 QueueHandler + deque 异步写出，避免阻塞主线程的消息处理。

多房间模式：通过 RoomLogFilter 自动根据线程名添加 [主播名] 前缀，
使并发采集时的日志可区分来源。多房间时控制台仅显示 WARNING/ERROR
和状态面板，INFO/DEBUG 仅写入文件。
"""

import logging
import os
import sys
import threading
import time
import unicodedata
from collections import deque

from base.utils import SCRIPT_DIR


def is_ci_environment():
    """检测是否在 CI 环境中运行（GitHub Actions 等）。

    CI 环境不支持 \\r 回到行首，需要禁用单行刷新输出。
    """
    return bool(os.environ.get('CI') or os.environ.get('GITHUB_ACTIONS'))


def display_width(s: str) -> int:
    """计算字符串在终端中的显示列宽（处理 CJK / emoji 等宽字符）。"""
    width = 0
    for ch in s:
        w = unicodedata.east_asian_width(ch)
        if w in ('F', 'W'):
            width += 2
        elif w == 'A':
            width += 2
        elif unicodedata.category(ch) == 'Cs':
            width += 2
        else:
            width += 1
    return width

BARRAGE = 15
logging.addLevelName(BARRAGE, 'BARRAGE')


class RoomLogFilter(logging.Filter):
    """根据当前线程名自动添加 [主播名] 前缀。

    线程命名规则：room-{live_id} → 日志前缀 [{anchor}]
    未获取到主播名时降级显示 [{live_id}]。
    非房间线程（主线程等）不添加前缀。
    """

    _anchor_map = {}

    @classmethod
    def update_anchor(cls, live_id, anchor):
        if anchor and anchor != live_id:
            cls._anchor_map[live_id] = anchor

    def filter(self, record):
        thread_name = threading.current_thread().name
        if thread_name.startswith('room-'):
            live_id = thread_name[5:]
            label = self._anchor_map.get(live_id, live_id)
            record.msg = f"[{label}] {record.msg}"
        return True

class BarragePassFilter(logging.Filter):
    """仅在 DEBUG / BARRAGE 级别显示弹幕，其余级别隐藏。"""
    def __init__(self, user_level):
        super().__init__()
        self._user_level = user_level

    def filter(self, record):
        if record.levelno == BARRAGE:
            return self._user_level in (logging.DEBUG, BARRAGE)
        return record.levelno >= self._user_level

class QueueHandler(logging.Handler):
    
    """异步日志处理器，将日志放入 deque，后台线程批量写出。

    内部使用 maxlen=50000 的 deque 做缓冲，溢出时丢弃新日志并
    在下次刷新时输出丢弃计数。后台线程每 2s 刷新一次。

    多房间模式 (multi_room=True)：
        - WARNING/ERROR 输出到控制台，INFO/DEBUG 仅写文件
        - 状态面板通过 \\r 单行轮显，每次显示一个房间
        - 每 2s 刷新一次，覆盖所有房间状态
    """

    def __init__(self):
        super().__init__()
        self._buf = deque(maxlen=50_000)
        self._handlers: list[logging.Handler] = []
        self._stop = threading.Event()
        self._dropped = 0
        self._thread = None
        # ── 多房间状态面板 ──
        self._room_status = {}      # {live_id: {status, anchor, ...}}
        self._status_lock = threading.Lock()
        self._panel_idx = 0         # (保留，兼容性)
        self.multi_room = False     # 由外部设置
        self._shutting_down = False # 退出时设为 True，抑制面板渲染
        self._panel_max_len = 0    # 历史最大面板长度，用于精确清除
        self._polling_len = 0

    def _ensure_started(self):
        """首次添加 handler 时启动后台刷新线程（幂等）。"""
        if self._thread is None:
            self._thread = threading.Thread(target=self._drain_loop, daemon=True, name='log-drain')
            self._thread.start()

    def add_handler(self, h):
        """添加一个下游日志 handler（如 FileHandler、StreamHandler）。

        Args:
            h: logging.Handler 实例。
        """
        self._handlers.append(h)
        self._ensure_started()

    def emit(self, record):
        """将日志记录放入内部缓冲区（非阻塞）。"""
        try:
            self._buf.append(record)
        except Exception:
            self._dropped += 1

    def _drain_loop(self):
        """后台刷新循环，每 2s 将缓冲区日志批量写出。"""
        while not self._stop.is_set():
            self._drain()
            time.sleep(2)
        self._drain()

    def _drain(self):
        """从缓冲区取出最多 500 条日志，分发到所有下游 handler。

        多房间模式：WARNING/ERROR 输出到控制台，INFO/DEBUG 仅写文件。
        单房间模式：所有消息写入所有 handler（保持原有行为）。
        """
        batch = []
        while len(batch) < 500:
            try:
                batch.append(self._buf.popleft())
            except IndexError:
                break

        for h in self._handlers:
            is_console = type(h) is logging.StreamHandler
            for r in batch:
                try:
                    if r.levelno < h.level:
                        continue
                    if self.multi_room and is_console:
                        if not is_ci_environment():
                            try:
                                sys.stderr.write('\r' + ' ' * self._panel_max_len + '\r')
                            except OSError:
                                pass
                    elif is_console and self._polling_len > 0:
                        if not is_ci_environment():
                            try:
                                sys.stderr.write('\r' + ' ' * self._polling_len + '\r')
                            except OSError:
                                pass
                    h.emit(r)
                except Exception:
                    pass
            try:
                h.flush()
            except Exception:
                pass

        # 多房间：刷新状态面板（即使 batch 为空也要刷新，确保面板持续更新）
        if self.multi_room:
            self._render_panel()

        if self._dropped:
            for h in self._handlers:
                if type(h) is not logging.StreamHandler:
                    try:
                        h.emit(logging.LogRecord(
                            'logger', logging.WARNING, '', 0,
                            f'⚠️ 日志缓冲区溢出，已丢弃 {self._dropped} 条', (), None
                        ))
                    except Exception:
                        pass
            self._dropped = 0

    # ── 房间状态面板 ─────────────────────────────

    def set_room_status(self, live_id, status, **info):
        """更新房间状态（线程安全）。

        Args:
            live_id: 直播间 ID。
            status: 'waiting' 或 'collecting'。
            **info: 额外信息（anchor, msg_count, elapsed, interval）。
        """
        with self._status_lock:
            entry = self._room_status.get(live_id, {})
            entry['status'] = status
            entry['_updated'] = time.monotonic()
            entry.update(info)
            self._room_status[live_id] = entry
        anchor = info.get('anchor')
        if anchor:
            RoomLogFilter.update_anchor(live_id, anchor)

    def clear_room_status(self, live_id):
        """移除房间状态（线程安全）。"""
        with self._status_lock:
            self._room_status.pop(live_id, None)

    def _render_panel(self):
        """用 \\r 单行轮显房间状态，每次刷新显示一个房间。超过 5 分钟未更新的条目自动清除。"""
        if self._shutting_down:
            return
        now = time.monotonic()
        with self._status_lock:
            stale = [lid for lid, info in self._room_status.items()
                     if now - info.get('_updated', 0) > 300]
            for lid in stale:
                del self._room_status[lid]
            items = list(self._room_status.items())

        if not items:
            return

        self._panel_idx = self._panel_idx % len(items)
        live_id, info = items[self._panel_idx]
        anchor = info.get('anchor', '?')
        st = info.get('status', 'unknown')
        if st == 'waiting':
            interval = info.get('interval', 30)
            updated = info.get('_updated', now)
            remaining = max(0, interval - int(now - updated))
            text = f'{anchor} 等待({remaining}s)'
        elif st == 'collecting':
            count = info.get('msg_count', 0)
            elapsed = info.get('elapsed', 0)
            rate = count / elapsed if elapsed > 0 else 0
            flow = info.get('flow', {})
            pc = info.get('parsed_chat', 0)
            pg = info.get('parsed_gift', 0)
            cbuf = info.get('combo_buf', 0)
            ss = info.get('sub_seen', 0)
            sd = info.get('sub_deduped', 0)
            ft = info.get('frame_total', 0)
            fg = info.get('frame_gaps', 0)
            if flow:
                ce = flow.get('chat_enqueued', 0)
                ge = flow.get('gift_enqueued', 0)
                cw = flow.get('chat_written', 0)
                gw = flow.get('gift_written', 0)
                cp = flow.get('combo_progress', 0)
                text = f'{anchor} {count}条({rate:.1f}m/s) par[C:{pc} G:{pg}] enq[C:{ce} G:{ge}] db[C:{cw} G:{gw}]'
                if cp:
                    text += f' prog:{cp}'
                if cbuf:
                    text += f' buf:{cbuf}'
                if ss:
                    text += f' sub:{ss}'
                if sd:
                    text += f' sub_dedup:{sd}'
                if ft and fg:
                    loss = fg / (ft + fg) * 100
                    text += f' loss:{loss:.1f}%'
            else:
                text = f'{anchor} {count}条({rate:.1f}m/s) par[C:{pc} G:{pg}]'
        else:
            text = f'{anchor} {st}'

        if len(items) > 1:
            text = f'[{self._panel_idx + 1}/{len(items)}] {text}'

        self._panel_idx += 1
        new_len = display_width(text)
        pad = max(self._panel_max_len - new_len, 0)
        self._panel_max_len = max(self._panel_max_len, new_len)
        try:
            if is_ci_environment():
                print(text)
            else:
                sys.stderr.write('\r' + text + ' ' * pad)
                sys.stderr.flush()
        except OSError:
            pass

    def close(self):
        """停止后台线程，刷新剩余日志，关闭所有下游 handler。"""
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)
        self._drain()
        for h in self._handlers:
            try:
                h.close()
            except Exception:
                pass
        super().close()


def setup_logger(log_dir='logs', log_level='INFO', multi_room=False):
    """配置全局 logger，返回 (logger, queue_handler)。

    日志级别为 NONE 时关闭日志输出，但数据文件照常写入。
    同时输出到控制台和按日期命名的日志文件。

    多房间模式下，首次调用创建 handler，后续调用复用已有 handler，
    仅更新日志级别（取最低级别），不重复添加 handler。

    Args:
        log_dir: 日志文件输出目录。
        log_level: 日志级别，'NONE' 表示关闭日志。
        multi_room: 多房间模式，控制台仅显示状态面板，日志仅写文件。

    Returns:
        (logging.Logger, QueueHandler) 元组。
    """
    logger = logging.getLogger()
    log_enabled = log_level.upper() != 'NONE'
    level_name = log_level.upper()
    if level_name == 'BARRAGE':
        user_level = BARRAGE
    else:
        user_level = getattr(logging, level_name, logging.INFO)

    # 多实例安全：如果已有 handler，说明其他实例已初始化，复用即可
    if logger.handlers:
        if log_enabled:
            current = logger.level or logging.CRITICAL
            if min(user_level, BARRAGE) < current:
                logger.setLevel(min(user_level, BARRAGE))
        for h in logger.handlers:
            if isinstance(h, QueueHandler):
                if not any(isinstance(f, RoomLogFilter) for f in h.filters):
                    h.addFilter(RoomLogFilter())
                if multi_room:
                    h.multi_room = True
                    for sh in h._handlers:
                        if isinstance(sh, logging.StreamHandler) and not isinstance(sh, logging.FileHandler):
                            sh.setLevel(min(logging.WARNING, user_level))
                return logger, h
        queue_handler = QueueHandler()
        queue_handler.multi_room = multi_room
        queue_handler.addFilter(RoomLogFilter())
        logger.addHandler(queue_handler)
        return logger, queue_handler

    if log_enabled:
        logger.setLevel(min(user_level, BARRAGE))
    else:
        logger.setLevel(logging.CRITICAL + 1)

    queue_handler = QueueHandler()
    queue_handler.multi_room = multi_room
    queue_handler.addFilter(RoomLogFilter())

    if log_enabled:
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, time.strftime('%Y-%m-%d') + '.log')
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(logging.Formatter(
            '[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%m-%d %H:%M'
        ))
        queue_handler.add_handler(file_handler)

    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter('%(message)s'))
    console.setLevel(user_level)
    console.addFilter(BarragePassFilter(user_level))
    queue_handler.add_handler(console)

    logger.addHandler(queue_handler)
    return logger, queue_handler


logger = logging.getLogger(__name__)


class ThroughputCounter:
    """消息吞吐量计数器，统计总消息数和按类型分布。

    用于定时打印采集速率（msg/s）和 Top 5 消息类型。
    只计数 enabled 的消息类型。
    """

    __slots__ = ('_count', '_start', '_by_type')

    def __init__(self):
        self._count = 0
        self._start = time.monotonic()
        self._by_type = {}

    def inc(self, msg_type: str = '', enabled: bool = True):
        """递增计数。

        Args:
            msg_type: 消息类型标识（如 'chat'、'gift'），为空时仅计总数。
            enabled: 是否计入统计（False 时跳过）。
        """
        if not enabled:
            return
        self._count += 1
        if msg_type:
            self._by_type[msg_type] = self._by_type.get(msg_type, 0) + 1

    def report(self) -> str:
        """生成统计报告字符串。

        Returns:
            '总计:N | X.Xmsg/s [top5 类型]' 格式的报告。
        """
        elapsed = time.monotonic() - self._start
        if elapsed < 0.1:
            return "统计中..."
        rate = self._count / elapsed
        parts = [f"总计:{self._count}", f"{rate:.1f}msg/s"]
        if self._by_type:
            top = sorted(self._by_type.items(), key=lambda x: -x[1])[:5]
            parts.append("[" + ", ".join(f"{k}:{v}" for k, v in top) + "]")
        return " | ".join(parts)




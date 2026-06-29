# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Single room — direct ID
python main.py <live_id>

# Single room — interactive
python main.py

# Multi-room (from rooms.txt)
python main.py

# Web management panel
python app.py --port 8080

# Debug logging
python main.py <live_id> --log-level DEBUG

# End on stream end (don't wait for re-broadcast)
python main.py <live_id> --live-stop

# Override cookie/data dir
python main.py <live_id> --cookie-file cookie.txt --data-dir data

# Bind to specific network interface
python main.py <live_id> --bind-ip 192.168.1.100

# Web panel with password
python app.py --port 8000
# Then open http://localhost:8000

# Install dependencies
pip install -r requirements.txt

# Requirements: Python 3.11+, Node.js 20+ (for X-Bogus sign.js)

# Syntax check
python -c "import py_compile; py_compile.compile('base/parser.py', doraise=True)"
```

## Key Architecture

### Data Flow

```
Douyin Live WebSocket
  ↓ PushFrame (compressed)
  ↓ gzip decompress
  ↓ Response (message list)
  ↓ async _handle_message() → msg_queue (producer)
  ↓ _process_loop thread (consumer)
  ↓ HANDLERS dispatch by method name (e.g. 'WebcastGiftMessage')
  ↓ parse_*_msg() → [{type, msg, data}, ...]
  ↓ SQLite write (upsert_user, record_gift, record_chat)
```

### Module Map

| Module | Role |
|--------|------|
| `main.py` | Entry point — CLI args, room selection, multi-room orchestration |
| `app.py` | Flask web panel — dashboard, leaderboard, user detail, SSE events, CSV export |
| `service/fetcher.py` | `DouyinBarrage` class — WebSocket lifecycle, reconnect, heartbeat, watchdog, msg queue |
| `service/network.py` | HTTP requests — ttwid, room API, user info API, retry logic |
| `service/signer.py` | X-Bogus signature — subprocess Node.js call |
| `base/messages.py` | Proto-plus protobuf message definitions (~30 message types across transport/common/business layers) |
| `base/parser.py` | Message decoders (`HANDLERS` dict), gift dedup (delta method), SQLite CRUD |
| `base/utils.py` | Config/Cookie loading, username sanitization, grade/fans_club formatting, UA rotation |
| `base/output.py` | Async logging, throughput counter, multi-room status panel |
| `sign.js` | Node.js X-Bogus signature algorithm |

### Thread Model

```
Main thread (start):
  ├─ HTTP preflight (ttwid, room API)
  └─ async-loop thread (asyncio event loop):
       ├─ _connect_loop() — WS connect + reconnect loop
       │   ├─ _heartbeat_task() — periodic WS ping
       │   ├─ _watchdog_task() — silence detection → reconnect
       │   ├─ _stats_task() — periodic throughput log + flush_to_sqlite
       │   └─ _handle_message() → push to msg_queue
       ├─ msg-processor thread (consumer):
       │   └─ _process_loop() — pull from queue, parse, write SQLite
       └─ cookie-watchdog thread — auto-reload cookie.txt, HTTP keepalive, Playwright refresh
```

### Message Handling

Each WebSocket message method (e.g. `WebcastGiftMessage`) maps to a handler in `HANDLERS` dict (`base/parser.py`). Handlers return a list of result dicts:

```python
{'type': 'gift',              # Routes to CSV file / SQLite logic
 'msg': '[礼物] ...',         # Log text
 'data': {'user_id': ...,     # Structured row data
          'gift_name': ...,
          'diamond_total': ...}}
```

Control actions (`'action': 'stop'`) are returned inline and handled by `_process_item`.

### Gift Dedup (delta method)

Combo gifts (repeat_count cumulatively increasing per message) are deduplicated by tracking `(group_id, gift_id, user_id) → last_repeat_count` and computing `delta = current - last`. Single gifts use a 500ms trace_id window.

### Web Panel

Flask app (`app.py`) with SQLite-backed pages:
- **/** — Live dashboard: top contributors, recent chats, per-session stats
- **/leaderboard** — Cross-session leaderboard with million-level aggregation
- **/user?uid=** — User detail: timeline, gift history, notes/tags
- **/chat** — Chat log search & export
- **/settings** — Cookie management, streamer CRUD
- **/compare** — Side-by-side session comparison
- **/audit** — Data integrity audit
- **/api/*** — JSON endpoints, CSV exports, SSE event stream

### SQLite Schema

Key tables: `sessions`, `users`, `gift_logs`, `chat_logs`, `contributions`, `daily_stats`, `monthly_stats`, `streamer_config`

Subscriptions (会员/星守护) are recorded as synthetic gift_logs entries with gift_name = event+sub_type.

### Douyin API Calls (Rate Limited)

Web panel calls Douyin API (max 30/min via `RateLimiter`) to fill missing `sec_uid`/`avatar_url`:
1. `fetch_user_info_by_sec_uid(sec_uid)` — direct lookup
2. `fetch_user_info(user_id)` — two-step: user_id → sec_uid → full profile

### Configuration

- `config.yaml` — Global settings (output toggles, network, reconnect, web panel)
- `rooms.txt` — Room list (id,name per line, `#` to disable)
- `cookie.txt` — Browser cookies for authenticated API access (Netscape/name=value/CSV format)

## Design Principles

### 复用优先
如果 GitHub / npm 上有成熟的开源方案，直接复用，不要自己实现。选择方案前先说清：用哪个库、多少 star、是否还在更新维护。避免为小功能拉大包，或引入无人维护的依赖。

### 第一性原理分析
分析 bug 时，要从第一性原理出发。不要加兜底实现——兜底实现会掩盖主流程的错误。先理解问题的本质原因，再修复根因。

### 部署前冲突检查
部署之前优先检查方案与当前项目是否存在逻辑冲突或互斥关系。如有冲突则停止部署，开始分析原因并形成建议。

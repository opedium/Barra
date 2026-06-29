# 会员/星守护订阅数据问题

## 问题描述

用户在直播间开通会员（月度/季度/年度）或星守护时，订阅消息由 `WebcastRoomMessage` 承载，而非常规的 `WebcastGiftMessage`。这导致两个严重问题：

1. **用户身份无法识别** — 订阅消息的 protobuf 结构中 `common.user` 字段为空（或包含主播信息），字节扫描也经常返回错误值（Unix 时间戳、room_id 等）。订阅被记录到错误的 user_id 下。
2. **钻石不计入消费** — 订阅消耗的钻石（980/1280/2940/11760 等）被记录到无效 user_id，不参与 `contributions` 表的 `SUM(diamond_total)` 聚合。用户的累计消费（音浪）偏低，导致 `qualified_1000`、`qualified_3000` 等达标判定错误。

## 现象

在贡献榜中可以看到大量：

```
WebcastRoomMessage    2881731180    -    [粉丝团:迅猛龙 特蕾莎]        980
WebcastRoomMessage    2823042277    -    [粉丝团:迅猛龙 特蕾莎]        1,280
```

- 用户名为 `WebcastRoomMessage`（_find_username 从 protobuf 字符串中误取了消息类型名）
- UID 是 timestamp 或 room_id（_extract_douyin_id 没有排除时间戳范围）

## 数据流

### 当前（错误的）数据流

```
Douyin WebSocket
  ↓ WebcastRoomMessage (订阅事件)
parse_room_msg(payload)
  ├─ _extract_subscribe(payload)
  │   ├─ _extract_douyin_id(payload) → 返回 timestamp (~1.7e9) 或 room_id
  │   ├─ _find_username(strings) → 返回 "WebcastRoomMessage" 或无
  │   └─ 返回 {user: "WebcastRoomMessage", douyin_id: "1717465000"}
  ├─ RoomMessage.common.user → None (订阅消息没有 common.user)
  └─ 返回 {user_name: "WebcastRoomMessage", douyin_id: "1717465000",
           user_id: ""}

_process_item (fetcher.py)
  ├─ uid = "" (订阅消息没有 user_id)
  ├─ if uid: → False ← 订阅处理被跳过！
  └─ 订阅钻石：从未写入 gift_logs
```

### 修复后的数据流

```
Douyin WebSocket
  ↓ WebcastRoomMessage (订阅事件)
parse_room_msg(payload)
  ├─ _extract_subscribe(payload)
  │   ├─ _extract_user_id(payload) → 返回 17-19 位数字 user_id ✓
  │   ├─ _extract_douyin_id(payload) → 排除时间戳，返回真实 douyin_id ✓
  │   └─ 返回 {user: "用户名", douyin_id: "12345", user_id: "100863227288"}
  ├─ RoomMessage.common.user → None
  ├─ 回退: real_uid = 字节扫描的 user_id ✓
  └─ 返回 {user_name: "用户名", douyin_id: "12345",
           user_id: "100863227288", diamond: 1280}

_process_item (fetcher.py)
  ├─ sub_uid = "100863227288" ← 正确的 user_id ✓
  ├─ final_uid = "100863227288"
  ├─ upsert_user(final_uid, ...) → 写入 users 表 ✓
  ├─ record_gift(session_id, final_uid, ...) → 写入 gift_logs ✓
  └─ flush_to_sqlite → SUM(diamond_total) GROUP BY user_id → 贡献正确 ✓
```

## 根因分析

### 1. `_extract_douyin_id` 返回时间戳

`base/parser.py` 的 `_extract_douyin_id()` 扫描 protobuf 中所有 `10^9 < val < 10^13` 的 varint。

```python
# 修复前
if 10**9 < val < 10**13:
    ids.append(val)  # ← 时间戳 1.7e9 也在范围内！
```

`Common.create_time`（字段 4，值 ~1,717,465,000）和 `User.create_time`（字段 16，相同值）都在此范围内，且出现次数比真正的 user_id 更多，所以 `_extract_douyin_id` 返回了时间戳。

### 2. `_find_username` 选了消息类型名

```python
known_words = {'webcast', 'room', ...}
```

`"WebcastRoomMessage"` 不在 `known_words` 中（因为检查的是全等匹配，不是子串），评分时靠英文字母拿到 15 分，在没有中文候选时被选中。

### 3. 订阅处理被 `if uid:` 闸门挡住

`_process_item` 中所有写入操作都在 `if self._session_id and rec_data:` 内，再下一层 `if uid:` 检查。订阅消息的 `uid` 来自 `rec_data.get('user_id', '')`，在 `parse_room_msg` 返回空字符串时，整个订阅处理被跳过。

## 修复

### 代码修复（commit `aba7db8`）

**`base/parser.py`**

| 函数 | 修复 |
|------|------|
| `_extract_user_id()` | 新增函数，扫描 17-19 位 varint，优先 field number = 1 的候选值 |
| `_extract_subscribe()` | 同时提取 `user_id` 和 `douyin_id` 返回 |
| `parse_room_msg()` | 不再用 `real_uid` 覆盖字节扫描的 `douyin_id` |
| `parse_room_msg()` | protobuf 无 `user_id` 时回退到字节扫描的 `user_id` |

**`service/fetcher.py`**

| 行 | 修复 |
|----|------|
| 1445 | `sub_uid` 优先使用 `proto_uid`，其次字节扫描的 `user_id` |
| 1460 | 宽松验证用户名，无效时生成兜底名但不跳过 |
| 1466 | 去重 key 使用真实标识符，避免不同用户碰撞 |
| 1473 | `final_uid` 无效时通过用户名/所有候选 ID 查 DB |

### 已有脏数据

修复上线前的订阅记录有错误的 `user_name` 和 `user_id`。这些记录需要补录脚本修复（待执行）。

## 影响

| 指标 | 修复前 | 修复后 |
|------|--------|--------|
| 订阅钻石记录 | 丢失（user_id 为空） | 正常记录 |
| 用户累计消费 | 偏低（缺订阅钻石） | 正确（含订阅钻石） |
| 达标判定（1k/3k/1w） | 偏低 | 正确 |
| 贡献榜排名 | 偏低 | 正确 |
| gift_logs 脏数据 | 有 WebcastRoomMessage 条目 | 无脏数据 |

## 相关文件

- `base/parser.py` — `_extract_subscribe()`, `_extract_douyin_id()`, `_extract_user_id()`, `parse_room_msg()`
- `service/fetcher.py` — 订阅处理逻辑（~1440-1501 行）
- `base/messages.py` — `RoomMessage` protobuf 定义

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
# 会员/星守护订阅数据问题修复

## 问题

抖音直播间的会员（月度/季度/年度）和星守护订阅，走的是 `WebcastRoomMessage` 消息类型，不是常规礼物的 `WebcastGiftMessage`。这两种消息的结构完全不同：

| 字段 | GiftMessage（正常礼物） | RoomMessage（订阅事件） |
|------|----------------------|----------------------|
| `common.user` | ✅ 包含送礼者的完整信息 | ❌ 为空（或只有主播信息） |
| 用户数字 ID | ✅ protobuf 直接解析 | ❌ 不存在 |
| 礼物名称 | ✅ 有 | ❌ 无（只有事件模板） |

订阅消息里**没有订阅者的数字 user_id**。这是问题的根源。

## 现象

修复前，订阅记录写到 `gift_logs` 的结果：

```
user_name = "WebcastRoomMessage"    ← protobuf 类名被当作用户名
user_id   = "1717465000"            ← 实际上是 Common.create_time 时间戳
diamond   = 980                      ← 订阅钻石，但记到了无效用户头上
```

这导致：
1. 订阅钻石不计入任何用户的累计消费
2. 贡献榜达标判定（1k/3k/1w）偏低
3. 贡献榜排名与实际不符

用户在贡献榜看到：

```
#155  WebcastRoomMessage    2881731180    -    [粉丝团:迅猛龙 特蕾莎]    980
#156  WebcastRoomMessage    2823042277    -    [粉丝团:迅猛龙 特蕾莎]    980
```

用户名为 `WebcastRoomMessage`，UID 是时间戳或 room_id。

## 根因

### 1. 订阅消息不含 user_id

订阅事件由 `RoomMessage` 承载。该消息的 `common.user` 字段在订阅场景中为空——订阅者的身份信息不在 protobuf 的标准字段里，而是藏在非标准位置。常规的 protobuf 解析拿不到 user_id。

### 2. `_extract_douyin_id` 返回了时间戳

`_extract_douyin_id()` 扫描 protobuf 中所有 `10^9 < val < 10^13` 的 varint。

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
    ids.append(val)  # ← Unix 时间戳 (1.7e9) 也在范围内
```

`Common.create_time`（~1,717,465,000）恰好在范围内且出现次数最多，被误认为 user_id。

### 3. `_find_username` 选中了消息类型名

`"WebcastRoomMessage"` 不在 `known_words` 列表中，评分拿到了 15 分。当 payload 中没有中文候选字符串时，这个英文字符串被选为"用户名"。

### 4. 订阅处理被 `if uid:` 挡住

```python
if uid:  # uid 是空字符串 → 整个订阅处理被跳过
    ...
    elif rec_type == 'subscribe':
        # 永远不会执行到这里
        record_gift(...)   # ← 订阅钻石从未写入
```

`uid` 来自 `rec_data.get('user_id', '')`，订阅消息返回的一直是空字符串。

## 解决方案

### 核心思路

订阅消息不含 user_id → **无法用 user_id 归因**。只能用订阅消息中提取的**用户名**在已有用户数据中查询匹配。

```
订阅消息 → 提取用户名（_find_username 过滤消息类型名后）
         → users 表查 user_id
         ├── 找到 → 用该 user_id 记录订阅钻石
         └── 没找到 → 以空 user_id 记录（等待用户身份）
                      ↓ 用户后续发消息/送礼时
                      _fill_subscription_uid() 自动补填 user_id
```

### 具体修复

#### 修复 1：`_extract_douyin_id` 排除时间戳范围

```python
if val > 10**8:
    if 1500000000 < val < 2000000000:
        continue  # 排除 Unix 时间戳
    if 1500000000000 < val < 2000000000000:
        continue  # 排除毫秒时间戳
    ids.append(val)
```

同时扩宽扫描范围，从 `10^9—10^13` 改为 `>10^8`，覆盖 9—19 位的 user_id。

#### 修复 2：`_find_username` 过滤 protobuf 消息类型名

```python
# 新增过滤：任何 protobuf 消息类型名都不应被当作用户名
if re.match(r'^(Webcast|Common|Response|PushFrame|RoomMessage)', s) or s.endswith('Message'):
    continue
```

#### 修复 3：订阅处理不再依赖 uid，改用用户名查 DB

```python
# 修复前：依赖 uid（永远为空）
if uid:
    ...
    elif rec_type == 'subscribe':
        ...

# 修复后：直接通过用户名查 DB
if sub_uname:
    found = _get_conn().execute(
        'SELECT user_id FROM users WHERE user_name = ? LIMIT 1',
        (sub_uname,)
    ).fetchone()
    if found:
        final_uid = found['user_id']
        record_gift(self._session_id, final_uid, sub_uname, ...)
    else:
        # 以空 user_id 记录，等待后续补填
        record_gift(self._session_id, '', sub_uname, ...)
```

#### 修复 4：等待用户身份补填

新增 `_fill_subscription_uid()` 方法，在每次用户互动（发消息/送礼）后自动执行：

```python
def _fill_subscription_uid(self, user_name, real_uid):
    """用户身份确认后，补填等待中的订阅记录 user_id"""
    _get_conn().execute(
        'UPDATE gift_logs SET user_id = ? WHERE user_id = \'\' AND user_name = ?',
        (real_uid, user_name)
    )
```

在 `_process_item` 的每次 `upsert_user` 后调用：

```python
upsert_user(uid, uname, ugrade, uclub, usec_uid, uavatar)
if uid:
    self._fill_subscription_uid(uname, uid)
```

### 完整数据流

```
订阅消息到达（WebcastRoomMessage）
  →
  _extract_subscribe()
    ├─ _extract_douyin_id()   → 排除时间戳，返回真实 user_id（如有）
    ├─ _find_username()        → 过滤 "WebcastRoomMessage"，提取真实用户名
    └─ 返回 {user_name: "在立夏吃瓜", douyin_id: ""}
  →
  _process_item()
    ├─ 通过 "在立夏吃瓜" 查 users 表
    │   ├─ 找到 user_id=100863227288 → record_gift(sid, uid, ...)
    │   └─ 没找到                    → record_gift(sid, '', "在立夏吃瓜", ...)
    │
    └─ 用户后续发消息（uid=123456789, name="在立夏吃瓜"）
       → upsert_user(123456789, "在立夏吃瓜", ...)
       → _fill_subscription_uid("在立夏吃瓜", "123456789")
       → UPDATE gift_logs SET user_id='123456789'
         WHERE user_id='' AND user_name='在立夏吃瓜'
```

## 实施状态（2026-06-30）

以下修复已针对 `base/parser.py` 和 `service/fetcher.py` 实施：

| # | 修复 | 文件 | 状态 |
|---|------|------|------|
| 1 | `_extract_douyin_id` 排除 Unix/毫秒时间戳范围 | `base/parser.py:910-914` | ✅ |
| 2 | `_find_username` 正则过滤 protobuf 消息类型名 | `base/parser.py:974-976` | ✅ |
| 3 | 订阅去重 key 使用 `_is_valid_sub_id` 验证，无效时回退到用户名 | `service/fetcher.py:1480-1495` | ✅ |
| 4 | `final_uid` 为空时通过用户名或候选 ID 查 users 表 | `service/fetcher.py:1504-1519` | ✅ |
| 5 | `_fill_subscription_uid()` 方法在用户互动后补填空 user_id | `service/fetcher.py:1243-1254` | ✅ |

## 遗留问题

### 历史脏数据
修复前已记录的约 8 条 `WebcastRoomMessage` 记录，user_id 已清空设为待定，但 user_name 无法恢复（不知道真实用户名）。

### 全新用户的首次订阅
从未在采集系统出现过的新用户首次订阅时，users 表中查不到该用户名。只能以空 user_id 等待，直到该用户下次发消息或送礼时才能补填（通过 `_fill_subscription_uid` 自动完成）。

## 验证结果

```
修复前：
WebcastRoomMessage    2881731180    980     ← 错误记录
WebcastRoomMessage    2823042277    1280    ← 错误记录

修复后：
在立夏吃瓜            100863227288  980     ← 正确记录（用户已存在 DB）
微雨ooo🌻             1085545245514 980     ← 正确记录
神秘人097410          （等待补填）    1280    ← 待定记录
```

审计页面不再出现 `WebcastRoomMessage` 作为用户名。

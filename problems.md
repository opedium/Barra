# 技术问题报告：抖音弹幕采集系统稳定性问题

## 1. SQLite 写锁冲突 (`database is locked`)

**现象：**
- `[DB] SQLite write failed in _process_item: database is locked`
- Web 面板操作失败：`sqlite3.OperationalError: database is locked`
- combo 礼物写入持续失败
- 最终 WebSocket 断连、采集终止

**原因：** 多线程同时写入同一个 SQLite 文件。9 个房间各有一个 `_process_loop` 线程，加上 stats 定时器线程、combo 定时器线程、Flask 线程，全部抢 SQLite 写锁。`flush_to_sqlite` 的全表 `SUM/GROUP BY` 聚合扫描长时间锁住数据库（>30s），导致其他线程全部超时失败。

---

## 2. 看门狗误杀导致 WebSocket 频繁断连

**现象：**
- `[看门狗] 30s 无用户互动消息，触发重连`
- `[连接] WebSocket 已关闭 (code=1006)`
- 每隔 30 秒 - 3 分钟断连一次
- 断连期间消息全部丢失

**原因：** 两个看门狗超时太短：
- `normal_check_timeout = 30s`：业务消息（送礼、弹幕）静默 30s 就断连
- `user_msg_timeout = 30s`：用户互动消息静默 30s 就断连

`roomstats`（直播统计）不属于业务消息或用户互动消息。当直播间只有 roomstats 持续到达、没有用户互动时，看门狗误判为"连接异常"并主动断连。断连后重连、重连后又断连，循环往复。

---

## 3. websockets 库版本不兼容

**现象：**
- `create_connection() got an unexpected keyword argument 'extra_headers'`
- `create_connection() got an unexpected keyword argument 'additional_headers'`

**原因：** websockets v10 用参数名 `extra_headers`，v14+ 改名为 `additional_headers`。本地开发环境装 v10.4，生产服务器装 v14.2，同一份代码无法同时兼容。

---

## 4. 文件描述符耗尽 (`Too many open files`)

**现象：**
- `OSError: [Errno 24] Too many open files`
- `unable to open database file`
- 采集进程挂死

**原因：**
- SQLite 连接在线程退出时不关闭，线程越多积累越多
- HTTP 请求连接池堆积空闲 TCP 连接
- 系统默认 fd 限制 1024，长时间运行后耗尽

---

## 5. 礼物数据重复写入

**现象：**
- 同一条礼物在日志出现 2 次
- 数据库偶尔出现重复记录

**原因：** 抖音 WebSocket 协议为可靠性可能重复发送同一条消息。部分消息缺少 `trace_id`，基于 trace_id 的去重机制失效。SQL 层的 UNIQUE 索引包含 `created_at`（精确到秒），同一秒内的去重可以拦住，但隔了一秒的重复写不进去。

---

## 6. 升级检测完全失效

**现象：**
- `upgrade_logs` 表始终为 0 条记录
- 用户达到 40 级、粉丝团 15 级不记录

**原因：** 代码在第 1328 行先调用 `upsert_user()` 把新等级写入数据库，然后在第 1395 行才查询"旧"等级做比较。此时数据库里已经是新值，`new > old` 永远为 False，升级从未被记录过。从功能上线到修复前，**零条升级记录**。

---

## 7. 匿名解析未标记 `is_anonymous=0`

**现象：**
- 已解析的用户仍在匿名页面显示
- `is_anonymous` 未被清除

**原因：** `_process_item` 中的内联匿名解析（第 1379 行）只更新了 `user_name`，漏了设置 `is_anonymous = 0`。而 `_batch_resolve_anonymous`（第 1748 行）正确设置了该字段，两个代码路径不一致。

---

## 8. RLock 模块级残留代码

**现象：**
- `cannot release un-acquired lock` 持续报错
- 所有加锁的写入操作全部失败

**原因：** 清理 `_write_lock_conn` / `_write_unlock` 函数时，只删除了函数定义行，漏删了函数体中的 `_db_write_lock.release()`。这段代码作为模块级语句在 import 时被执行了一次，将 RLock 内部计数减为 -1，导致之后所有 `with _db_write_lock` 退出时释放一个从未获取的锁。RLock 完全失效。

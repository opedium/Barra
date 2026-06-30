# 连击礼物计数问题修复

## 问题

用户在直播间快速连击送同一礼物时，抖音会合并成一条 `combo_count` 递增的连击消息。正常情况下系统能正确记录最终连击数。但当连击间隔超过超时时间时，连击被拆分为多条记录，导致重复计数。

```
实际送礼：100 个"为爱启航"（连击发送）

数据库记录（修复前）：
20:55  为爱启航  30   300030   ← 中间状态被记录
20:56  为爱启航  100  1000100  ← 最终状态也被记录
                                合计: 130（多记了 30）
```

## 根因

### 1. Combo 超时太短

```python
_gift_combo_timeout = 5.0  # 5 秒无更新就写入
```

用户连击间隔如果超过 5 秒（如中途停顿），定时器触发，将中间状态的连击数写入数据库。后续的新消息创建新的组合包再次写入，导致同一组连击被记录多次。

### 2. 连击拆分后低 count 未被覆盖

5 秒超时写入 30 后，后续到达的 100 消息走新的组合包流程，各记各的。没有机制将 30 替换为 100。

## 修复

### 修复 1：Combo 超时延长

```python
_gift_combo_timeout: 5.0 → 30.0
```

30 秒的超时能覆盖绝大部分连击间隔，减少拆分的概率。

### 修复 2：高 count 覆盖低 count

写者线程在写入礼物前，先删除同用户同礼物的更低 count 记录：

```python
# _flush_write_batch 中的 'gift' 操作
conn.execute('DELETE FROM gift_logs
    WHERE session_id = ? AND user_id = ?
    AND gift_name = ? AND diamond_total = ?
    AND gift_count < ?',
    (sid, uid, gname, dia, cnt))
conn.execute('INSERT OR IGNORE INTO gift_logs ...')
```

即使连击仍然被拆分，最终高 count 到达时会自动删除低 count 的记录：

```
超时写入 30        → INSERT 30
最终 100 到达      → DELETE 30 → INSERT 100
                  → DB 只剩 100 ✅
```

### 修复 3：total_count 全局去重（已删除）

**已回退。** `total_count` 跨场次时会重置（新场的 total_count 从 0 开始），导致缓存中的旧值 > 新值，大量合法消息被误判为重复，拒绝率高达 42%。已恢复到旧的 `combo_count` + `repeat_count` 逻辑。

## 验证

```
修复前：
 礼物 "为爱启航" → 记录 30 + 100 = 130 ❌

修复后（DELETE + 超时延长）：
 礼物 "为爱启航" → DELETE 30 → INSERT 100 → DB 只有 100 ✅
```

## 实施状态（2026-06-30）

| # | 修复 | 文件 | 状态 |
|---|------|------|------|
| 1 | `_gift_combo_timeout`: 5.0→30.0 | `base/parser.py:312` | ✅ |
| 2 | `_flush_write_batch` gift 操作前 DELETE 更低 count | `base/parser.py:2187-2189` | ✅ |

## 影响

| 指标 | 修复前 | 修复后 |
|------|--------|--------|
| 连击拆分概率 | 高（5s 超时） | 低（30s 超时） |
| 拆分后重复计数 | 130（30+100） | 100（仅保留最高） |
| DB 写入次数 | 每次拆分多一条 | DELETE + INSERT |

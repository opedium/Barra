# 更新记录

## 2026-05-09

### 修复

- 修复礼物统计逻辑：按 `gift_count` 递增识别连送，避免重复统计
- 删除 `docs/data/` 构建产物，不再上传到 GitHub（改为 Actions 实时构建）

### 优化

- 更新 GitHub Description 和 Topics

## 2026-05-05

### 变更

- 房间配置从 `config.yaml` 移至独立的 `rooms.txt` 文件
- 新格式更简洁：每行 `id,name`，`#` 开头表示禁用
- 自动更新主播名功能保持不变，只输入房间 ID 即可在采集时自动更新主播名

"""会话持久化管理器 — 登录密钥跨重启保存与恢复。

基于 MediaCrawler Server Integration Guide §7.2 实现。
将登录会话（session_id + cookies）保存为 JSON 文件，支持多会话管理。
"""

import json
import os
import time
from datetime import datetime
from typing import Optional, Dict, List


class SessionManager:
    """持久化和管理 Douyin 登录会话。

    会话以 JSON 文件存储在 storage_dir 中，每个文件包含：
    - session_id: 会话唯一 ID
    - cookies: {name: value} 字典
    - nickname: 登录用户的抖音昵称
    - label: 用户自定义标签（默认使用 session_id 前 8 位）
    - created_at: 创建时间（ISO 格式）
    - updated_at: 最后更新时间
    """

    def __init__(self, storage_dir: str = "./sessions"):
        self.storage_dir = storage_dir
        os.makedirs(storage_dir, exist_ok=True)

    def _path(self, session_id: str) -> str:
        return os.path.join(self.storage_dir, f"{session_id}.json")

    def save_session(
        self,
        session_id: str,
        cookies: dict,
        nickname: str = "",
        label: str = "",
    ) -> dict:
        """保存或更新一个会话。

        Args:
            session_id: 会话唯一标识。
            cookies: {name: value} Cookie 字典。
            nickname: 登录用户的抖音昵称。
            label: 可读标签，为空时自动使用 session_id 前 8 位。

        Returns:
            保存的会话 dict。
        """
        now = datetime.now().isoformat()
        existing = self.load_session(session_id)
        if existing:
            data = existing
            data["cookies"] = cookies
            data["updated_at"] = now
            if nickname:
                data["nickname"] = nickname
            if label:
                data["label"] = label
        else:
            data = {
                "session_id": session_id,
                "cookies": cookies,
                "nickname": nickname,
                "label": label or session_id[:8],
                "created_at": now,
                "updated_at": now,
            }
        with open(self._path(session_id), "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return data

    def load_session(self, session_id: str) -> Optional[dict]:
        """加载指定会话，文件不存在时返回 None。"""
        path = self._path(session_id)
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return None

    def load_cookies(self, session_id: str) -> dict:
        """仅加载指定会话的 Cookie 字典，会话不存在时返回空 dict。"""
        session = self.load_session(session_id)
        return session["cookies"] if session and "cookies" in session else {}

    def list_sessions(self) -> List[dict]:
        """列出所有已保存的会话摘要（不包含 cookies）。"""
        sessions = []
        for fname in sorted(os.listdir(self.storage_dir)):
            if not fname.endswith(".json"):
                continue
            path = os.path.join(self.storage_dir, fname)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                sessions.append({
                    "session_id": data.get("session_id", fname[:-5]),
                    "nickname": data.get("nickname", ""),
                    "label": data.get("label", ""),
                    "cookie_count": len(data.get("cookies", {})),
                    "created_at": data.get("created_at", ""),
                    "updated_at": data.get("updated_at", ""),
                })
            except (json.JSONDecodeError, OSError):
                continue
        return sessions

    def delete_session(self, session_id: str) -> bool:
        """删除一个会话文件。返回 True 表示成功删除。"""
        path = self._path(session_id)
        if os.path.exists(path):
            os.remove(path)
            return True
        return False

    def get_active_sessions(self, max_age_hours: int = 24) -> List[dict]:
        """获取最近 max_age_hours 小时内更新过的会话列表。"""
        cutoff = time.time() - max_age_hours * 3600
        active = []
        for session in self.list_sessions():
            updated = session.get("updated_at", "")
            if updated:
                try:
                    dt = datetime.fromisoformat(updated)
                    if dt.timestamp() >= cutoff:
                        active.append(session)
                except (ValueError, TypeError):
                    continue
        return active

    def cleanup_expired(self, max_age_hours: int = 72) -> int:
        """清理超过 max_age_hours 未更新的过期会话。返回删除数量。"""
        cutoff = time.time() - max_age_hours * 3600
        removed = 0
        for fname in list(os.listdir(self.storage_dir)):
            if not fname.endswith(".json"):
                continue
            path = os.path.join(self.storage_dir, fname)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                updated = data.get("updated_at", "")
                if updated:
                    dt = datetime.fromisoformat(updated)
                    if dt.timestamp() < cutoff:
                        os.remove(path)
                        removed += 1
            except (json.JSONDecodeError, OSError, ValueError):
                os.remove(path)
                removed += 1
        return removed

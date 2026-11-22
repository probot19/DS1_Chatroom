"""
Local JSON Persistence Layer for Group 53 Distributed P2P Chat Room.

Course: Distributed Systems I: Foundations (WS2026)
Group: Group 53

Implements:
- Durable local JSON persistence of delivered chat history per node.
- Atomic file writes (write to .tmp then os.replace) for crash safety.
- Loading of last_delivered_seq and history replica upon restart for reconnect catch-up.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from chatroom.multicast import OrderedMessage


class StorageManager:
    """
    Manages local disk persistence for a chatroom node.
    Saves and reloads delivered chat history and last_delivered_seq.
    """

    def __init__(
        self,
        node_id: int,
        data_dir: str = ".chat_data",
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.node_id = node_id
        self.data_dir = Path(data_dir)
        self.file_path = self.data_dir / f"node_{node_id}_history.json"
        self.logger = logger or logging.getLogger(f"Storage-{node_id}")

        self._ensure_dir()

    def _ensure_dir(self) -> None:
        """Create storage directory if it doesn't already exist."""
        try:
            self.data_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            self.logger.warning("Could not create storage directory %s: %s", self.data_dir, e)

    def save_history(self, delivered_messages: List[OrderedMessage]) -> None:
        """
        Atomically persist delivered chat history and last_delivered_seq to disk.
        Writes to a temporary file before renaming to prevent corrupted writes.
        """
        self._ensure_dir()
        last_seq = max((m.seq for m in delivered_messages), default=0)
        data: Dict[str, Any] = {
            "node_id": self.node_id,
            "last_delivered_seq": last_seq,
            "delivered_messages": [m.to_dict() for m in delivered_messages],
        }

        tmp_path = self.file_path.with_suffix(".tmp")
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
                f.flush()
                os.fsync(f.fileno())

            os.replace(tmp_path, self.file_path)
            self.logger.debug(
                "Persisted %d messages to disk (last_delivered_seq=%d)",
                len(delivered_messages),
                last_seq,
            )
        except Exception as e:
            self.logger.error("Failed to save history to %s: %s", self.file_path, e)
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except OSError:
                    pass

    def load_history(self) -> Tuple[int, List[OrderedMessage]]:
        """
        Load persisted history from disk.
        Returns:
          Tuple[last_delivered_seq, List[OrderedMessage]]
          If file does not exist or is corrupted, returns (0, []).
        """
        if not self.file_path.exists():
            return 0, []

        try:
            with open(self.file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            last_seq = int(data.get("last_delivered_seq", 0))
            raw_msgs = data.get("delivered_messages", [])
            messages = [OrderedMessage.from_dict(m) for m in raw_msgs]

            self.logger.info(
                "Loaded %d persisted messages from %s (last_delivered_seq=%d)",
                len(messages),
                self.file_path,
                last_seq,
            )
            return last_seq, messages
        except Exception as e:
            self.logger.warning("Failed to load history from %s: %s. Starting with clean state.", self.file_path, e)
            return 0, []

    def clear_history(self) -> None:
        """Remove persisted history file from disk (useful for clean tests)."""
        try:
            if self.file_path.exists():
                self.file_path.unlink()
            tmp_path = self.file_path.with_suffix(".tmp")
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError as e:
            self.logger.debug("Error clearing history file: %s", e)

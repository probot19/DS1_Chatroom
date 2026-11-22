"""
Reliable Totally Ordered Multicast Layer for Group 53 Distributed P2P Chat Room.

Course: Distributed Systems I: Foundations (WS2026)
Group: Group 53

Implements:
- Centralized sequencer at leader node.
- Strict sequential delivery (expected_seq).
- Priority hold-back queue for out-of-order packets.
- Gap detection and TCP retransmission (RETRANSMIT_REQ, RETRANSMIT_RES).
- Double deduplication by msg_id (seen_msg_ids on leader, delivered_msg_ids on receiver).
- Sender retry loop for unconfirmed messages.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from chatroom.membership import MembershipTable, PeerInfo
from chatroom.protocol import (
    MessageType,
    create_chat_ack_message,
    create_chat_client_message,
    create_chat_ordered_message,
    create_retransmit_req_message,
    create_retransmit_res_message,
    send_framed_message,
)
from chatroom.transport import PeerTimeoutError, PeerUnreachableError, TcpTransport


@dataclass
class OrderedMessage:
    """Represents a chat message with a globally assigned sequence number."""

    seq: int
    msg_id: str
    sender_id: int
    text: str
    timestamp: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "seq": self.seq,
            "msg_id": self.msg_id,
            "origin_sender_id": self.sender_id,
            "text": self.text,
            "orig_timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> OrderedMessage:
        return cls(
            seq=int(data["seq"]),
            msg_id=str(data["msg_id"]),
            sender_id=int(data.get("origin_sender_id", data.get("sender_id", 0))),
            text=str(data.get("text", "")),
            timestamp=float(data.get("orig_timestamp", data.get("timestamp", 0.0))),
        )


class HoldBackQueue:
    """
    Buffers messages arriving out of order (seq > expected_seq).
    Maintains a sorted buffer and yields contiguous sequences once gaps are resolved.
    """

    def __init__(self) -> None:
        self._buffer: Dict[int, OrderedMessage] = {}

    def insert(self, msg: OrderedMessage) -> None:
        self._buffer[msg.seq] = msg

    def pop_consecutive(self, expected_seq: int) -> List[OrderedMessage]:
        """Pops and returns contiguous messages starting from expected_seq."""
        consecutive: List[OrderedMessage] = []
        curr = expected_seq
        while curr in self._buffer:
            msg = self._buffer.pop(curr)
            consecutive.append(msg)
            curr += 1
        return consecutive

    def get_missing_sequences(self, expected_seq: int) -> List[int]:
        """Returns sequence numbers between expected_seq and max buffered sequence."""
        if not self._buffer:
            return []
        max_seq = max(self._buffer.keys())
        return [s for s in range(expected_seq, max_seq) if s not in self._buffer]

    def contains(self, seq: int) -> bool:
        return seq in self._buffer

    def __len__(self) -> int:
        return len(self._buffer)


class MulticastManager:
    """
    Manages reliable totally ordered multicast:
    - Leader assigns monotonic sequence numbers and multicasts to all peers.
    - Receivers deliver strictly in sequence-order via HoldBackQueue and gap recovery.
    - Double deduplication by msg_id on both sequencer and receivers.
    """

    def __init__(
        self,
        node_id: int,
        table: MembershipTable,
        tcp_transport: TcpTransport,
        role_getter: Callable[[], str],
        leader_id_getter: Callable[[], Optional[int]],
        ack_timeout: float = 1.5,
        retransmit_timeout: float = 1.5,
        on_deliver: Optional[Callable[[OrderedMessage], Any]] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.node_id = node_id
        self.table = table
        self.tcp_transport = tcp_transport
        self.role_getter = role_getter
        self.leader_id_getter = leader_id_getter
        self.ack_timeout = ack_timeout
        self.retransmit_timeout = retransmit_timeout

        self.on_deliver = on_deliver
        self.logger = logger or logging.getLogger(f"Multicast-{node_id}")

        # Sequencer state (used when LEADER)
        self.current_seq: int = 0
        self.seen_msg_ids: Dict[str, int] = {}  # msg_id -> assigned seq
        self.chat_history: Dict[int, OrderedMessage] = {}  # seq -> OrderedMessage
        self._seq_lock = asyncio.Lock()

        # Receiver & Delivery Engine state (all nodes)
        self.expected_seq: int = 1
        self.delivered_msg_ids: Set[str] = set()
        self.delivered_messages: List[OrderedMessage] = []
        self.hold_back_queue = HoldBackQueue()
        self._delivery_lock = asyncio.Lock()

        # Sender tracking & unconfirmed sends
        self._pending_confirmations: Dict[str, asyncio.Event] = {}

        # Testing & Demo simulation hook: allows injecting dropped messages
        self.drop_filter: Optional[Callable[[OrderedMessage], bool]] = None

    # =====================================================================
    # Client Send API & Sender Retry Loop
    # =====================================================================

    async def send_chat(self, text: str, msg_id: Optional[str] = None) -> str:
        """
        Send a chat message with Total Order guarantee.
        If leader: sequences and broadcasts directly.
        If follower: sends to leader via TCP and retries if unconfirmed.
        """
        mid = msg_id or uuid.uuid4().hex
        current_role = self.role_getter()

        if current_role == "LEADER":
            await self.sequence_and_broadcast(
                sender_id=self.node_id,
                msg_id=mid,
                text=text,
                timestamp=time.time(),
            )
            return mid

        # Follower send path with retry loop
        confirm_event = asyncio.Event()
        self._pending_confirmations[mid] = confirm_event

        max_send_attempts = 5
        for attempt in range(1, max_send_attempts + 1):
            leader_id = self.leader_id_getter()
            if leader_id is None:
                await asyncio.sleep(0.5)
                continue

            leader_peer = self.table.get(leader_id)
            if not leader_peer or leader_peer.port == 0:
                await asyncio.sleep(0.5)
                continue

            client_msg = create_chat_client_message(self.node_id, mid, text)
            try:
                self.logger.debug(
                    "Sending chat message msg_id=%s to leader Node %d (attempt %d/%d)",
                    mid[:8],
                    leader_id,
                    attempt,
                    max_send_attempts,
                )
                await self.tcp_transport.send_message(
                    leader_peer.host,
                    leader_peer.port,
                    client_msg,
                    timeout=self.ack_timeout,
                )
            except Exception as e:
                self.logger.debug("Failed sending to leader Node %d: %s", leader_id, e)

            # Wait for message to be delivered back over multicast
            try:
                await asyncio.wait_for(confirm_event.wait(), timeout=self.ack_timeout)
                self.logger.debug("Message msg_id=%s confirmed and delivered!", mid[:8])
                break
            except asyncio.TimeoutError:
                self.logger.warning(
                    "Unconfirmed send for msg_id=%s (timeout %.1fs). Retrying...",
                    mid[:8],
                    self.ack_timeout,
                )

        self._pending_confirmations.pop(mid, None)
        return mid

    # =====================================================================
    # Sequencer (Leader Logic)
    # =====================================================================

    async def sequence_and_broadcast(
        self,
        sender_id: int,
        msg_id: str,
        text: str,
        timestamp: Optional[float] = None,
    ) -> OrderedMessage:
        """
        Assign next global sequence number and multicast ordered message to all members.
        Deduplicates by msg_id to ensure retried messages keep their original sequence.
        """
        async with self._seq_lock:
            if msg_id in self.seen_msg_ids:
                assigned_seq = self.seen_msg_ids[msg_id]
                self.logger.info(
                    "Leader deduplicated msg_id=%s (already assigned seq=%d). Re-broadcasting.",
                    msg_id[:8],
                    assigned_seq,
                )
                ordered_msg = self.chat_history[assigned_seq]
            else:
                self.current_seq += 1
                assigned_seq = self.current_seq
                ordered_msg = OrderedMessage(
                    seq=assigned_seq,
                    msg_id=msg_id,
                    sender_id=sender_id,
                    text=text,
                    timestamp=timestamp if timestamp is not None else time.time(),
                )
                self.seen_msg_ids[msg_id] = assigned_seq
                self.chat_history[assigned_seq] = ordered_msg

                self.logger.info(
                    "Leader assigned seq=%d to msg_id=%s from Node %d: '%s'",
                    assigned_seq,
                    msg_id[:8],
                    sender_id,
                    text,
                )

        # Broadcast CHAT_MULTICAST_ORDERED to all peers in membership table
        multicast_msg = create_chat_ordered_message(
            leader_id=self.node_id,
            seq=ordered_msg.seq,
            sender_id=ordered_msg.sender_id,
            msg_id=ordered_msg.msg_id,
            text=ordered_msg.text,
            orig_timestamp=ordered_msg.timestamp,
        )

        members = [
            p for p in self.table.other_members(self.node_id)
            if p.is_alive and p.port > 0
        ]

        async def send_to_member(peer: PeerInfo) -> None:
            try:
                await self.tcp_transport.send_message(peer.host, peer.port, multicast_msg, timeout=2.0)
            except Exception as e:
                self.logger.debug("Failed sending ordered msg to Node %d: %s", peer.node_id, e)

        if members:
            await asyncio.gather(*(send_to_member(p) for p in members), return_exceptions=True)

        # Deliver locally on leader
        await self._deliver_ordered_message(ordered_msg)
        return ordered_msg

    # =====================================================================
    # Delivery Engine & Hold-Back Queue (Receiver Logic)
    # =====================================================================

    async def _deliver_ordered_message(self, ordered_msg: OrderedMessage) -> None:
        """
        Enforce strict totally ordered delivery:
        - Check drop filter (for testing gap recovery).
        - Check delivered_msg_ids deduplication.
        - If seq == expected_seq: deliver, then pop consecutive from hold_back_queue.
        - If seq > expected_seq: buffer in hold_back_queue and request retransmission.
        """
        # Simulated packet drop hook for tests/demos
        if self.drop_filter and self.drop_filter(ordered_msg):
            self.logger.warning(
                "SIMULATION: Deliberately dropping incoming seq=%d (msg_id=%s)",
                ordered_msg.seq,
                ordered_msg.msg_id[:8],
            )
            return

        async with self._delivery_lock:
            # 1. Receiver Deduplication by msg_id
            if ordered_msg.msg_id in self.delivered_msg_ids:
                self.logger.debug(
                    "Dropping duplicate msg_id=%s (already delivered)",
                    ordered_msg.msg_id[:8],
                )
                if ordered_msg.msg_id in self._pending_confirmations:
                    self._pending_confirmations[ordered_msg.msg_id].set()
                return

            # 2. Already delivered sequence number
            if ordered_msg.seq < self.expected_seq:
                self.logger.debug(
                    "Dropping old seq=%d (expected_seq is %d)",
                    ordered_msg.seq,
                    self.expected_seq,
                )
                return

            # 3. Consecutive arrival: Deliver immediately!
            if ordered_msg.seq == self.expected_seq:
                self._apply_delivery(ordered_msg)

                # Unblock consecutive messages waiting in hold-back queue
                consecutive = self.hold_back_queue.pop_consecutive(self.expected_seq)
                for buffered in consecutive:
                    if buffered.msg_id not in self.delivered_msg_ids:
                        self.logger.info(
                            "Unblocking buffered seq=%d from hold-back queue",
                            buffered.seq,
                        )
                        self._apply_delivery(buffered)

            # 4. Out of order: Gap detected!
            elif ordered_msg.seq > self.expected_seq:
                self.logger.warning(
                    "Gap detected! Received seq=%d, but expected seq=%d. Buffering in hold-back queue.",
                    ordered_msg.seq,
                    self.expected_seq,
                )
                self.hold_back_queue.insert(ordered_msg)
                missing = self.hold_back_queue.get_missing_sequences(self.expected_seq)
                if missing:
                    asyncio.create_task(self._request_retransmission(missing))

    def _apply_delivery(self, msg: OrderedMessage) -> None:
        """Append message to delivery log and increment expected_seq."""
        self.delivered_msg_ids.add(msg.msg_id)
        self.delivered_messages.append(msg)
        self.chat_history[msg.seq] = msg

        self.logger.info(
            "DELIVERED [seq=%d | from Node %d]: '%s' (msg_id=%s)",
            msg.seq,
            msg.sender_id,
            msg.text,
            msg.msg_id[:8],
        )

        self.expected_seq = msg.seq + 1

        # Signal sender confirmation event
        if msg.msg_id in self._pending_confirmations:
            self._pending_confirmations[msg.msg_id].set()

        if self.on_deliver:
            try:
                res = self.on_deliver(msg)
                if asyncio.iscoroutine(res):
                    asyncio.create_task(res)
            except Exception as e:
                self.logger.error("Error in on_deliver callback: %s", e)

    async def _request_retransmission(self, missing_seqs: List[int]) -> None:
        """Send TCP RETRANSMIT_REQ to leader for missing sequence range."""
        if not missing_seqs:
            return

        leader_id = self.leader_id_getter()
        if leader_id is None:
            return

        leader_peer = self.table.get(leader_id)
        if not leader_peer or leader_peer.port == 0:
            return

        req_msg = create_retransmit_req_message(self.node_id, missing_seqs)
        self.logger.info("Requesting retransmission of missing seqs %s from leader Node %d", missing_seqs, leader_id)

        try:
            res = await self.tcp_transport.send_request(
                leader_peer.host,
                leader_peer.port,
                req_msg,
                timeout=self.retransmit_timeout,
            )
            if res.get("type") == MessageType.RETRANSMIT_RES:
                messages = res.get("messages", [])
                self.logger.info("Received %d retransmitted message(s) from leader", len(messages))
                for raw in messages:
                    recovered = OrderedMessage.from_dict(raw)
                    await self._deliver_ordered_message(recovered)
        except Exception as e:
            self.logger.warning("Retransmission request failed: %s", e)

    # =====================================================================
    # TCP Message Dispatch
    # =====================================================================

    async def handle_tcp_message(
        self,
        msg: Dict[str, Any],
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> bool:
        """Handle incoming chat and retransmission messages."""
        mtype = msg.get("type")
        sender_id = int(msg["sender_id"])

        if mtype == MessageType.CHAT_CLIENT_TO_LEADER:
            if self.role_getter() != "LEADER":
                self.logger.warning("Received CHAT_CLIENT_TO_LEADER but this node is not LEADER")
                return True

            mid = str(msg["msg_id"])
            text = str(msg["text"])
            orig_ts = float(msg.get("orig_timestamp", msg.get("timestamp", time.time())))

            asyncio.create_task(
                self.sequence_and_broadcast(sender_id, mid, text, orig_ts)
            )
            return True

        elif mtype == MessageType.CHAT_MULTICAST_ORDERED:
            ordered = OrderedMessage.from_dict(msg)
            asyncio.create_task(self._deliver_ordered_message(ordered))
            return True

        elif mtype == MessageType.RETRANSMIT_REQ:
            missing_seqs = [int(s) for s in msg.get("missing_seqs", [])]
            self.logger.info("Node %d requested retransmission for seqs %s", sender_id, missing_seqs)

            found: List[Dict[str, Any]] = []
            for s in missing_seqs:
                if s in self.chat_history:
                    found.append(self.chat_history[s].to_dict())

            reply = create_retransmit_res_message(self.node_id, found)
            try:
                await send_framed_message(writer, reply)
                self.logger.debug("Sent RETRANSMIT_RES (%d messages) to Node %d", len(found), sender_id)
            except Exception as e:
                self.logger.warning("Failed sending RETRANSMIT_RES to Node %d: %s", sender_id, e)
            return True

        elif mtype == MessageType.RETRANSMIT_RES:
            messages = msg.get("messages", [])
            for raw in messages:
                recovered = OrderedMessage.from_dict(raw)
                asyncio.create_task(self._deliver_ordered_message(recovered))
            return True

        return False

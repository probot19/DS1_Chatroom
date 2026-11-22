"""
Membership and Failure Detection Layer for Group 53 Distributed P2P Chat Room.

Course: Distributed Systems I: Foundations (WS2026)
Group: Group 53

Implements:
- PeerInfo dataclass and MembershipTable.
- Dynamic Discovery via UDP multicast (ANNOUNCE / LEADER_INFO).
- Centralized TCP JOIN / JOIN_ACK / LEAVE / MEMBERSHIP_UPDATE synchronization.
- Periodic UDP multicast HEARTBEAT broadcast.
- Crash-stop failure detection via heartbeat timeout.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import asdict, dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from chatroom.protocol import (
    MessageType,
    create_announce_message,
    create_heartbeat_message,
    create_join_ack_message,
    create_join_message,
    create_leader_info_message,
    create_leave_message,
    create_membership_update_message,
    send_framed_message,
)
from chatroom.transport import MulticastTransport, PeerTimeoutError, PeerUnreachableError, TcpTransport


@dataclass
class PeerInfo:
    """Represents a single known cluster participant."""

    node_id: int
    host: str
    port: int
    role: str  # "LEADER" or "FOLLOWER"
    last_seen: float
    is_alive: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "host": self.host,
            "port": self.port,
            "role": self.role,
            "last_seen": self.last_seen,
            "is_alive": self.is_alive,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> PeerInfo:
        return cls(
            node_id=int(data["node_id"]),
            host=str(data["host"]),
            port=int(data["port"]),
            role=str(data["role"]),
            last_seen=float(data["last_seen"]),
            is_alive=bool(data.get("is_alive", True)),
        )


class MembershipTable:
    """Maintains the cluster membership mapping node_id -> PeerInfo."""

    def __init__(self) -> None:
        self._members: Dict[int, PeerInfo] = {}

    def add_or_update(
        self,
        node_id: int,
        host: str,
        port: int,
        role: str,
        last_seen: Optional[float] = None,
    ) -> PeerInfo:
        ts = last_seen if last_seen is not None else time.time()
        if node_id in self._members:
            peer = self._members[node_id]
            peer.host = host
            peer.port = port
            peer.role = role
            peer.last_seen = ts
            peer.is_alive = True
            return peer

        peer = PeerInfo(
            node_id=node_id,
            host=host,
            port=port,
            role=role,
            last_seen=ts,
            is_alive=True,
        )
        self._members[node_id] = peer
        return peer

    def update_last_seen(self, node_id: int, timestamp: Optional[float] = None) -> bool:
        if node_id in self._members:
            self._members[node_id].last_seen = timestamp if timestamp is not None else time.time()
            self._members[node_id].is_alive = True
            return True
        return False

    def remove(self, node_id: int) -> Optional[PeerInfo]:
        return self._members.pop(node_id, None)

    def get(self, node_id: int) -> Optional[PeerInfo]:
        return self._members.get(node_id)

    def get_leader(self) -> Optional[PeerInfo]:
        for peer in self._members.values():
            if peer.role == "LEADER" and peer.is_alive:
                return peer
        return None

    def all_members(self) -> List[PeerInfo]:
        return sorted(self._members.values(), key=lambda p: p.node_id)

    def other_members(self, exclude_node_id: int) -> List[PeerInfo]:
        return [p for p in self.all_members() if p.node_id != exclude_node_id]

    def contains(self, node_id: int) -> bool:
        return node_id in self._members

    def __len__(self) -> int:
        return len(self._members)

    def to_dict(self) -> Dict[str, Any]:
        return {str(k): v.to_dict() for k, v in self._members.items()}

    def load_from_dict(self, data: Dict[str, Any]) -> None:
        self._members.clear()
        for k, v in data.items():
            peer = PeerInfo.from_dict(v)
            self._members[peer.node_id] = peer


class MembershipManager:
    """
    Coordinates dynamic discovery, membership synchronization, heartbeats,
    and heartbeat-timeout failure detection.
    """

    def __init__(
        self,
        node_id: int,
        host: str,
        port: int,
        tcp_transport: TcpTransport,
        mcast_transport: MulticastTransport,
        initial_role: str = "FOLLOWER",
        heartbeat_interval: float = 0.8,
        heartbeat_timeout: float = 2.5,
        on_membership_changed: Optional[Callable[[MembershipTable, str, Optional[int]], Any]] = None,
        on_leader_crashed: Optional[Callable[[int], Any]] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.node_id = node_id
        self.host = host
        self.port = port
        self.role = initial_role
        self.leader_id: Optional[int] = node_id if initial_role == "LEADER" else None

        self.tcp_transport = tcp_transport
        self.mcast_transport = mcast_transport
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout

        self.on_membership_changed = on_membership_changed
        self.on_leader_crashed = on_leader_crashed
        self.logger = logger or logging.getLogger(f"Membership-{node_id}")

        self.table = MembershipTable()
        self._is_running = False
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._failure_detector_task: Optional[asyncio.Task] = None

        # Discovery sync primitive
        self._discovery_leader_info: Optional[Dict[str, Any]] = None
        self._leader_info_event = asyncio.Event()

    async def start(self) -> None:
        """Start the membership manager: discovery, join, and background heartbeat loops."""
        self._is_running = True

        # Register self in initial membership table
        self.table.add_or_update(
            node_id=self.node_id,
            host=self.host,
            port=self.port,
            role=self.role,
            last_seen=time.time(),
        )

        # Run dynamic discovery
        await self.discover_and_join()

        # Start periodic heartbeats and failure detector loops
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._failure_detector_task = asyncio.create_task(self._failure_detector_loop())

    async def discover_and_join(self, max_retries: int = 3, retry_delay: float = 0.5) -> None:
        """
        Broadcast ANNOUNCE via UDP multicast.
        If LEADER_INFO is received, join via TCP.
        If no LEADER_INFO after max_retries, become LEADER.
        """
        self.logger.info("Starting peer discovery on multicast group...")
        self._leader_info_event.clear()
        self._discovery_leader_info = None

        announce_msg = create_announce_message(self.node_id, self.host, self.port)

        for attempt in range(1, max_retries + 1):
            try:
                await self.mcast_transport.send(announce_msg)
                self.logger.debug("Broadcasted ANNOUNCE (attempt %d/%d)", attempt, max_retries)
            except Exception as e:
                self.logger.warning("Failed to send ANNOUNCE datagram: %s", e)

            try:
                await asyncio.wait_for(self._leader_info_event.wait(), timeout=retry_delay)
                break
            except asyncio.TimeoutError:
                pass

        if self._discovery_leader_info is not None:
            # Existing leader discovered! Send TCP JOIN request
            leader_id = int(self._discovery_leader_info["leader_id"])
            leader_host = str(self._discovery_leader_info["leader_host"])
            leader_port = int(self._discovery_leader_info["leader_port"])
            self.logger.info("Discovered active leader Node %d at %s:%d. Joining...", leader_id, leader_host, leader_port)

            await self._join_leader(leader_id, leader_host, leader_port)
        else:
            # No leader responded after max_retries -> Self-promote to leader
            self.role = "LEADER"
            self.leader_id = self.node_id
            self.table.add_or_update(
                node_id=self.node_id,
                host=self.host,
                port=self.port,
                role="LEADER",
                last_seen=time.time(),
            )
            self.logger.info("No active leader discovered after %d retries. Self-promoted to LEADER.", max_retries)

    async def _join_leader(self, leader_id: int, leader_host: str, leader_port: int) -> None:
        """Send framed TCP JOIN request to the leader and process JOIN_ACK."""
        join_req = create_join_message(self.node_id, self.host, self.port)
        try:
            ack = await self.tcp_transport.send_request(leader_host, leader_port, join_req, timeout=3.0)
            if ack.get("type") != MessageType.JOIN_ACK:
                raise PeerUnreachableError(f"Unexpected response to JOIN: {ack}")

            self.role = "FOLLOWER"
            self.leader_id = leader_id
            membership_data = ack.get("membership", {})
            self.table.load_from_dict(membership_data)

            # Ensure self entry is present and accurate
            self.table.add_or_update(
                node_id=self.node_id,
                host=self.host,
                port=self.port,
                role="FOLLOWER",
                last_seen=time.time(),
            )

            member_ids = [p.node_id for p in self.table.all_members()]
            self.logger.info(
                "Joined cluster successfully. Leader: Node %d. Cluster members: %s",
                self.leader_id,
                member_ids,
            )
            self._notify_change("JOIN_ACK", None)
        except Exception as e:
            self.logger.error("Failed to connect or join leader Node %d at %s:%d: %s", leader_id, leader_host, leader_port, e)
            # Fall back to becoming initial leader if leader is unreachable
            self.role = "LEADER"
            self.leader_id = self.node_id
            self.table.add_or_update(self.node_id, self.host, self.port, "LEADER", time.time())
            self.logger.info("Leader unreachable. Initialized as group LEADER.")

    # =====================================================================
    # Message Dispatch Handlers
    # =====================================================================

    async def handle_udp_message(self, msg: Dict[str, Any], addr: Tuple[str, int]) -> None:
        """Dispatch incoming UDP multicast message to membership logic."""
        mtype = msg.get("type")
        sender_id = msg.get("sender_id")

        if sender_id == self.node_id:
            # Own multicast loopback
            return

        if mtype == MessageType.ANNOUNCE:
            # A new node is looking for the group
            joining_id = int(msg["sender_id"])
            joining_host = str(msg.get("tcp_host", addr[0]))
            joining_port = int(msg["tcp_port"])
            self.logger.info("Received ANNOUNCE from Node %d (%s:%d)", joining_id, joining_host, joining_port)

            if self.role == "LEADER":
                # Leader responds with LEADER_INFO
                info_msg = create_leader_info_message(
                    sender_id=self.node_id,
                    leader_id=self.node_id,
                    leader_host=self.host,
                    leader_port=self.port,
                )
                try:
                    await self.mcast_transport.send(info_msg)
                    self.logger.info("Responded with LEADER_INFO to Node %d", joining_id)
                except Exception as e:
                    self.logger.warning("Failed sending LEADER_INFO datagram: %s", e)
            elif self.leader_id is not None:
                # Follower can also advertise the known leader
                leader_peer = self.table.get(self.leader_id)
                if leader_peer:
                    info_msg = create_leader_info_message(
                        sender_id=self.node_id,
                        leader_id=self.leader_id,
                        leader_host=leader_peer.host,
                        leader_port=leader_peer.port,
                    )
                    try:
                        await self.mcast_transport.send(info_msg)
                    except Exception:
                        pass

        elif mtype == MessageType.LEADER_INFO:
            if not self._leader_info_event.is_set():
                self._discovery_leader_info = msg
                self._leader_info_event.set()

        elif mtype == MessageType.HEARTBEAT:
            # Update last_seen in membership table
            updated = self.table.update_last_seen(sender_id, time.time())
            if not updated and self.role == "LEADER":
                # Known active node transmitting heartbeats but missing from table
                sender_role = msg.get("role", "FOLLOWER")
                self.table.add_or_update(
                    node_id=sender_id,
                    host=addr[0],
                    port=0,
                    role=sender_role,
                    last_seen=time.time(),
                )

    async def handle_tcp_message(
        self,
        msg: Dict[str, Any],
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> bool:
        """
        Handle incoming TCP membership messages.
        Returns True if the message was handled by the membership layer.
        """
        mtype = msg.get("type")
        sender_id = int(msg["sender_id"])

        if mtype == MessageType.JOIN:
            if self.role != "LEADER":
                self.logger.warning("Received JOIN from Node %d but this node is not LEADER", sender_id)
                return True

            new_host = str(msg.get("tcp_host", writer.get_extra_info("peername")[0]))
            new_port = int(msg["tcp_port"])

            self.table.add_or_update(
                node_id=sender_id,
                host=new_host,
                port=new_port,
                role="FOLLOWER",
                last_seen=time.time(),
            )
            self.logger.info("Node %d joined via TCP (%s:%d)", sender_id, new_host, new_port)

            # Reply with JOIN_ACK
            ack = create_join_ack_message(
                leader_id=self.node_id,
                membership=self.table.to_dict(),
            )
            await send_framed_message(writer, ack)

            # Broadcast MEMBERSHIP_UPDATE to all existing followers
            asyncio.create_task(
                self._broadcast_membership_update(reason="JOIN", affected_node_id=sender_id)
            )
            self._notify_change("JOIN", sender_id)
            return True

        elif mtype == MessageType.LEAVE:
            self.logger.info("Node %d sent graceful LEAVE", sender_id)
            removed = self.table.remove(sender_id)
            if self.role == "LEADER" and removed:
                asyncio.create_task(
                    self._broadcast_membership_update(reason="LEAVE", affected_node_id=sender_id)
                )
            self._notify_change("LEAVE", sender_id)
            return True

        elif mtype == MessageType.MEMBERSHIP_UPDATE:
            membership_data = msg.get("membership", {})
            reason = msg.get("reason", "UPDATE")
            affected_id = msg.get("affected_node_id")
            self.table.load_from_dict(membership_data)

            # Keep self coordinates accurate
            self.table.add_or_update(
                node_id=self.node_id,
                host=self.host,
                port=self.port,
                role=self.role,
                last_seen=time.time(),
            )

            member_ids = [p.node_id for p in self.table.all_members()]
            self.logger.info(
                "Membership update from leader (reason=%s, affected=%s). Current members: %s",
                reason,
                affected_id,
                member_ids,
            )
            self._notify_change(reason, affected_id)
            return True

        return False

    async def _broadcast_membership_update(
        self,
        reason: str,
        affected_node_id: Optional[int] = None,
    ) -> None:
        """Broadcast updated membership table over TCP to all current followers."""
        update_msg = create_membership_update_message(
            leader_id=self.node_id,
            membership=self.table.to_dict(),
            reason=reason,
            affected_node_id=affected_node_id,
        )

        followers = [
            p for p in self.table.other_members(self.node_id)
            if p.port > 0 and p.is_alive
        ]

        async def send_to_peer(peer: PeerInfo) -> None:
            try:
                await self.tcp_transport.send_message(peer.host, peer.port, update_msg, timeout=2.0)
            except Exception as e:
                self.logger.debug("Could not push MEMBERSHIP_UPDATE to Node %d: %s", peer.node_id, e)

        if followers:
            await asyncio.gather(*(send_to_peer(p) for p in followers), return_exceptions=True)

    # =====================================================================
    # Heartbeat & Failure Detection Loops
    # =====================================================================

    async def _heartbeat_loop(self) -> None:
        """Periodically broadcast UDP multicast heartbeat datagram."""
        while self._is_running:
            try:
                hb = create_heartbeat_message(self.node_id, self.role)
                await self.mcast_transport.send(hb)
                self.logger.debug("Multicast HEARTBEAT sent (role=%s)", self.role)
            except Exception as e:
                self.logger.debug("Failed sending HEARTBEAT datagram: %s", e)

            await asyncio.sleep(self.heartbeat_interval)

    async def _failure_detector_loop(self) -> None:
        """Check peer last_seen timestamps every 0.3s; declare crash if threshold exceeded."""
        while self._is_running:
            await asyncio.sleep(0.3)
            now = time.time()

            for peer in list(self.table.other_members(self.node_id)):
                elapsed = now - peer.last_seen
                if elapsed > self.heartbeat_timeout and peer.is_alive:
                    peer.is_alive = False
                    self.logger.warning(
                        "Heartbeat timeout expired for Node %d (last seen %.2fs ago). Suspected crash.",
                        peer.node_id,
                        elapsed,
                    )

                    if self.role == "LEADER":
                        # Leader removes node from membership table and informs surviving followers
                        self.table.remove(peer.node_id)
                        self.logger.info(
                            "Removed crashed Node %d from membership list. Surviving members: %s",
                            peer.node_id,
                            [p.node_id for p in self.table.all_members()],
                        )
                        asyncio.create_task(
                            self._broadcast_membership_update(
                                reason="CRASH_TIMEOUT",
                                affected_node_id=peer.node_id,
                            )
                        )
                        self._notify_change("CRASH_TIMEOUT", peer.node_id)

                    elif peer.node_id == self.leader_id:
                        peer.is_alive = False
                        self.logger.warning("Leader Node %d has crashed!", peer.node_id)
                        if self.on_leader_crashed:
                            res = self.on_leader_crashed(peer.node_id)
                            if asyncio.iscoroutine(res):
                                asyncio.create_task(res)

    def _notify_change(self, reason: str, affected_node_id: Optional[int]) -> None:
        if self.on_membership_changed:
            try:
                res = self.on_membership_changed(self.table, reason, affected_node_id)
                if asyncio.iscoroutine(res):
                    asyncio.create_task(res)
            except Exception as e:
                self.logger.error("Error in on_membership_changed callback: %s", e)

    async def leave(self) -> None:
        """Gracefully notify cluster leader and terminate membership services."""
        self._is_running = False
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self._failure_detector_task:
            self._failure_detector_task.cancel()

        if self.role == "FOLLOWER" and self.leader_id is not None:
            leader_peer = self.table.get(self.leader_id)
            if leader_peer:
                try:
                    leave_msg = create_leave_message(self.node_id)
                    await self.tcp_transport.send_message(
                        leader_peer.host,
                        leader_peer.port,
                        leave_msg,
                        timeout=2.0,
                    )
                    self.logger.info("Sent graceful LEAVE notification to leader Node %d", self.leader_id)
                except Exception as e:
                    self.logger.debug("Failed sending LEAVE to leader: %s", e)

        self.logger.info("Membership manager stopped")

"""
Node Orchestrator for Group 53 Distributed P2P Chat Room.

Course: Distributed Systems I: Foundations (WS2026)
Group: Group 53

Coordinates:
- Transport Layer (TCP server/client & UDP multicast)
- Membership & Failure Detection Layer
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Dict, Optional, Tuple

from chatroom.election import ElectionManager
from chatroom.membership import MembershipManager, MembershipTable
from chatroom.multicast import MulticastManager, OrderedMessage
from chatroom.protocol import MessageType, create_message, send_framed_message
from chatroom.storage import StorageManager
from chatroom.transport import MulticastTransport, TcpTransport, get_lan_ip


class Node:
    """
    Main distributed chat room node orchestrator.
    Identical software stack running on all peers.
    """

    def __init__(
        self,
        node_id: int,
        host: Optional[str] = None,
        tcp_port: int = 6000,
        mcast_group: str = "239.255.0.1",
        mcast_port: int = 5000,
        initial_role: str = "FOLLOWER",
        heartbeat_interval: float = 0.8,
        heartbeat_timeout: float = 2.5,
        election_timeout: float = 1.5,
        reconciliation_timeout: float = 1.5,
        data_dir: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.node_id = node_id
        self.host = host or get_lan_ip()
        self.tcp_port = tcp_port
        self.mcast_group = mcast_group
        self.mcast_port = mcast_port
        self.initial_role = initial_role
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.election_timeout = election_timeout
        self.reconciliation_timeout = reconciliation_timeout
        self.data_dir = data_dir

        self.logger = logger or logging.getLogger(f"Node-{node_id}")

        # Local history and sequence number state for reconciliation
        self.local_max_seq: int = 0
        self.local_history: list = []

        # Local JSON disk persistence (enabled when data_dir is provided)
        self.storage: Optional[StorageManager] = (
            StorageManager(
                node_id=self.node_id,
                data_dir=self.data_dir,
                logger=self.logger,
            )
            if self.data_dir is not None
            else None
        )

        # Transport instances
        self.tcp_transport = TcpTransport(
            node_id=self.node_id,
            bind_host="0.0.0.0",
            port=self.tcp_port,
            advertised_ip=self.host,
            on_message=self._on_tcp_message,
            logger=self.logger,
        )

        self.mcast_transport = MulticastTransport(
            group=self.mcast_group,
            port=self.mcast_port,
            interface_ip=self.host,
            on_message=self._on_udp_message,
            logger=self.logger,
        )

        # Membership Manager
        self.membership = MembershipManager(
            node_id=self.node_id,
            host=self.host,
            port=self.tcp_port,
            tcp_transport=self.tcp_transport,
            mcast_transport=self.mcast_transport,
            initial_role=self.initial_role,
            heartbeat_interval=self.heartbeat_interval,
            heartbeat_timeout=self.heartbeat_timeout,
            on_membership_changed=self._on_membership_changed,
            on_leader_crashed=self._on_leader_crashed,
            logger=self.logger,
        )

        # Connect reconnect catch-up hooks
        self.membership.last_delivered_seq_getter = lambda: (
            max(self.local_max_seq, self.multicast.expected_seq - 1)
        )
        self.membership.get_history_delta = self._get_history_delta
        self.membership.on_catch_up = self._on_catch_up

        # Election Manager (Bully Algorithm + State Reconciliation)
        self.election = ElectionManager(
            node_id=self.node_id,
            table=self.membership.table,
            tcp_transport=self.tcp_transport,
            election_timeout=self.election_timeout,
            reconciliation_timeout=self.reconciliation_timeout,
            get_local_state=self._get_local_state,
            on_elected=self._on_elected,
            on_new_leader=self._on_new_leader,
            logger=self.logger,
        )

        # Multicast Manager (Total Order Sequencer, Hold-Back Queue, Gap Detection)
        self.multicast = MulticastManager(
            node_id=self.node_id,
            table=self.membership.table,
            tcp_transport=self.tcp_transport,
            role_getter=lambda: self.role,
            leader_id_getter=lambda: self.leader_id,
            ack_timeout=1.5,
            retransmit_timeout=1.5,
            on_deliver=self._on_chat_delivered,
            logger=self.logger,
        )

        self._is_running = False

    @property
    def role(self) -> str:
        return self.membership.role

    @property
    def leader_id(self) -> Optional[int]:
        return self.membership.leader_id

    @property
    def table(self) -> MembershipTable:
        return self.membership.table

    def _get_local_state(self) -> Tuple[int, list]:
        """Return (max_delivered_seq, chat_history_list) for state reconciliation."""
        multicast_seq = self.multicast.current_seq if self.role == "LEADER" else (self.multicast.expected_seq - 1)
        max_seq = max(self.local_max_seq, multicast_seq)
        history = [m.to_dict() for m in self.multicast.delivered_messages] if self.multicast.delivered_messages else self.local_history
        return max_seq, history

    def _on_elected(self, leader_id: int, max_seq: int, history: list) -> None:
        """Callback when this node wins election and finishes reconciliation."""
        self.membership.role = "LEADER"
        self.membership.leader_id = leader_id
        self.local_max_seq = max_seq
        self.multicast.current_seq = max_seq

        # Populate sequencer chat history with reconciled history
        for item in history:
            if isinstance(item, dict) and "seq" in item:
                m = OrderedMessage.from_dict(item)
                self.multicast.chat_history[m.seq] = m
                self.multicast.seen_msg_ids[m.msg_id] = m.seq

        self.logger.info("Node %d is elected LEADER. Reconciled max sequence: %d", leader_id, max_seq)

    def _on_new_leader(self, leader_id: int, max_seq: int) -> None:
        """Callback when another node claims COORDINATOR."""
        self.membership.role = "FOLLOWER"
        self.membership.leader_id = leader_id
        self.logger.info("Acknowledged new cluster LEADER Node %d (max_seq=%d)", leader_id, max_seq)

    def _on_chat_delivered(self, msg: OrderedMessage) -> None:
        """Callback when a chat message is delivered in total order."""
        # Save delivered message history to disk if persistence is enabled
        if self.storage:
            self.storage.save_history(self.multicast.delivered_messages)

    def _get_history_delta(self, last_seq: int) -> List[Dict[str, Any]]:
        """Return history messages strictly after last_seq for reconnect catch-up."""
        # Check both sequencer chat_history and delivered_messages
        source = self.multicast.chat_history if self.role == "LEADER" else {m.seq: m for m in self.multicast.delivered_messages}
        delta = [
            m.to_dict()
            for seq, m in sorted(source.items())
            if seq > last_seq
        ]
        return delta

    async def _on_catch_up(self, delta_history: List[Dict[str, Any]]) -> None:
        """Process catch-up history delivered during reconnect."""
        if not delta_history:
            return
        self.logger.info("Catching up on %d missed message(s) from leader...", len(delta_history))
        for item in delta_history:
            m = OrderedMessage.from_dict(item)
            await self.multicast._deliver_ordered_message(m)

    async def send_chat(self, text: str, msg_id: Optional[str] = None) -> str:
        """Public API: send a chat message to the room with Total Order."""
        return await self.multicast.send_chat(text, msg_id)

    def get_delivered_messages(self) -> List[OrderedMessage]:
        """Return sequential delivery log."""
        return list(self.multicast.delivered_messages)

    def _on_leader_crashed(self, leader_id: int) -> None:
        """Invoked by failure detector when heartbeat timeout expires for leader."""
        self.logger.warning("Leader Node %d crashed! Triggering Bully election...", leader_id)
        asyncio.create_task(self.election.start_election(f"leader_{leader_id}_heartbeat_timeout"))

    async def start(self) -> None:
        """Initialize transports, load persisted history, and start discovery."""
        if self._is_running:
            return
        self._is_running = True

        # Load persisted local history if persistence is enabled
        if self.storage:
            last_seq, stored_history = self.storage.load_history()
            if stored_history:
                self.multicast.expected_seq = last_seq + 1
                self.multicast.delivered_messages = list(stored_history)
                self.multicast.delivered_msg_ids = {m.msg_id for m in stored_history}
                self.multicast.chat_history = {m.seq: m for m in stored_history}
                self.local_max_seq = last_seq
                self.local_history = [m.to_dict() for m in stored_history]
                if self.initial_role == "LEADER":
                    self.multicast.current_seq = last_seq
                self.logger.info("Restored local history: last_seq=%d (%d messages)", last_seq, len(stored_history))

        actual_port = await self.tcp_transport.start()
        self.tcp_port = actual_port
        self.membership.port = actual_port

        await self.mcast_transport.start()
        await self.membership.start()

        self.logger.info(
            "Node %d operational as %s on %s:%d",
            self.node_id,
            self.role,
            self.host,
            self.tcp_port,
        )

    async def _on_tcp_message(
        self,
        msg: Dict[str, Any],
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Route incoming TCP messages to respective layers."""
        # 1. Election messages (ELECTION, ANSWER, COORDINATOR, STATE_REQ, STATE_RES)
        handled = await self.election.handle_tcp_message(msg, reader, writer)
        if handled:
            return

        # 2. Membership messages (JOIN, LEAVE, MEMBERSHIP_UPDATE)
        handled = await self.membership.handle_tcp_message(msg, reader, writer)
        if handled:
            return

        # 3. Multicast & Chat messages (CHAT_CLIENT_TO_LEADER, CHAT_MULTICAST_ORDERED, RETRANSMIT_REQ/RES)
        handled = await self.multicast.handle_tcp_message(msg, reader, writer)
        if handled:
            return

        # 3. Diagnostics / Echo messages
        mtype = msg.get("type")
        sender = msg.get("sender_id")
        if mtype in (MessageType.PING, MessageType.TEST_TCP):
            reply = create_message(
                MessageType.PONG,
                self.node_id,
                original_msg_type=mtype,
            )
            await send_framed_message(writer, reply)
            return

        self.logger.debug("Unhandled TCP message type: %s from Node %s", mtype, sender)

    async def _on_udp_message(self, msg: Dict[str, Any], addr: Tuple[str, int]) -> None:
        """Route incoming UDP multicast messages to respective layers."""
        await self.membership.handle_udp_message(msg, addr)

    def _on_membership_changed(
        self,
        table: MembershipTable,
        reason: str,
        affected_node_id: Optional[int],
    ) -> None:
        """Callback triggered when cluster membership changes."""
        members = [p.node_id for p in table.all_members()]
        self.logger.info(
            "Cluster membership updated: %s (reason: %s, affected: %s)",
            members,
            reason,
            affected_node_id,
        )

        # Check for higher-ID preemption when a new node joins or is acknowledged
        if affected_node_id and affected_node_id > self.node_id and self.role == "LEADER":
            self.election.check_higher_id_preemption(affected_node_id)
        elif self.role == "FOLLOWER" and self.leader_id and self.node_id > self.leader_id:
            self.logger.info(
                "Self Node %d has higher ID than current leader Node %d. Initiating election.",
                self.node_id,
                self.leader_id,
            )
            asyncio.create_task(self.election.start_election("higher_self_id_than_leader"))

    async def stop(self) -> None:
        """Cleanly leave the group and shut down all network services."""
        if not self._is_running:
            return
        self._is_running = False

        self.logger.info("Stopping Node %d...", self.node_id)
        await self.membership.leave()
        await self.mcast_transport.close()
        await self.tcp_transport.close()
        self.logger.info("Node %d stopped cleanly", self.node_id)

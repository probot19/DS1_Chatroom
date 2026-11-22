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

from chatroom.membership import MembershipManager, MembershipTable
from chatroom.protocol import MessageType, create_message, send_framed_message
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

        self.logger = logger or logging.getLogger(f"Node-{node_id}")

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

    async def start(self) -> None:
        """Initialize transports and start discovery and membership services."""
        if self._is_running:
            return
        self._is_running = True

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
        # 1. Membership messages (JOIN, LEAVE, MEMBERSHIP_UPDATE)
        handled = await self.membership.handle_tcp_message(msg, reader, writer)
        if handled:
            return

        # 2. Diagnostics / Echo messages
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

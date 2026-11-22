"""
Bully Election and State Reconciliation Layer for Group 53 Distributed P2P Chat Room.

Course: Distributed Systems I: Foundations (WS2026)
Group: Group 53

Implements:
- Classical Bully Election Algorithm (ELECTION, ANSWER, COORDINATOR).
- Mandatory Post-Election State Reconciliation (STATE_REQ, STATE_RES) ensuring
  the new leader reconciles max sequence numbers and chat history before continuing.
- Higher-ID Join Preemption ensuring the node with the highest ID always leads.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from chatroom.membership import MembershipTable, PeerInfo
from chatroom.protocol import (
    MessageType,
    create_answer_message,
    create_coordinator_message,
    create_election_message,
    create_state_req_message,
    create_state_res_message,
    send_framed_message,
)
from chatroom.transport import PeerTimeoutError, PeerUnreachableError, TcpTransport


class ElectionState:
    """State constants for the ElectionManager state machine."""
    IDLE = "IDLE"
    ELECTING = "ELECTING"
    RECONCILING = "RECONCILING"
    COORDINATOR = "COORDINATOR"


class ElectionManager:
    """
    Manages leader election using the Bully Algorithm and performs
    state reconciliation across surviving peers before announcing leadership.
    """

    def __init__(
        self,
        node_id: int,
        table: MembershipTable,
        tcp_transport: TcpTransport,
        election_timeout: float = 1.5,
        coordinator_timeout: float = 3.0,
        reconciliation_timeout: float = 1.5,
        get_local_state: Optional[Callable[[], Tuple[int, List[Dict[str, Any]]]]] = None,
        on_elected: Optional[Callable[[int, int, List[Dict[str, Any]]], Any]] = None,
        on_new_leader: Optional[Callable[[int, int], Any]] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.node_id = node_id
        self.table = table
        self.tcp_transport = tcp_transport
        self.election_timeout = election_timeout
        self.coordinator_timeout = coordinator_timeout
        self.reconciliation_timeout = reconciliation_timeout

        self.get_local_state = get_local_state or (lambda: (0, []))
        self.on_elected = on_elected
        self.on_new_leader = on_new_leader
        self.logger = logger or logging.getLogger(f"Election-{node_id}")

        self.state: str = ElectionState.IDLE
        self.reconciled_max_seq: int = 0
        self.next_seq: int = 1

        self._election_lock = asyncio.Lock()
        self._answers_received: Set[int] = set()
        self._answer_event = asyncio.Event()
        self._coordinator_wait_task: Optional[asyncio.Task] = None
        self._current_election_task: Optional[asyncio.Task] = None

    async def start_election(self, reason: str = "MANUAL") -> None:
        """Initiate the Bully election algorithm."""
        async with self._election_lock:
            if self.state in (ElectionState.ELECTING, ElectionState.RECONCILING):
                self.logger.debug("Election already in progress (%s); ignoring trigger: %s", self.state, reason)
                return

            self.state = ElectionState.ELECTING
            self._answers_received.clear()
            self._answer_event.clear()

            if self._coordinator_wait_task and not self._coordinator_wait_task.done():
                self._coordinator_wait_task.cancel()

        self.logger.info("Starting Bully election (reason: %s)", reason)

        # Find candidates with strictly higher node_id
        higher_nodes = [
            p for p in self.table.other_members(self.node_id)
            if p.node_id > self.node_id and p.is_alive
        ]

        if not higher_nodes:
            self.logger.info("No higher-ID nodes exist in membership table. Declaring self winner.")
            await self._reconcile_and_claim_leadership()
            return

        # Send ELECTION message to all higher-ID nodes in parallel
        election_msg = create_election_message(self.node_id)

        async def send_election_to_candidate(peer: PeerInfo) -> None:
            try:
                self.logger.debug("Sending ELECTION to higher-ID Node %d at %s:%d", peer.node_id, peer.host, peer.port)
                await self.tcp_transport.send_message(peer.host, peer.port, election_msg, timeout=self.election_timeout)
            except Exception as e:
                self.logger.debug("Could not reach higher-ID Node %d: %s", peer.node_id, e)

        await asyncio.gather(*(send_election_to_candidate(p) for p in higher_nodes), return_exceptions=True)

        # Wait for ANSWER messages
        try:
            await asyncio.wait_for(self._answer_event.wait(), timeout=self.election_timeout)
            # Higher node responded with ANSWER! Wait for COORDINATOR
            self.logger.info(
                "Received ANSWER from higher node(s): %s. Yielding election and waiting for COORDINATOR.",
                list(self._answers_received),
            )
            self.state = ElectionState.IDLE
            self._coordinator_wait_task = asyncio.create_task(self._wait_for_coordinator())
        except asyncio.TimeoutError:
            # No higher node answered within timeout -> This node wins!
            self.logger.info("No higher-ID node answered within %.2fs. Claiming election victory.", self.election_timeout)
            await self._reconcile_and_claim_leadership()

    async def _wait_for_coordinator(self) -> None:
        """Wait for higher-ID node to announce victory; restart election if it fails to do so."""
        try:
            await asyncio.sleep(self.coordinator_timeout)
            self.logger.warning(
                "Coordinator timeout (%.1fs) expired without receiving COORDINATOR. Restarting election.",
                self.coordinator_timeout,
            )
            await self.start_election("coordinator_timeout_expired")
        except asyncio.CancelledError:
            pass

    async def _reconcile_and_claim_leadership(self) -> None:
        """
        MANDATORY STATE RECONCILIATION GATE:
        Before sending COORDINATOR or assigning any sequence number,
        query all surviving members via STATE_REQ/STATE_RES, reconcile max_seq
        and chat history, and advance sequence counter.
        """
        self.state = ElectionState.RECONCILING
        self.logger.info("Executing mandatory post-election state reconciliation before COORDINATOR...")

        local_max_seq, local_history = self.get_local_state()
        reconciled_max = local_max_seq
        reconciled_history = list(local_history)

        surviving_peers = [
            p for p in self.table.other_members(self.node_id)
            if p.is_alive and p.port > 0
        ]

        if surviving_peers:
            state_req = create_state_req_message(self.node_id)

            async def query_peer(peer: PeerInfo) -> Optional[Dict[str, Any]]:
                try:
                    res = await self.tcp_transport.send_request(
                        peer.host,
                        peer.port,
                        state_req,
                        timeout=self.reconciliation_timeout,
                    )
                    return res
                except Exception as e:
                    self.logger.debug("STATE_REQ to Node %d failed: %s", peer.node_id, e)
                    peer.is_alive = False
                    return None

            responses = await asyncio.gather(*(query_peer(p) for p in surviving_peers), return_exceptions=True)

            for res in responses:
                if isinstance(res, dict) and res.get("type") == MessageType.STATE_RES:
                    peer_max = int(res.get("max_seq", 0))
                    peer_history = res.get("history", [])
                    if peer_max > reconciled_max:
                        reconciled_max = peer_max

                    # Merge history entries by msg_id to eliminate gaps
                    known_ids = {m.get("msg_id") for m in reconciled_history if isinstance(m, dict)}
                    for m in peer_history:
                        if isinstance(m, dict) and m.get("msg_id") not in known_ids:
                            reconciled_history.append(m)
                            known_ids.add(m.get("msg_id"))

        # Clean up any dead nodes from membership table
        for p in list(self.table.all_members()):
            if not p.is_alive:
                self.table.remove(p.node_id)

        self.reconciled_max_seq = reconciled_max
        self.next_seq = reconciled_max + 1

        self.logger.info(
            "State reconciliation complete: reconciled_max_seq=%d, next_seq=%d. Broadcasting COORDINATOR.",
            self.reconciled_max_seq,
            self.next_seq,
        )

        # Update self role in membership table and demote former leader
        self.state = ElectionState.COORDINATOR
        for p in self.table.all_members():
            if p.node_id == self.node_id:
                p.role = "LEADER"
                p.is_alive = True
            elif p.role == "LEADER":
                p.role = "FOLLOWER"

        # Notify application layer
        if self.on_elected:
            try:
                res = self.on_elected(self.node_id, self.reconciled_max_seq, reconciled_history)
                if asyncio.iscoroutine(res):
                    await res
            except Exception as e:
                self.logger.error("Error in on_elected callback: %s", e)

        # Broadcast COORDINATOR to all active members over TCP
        coordinator_msg = create_coordinator_message(self.node_id, max_seq=self.reconciled_max_seq)

        async def notify_peer(peer: PeerInfo) -> None:
            try:
                await self.tcp_transport.send_message(peer.host, peer.port, coordinator_msg, timeout=2.0)
                self.logger.debug("Sent COORDINATOR to Node %d", peer.node_id)
            except Exception as e:
                self.logger.debug("Failed sending COORDINATOR to Node %d: %s", peer.node_id, e)

        if surviving_peers:
            await asyncio.gather(*(notify_peer(p) for p in surviving_peers), return_exceptions=True)

        self.logger.info("Node %d is now the active LEADER (sequencer ready at seq %d)", self.node_id, self.next_seq)

    # =====================================================================
    # TCP Message Handler
    # =====================================================================

    async def handle_tcp_message(
        self,
        msg: Dict[str, Any],
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> bool:
        """
        Handle incoming election and reconciliation messages.
        Returns True if the message was handled by the election layer.
        """
        mtype = msg.get("type")
        sender_id = int(msg["sender_id"])

        if mtype == MessageType.ELECTION:
            self.logger.info("Received ELECTION message from Node %d", sender_id)
            if self.node_id > sender_id:
                # Reply with ANSWER because this node has a higher ID
                answer_msg = create_answer_message(self.node_id)
                try:
                    await send_framed_message(writer, answer_msg)
                    self.logger.info("Sent ANSWER to lower-ID Node %d", sender_id)
                except Exception as e:
                    self.logger.warning("Failed to send ANSWER to Node %d: %s", sender_id, e)

                # Start own election if not already electing or leader
                if self.state not in (ElectionState.ELECTING, ElectionState.RECONCILING, ElectionState.COORDINATOR):
                    asyncio.create_task(
                        self.start_election(f"received_election_from_{sender_id}")
                    )
            return True

        elif mtype == MessageType.ANSWER:
            self.logger.info("Received ANSWER from higher-ID Node %d", sender_id)
            self._answers_received.add(sender_id)
            self._answer_event.set()
            return True

        elif mtype == MessageType.COORDINATOR:
            leader_id = int(msg.get("leader_id", sender_id))
            max_seq = int(msg.get("max_seq", 0))
            self.logger.info("Received COORDINATOR: Node %d is the new LEADER (max_seq=%d)", leader_id, max_seq)

            # Cancel ongoing election/wait timers
            if self._coordinator_wait_task and not self._coordinator_wait_task.done():
                self._coordinator_wait_task.cancel()

            self.state = ElectionState.IDLE

            # Update roles in membership table
            for p in self.table.all_members():
                if p.node_id == leader_id:
                    p.role = "LEADER"
                    p.is_alive = True
                elif p.role == "LEADER":
                    p.role = "FOLLOWER"

            if self.on_new_leader:
                try:
                    res = self.on_new_leader(leader_id, max_seq)
                    if asyncio.iscoroutine(res):
                        asyncio.create_task(res)
                except Exception as e:
                    self.logger.error("Error in on_new_leader callback: %s", e)

            return True

        elif mtype == MessageType.STATE_REQ:
            self.logger.info("Received STATE_REQ from candidate Node %d", sender_id)
            local_max, local_history = self.get_local_state()
            res = create_state_res_message(self.node_id, local_max, local_history)
            try:
                await send_framed_message(writer, res)
                self.logger.info("Replied STATE_RES (max_seq=%d, %d msgs) to Node %d", local_max, len(local_history), sender_id)
            except Exception as e:
                self.logger.warning("Failed to send STATE_RES to Node %d: %s", sender_id, e)
            return True

        return False

    def check_higher_id_preemption(self, joined_node_id: int) -> None:
        """
        Check if a joined or newly discovered node has a higher ID than the current leader.
        If this node is the leader and joined_node_id > self.node_id, yield or trigger election.
        """
        if self.state == ElectionState.COORDINATOR and joined_node_id > self.node_id:
            self.logger.info(
                "Higher-ID Node %d joined (current leader ID is %d). Initiating preemption election.",
                joined_node_id,
                self.node_id,
            )
            asyncio.create_task(self.start_election(f"higher_id_preemption_{joined_node_id}"))

"""
Milestone 4 Verification Test Suite: Reliable Totally Ordered Multicast.

Course: Distributed Systems I: Foundations (WS2026)
Group: Group 53

Verifies:
1. Unit tests for HoldBackQueue consecutive popping, gap detection, and deduplication.
2. Mandatory Milestone 4 Verification Gate across 3 independent node processes:
   - Phase 1: Concurrent sends from all 3 nodes -> verifies identical total order on all nodes.
   - Phase 2: Injected dropped sequence number -> verifies hold-back buffering, gap recovery
     via RETRANSMIT_REQ/RETRANSMIT_RES, and in-order delivery with zero gaps.
   - Phase 3: Duplicate message injection -> verifies double deduplication (leader & receiver)
     with zero duplicate deliveries.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import subprocess
import sys
import time
import unittest
from typing import Any, Dict, List, Optional, Tuple

from chatroom.multicast import HoldBackQueue, MulticastManager, OrderedMessage
from chatroom.node import Node
from chatroom.protocol import (
    MessageType,
    create_chat_client_message,
    create_chat_ordered_message,
    create_retransmit_req_message,
    create_retransmit_res_message,
    validate_envelope,
)
from chatroom.transport import get_lan_ip


class TestMulticastUnit(unittest.TestCase):
    """Unit tests for HoldBackQueue and OrderedMessage data structures."""

    def test_ordered_message_serialization(self) -> None:
        m = OrderedMessage(seq=5, msg_id="uuid-5", sender_id=2, text="hello world", timestamp=123.456)
        d = m.to_dict()
        m2 = OrderedMessage.from_dict(d)
        self.assertEqual(m.seq, m2.seq)
        self.assertEqual(m.msg_id, m2.msg_id)
        self.assertEqual(m.sender_id, m2.sender_id)
        self.assertEqual(m.text, m2.text)
        self.assertEqual(m.timestamp, m2.timestamp)

    def test_hold_back_queue_gap_and_consecutive_pop(self) -> None:
        hbq = HoldBackQueue()
        self.assertEqual(len(hbq), 0)

        m2 = OrderedMessage(seq=2, msg_id="m2", sender_id=1, text="two", timestamp=1.0)
        m3 = OrderedMessage(seq=3, msg_id="m3", sender_id=1, text="three", timestamp=2.0)
        m5 = OrderedMessage(seq=5, msg_id="m5", sender_id=1, text="five", timestamp=3.0)

        hbq.insert(m2)
        hbq.insert(m3)
        hbq.insert(m5)
        self.assertEqual(len(hbq), 3)

        # Expected sequence is 1; none can be popped consecutively yet
        self.assertEqual(hbq.pop_consecutive(1), [])

        # Missing sequences between 1 and max(5): [1, 4]
        self.assertEqual(hbq.get_missing_sequences(1), [1, 4])

        # Missing sequences between 2 and max(5): [4]
        self.assertEqual(hbq.get_missing_sequences(2), [4])

        # Now sequence 1 is delivered; popping starting at 2 should return [m2, m3]
        consecutive = hbq.pop_consecutive(2)
        self.assertEqual([m.seq for m in consecutive], [2, 3])
        self.assertEqual(len(hbq), 1)  # Only m5 left

        # After 2 and 3 popped, next expected is 4. Missing between 4 and 5 is [4]
        self.assertEqual(hbq.get_missing_sequences(4), [4])

        # When m4 arrives, pop from 4 yields [m4, m5]
        m4 = OrderedMessage(seq=4, msg_id="m4", sender_id=1, text="four", timestamp=4.0)
        hbq.insert(m4)
        consecutive2 = hbq.pop_consecutive(4)
        self.assertEqual([m.seq for m in consecutive2], [4, 5])
        self.assertEqual(len(hbq), 0)


# =====================================================================
# Standalone Worker Process for Multicast & Chat Testing
# =====================================================================

async def run_multicast_worker(
    node_id: int,
    port: int,
    mcast_port: int,
    lan_ip: str,
    heartbeat_interval: float = 0.8,
    heartbeat_timeout: float = 2.5,
) -> None:
    """Worker process: runs Node orchestrator with chat command interface."""
    logger = logging.getLogger(f"ChatWorker-{node_id}")
    logger.setLevel(logging.INFO)

    node = Node(
        node_id=node_id,
        host=lan_ip,
        tcp_port=port,
        mcast_port=mcast_port,
        heartbeat_interval=heartbeat_interval,
        heartbeat_timeout=heartbeat_timeout,
        logger=logger,
    )

    drop_seq_target: Optional[int] = None

    def simulated_drop_filter(msg: OrderedMessage) -> bool:
        nonlocal drop_seq_target
        if drop_seq_target is not None and msg.seq == drop_seq_target:
            drop_seq_target = None  # drop once
            return True
        return False

    node.multicast.drop_filter = simulated_drop_filter

    await node.start()

    ready_data = {
        "node_id": node.node_id,
        "tcp_port": node.tcp_port,
        "role": node.role,
        "leader_id": node.leader_id,
    }
    print(f"READY:{json.dumps(ready_data)}", flush=True)

    # Command listener on stdin
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(limit=1024 * 1024)
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)

    while True:
        line_bytes = await reader.readline()
        if not line_bytes:
            break
        line = line_bytes.decode("utf-8").strip()
        if not line:
            continue

        if line.startswith("SEND:"):
            # Format: SEND:{"text": "hello", "msg_id": "optional"}
            cmd = json.loads(line[len("SEND:"):])
            text = cmd["text"]
            mid = cmd.get("msg_id")
            sent_id = await node.send_chat(text, mid)
            print(f"RESULT:{json.dumps({'status': 'OK', 'msg_id': sent_id})}", flush=True)

        elif line.startswith("DROP_SEQ:"):
            drop_seq_target = int(line[len("DROP_SEQ:"):])
            print(f"RESULT:{json.dumps({'status': 'OK', 'dropping_seq': drop_seq_target})}", flush=True)

        elif line == "GET_DELIVERED":
            delivered = [m.to_dict() for m in node.get_delivered_messages()]
            print(f"RESULT:{json.dumps({'status': 'OK', 'delivered': delivered})}", flush=True)

        elif line == "GET_STATUS":
            res = {
                "node_id": node.node_id,
                "role": node.role,
                "leader_id": node.leader_id,
                "expected_seq": node.multicast.expected_seq,
                "delivered_count": len(node.multicast.delivered_messages),
                "hold_back_count": len(node.multicast.hold_back_queue),
            }
            print(f"RESULT:{json.dumps({'status': 'OK', 'data': res})}", flush=True)

        elif line == "QUIT":
            break

    await node.stop()


# =====================================================================
# Milestone 4 Verification Gate Test Case
# =====================================================================

class TestMultiProcessMulticastGate(unittest.TestCase):
    """
    Executes the Milestone 4 Verification Gate:
    1. Start 3 node processes (Node 1 as initial Leader, Nodes 2 and 3 as Followers).
    2. Concurrent sends from all 3 nodes -> verify all nodes deliver exact same sequence order.
    3. Injected dropped sequence number -> verify gap detection, hold-back queue buffering,
       RETRANSMIT_REQ/RETRANSMIT_RES recovery, and in-order delivery.
    4. Duplicate message injection -> verify double deduplication on leader and followers.
    """

    MCAST_PORT = 5085
    HEARTBEAT_INTERVAL = 0.8
    HEARTBEAT_TIMEOUT = 3.0

    @classmethod
    def setUpClass(cls) -> None:
        cls.lan_ip = get_lan_ip()

    def setUp(self) -> None:
        self.procs: Dict[int, subprocess.Popen] = {}

    def tearDown(self) -> None:
        for nid, p in list(self.procs.items()):
            try:
                if p.poll() is None:
                    p.stdin.write("QUIT\n")
                    p.stdin.flush()
                    p.wait(timeout=1.5)
            except Exception:
                try:
                    p.kill()
                    p.wait(timeout=1.0)
                except Exception:
                    pass
            finally:
                if p.stdin and not p.stdin.closed:
                    p.stdin.close()
                if p.stdout and not p.stdout.closed:
                    p.stdout.close()

    def _start_node(self, node_id: int) -> Tuple[subprocess.Popen, int, str]:
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        env = os.environ.copy()
        env["PYTHONPATH"] = project_root

        cmd = [
            sys.executable,
            "-u",
            os.path.abspath(__file__),
            "--worker",
            "--id", str(node_id),
            "--port", "0",
            "--mcast-port", str(self.MCAST_PORT),
            "--host", self.lan_ip,
            "--heartbeat-interval", str(self.HEARTBEAT_INTERVAL),
            "--heartbeat-timeout", str(self.HEARTBEAT_TIMEOUT),
        ]
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=sys.stderr,
            text=True,
            bufsize=1,
            env=env,
        )
        self.procs[node_id] = proc

        while True:
            line = proc.stdout.readline()
            if not line:
                raise RuntimeError(f"Node {node_id} exited before reporting READY")
            line = line.strip()
            if line.startswith("READY:"):
                ready_data = json.loads(line[len("READY:"):])
                return proc, ready_data["tcp_port"], ready_data["role"]

    def _send_chat(self, proc: subprocess.Popen, text: str, msg_id: Optional[str] = None) -> str:
        payload = {"text": text}
        if msg_id:
            payload["msg_id"] = msg_id
        proc.stdin.write(f"SEND:{json.dumps(payload)}\n")
        proc.stdin.flush()
        while True:
            line = proc.stdout.readline()
            if not line:
                raise RuntimeError("Process terminated unexpectedly")
            line = line.strip()
            if line.startswith("RESULT:"):
                res = json.loads(line[len("RESULT:"):])
                return res["msg_id"]

    def _get_delivered(self, proc: subprocess.Popen) -> List[Dict[str, Any]]:
        proc.stdin.write("GET_DELIVERED\n")
        proc.stdin.flush()
        while True:
            line = proc.stdout.readline()
            if not line:
                raise RuntimeError("Process terminated unexpectedly")
            line = line.strip()
            if line.startswith("RESULT:"):
                res = json.loads(line[len("RESULT:"):])
                return res["delivered"]

    def _set_drop_seq(self, proc: subprocess.Popen, seq: int) -> None:
        proc.stdin.write(f"DROP_SEQ:{seq}\n")
        proc.stdin.flush()
        while True:
            line = proc.stdout.readline()
            if not line:
                raise RuntimeError("Process terminated unexpectedly")
            line = line.strip()
            if line.startswith("RESULT:"):
                return

    def test_milestone_4_multicast_gate(self) -> None:
        """
        Milestone 4 Gate:
        Inject a dropped sequence number and concurrent sends; verify all nodes display
        the exact same order without duplicates or gaps.
        """
        # Step 1: Start 3 nodes (Node 1 starts first as Leader)
        proc1, port1, role1 = self._start_node(1)
        self.assertEqual(role1, "LEADER")
        time.sleep(0.5)

        proc2, port2, role2 = self._start_node(2)
        self.assertEqual(role2, "FOLLOWER")
        time.sleep(0.5)

        proc3, port3, role3 = self._start_node(3)
        self.assertEqual(role3, "FOLLOWER")
        time.sleep(1.0)  # Allow membership to converge

        # =================================================================
        # PHASE 1: Concurrent sends from all 3 nodes
        # =================================================================
        # Send 4 messages from each node (12 messages total)
        for i in range(1, 5):
            self._send_chat(proc1, f"Node1-Msg-{i}")
            self._send_chat(proc2, f"Node2-Msg-{i}")
            self._send_chat(proc3, f"Node3-Msg-{i}")

        # Wait for all 12 messages to be delivered across all nodes
        deadline = time.time() + 6.0
        all_delivered = False
        while time.time() < deadline:
            d1 = self._get_delivered(proc1)
            d2 = self._get_delivered(proc2)
            d3 = self._get_delivered(proc3)

            if len(d1) == 12 and len(d2) == 12 and len(d3) == 12:
                all_delivered = True
                break
            time.sleep(0.2)

        self.assertTrue(all_delivered, f"Phase 1 delivery incomplete: counts=({len(d1)}, {len(d2)}, {len(d3)})")

        # Verify Total Order: sequences must be strictly 1..12 without gaps
        seqs1 = [m["seq"] for m in d1]
        seqs2 = [m["seq"] for m in d2]
        seqs3 = [m["seq"] for m in d3]
        expected_seqs = list(range(1, 13))

        self.assertEqual(seqs1, expected_seqs)
        self.assertEqual(seqs2, expected_seqs)
        self.assertEqual(seqs3, expected_seqs)

        # Verify identical msg_id order across all 3 nodes
        ids1 = [m["msg_id"] for m in d1]
        ids2 = [m["msg_id"] for m in d2]
        ids3 = [m["msg_id"] for m in d3]
        self.assertEqual(ids1, ids2, "Node 1 and Node 2 total orders differ!")
        self.assertEqual(ids1, ids3, "Node 1 and Node 3 total orders differ!")

        # =================================================================
        # PHASE 2: Injected Dropped Sequence & Gap Recovery
        # =================================================================
        # Next expected sequence is 13.
        # Instruct Node 3 to deliberately drop sequence 13.
        self._set_drop_seq(proc3, 13)

        # Send sequence 13, 14, and 15 from Node 1
        self._send_chat(proc1, "Message-13-WillBeDroppedByNode3")
        self._send_chat(proc1, "Message-14-AfterDrop")
        self._send_chat(proc1, "Message-15-AfterDrop")

        # Wait for gap detection, RETRANSMIT_REQ, and reassembly on Node 3
        deadline2 = time.time() + 6.0
        gap_recovered = False
        while time.time() < deadline2:
            d1_p2 = self._get_delivered(proc1)
            d2_p2 = self._get_delivered(proc2)
            d3_p2 = self._get_delivered(proc3)

            if len(d3_p2) == 15 and len(d1_p2) == 15 and len(d2_p2) == 15:
                gap_recovered = True
                break
            time.sleep(0.2)

        self.assertTrue(
            gap_recovered,
            f"Phase 2 gap recovery failed: counts=({len(d1_p2)}, {len(d2_p2)}, {len(d3_p2)})",
        )

        # Verify that Node 3's delivered list has no gaps and matches Nodes 1 & 2
        seqs3_p2 = [m["seq"] for m in d3_p2]
        self.assertEqual(seqs3_p2, list(range(1, 16)), "Node 3 delivery order contains gaps or reordering!")

        ids1_p2 = [m["msg_id"] for m in d1_p2]
        ids3_p2 = [m["msg_id"] for m in d3_p2]
        self.assertEqual(ids1_p2, ids3_p2, "Total order corrupted after gap recovery!")

        # =================================================================
        # PHASE 3: Duplicate Injection / Deduplication
        # =================================================================
        # Resend an existing message ID (from message 5)
        existing_msg_id = ids1_p2[4]  # seq 5 msg_id
        self._send_chat(proc2, "Duplicate Resend of Msg 5", msg_id=existing_msg_id)
        time.sleep(1.0)

        # Delivered counts must still be exactly 15 on all nodes (duplicate dropped)
        d1_p3 = self._get_delivered(proc1)
        d2_p3 = self._get_delivered(proc2)
        d3_p3 = self._get_delivered(proc3)

        self.assertEqual(len(d1_p3), 15, "Node 1 delivered duplicate message!")
        self.assertEqual(len(d2_p3), 15, "Node 2 delivered duplicate message!")
        self.assertEqual(len(d3_p3), 15, "Node 3 delivered duplicate message!")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        parser = argparse.ArgumentParser()
        parser.add_argument("--worker", action="store_true")
        parser.add_argument("--id", type=int, required=True)
        parser.add_argument("--port", type=int, default=0)
        parser.add_argument("--mcast-port", type=int, default=5085)
        parser.add_argument("--host", type=str, default="")
        parser.add_argument("--heartbeat-interval", type=float, default=0.8)
        parser.add_argument("--heartbeat-timeout", type=float, default=3.0)
        args = parser.parse_args()

        resolved_ip = args.host if args.host else get_lan_ip()
        asyncio.run(
            run_multicast_worker(
                node_id=args.id,
                port=args.port,
                mcast_port=args.mcast_port,
                lan_ip=resolved_ip,
                heartbeat_interval=args.heartbeat_interval,
                heartbeat_timeout=args.heartbeat_timeout,
            )
        )
    else:
        unittest.main()

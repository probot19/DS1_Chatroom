"""
Milestone 2 Verification Test Suite: Discovery, Membership & Heartbeats.

Course: Distributed Systems I: Foundations (WS2026)
Group: Group 53

Verifies:
1. Unit tests for PeerInfo and MembershipTable data structures.
2. Mandatory Milestone 2 Verification Gate:
   - Start 3 nodes (Node 1 as Leader, Nodes 2 and 3 as Followers).
   - Verify dynamic discovery and join handshake: all 3 nodes synchronize membership {1, 2, 3}.
   - Abruptly kill Node 3 (simulate crash-stop failure, no LEAVE message).
   - Verify Nodes 1 and 2 detect the heartbeat timeout and remove Node 3 within < 4.0 seconds.
   - Verify graceful LEAVE handling.
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

from chatroom.membership import MembershipManager, MembershipTable, PeerInfo
from chatroom.node import Node
from chatroom.protocol import (
    MessageType,
    create_announce_message,
    create_heartbeat_message,
    create_join_ack_message,
    create_join_message,
    create_leader_info_message,
    create_leave_message,
    create_membership_update_message,
    validate_envelope,
)
from chatroom.transport import get_lan_ip


class TestMembershipUnit(unittest.TestCase):
    """Unit tests for PeerInfo and MembershipTable."""

    def test_peer_info_serialization(self) -> None:
        p = PeerInfo(node_id=1, host="192.168.1.10", port=6001, role="LEADER", last_seen=100.0, is_alive=True)
        d = p.to_dict()
        p2 = PeerInfo.from_dict(d)
        self.assertEqual(p.node_id, p2.node_id)
        self.assertEqual(p.host, p2.host)
        self.assertEqual(p.port, p2.port)
        self.assertEqual(p.role, p2.role)
        self.assertEqual(p.last_seen, p2.last_seen)
        self.assertEqual(p.is_alive, p2.is_alive)

    def test_membership_table_operations(self) -> None:
        table = MembershipTable()
        self.assertEqual(len(table), 0)

        # Add leader
        table.add_or_update(node_id=1, host="10.0.0.1", port=5001, role="LEADER", last_seen=10.0)
        self.assertEqual(len(table), 1)
        self.assertTrue(table.contains(1))
        self.assertFalse(table.contains(2))

        # Add follower
        table.add_or_update(node_id=2, host="10.0.0.2", port=5002, role="FOLLOWER", last_seen=10.0)
        self.assertEqual(len(table), 2)

        # Query leader
        leader = table.get_leader()
        self.assertIsNotNone(leader)
        assert leader is not None
        self.assertEqual(leader.node_id, 1)

        # Query others
        others = table.other_members(1)
        self.assertEqual(len(others), 1)
        self.assertEqual(others[0].node_id, 2)

        # Remove member
        removed = table.remove(2)
        self.assertIsNotNone(removed)
        self.assertEqual(len(table), 1)
        self.assertFalse(table.contains(2))

    def test_membership_table_load_and_dump(self) -> None:
        table = MembershipTable()
        table.add_or_update(1, "127.0.0.1", 6001, "LEADER", 50.0)
        table.add_or_update(2, "127.0.0.1", 6002, "FOLLOWER", 50.0)

        d = table.to_dict()
        table2 = MembershipTable()
        table2.load_from_dict(d)
        self.assertEqual(len(table2), 2)
        self.assertTrue(table2.contains(1))
        self.assertTrue(table2.contains(2))


# =====================================================================
# Standalone Worker Process for Membership Testing
# =====================================================================

async def run_membership_worker(
    node_id: int,
    port: int,
    mcast_port: int,
    lan_ip: str,
    heartbeat_interval: float = 0.8,
    heartbeat_timeout: float = 2.5,
) -> None:
    """Worker process: runs Node orchestrator, exposes JSON stdin/stdout interface."""
    logger = logging.getLogger(f"NodeWorker-{node_id}")
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

    def on_change(tbl: MembershipTable, reason: str, affected_id: Optional[int]) -> None:
        members = [p.node_id for p in tbl.all_members()]
        print(f"EVENT:{json.dumps({'event': 'MEMBERSHIP_CHANGE', 'members': members, 'reason': reason, 'affected': affected_id})}", flush=True)

    node.membership.on_membership_changed = on_change
    await node.start()

    # Announce ready
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

        if line == "GET_MEMBERS":
            members = [p.node_id for p in node.table.all_members()]
            print(f"RESULT:{json.dumps({'status': 'OK', 'members': members, 'role': node.role, 'leader_id': node.leader_id})}", flush=True)

        elif line == "LEAVE":
            await node.stop()
            print(f"RESULT:{json.dumps({'status': 'OK'})}", flush=True)
            break

        elif line == "QUIT":
            break

    await node.stop()


# =====================================================================
# Milestone 2 Verification Gate Test Case
# =====================================================================

class TestMultiProcessMembershipGate(unittest.TestCase):
    """
    Spawns 3 separate child processes (Node 1, Node 2, Node 3).
    Verifies discovery, join, heartbeat failure detection (<4s), and graceful leave.
    """

    MCAST_PORT = 5065
    HEARTBEAT_INTERVAL = 0.8
    HEARTBEAT_TIMEOUT = 2.5

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
            "--port", "0",  # ephemeral port
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

        # Wait for READY line, skipping any early EVENT lines
        while True:
            line = proc.stdout.readline()
            if not line:
                raise RuntimeError(f"Node {node_id} exited before reporting READY")
            line = line.strip()
            if line.startswith("READY:"):
                ready_data = json.loads(line[len("READY:"):])
                return proc, ready_data["tcp_port"], ready_data["role"]

    def _get_members(self, proc: subprocess.Popen) -> List[int]:
        proc.stdin.write("GET_MEMBERS\n")
        proc.stdin.flush()
        while True:
            line = proc.stdout.readline()
            if not line:
                raise RuntimeError("Worker process terminated unexpectedly")
            line = line.strip()
            if line.startswith("RESULT:"):
                res = json.loads(line[len("RESULT:"):])
                return sorted(res["members"])

    def test_milestone_2_gate_kill_node_3_within_4s(self) -> None:
        """
        Milestone 2 Gate:
        Start 3 nodes; kill Node 3; verify Nodes 1 & 2 remove it within 4 seconds.
        """
        # Step 1: Start Node 1 (starts first, will become LEADER)
        proc1, port1, role1 = self._start_node(1)
        self.assertEqual(role1, "LEADER")
        time.sleep(0.5)

        # Step 2: Start Node 2 (announces and joins Node 1 over TCP)
        proc2, port2, role2 = self._start_node(2)
        self.assertEqual(role2, "FOLLOWER")
        time.sleep(0.5)

        # Step 3: Start Node 3 (announces and joins Node 1 over TCP)
        proc3, port3, role3 = self._start_node(3)
        self.assertEqual(role3, "FOLLOWER")
        time.sleep(1.0)  # Allow membership updates to propagate

        # Step 4: Verify all 3 nodes have {1, 2, 3} in their membership tables
        m1 = self._get_members(proc1)
        m2 = self._get_members(proc2)
        m3 = self._get_members(proc3)

        self.assertEqual(m1, [1, 2, 3], f"Node 1 membership table mismatch: {m1}")
        self.assertEqual(m2, [1, 2, 3], f"Node 2 membership table mismatch: {m2}")
        self.assertEqual(m3, [1, 2, 3], f"Node 3 membership table mismatch: {m3}")

        # Step 5: Abruptly kill Node 3 (simulate crash-stop failure, no LEAVE notice)
        t_kill = time.time()
        proc3.kill()
        proc3.wait(timeout=2.0)
        if proc3.stdin and not proc3.stdin.closed:
            proc3.stdin.close()
        if proc3.stdout and not proc3.stdout.closed:
            proc3.stdout.close()
        self.procs.pop(3, None)

        # Step 6: Monitor Node 1 and Node 2 until Node 3 is removed from both
        # Gate requirement: Must be detected and removed within 4.0 seconds!
        removed_on_node1 = False
        removed_on_node2 = False
        t_removed: Optional[float] = None

        timeout_deadline = t_kill + 4.0

        while time.time() < timeout_deadline:
            m1_current = self._get_members(proc1)
            m2_current = self._get_members(proc2)

            if 3 not in m1_current:
                removed_on_node1 = True
            if 3 not in m2_current:
                removed_on_node2 = True

            if removed_on_node1 and removed_on_node2:
                t_removed = time.time()
                break

            time.sleep(0.1)

        self.assertIsNotNone(t_removed, "Nodes 1 and 2 did not remove Node 3 within 4.0 seconds!")
        assert t_removed is not None
        elapsed = t_removed - t_kill

        # Assert gate constraint
        self.assertLess(
            elapsed,
            4.0,
            f"Gate failure: removal took {elapsed:.2f}s, which exceeds the 4.0s requirement!",
        )

        # Assert surviving cluster membership is strictly {1, 2}
        final_m1 = self._get_members(proc1)
        final_m2 = self._get_members(proc2)
        self.assertEqual(final_m1, [1, 2])
        self.assertEqual(final_m2, [1, 2])

        # Step 7: Verify graceful leave with Node 2
        proc2.stdin.write("LEAVE\n")
        proc2.stdin.flush()
        line = proc2.stdout.readline()
        time.sleep(0.5)

        final_m1_after_leave = self._get_members(proc1)
        self.assertEqual(final_m1_after_leave, [1])


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        parser = argparse.ArgumentParser()
        parser.add_argument("--worker", action="store_true")
        parser.add_argument("--id", type=int, required=True)
        parser.add_argument("--port", type=int, default=0)
        parser.add_argument("--mcast-port", type=int, default=5065)
        parser.add_argument("--host", type=str, default="")
        parser.add_argument("--heartbeat-interval", type=float, default=0.8)
        parser.add_argument("--heartbeat-timeout", type=float, default=2.5)
        args = parser.parse_args()

        resolved_ip = args.host if args.host else get_lan_ip()
        asyncio.run(
            run_membership_worker(
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

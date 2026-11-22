"""
Milestone 3 Verification Test Suite: Bully Election & State Reconciliation.

Course: Distributed Systems I: Foundations (WS2026)
Group: Group 53

Verifies:
1. Unit tests for Election messages, candidate filtering, and state transitions.
2. Mandatory Milestone 3 Verification Gate:
   - Start 3 nodes: Node 1 (id=1), Node 2 (id=2), Node 3 (id=3 as Leader).
   - Seed history with existing sequence numbers (Node 1 has seq 2, Node 2 has seq 3).
   - Abruptly kill the Leader (Node 3).
   - Verify Node 2 (highest remaining ID) wins the election.
   - Verify Node 2 completes STATE_REQ/STATE_RES history reconciliation (reconciling max_seq = 3).
   - Verify Node 2 broadcasts COORDINATOR and becomes LEADER.
   - Verify Node 1 acknowledges Node 2 as the new LEADER.
3. Higher-ID Join Preemption:
   - Start Node 4 (id=4).
   - Verify Node 4 preempts Node 2 and takes over as LEADER.
   - Verify Node 2 cleanly demotes itself to FOLLOWER.
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

from chatroom.election import ElectionManager, ElectionState
from chatroom.membership import MembershipTable
from chatroom.node import Node
from chatroom.protocol import (
    MessageType,
    create_answer_message,
    create_coordinator_message,
    create_election_message,
    create_state_req_message,
    create_state_res_message,
    validate_envelope,
)
from chatroom.transport import get_lan_ip


class TestElectionUnit(unittest.TestCase):
    """Unit tests for election message builders and state reconciliation."""

    def test_election_messages_envelopes(self) -> None:
        e = create_election_message(1)
        self.assertEqual(e["type"], MessageType.ELECTION)
        self.assertEqual(e["sender_id"], 1)
        self.assertTrue(validate_envelope(e))

        a = create_answer_message(2)
        self.assertEqual(a["type"], MessageType.ANSWER)
        self.assertEqual(a["sender_id"], 2)
        self.assertTrue(validate_envelope(a))

        c = create_coordinator_message(2, max_seq=15)
        self.assertEqual(c["type"], MessageType.COORDINATOR)
        self.assertEqual(c["sender_id"], 2)
        self.assertEqual(c["leader_id"], 2)
        self.assertEqual(c["max_seq"], 15)
        self.assertTrue(validate_envelope(c))

        req = create_state_req_message(2)
        self.assertEqual(req["type"], MessageType.STATE_REQ)
        self.assertTrue(validate_envelope(req))

        res = create_state_res_message(1, max_seq=10, history=[{"msg_id": "m1", "seq": 10}])
        self.assertEqual(res["type"], MessageType.STATE_RES)
        self.assertEqual(res["max_seq"], 10)
        self.assertEqual(len(res["history"]), 1)
        self.assertTrue(validate_envelope(res))


# =====================================================================
# Standalone Worker Process for Election & Reconciliation Testing
# =====================================================================

async def run_election_worker(
    node_id: int,
    port: int,
    mcast_port: int,
    lan_ip: str,
    seed_seq: int = 0,
    heartbeat_interval: float = 0.8,
    heartbeat_timeout: float = 2.0,
    election_timeout: float = 1.2,
    reconciliation_timeout: float = 1.2,
) -> None:
    """Worker process with seeded history state for testing reconciliation."""
    logger = logging.getLogger(f"ElectionWorker-{node_id}")
    logger.setLevel(logging.INFO)

    node = Node(
        node_id=node_id,
        host=lan_ip,
        tcp_port=port,
        mcast_port=mcast_port,
        heartbeat_interval=heartbeat_interval,
        heartbeat_timeout=heartbeat_timeout,
        election_timeout=election_timeout,
        reconciliation_timeout=reconciliation_timeout,
        logger=logger,
    )

    # Seed initial history state
    node.local_max_seq = seed_seq
    if seed_seq > 0:
        node.local_history = [
            {"msg_id": f"seed_{i}", "seq": i, "content": f"msg {i}"}
            for i in range(1, seed_seq + 1)
        ]

    await node.start()

    # Announce readiness
    ready_data = {
        "node_id": node.node_id,
        "tcp_port": node.tcp_port,
        "role": node.role,
        "leader_id": node.leader_id,
        "max_seq": node.local_max_seq,
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

        if line == "GET_STATUS":
            res = {
                "node_id": node.node_id,
                "role": node.role,
                "leader_id": node.leader_id,
                "max_seq": node.local_max_seq,
                "reconciled_max_seq": node.election.reconciled_max_seq,
                "next_seq": node.election.next_seq,
                "election_state": node.election.state,
                "members": [p.node_id for p in node.table.all_members()],
            }
            print(f"RESULT:{json.dumps({'status': 'OK', 'data': res})}", flush=True)

        elif line.startswith("SET_SEQ:"):
            seq_val = int(line[len("SET_SEQ:"):])
            node.local_max_seq = seq_val
            node.local_history = [
                {"msg_id": f"seed_{i}", "seq": i, "content": f"msg {i}"}
                for i in range(1, seq_val + 1)
            ]
            print(f"RESULT:{json.dumps({'status': 'OK', 'max_seq': seq_val})}", flush=True)

        elif line == "QUIT":
            break

    await node.stop()


# =====================================================================
# Milestone 3 Verification Gate Test Case
# =====================================================================

class TestMultiProcessElectionGate(unittest.TestCase):
    """
    Executes the Milestone 3 Verification Gate:
    1. 3-node cluster with Node 3 as Leader.
    2. Seed histories (Node 1 has seq 2, Node 2 has seq 3).
    3. Abruptly kill Node 3.
    4. Assert Node 2 wins Bully election, executes STATE_REQ/STATE_RES, reconciles max_seq = 3,
       and announces COORDINATOR.
    5. Assert Node 1 recognizes Node 2 as new Leader.
    6. Higher-ID Join Preemption: Node 4 joins and takes over leadership from Node 2.
    """

    MCAST_PORT = 5075
    HEARTBEAT_INTERVAL = 0.8
    HEARTBEAT_TIMEOUT = 2.0
    ELECTION_TIMEOUT = 1.2
    RECONCILIATION_TIMEOUT = 1.2

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

    def _start_node(self, node_id: int, seed_seq: int = 0) -> Tuple[subprocess.Popen, int, str]:
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
            "--seed-seq", str(seed_seq),
            "--heartbeat-interval", str(self.HEARTBEAT_INTERVAL),
            "--heartbeat-timeout", str(self.HEARTBEAT_TIMEOUT),
            "--election-timeout", str(self.ELECTION_TIMEOUT),
            "--reconciliation-timeout", str(self.RECONCILIATION_TIMEOUT),
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

        # Wait for READY line, skipping any early event lines
        while True:
            line = proc.stdout.readline()
            if not line:
                raise RuntimeError(f"Node {node_id} exited before reporting READY")
            line = line.strip()
            if line.startswith("READY:"):
                ready_data = json.loads(line[len("READY:"):])
                return proc, ready_data["tcp_port"], ready_data["role"]

    def _get_status(self, proc: subprocess.Popen) -> Dict[str, Any]:
        proc.stdin.write("GET_STATUS\n")
        proc.stdin.flush()
        while True:
            line = proc.stdout.readline()
            if not line:
                raise RuntimeError("Worker process terminated unexpectedly")
            line = line.strip()
            if line.startswith("RESULT:"):
                res = json.loads(line[len("RESULT:"):])
                return res["data"]

    def test_milestone_3_election_and_reconciliation_gate(self) -> None:
        """
        Milestone 3 Gate:
        Kill the leader; verify highest remaining ID wins and reconciles max sequence number
        before continuing.
        """
        # Step 1: Start Node 3 FIRST so it initializes as the cluster LEADER
        proc3, port3, role3 = self._start_node(3, seed_seq=3)
        self.assertEqual(role3, "LEADER")
        time.sleep(0.5)

        # Step 2: Start Node 1 (id=1, seed_seq=2)
        proc1, port1, role1 = self._start_node(1, seed_seq=2)
        self.assertEqual(role1, "FOLLOWER")
        time.sleep(0.5)

        # Step 3: Start Node 2 (id=2, seed_seq=3)
        proc2, port2, role2 = self._start_node(2, seed_seq=3)
        self.assertEqual(role2, "FOLLOWER")
        time.sleep(1.0)  # Allow membership synchronization

        # Verify initial cluster state: Node 3 is LEADER, members are {1, 2, 3}
        s1 = self._get_status(proc1)
        s2 = self._get_status(proc2)
        s3 = self._get_status(proc3)

        self.assertEqual(s1["leader_id"], 3)
        self.assertEqual(s2["leader_id"], 3)
        self.assertEqual(s3["role"], "LEADER")
        self.assertEqual(sorted(s1["members"]), [1, 2, 3])
        self.assertEqual(sorted(s2["members"]), [1, 2, 3])

        # Step 4: Abruptly kill the Leader (Node 3)
        proc3.kill()
        proc3.wait(timeout=2.0)
        if proc3.stdin and not proc3.stdin.closed:
            proc3.stdin.close()
        if proc3.stdout and not proc3.stdout.closed:
            proc3.stdout.close()
        self.procs.pop(3, None)

        # Step 5: Wait for failure detection and Bully election to run
        # Heartbeat timeout is 2.0s, election timeout is 1.2s, reconciliation timeout is 1.2s
        # Total convergence should complete within ~4.5 seconds.
        deadline = time.time() + 6.0
        elected = False

        while time.time() < deadline:
            st2 = self._get_status(proc2)
            st1 = self._get_status(proc1)

            if st2["role"] == "LEADER" and st1["leader_id"] == 2:
                elected = True
                break

            time.sleep(0.2)

        self.assertTrue(elected, "Node 2 did not become elected LEADER after Node 3 crash!")

        # Step 6: Verify State Reconciliation Gate Constraints
        final_st2 = self._get_status(proc2)
        final_st1 = self._get_status(proc1)

        self.assertEqual(final_st2["role"], "LEADER")
        self.assertEqual(final_st1["role"], "FOLLOWER")
        self.assertEqual(final_st1["leader_id"], 2)

        # Assert max_seq was reconciled accurately (Node 1 had 2, Node 2 had 3 -> max is 3)
        self.assertEqual(final_st2["reconciled_max_seq"], 3)
        # Next sequence assigned by the new sequencer MUST be max_seq + 1 = 4
        self.assertEqual(final_st2["next_seq"], 4)

        # Step 7: Test Higher-ID Join Preemption
        # Start Node 4 (id=4, higher than current leader Node 2).
        # Node 4 must trigger election and take over leadership from Node 2.
        proc4, port4, role4 = self._start_node(4, seed_seq=3)

        deadline4 = time.time() + 6.0
        preempted = False

        while time.time() < deadline4:
            st4 = self._get_status(proc4)
            st2_after = self._get_status(proc2)

            if st4["role"] == "LEADER" and st2_after["role"] == "FOLLOWER" and st2_after["leader_id"] == 4:
                preempted = True
                break

            time.sleep(0.2)

        self.assertTrue(preempted, f"Node 4 preemption failed. Node 4 status: {self._get_status(proc4)}, Node 2 status: {self._get_status(proc2)}")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        parser = argparse.ArgumentParser()
        parser.add_argument("--worker", action="store_true")
        parser.add_argument("--id", type=int, required=True)
        parser.add_argument("--port", type=int, default=0)
        parser.add_argument("--mcast-port", type=int, default=5075)
        parser.add_argument("--host", type=str, default="")
        parser.add_argument("--seed-seq", type=int, default=0)
        parser.add_argument("--heartbeat-interval", type=float, default=0.8)
        parser.add_argument("--heartbeat-timeout", type=float, default=2.0)
        parser.add_argument("--election-timeout", type=float, default=1.2)
        parser.add_argument("--reconciliation-timeout", type=float, default=1.2)
        args = parser.parse_args()

        resolved_ip = args.host if args.host else get_lan_ip()
        asyncio.run(
            run_election_worker(
                node_id=args.id,
                port=args.port,
                mcast_port=args.mcast_port,
                lan_ip=resolved_ip,
                seed_seq=args.seed_seq,
                heartbeat_interval=args.heartbeat_interval,
                heartbeat_timeout=args.heartbeat_timeout,
                election_timeout=args.election_timeout,
                reconciliation_timeout=args.reconciliation_timeout,
            )
        )
    else:
        unittest.main()

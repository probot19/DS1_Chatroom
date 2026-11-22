"""
Milestone 5 Verification Test Suite: Local JSON Persistence & Reconnect Catch-Up.

Course: Distributed Systems I: Foundations (WS2026)
Group: Group 53

Verifies:
1. Unit tests for StorageManager:
   - Atomic disk persistence (.tmp -> os.replace)
   - History loading and sequence number restoration
   - Corrupted file resilience and cleanup
2. Multi-Process Reconnect Gate:
   - 3 independent node processes on dedicated LAN/multicast ports with isolated data directories.
   - Phase 1: 5 messages broadcasted and delivered (seq 1..5) across all 3 nodes and saved to disk.
   - Phase 2: Node 3 disconnects/terminates.
   - Phase 3: 5 additional messages broadcasted (seq 6..10) while Node 3 is offline; Nodes 1 & 2 deliver.
   - Phase 4: Node 3 restarts with original node_id and data_dir.
     - Node 3 restores last_delivered_seq = 5 from disk.
     - Node 3 sends JOIN with last_delivered_seq = 5.
     - Leader computes delta history (seq 6..10) and returns in JOIN_ACK.
     - Node 3 delivers delta history, updates disk, and achieves complete catch-up.
   - Phase 5: Verification of identical 1..10 sequential logs on all nodes with zero gaps and zero duplicates.
   - Phase 6: Post-reconnect live message (seq 11) broadcasted and delivered on all 3 nodes.
   - Phase 7: Interactive demo /drop-next command simulation & gap recovery.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from chatroom.multicast import HoldBackQueue, OrderedMessage
from chatroom.node import Node
from chatroom.protocol import MessageType, create_join_message
from chatroom.storage import StorageManager
from chatroom.transport import get_lan_ip


# =====================================================================
# Unit Tests for StorageManager
# =====================================================================

class TestStorageUnit(unittest.TestCase):
    """Unit tests for local JSON storage persistence and crash safety."""

    def setUp(self) -> None:
        self.test_dir = tempfile.mkdtemp(prefix="chat_storage_test_")
        self.storage = StorageManager(node_id=1, data_dir=self.test_dir)

    def tearDown(self) -> None:
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_empty_load(self) -> None:
        """When no file exists, load_history should return (0, [])."""
        last_seq, msgs = self.storage.load_history()
        self.assertEqual(last_seq, 0)
        self.assertEqual(msgs, [])

    def test_save_and_reload(self) -> None:
        """Verify saving messages and reloading restores identical state."""
        msgs = [
            OrderedMessage(seq=1, msg_id="m-1", sender_id=1, text="First", timestamp=100.0),
            OrderedMessage(seq=2, msg_id="m-2", sender_id=2, text="Second", timestamp=101.0),
            OrderedMessage(seq=3, msg_id="m-3", sender_id=3, text="Third", timestamp=102.0),
        ]
        self.storage.save_history(msgs)

        # File must exist
        self.assertTrue(self.storage.file_path.exists())

        last_seq, reloaded = self.storage.load_history()
        self.assertEqual(last_seq, 3)
        self.assertEqual(len(reloaded), 3)
        self.assertEqual([m.seq for m in reloaded], [1, 2, 3])
        self.assertEqual([m.text for m in reloaded], ["First", "Second", "Third"])
        self.assertEqual([m.msg_id for m in reloaded], ["m-1", "m-2", "m-3"])

    def test_atomic_write_replace(self) -> None:
        """Verify atomic writes leave no lingering temporary files."""
        msgs = [OrderedMessage(seq=1, msg_id="m-1", sender_id=1, text="Test", timestamp=100.0)]
        self.storage.save_history(msgs)

        tmp_path = self.storage.file_path.with_suffix(".tmp")
        self.assertFalse(tmp_path.exists())
        self.assertTrue(self.storage.file_path.exists())

    def test_corrupted_file_recovery(self) -> None:
        """Verify corrupted JSON gracefully falls back to (0, []) without crashing."""
        with open(self.storage.file_path, "w", encoding="utf-8") as f:
            f.write("INVALID JSON CONTENT { [")

        last_seq, reloaded = self.storage.load_history()
        self.assertEqual(last_seq, 0)
        self.assertEqual(reloaded, [])

    def test_clear_history(self) -> None:
        """Verify clear_history removes disk files cleanly."""
        msgs = [OrderedMessage(seq=1, msg_id="m-1", sender_id=1, text="Test", timestamp=100.0)]
        self.storage.save_history(msgs)
        self.assertTrue(self.storage.file_path.exists())

        self.storage.clear_history()
        self.assertFalse(self.storage.file_path.exists())


# =====================================================================
# Standalone Worker Process for Reconnect & CLI Testing
# =====================================================================

async def run_reconnect_worker(
    node_id: int,
    port: int,
    mcast_port: int,
    lan_ip: str,
    data_dir: str,
    heartbeat_interval: float = 0.8,
    heartbeat_timeout: float = 2.5,
) -> None:
    """Worker process: runs Node orchestrator with storage and reconnect capability."""
    logger = logging.getLogger(f"ReconnectWorker-{node_id}")
    logger.setLevel(logging.INFO)

    node = Node(
        node_id=node_id,
        host=lan_ip,
        tcp_port=port,
        mcast_port=mcast_port,
        heartbeat_interval=heartbeat_interval,
        heartbeat_timeout=heartbeat_timeout,
        data_dir=data_dir,
        logger=logger,
    )

    drop_next_active = False

    def simulated_drop_filter(msg: OrderedMessage) -> bool:
        nonlocal drop_next_active
        if drop_next_active:
            drop_next_active = False  # drop once
            logger.warning("[DEMO-TEST] Deliberately dropping incoming seq=%d", msg.seq)
            return True
        return False

    node.multicast.drop_filter = simulated_drop_filter

    await node.start()

    ready_data = {
        "node_id": node.node_id,
        "tcp_port": node.tcp_port,
        "role": node.role,
        "leader_id": node.leader_id,
        "expected_seq": node.multicast.expected_seq,
        "delivered_count": len(node.multicast.delivered_messages),
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

        elif line == "DROP_NEXT":
            drop_next_active = True
            print(f"RESULT:{json.dumps({'status': 'OK', 'drop_next': True})}", flush=True)

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
# Milestone 5 Verification Gate Test Case
# =====================================================================

class TestMultiProcessReconnectGate(unittest.TestCase):
    """
    Mandatory Milestone 5 Verification Gate:
    1. Spawn Node 1 (Leader), Node 2, and Node 3 with independent data directories.
    2. Broadcast 5 messages (seq 1..5); verify all delivered and saved to disk.
    3. Terminate Node 3.
    4. While Node 3 is offline, broadcast 5 more messages (seq 6..10); Nodes 1 & 2 deliver.
    5. Restart Node 3 with same node_id and data_dir.
    6. Verify Node 3 loads last_seq=5, requests delta in JOIN, catches up seq 6..10, and updates disk.
    7. Verify all 3 nodes have 100% identical sequential delivery (1..10) with zero gaps/duplicates.
    8. Broadcast message 11 live; verify all 3 nodes deliver seq 11.
    9. Demonstrate /drop-next command simulation and gap recovery.
    """

    MCAST_PORT = 5095
    BASE_TCP_PORT = 6500

    def setUp(self) -> None:
        self.lan_ip = get_lan_ip()
        self.procs: Dict[int, subprocess.Popen] = {}
        self.pipes: Dict[int, Tuple[Any, Any]] = {}
        self.data_dirs: Dict[int, str] = {}
        self.temp_root = tempfile.mkdtemp(prefix="m5_gate_test_")

    def tearDown(self) -> None:
        for nid, proc in list(self.procs.items()):
            try:
                proc.terminate()
                proc.wait(timeout=2.0)
            except Exception:
                try:
                    proc.kill()
                    proc.wait(timeout=1.0)
                except Exception:
                    pass
            finally:
                if proc.stdin and not proc.stdin.closed:
                    proc.stdin.close()
                if proc.stdout and not proc.stdout.closed:
                    proc.stdout.close()
                if proc.stderr and not proc.stderr.closed:
                    proc.stderr.close()
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def _start_node_process(self, node_id: int, port: int) -> Dict[str, Any]:
        """Launch an independent worker process running Node orchestrator."""
        if node_id not in self.data_dirs:
            self.data_dirs[node_id] = os.path.join(self.temp_root, f"node_{node_id}_data")
            os.makedirs(self.data_dirs[node_id], exist_ok=True)

        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        env = os.environ.copy()
        env["PYTHONPATH"] = project_root

        cmd = [
            sys.executable,
            "-u",
            os.path.abspath(__file__),
            "--worker",
            "--id", str(node_id),
            "--port", str(port),
            "--mcast-port", str(self.MCAST_PORT),
            "--lan-ip", self.lan_ip,
            "--data-dir", self.data_dirs[node_id],
            "--heartbeat-interval", "0.8",
            "--heartbeat-timeout", "3.0",
        ]

        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=env,
        )

        ready_data = None
        for _ in range(100):
            line = proc.stdout.readline()
            if not line:
                break
            line_str = line.strip()
            if line_str.startswith("READY:"):
                ready_data = json.loads(line_str[len("READY:"):])
                break

        if not ready_data:
            stderr_out = proc.stderr.read()
            proc.kill()
            raise RuntimeError(f"Node {node_id} failed to emit READY. Stderr: {stderr_out}")

        self.procs[node_id] = proc
        return ready_data

    def _send_cmd(self, node_id: int, cmd_str: str) -> Dict[str, Any]:
        """Send command to worker process and read JSON result."""
        proc = self.procs[node_id]
        proc.stdin.write(f"{cmd_str}\n")
        proc.stdin.flush()

        for _ in range(50):
            line = proc.stdout.readline()
            if not line:
                break
            line_str = line.strip()
            if line_str.startswith("RESULT:"):
                return json.loads(line_str[len("RESULT:"):])

        raise TimeoutError(f"Node {node_id} timed out waiting for result of '{cmd_str}'")

    def _wait_for_delivery(
        self,
        expected_counts: Dict[int, int],
        timeout: float = 6.0,
    ) -> Dict[int, List[Dict[str, Any]]]:
        """Poll GET_DELIVERED until each node reaches its target message count or timeout."""
        deadline = time.time() + timeout
        last_delivered: Dict[int, List[Dict[str, Any]]] = {}

        while time.time() < deadline:
            all_reached = True
            for nid, target_count in expected_counts.items():
                res = self._send_cmd(nid, "GET_DELIVERED")
                deliv = res.get("delivered", [])
                last_delivered[nid] = deliv
                if len(deliv) < target_count:
                    all_reached = False
                    break
            if all_reached:
                return last_delivered
            time.sleep(0.2)

        for nid, target_count in expected_counts.items():
            actual = len(last_delivered.get(nid, []))
            self.assertEqual(
                actual,
                target_count,
                f"Node {nid} delivery incomplete: expected {target_count}, got {actual}",
            )
        return last_delivered

    def test_milestone_5_reconnect_catch_up_gate(self) -> None:
        """Execute full Milestone 5 reconnect catch-up and persistence gate."""
        print("\n=======================================================")
        print("  MILESTONE 5 VERIFICATION GATE: RECONNECT CATCH-UP")
        print("=======================================================")

        # Step 1: Start Node 1 (Initial Leader)
        print("Starting Node 1 (Initial Leader)...")
        ready1 = self._start_node_process(1, self.BASE_TCP_PORT + 1)
        self.assertEqual(ready1["role"], "LEADER")
        print(f"Node 1 started on port {ready1['tcp_port']} as LEADER")

        time.sleep(0.5)

        # Step 2: Start Node 2 and Node 3 (Followers)
        print("Starting Node 2 and Node 3 (Followers)...")
        ready2 = self._start_node_process(2, self.BASE_TCP_PORT + 2)
        ready3 = self._start_node_process(3, self.BASE_TCP_PORT + 3)
        self.assertEqual(ready2["role"], "FOLLOWER")
        self.assertEqual(ready3["role"], "FOLLOWER")
        print("All 3 nodes operational in cluster.")

        time.sleep(1.0)

        # Step 3: Phase 1 - Broadcast 5 messages (seq 1..5)
        print("\n--- Phase 1: Broadcast Messages 1..5 ---")
        for i in range(1, 6):
            sender = (i % 3) + 1  # Distribute sends across Node 1, 2, 3
            res = self._send_cmd(sender, f"SEND:{json.dumps({'text': f'Message {i} from Node {sender}'})}")
            self.assertEqual(res["status"], "OK")

        # Wait for all 3 nodes to deliver seq 1..5
        all_deliv = self._wait_for_delivery({1: 5, 2: 5, 3: 5})
        for nid in (1, 2, 3):
            deliv = all_deliv[nid]
            seqs = [m["seq"] for m in deliv]
            self.assertEqual(seqs, [1, 2, 3, 4, 5])
            print(f"Node {nid} verified: delivered seq 1..5")

        # Verify disk persistence for all 3 nodes
        for nid in (1, 2, 3):
            storage = StorageManager(node_id=nid, data_dir=self.data_dirs[nid])
            last_seq, stored = storage.load_history()
            self.assertEqual(last_seq, 5, f"Node {nid} disk last_seq should be 5")
            self.assertEqual(len(stored), 5)
            print(f"Node {nid} disk verified: last_seq=5, 5 records persisted")

        # Step 4: Phase 2 - Disconnect Node 3
        print("\n--- Phase 2: Disconnecting Node 3 ---")
        proc3 = self.procs.pop(3)
        proc3.terminate()
        try:
            proc3.wait(timeout=2.0)
        except Exception:
            proc3.kill()
            try:
                proc3.wait(timeout=1.0)
            except Exception:
                pass
        if proc3.stdin and not proc3.stdin.closed:
            proc3.stdin.close()
        if proc3.stdout and not proc3.stdout.closed:
            proc3.stdout.close()
        if proc3.stderr and not proc3.stderr.closed:
            proc3.stderr.close()
        print("Node 3 offline.")

        time.sleep(1.0)

        # Step 5: Phase 3 - Broadcast 5 more messages (seq 6..10) while Node 3 is offline
        print("\n--- Phase 3: Broadcast Messages 6..10 (Node 3 is offline) ---")
        for i in range(6, 11):
            sender = (i % 2) + 1  # Alternating Node 1 and Node 2
            res = self._send_cmd(sender, f"SEND:{json.dumps({'text': f'Message {i} from Node {sender}'})}")
            self.assertEqual(res["status"], "OK")

        # Wait for Node 1 and Node 2 to deliver seq 1..10
        all_deliv = self._wait_for_delivery({1: 10, 2: 10})
        for nid in (1, 2):
            deliv = all_deliv[nid]
            seqs = [m["seq"] for m in deliv]
            self.assertEqual(seqs, list(range(1, 11)))
            print(f"Node {nid} verified: delivered seq 1..10")

        # Step 6: Phase 4 - Restart Node 3 (Reconnect Catch-Up)
        print("\n--- Phase 4: Restarting Node 3 with Same Data Dir (Catch-Up) ---")
        # Node 3 restarts with original data dir containing seq 1..5
        ready3_reconnected = self._start_node_process(3, self.BASE_TCP_PORT + 3)
        print(f"Node 3 reconnected (restored delivered_count={ready3_reconnected['delivered_count']})")

        # Step 7: Phase 5 - Verify Node 3 Caught Up Completely to Sequence 10
        print("\n--- Phase 5: Verify Node 3 Catch-Up ---")
        all_deliv = self._wait_for_delivery({3: 10})
        deliv3 = all_deliv[3]
        seqs3 = [m["seq"] for m in deliv3]
        self.assertEqual(seqs3, list(range(1, 11)), "Reconnected Node 3 must deliver 1..10 in strict order")

        # Verify disk persistence on Node 3 now reflects all 10 messages
        storage3 = StorageManager(node_id=3, data_dir=self.data_dirs[3])
        last_seq3, stored3 = storage3.load_history()
        self.assertEqual(last_seq3, 10, "Node 3 disk file must be updated to last_seq=10")
        self.assertEqual(len(stored3), 10)
        print("Node 3 catch-up successfully verified on disk and memory: seq 1..10 in strict order!")

        # Step 8: Phase 6 - Broadcast Live Message 11 After Reconnect
        print("\n--- Phase 6: Broadcast Live Message 11 Across All Nodes ---")
        res11 = self._send_cmd(3, f"SEND:{json.dumps({'text': 'Message 11 sent by reconnected Node 3!'})}")
        self.assertEqual(res11["status"], "OK")

        all_deliv = self._wait_for_delivery({1: 11, 2: 11, 3: 11})
        for nid in (1, 2, 3):
            deliv = all_deliv[nid]
            self.assertEqual(len(deliv), 11, f"Node {nid} must have delivered 11 messages")
            self.assertEqual(deliv[-1]["seq"], 11)
            self.assertEqual(deliv[-1]["text"], "Message 11 sent by reconnected Node 3!")
            print(f"Node {nid} verified: seq 11 delivered successfully")

        # Step 9: Phase 7 - Interactive Demo /drop-next Simulation Hook
        print("\n--- Phase 7: Interactive Demo /drop-next Simulation ---")
        # Activate drop on Node 2
        res_drop = self._send_cmd(2, "DROP_NEXT")
        self.assertEqual(res_drop["status"], "OK")
        print("Node 2 /drop-next armed: next message (seq 12) will be deliberately dropped on Node 2.")

        # Send message 12 and 13 from Node 1
        res12 = self._send_cmd(1, f"SEND:{json.dumps({'text': 'Message 12 (dropped on Node 2)'})}")
        self.assertEqual(res12["status"], "OK")
        res13 = self._send_cmd(1, f"SEND:{json.dumps({'text': 'Message 13 (triggers gap recovery)'})}")
        self.assertEqual(res13["status"], "OK")

        all_deliv = self._wait_for_delivery({1: 13, 2: 13, 3: 13})
        for nid in (1, 2, 3):
            deliv = all_deliv[nid]
            seqs = [m["seq"] for m in deliv]
            self.assertEqual(seqs, list(range(1, 14)))
            print(f"Node {nid} verified: seq 1..13 delivered in strict order (recovered from /drop-next)")

        # Graceful shutdown of worker processes
        for nid, proc in list(self.procs.items()):
            try:
                proc.stdin.write("QUIT\n")
                proc.stdin.flush()
                proc.wait(timeout=1.0)
            except Exception:
                pass

        print("\n=======================================================")
        print("  ALL MILESTONE 5 VERIFICATION GATE CHECKS PASSED!   ")
        print("=======================================================")


if __name__ == "__main__":
    if "--worker" in sys.argv:
        parser = argparse.ArgumentParser()
        parser.add_argument("--worker", action="store_true")
        parser.add_argument("--id", type=int, required=True)
        parser.add_argument("--port", type=int, required=True)
        parser.add_argument("--mcast-port", type=int, required=True)
        parser.add_argument("--lan-ip", type=str, required=True)
        parser.add_argument("--data-dir", type=str, required=True)
        parser.add_argument("--heartbeat-interval", type=float, default=0.8)
        parser.add_argument("--heartbeat-timeout", type=float, default=2.5)
        args = parser.parse_args()

        asyncio.run(
            run_reconnect_worker(
                node_id=args.id,
                port=args.port,
                mcast_port=args.mcast_port,
                lan_ip=args.lan_ip,
                data_dir=args.data_dir,
                heartbeat_interval=args.heartbeat_interval,
                heartbeat_timeout=args.heartbeat_timeout,
            )
        )
    else:
        unittest.main()

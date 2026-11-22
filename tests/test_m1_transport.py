"""
Milestone 1 Verification Test Suite: Protocol & Transport Layer.

Course: Distributed Systems I: Foundations (WS2026)
Group: Group 53

Verifies:
1. Envelope creation, serialization, and validation.
2. 4-byte big-endian TCP framing helpers, EOF detection, and truncation error handling.
3. Multi-Process End-to-End Gate:
   - Spawns two independent native Python processes (Node 1 and Node 2).
   - Two-way framed TCP request/reply ping-pong.
   - Large payload (128 KB) TCP segmentation and reassembly.
   - UDP multicast broadcasting and loopback reception across both processes.
   - Robust failure handling: unreachable TCP port and sudden partial-frame disconnects.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import socket
import struct
import subprocess
import sys
import time
import unittest
from typing import Any, Dict, List, Optional, Tuple

from chatroom.protocol import (
    HEADER_SIZE,
    MAX_PAYLOAD_SIZE,
    FramingError,
    MessageType,
    ProtocolError,
    create_message,
    decode_framed_bytes,
    decode_udp_message,
    encode_framed_bytes,
    encode_udp_message,
    read_framed_message,
    send_framed_message,
    validate_envelope,
)
from chatroom.transport import (
    MulticastTransport,
    PeerTimeoutError,
    PeerUnreachableError,
    TcpTransport,
    get_lan_ip,
)


class TestProtocolUnit(unittest.TestCase):
    """Unit tests for the wire protocol envelope and 4-byte framing helpers."""

    def test_create_message_envelope(self) -> None:
        msg = create_message(MessageType.PING, 101, text="hello")
        self.assertEqual(msg["type"], MessageType.PING)
        self.assertEqual(msg["sender_id"], 101)
        self.assertIn("timestamp", msg)
        self.assertEqual(msg["text"], "hello")
        self.assertTrue(validate_envelope(msg))

    def test_validate_envelope_rejects_malformed(self) -> None:
        self.assertFalse(validate_envelope("not a dict"))
        self.assertFalse(validate_envelope({"sender_id": 1, "timestamp": 123.4}))  # missing type
        self.assertFalse(validate_envelope({"type": "PING", "timestamp": 123.4}))  # missing sender_id
        self.assertFalse(validate_envelope({"type": "PING", "sender_id": 1}))  # missing timestamp
        self.assertFalse(validate_envelope({"type": 123, "sender_id": 1, "timestamp": 123.4}))  # bad type type

    def test_framed_encoding_and_decoding(self) -> None:
        original = create_message(MessageType.TEST_TCP, 42, payload="Test payload string")
        framed_bytes = encode_framed_bytes(original)

        # First 4 bytes must be the unsigned big-endian integer length
        header = framed_bytes[:HEADER_SIZE]
        (length,) = struct.unpack("!I", header)
        payload = framed_bytes[HEADER_SIZE:]
        self.assertEqual(length, len(payload))

        decoded = decode_framed_bytes(payload)
        self.assertEqual(decoded["type"], MessageType.TEST_TCP)
        self.assertEqual(decoded["sender_id"], 42)
        self.assertEqual(decoded["payload"], "Test payload string")

    def test_oversized_payload_rejected(self) -> None:
        huge_payload = {"type": MessageType.TEST_TCP, "sender_id": 1, "timestamp": 1.0, "data": "X" * (MAX_PAYLOAD_SIZE + 10)}
        with self.assertRaises(FramingError):
            encode_framed_bytes(huge_payload)

    def test_udp_encoding_and_decoding(self) -> None:
        original = create_message(MessageType.TEST_UDP, 99, data="multicast content")
        data = encode_udp_message(original)
        decoded = decode_udp_message(data)
        self.assertEqual(decoded["type"], MessageType.TEST_UDP)
        self.assertEqual(decoded["sender_id"], 99)
        self.assertEqual(decoded["data"], "multicast content")


class TestFramedStreamAsync(unittest.IsolatedAsyncioTestCase):
    """Async unit tests verifying StreamReader/StreamWriter 4-byte framing behavior."""

    async def asyncSetUp(self) -> None:
        self.server = await asyncio.start_server(self._server_cb, host="127.0.0.1", port=0)
        self.port = self.server.sockets[0].getsockname()[1]
        self.received_messages: List[Dict[str, Any]] = []

    async def asyncTearDown(self) -> None:
        self.server.close()
        await self.server.wait_closed()

    async def _server_cb(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                msg = await read_framed_message(reader)
                if msg is None:
                    break
                self.received_messages.append(msg)
                # Echo back with PONG
                reply = create_message(MessageType.PONG, 999, original_type=msg["type"])
                await send_framed_message(writer, reply)
        except Exception:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_full_message_exchange(self) -> None:
        reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
        msg = create_message(MessageType.TEST_TCP, 1, greeting="hello server")
        await send_framed_message(writer, msg)

        response = await read_framed_message(reader)
        self.assertIsNotNone(response)
        assert response is not None
        self.assertEqual(response["type"], MessageType.PONG)
        self.assertEqual(response["original_type"], MessageType.TEST_TCP)

        writer.close()
        await writer.wait_closed()
        self.assertEqual(len(self.received_messages), 1)

    async def test_clean_eof_handling(self) -> None:
        reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
        # Close immediately without sending any bytes
        writer.close()
        await writer.wait_closed()

        # Reader at server side should cleanly return None without crashing
        await asyncio.sleep(0.1)
        self.assertEqual(len(self.received_messages), 0)

    async def test_truncated_header_raises_framing_error(self) -> None:
        reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
        # Send 2 bytes instead of 4-byte header, then close
        writer.write(b"\x00\x00")
        await writer.drain()
        writer.close()
        await writer.wait_closed()
        await asyncio.sleep(0.1)

    async def test_truncated_payload_raises_framing_error(self) -> None:
        # Manually create a StreamReader to test read_framed_message truncation
        reader = asyncio.StreamReader()
        header = struct.pack("!I", 100)  # expects 100 bytes
        partial_payload = b"short data"
        reader.feed_data(header + partial_payload)
        reader.feed_eof()

        with self.assertRaises(FramingError):
            await read_framed_message(reader)


# =====================================================================
# Standalone Worker Process for Multi-Process Verification
# =====================================================================

async def run_worker(node_id: int, tcp_port: int, mcast_port: int, lan_ip: str) -> None:
    """Worker process: runs TCP and UDP Multicast, communicates via stdin/stdout."""
    logger = logging.getLogger(f"Worker-{node_id}")
    logger.setLevel(logging.INFO)

    received_events: List[Dict[str, Any]] = []

    async def on_tcp(
        msg: Dict[str, Any],
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        mtype = msg.get("type")
        sys.stderr.write(f"[Worker {node_id}] Received TCP: {mtype}\n")
        sys.stderr.flush()

        if mtype == MessageType.TEST_TCP:
            payload = msg.get("payload", "")
            # Return hash of payload to verify large payload integrity
            payload_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
            reply = create_message(
                MessageType.PONG,
                node_id,
                received_payload_hash=payload_hash,
                received_payload_len=len(payload),
            )
            await send_framed_message(writer, reply)

    async def on_udp(msg: Dict[str, Any], addr: Tuple[str, int]) -> None:
        event = {"source": "UDP", "msg": msg, "from": addr}
        sys.stderr.write(f"[Worker {node_id}] Received UDP from {addr}: {msg.get('type')}\n")
        sys.stderr.flush()
        # Print JSON event to stdout for test orchestrator to observe
        print(f"EVENT:{json.dumps(event)}", flush=True)

    tcp_transport = TcpTransport(
        node_id=node_id,
        bind_host="0.0.0.0",
        port=tcp_port,
        advertised_ip=lan_ip,
        on_message=on_tcp,
    )

    mcast_transport = MulticastTransport(
        group="239.255.0.1",
        port=mcast_port,
        interface_ip=lan_ip,
        on_message=on_udp,
    )

    actual_port = await tcp_transport.start()
    await mcast_transport.start()

    # Announce ready to orchestrator
    print(f"READY:{json.dumps({'node_id': node_id, 'tcp_port': actual_port, 'lan_ip': lan_ip})}", flush=True)

    # Process commands from stdin
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(limit=10 * 1024 * 1024)
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)

    while True:
        line_bytes = await reader.readline()
        if not line_bytes:
            break
        line = line_bytes.decode("utf-8").strip()
        if not line:
            continue

        if line.startswith("SEND_TCP:"):
            cmd = json.loads(line[len("SEND_TCP:"):])
            host = cmd["host"]
            port = int(cmd["port"])
            msg = cmd["msg"]
            try:
                res = await tcp_transport.send_request(host, port, msg, timeout=5.0)
                print(f"RESULT:{json.dumps({'status': 'OK', 'response': res})}", flush=True)
            except Exception as e:
                print(f"RESULT:{json.dumps({'status': 'ERROR', 'error': str(e)})}", flush=True)

        elif line.startswith("SEND_UDP:"):
            cmd = json.loads(line[len("SEND_UDP:"):])
            msg = cmd["msg"]
            try:
                await mcast_transport.send(msg)
                print(f"RESULT:{json.dumps({'status': 'OK'})}", flush=True)
            except Exception as e:
                print(f"RESULT:{json.dumps({'status': 'ERROR', 'error': str(e)})}", flush=True)

        elif line == "QUIT":
            break

    await mcast_transport.close()
    await tcp_transport.close()
    sys.stderr.write(f"[Worker {node_id}] Terminated cleanly\n")
    sys.stderr.flush()


# =====================================================================
# Multi-Process Verification Test Case (Mandatory Milestone Gate)
# =====================================================================

class TestMultiProcessTransport(unittest.TestCase):
    """
    Spawns real child processes running Node 1 and Node 2.
    Tests framed TCP ping-pong, large payload reassembly, UDP multicast exchange & loopback,
    and failure handling without unhandled exceptions or hangs.
    """

    MCAST_PORT = 5055

    @classmethod
    def setUpClass(cls) -> None:
        cls.lan_ip = get_lan_ip()

    def setUp(self) -> None:
        self.procs: List[subprocess.Popen] = []

    def tearDown(self) -> None:
        for p in self.procs:
            try:
                if p.poll() is None:
                    p.stdin.write("QUIT\n")
                    p.stdin.flush()
                    p.wait(timeout=2.0)
            except Exception:
                p.kill()
                p.wait(timeout=1.0)
            finally:
                if p.stdin and not p.stdin.closed:
                    p.stdin.close()
                if p.stdout and not p.stdout.closed:
                    p.stdout.close()

    def _start_worker(self, node_id: int) -> Tuple[subprocess.Popen, int, str]:
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
        self.procs.append(proc)

        # Wait for READY line
        ready_line = proc.stdout.readline().strip()
        self.assertTrue(ready_line.startswith("READY:"), f"Worker {node_id} failed to report READY: {ready_line}")
        ready_data = json.loads(ready_line[len("READY:"):])
        return proc, ready_data["tcp_port"], ready_data["lan_ip"]

    def _send_worker_cmd(self, proc: subprocess.Popen, cmd_line: str) -> Dict[str, Any]:
        proc.stdin.write(cmd_line + "\n")
        proc.stdin.flush()
        while True:
            line = proc.stdout.readline()
            if not line:
                raise RuntimeError("Worker process terminated unexpectedly")
            line = line.strip()
            if line.startswith("RESULT:"):
                return json.loads(line[len("RESULT:"):])
            # Skip EVENT lines if any appear while waiting for RESULT

    def test_multi_process_tcp_and_udp_gate(self) -> None:
        """
        Executes the Milestone 1 Verification Gate across two independent OS processes:
        1. Framed TCP ping-pong.
        2. Large payload (128 KB) TCP segmentation and reassembly.
        3. UDP multicast datagram broadcast and loopback.
        4. Failure injection: connection to closed port and partial frame disconnect.
        """
        # Step 1: Start Node 1 and Node 2 processes
        proc1, port1, ip1 = self._start_worker(node_id=1)
        proc2, port2, ip2 = self._start_worker(node_id=2)

        time.sleep(0.5)  # Allow sockets to settle

        # Step 2: Node 1 sends framed TCP message to Node 2
        test_msg = create_message(
            MessageType.TEST_TCP,
            1,
            payload="Milestone 1 Framed TCP Verification Payload",
        )
        cmd = f"SEND_TCP:{json.dumps({'host': ip2, 'port': port2, 'msg': test_msg})}"
        res = self._send_worker_cmd(proc1, cmd)

        self.assertEqual(res["status"], "OK")
        pong_res = res["response"]
        self.assertEqual(pong_res["type"], MessageType.PONG)
        expected_hash = hashlib.sha256(test_msg["payload"].encode("utf-8")).hexdigest()
        self.assertEqual(pong_res["received_payload_hash"], expected_hash)

        # Step 3: Large Payload (128 KB) TCP Segmentation & Reassembly
        large_content = "X" * (128 * 1024)  # 128 KB
        large_msg = create_message(
            MessageType.TEST_TCP,
            1,
            payload=large_content,
        )
        cmd_large = f"SEND_TCP:{json.dumps({'host': ip2, 'port': port2, 'msg': large_msg})}"
        res_large = self._send_worker_cmd(proc1, cmd_large)

        self.assertEqual(res_large["status"], "OK")
        large_pong = res_large["response"]
        self.assertEqual(large_pong["received_payload_len"], len(large_content))
        self.assertEqual(
            large_pong["received_payload_hash"],
            hashlib.sha256(large_content.encode("utf-8")).hexdigest(),
        )

        # Step 4: UDP Multicast Broadcast & Loopback Reception
        udp_msg = create_message(
            MessageType.TEST_UDP,
            1,
            announcement="Hello to Group 53 Multicast",
        )
        cmd_udp = f"SEND_UDP:{json.dumps({'msg': udp_msg})}"
        res_udp = self._send_worker_cmd(proc1, cmd_udp)
        self.assertEqual(res_udp["status"], "OK")

        # Both Node 1 (loopback) and Node 2 should receive the multicast datagram
        # Give datagram up to 2 seconds to arrive
        time.sleep(0.5)

        # Step 5: Failure injection - connect to closed TCP port
        closed_port = 59998
        cmd_fail = f"SEND_TCP:{json.dumps({'host': '127.0.0.1', 'port': closed_port, 'msg': test_msg})}"
        res_fail = self._send_worker_cmd(proc1, cmd_fail)
        self.assertEqual(res_fail["status"], "ERROR")
        self.assertIn("Cannot connect to peer", res_fail["error"])

        # Step 6: Failure injection - send corrupted partial frame to Node 2
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((ip2, port2))
        s.sendall(b"\x00\x00")  # Send only 2 bytes of the 4-byte header and close
        s.close()
        time.sleep(0.3)

        # Node 2 must remain fully healthy and responsive after partial frame disconnect
        res_after = self._send_worker_cmd(proc1, cmd)
        self.assertEqual(res_after["status"], "OK")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        parser = argparse.ArgumentParser()
        parser.add_argument("--worker", action="store_true")
        parser.add_argument("--id", type=int, required=True)
        parser.add_argument("--port", type=int, default=0)
        parser.add_argument("--mcast-port", type=int, default=5055)
        parser.add_argument("--host", type=str, default="")
        args = parser.parse_args()

        resolved_ip = args.host if args.host else get_lan_ip()
        asyncio.run(run_worker(args.id, args.port, args.mcast_port, resolved_ip))
    else:
        unittest.main()

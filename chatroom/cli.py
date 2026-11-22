"""
Non-blocking Terminal CLI and Interactive Demo Interface for Group 53 P2P Chat Room.

Course: Distributed Systems I: Foundations (WS2026)
Group: Group 53

Implements:
- Non-blocking stdin reading via asyncio streams (never blocks event loop).
- Interactive messaging with Total Order Multicast.
- Interactive demo commands:
    /status    - Show node status, role, leader, sequencer state
    /members   - Show current cluster membership table
    /history   - Show all delivered messages in sequence order
    /drop-next - Simulate network drop of next incoming packet (live gap demo)
    /leave     - Gracefully leave and disconnect
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time
from typing import Any, Callable, Optional

from chatroom.multicast import OrderedMessage
from chatroom.node import Node


class CommandLineInterface:
    """Non-blocking asynchronous terminal interface for a chat room node."""

    def __init__(self, node: Node, logger: Optional[logging.Logger] = None) -> None:
        self.node = node
        self.logger = logger or logging.getLogger(f"CLI-{node.node_id}")
        self._stop_event = asyncio.Event()

        # Wire node delivery callback to print incoming messages in console
        original_deliver = self.node.multicast.on_deliver

        def on_msg_delivered(msg: OrderedMessage) -> None:
            self._display_message(msg)
            if original_deliver:
                original_deliver(msg)

        self.node.multicast.on_deliver = on_msg_delivered

    def _display_message(self, msg: OrderedMessage) -> None:
        """Format and print delivered chat message to stdout."""
        ts_str = time.strftime("%H:%M:%S", time.localtime(msg.timestamp))
        sender_label = f"Node {msg.sender_id}" if msg.sender_id != self.node.node_id else "You"
        sys.stdout.write(f"\n[{ts_str}] [seq={msg.seq} | {sender_label}]: {msg.text}\n> ")
        sys.stdout.flush()

    def print_banner(self) -> None:
        banner = f"""
===================================================================
  Group 53 Distributed P2P Chat Room
  Node ID: {self.node.node_id} | Role: {self.node.role} | Port: {self.node.tcp_port}
  Type a message and press Enter to broadcast.
  Commands:
    /status    - Display node role, leader, and sequencer metrics
    /members   - Display cluster membership table
    /history   - Display full replicated message history
    /drop-next - Simulate dropping next packet (gap recovery demo)
    /leave     - Gracefully leave the chat room
===================================================================
> """
        sys.stdout.write(banner)
        sys.stdout.flush()

    async def run(self) -> None:
        """Start non-blocking stdin reader loop."""
        self.print_banner()

        loop = asyncio.get_running_loop()
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)

        try:
            await loop.connect_read_pipe(lambda: protocol, sys.stdin)
        except Exception as e:
            self.logger.debug("Could not attach stdin pipe (non-interactive mode): %s", e)
            await self._stop_event.wait()
            return

        while not self._stop_event.is_set():
            try:
                line_bytes = await reader.readline()
                if not line_bytes:
                    break
                line = line_bytes.decode("utf-8").strip()
                if not line:
                    sys.stdout.write("> ")
                    sys.stdout.flush()
                    continue

                if line.startswith("/"):
                    await self._handle_command(line)
                else:
                    # Normal chat message: broadcast with Total Order
                    await self.node.send_chat(line)

                sys.stdout.write("> ")
                sys.stdout.flush()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Error processing CLI input: %s", e)

    async def _handle_command(self, cmd_line: str) -> None:
        parts = cmd_line.split()
        cmd = parts[0].lower()

        if cmd == "/status":
            delivered = self.node.get_delivered_messages()
            last_seq = delivered[-1].seq if delivered else 0
            sys.stdout.write(
                f"\n--- Node Status ---\n"
                f"  Node ID:          {self.node.node_id}\n"
                f"  Role:             {self.node.role}\n"
                f"  Leader ID:        {self.node.leader_id}\n"
                f"  Next Expected Seq:{self.node.multicast.expected_seq}\n"
                f"  Last Delivered:   {last_seq}\n"
                f"  Total Delivered:  {len(delivered)}\n"
                f"  Hold-Back Queue:  {len(self.node.multicast.hold_back_queue)} packet(s)\n"
                f"  Election State:   {self.node.election.state}\n"
                f"-------------------\n"
            )

        elif cmd == "/members":
            members = self.node.table.all_members()
            sys.stdout.write(f"\n--- Cluster Members ({len(members)} total) ---\n")
            for m in members:
                status_str = "ALIVE" if m.is_alive else "SUSPECTED/CRASHED"
                self_mark = " (You)" if m.node_id == self.node.node_id else ""
                sys.stdout.write(
                    f"  Node {m.node_id}{self_mark}: {m.host}:{m.port} [{m.role}] ({status_str})\n"
                )
            sys.stdout.write("-------------------------------------\n")

        elif cmd == "/history":
            delivered = self.node.get_delivered_messages()
            sys.stdout.write(f"\n--- Replicated Chat History ({len(delivered)} messages) ---\n")
            for m in delivered:
                ts = time.strftime("%H:%M:%S", time.localtime(m.timestamp))
                sys.stdout.write(f"  [seq={m.seq:03d} | {ts} | Node {m.sender_id}]: {m.text}\n")
            sys.stdout.write("---------------------------------------------------\n")

        elif cmd == "/drop-next":
            # Simulation hook: drop the next incoming sequenced message
            dropped = False

            def drop_one(msg: OrderedMessage) -> bool:
                nonlocal dropped
                if not dropped:
                    dropped = True
                    sys.stdout.write(
                        f"\n[DEMO] Deliberately dropped seq={msg.seq} ('{msg.text}') to demonstrate gap recovery!\n> "
                    )
                    sys.stdout.flush()
                    return True
                return False

            self.node.multicast.drop_filter = drop_one
            sys.stdout.write(
                "\n[DEMO] /drop-next activated: the next incoming chat message will be dropped.\n"
                "When subsequent messages arrive, gap detection and RETRANSMIT_REQ recovery will trigger automatically.\n"
            )

        elif cmd in ("/leave", "/quit", "/exit"):
            sys.stdout.write("\nLeaving chat room...\n")
            self._stop_event.set()

        elif cmd == "/help":
            self.print_banner()

        else:
            sys.stdout.write(f"\nUnknown command '{cmd}'. Type /help to see available commands.\n")

        sys.stdout.flush()

    def stop(self) -> None:
        self._stop_event.set()

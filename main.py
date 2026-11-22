"""
CLI Entrypoint and Configuration for Group 53 Distributed P2P Chat Room.

Course: Distributed Systems I: Foundations (WS2026)
Group: Group 53
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from chatroom.cli import CommandLineInterface
from chatroom.node import Node
from chatroom.protocol import MessageType, create_message, send_framed_message
from chatroom.transport import MulticastTransport, TcpTransport, get_lan_ip


@dataclass
class Config:
    """Centralized configuration for Group 53 P2P Chat Room node."""

    node_id: int
    host: str
    tcp_port: int = 6000
    mcast_group: str = "239.255.0.1"
    mcast_port: int = 5000
    role: str = "FOLLOWER"
    heartbeat_interval: float = 0.8
    heartbeat_timeout: float = 2.5
    election_timeout: float = 2.0
    ack_timeout: float = 1.5
    data_dir: str = ".chat_data"


class NodeLogFormatter(logging.Formatter):
    """Formats log messages with timestamps and observable [Node <id> | <ROLE>] prefixes."""

    def __init__(self, node_id: int, role_getter: Any) -> None:
        super().__init__()
        self.node_id = node_id
        self.role_getter = role_getter if callable(role_getter) else (lambda: role_getter)

    def format(self, record: logging.LogRecord) -> str:
        timestamp = self.formatTime(record, "%Y-%m-%d %H:%M:%S")
        msecs = int(record.msecs)
        current_role = self.role_getter()
        return (
            f"{timestamp}.{msecs:03d} [Node {self.node_id} | {current_role}] "
            f"{record.levelname}: {record.getMessage()}"
        )


def setup_logger(node_id: int, role_getter: Any, level: int = logging.INFO) -> logging.Logger:
    """Configure root and application logger with standardized distributed-systems formatting."""
    logger = logging.getLogger(f"Node-{node_id}")
    logger.setLevel(level)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(NodeLogFormatter(node_id, role_getter))

    logger.handlers.clear()
    logger.addHandler(handler)
    logger.propagate = False
    return logger


async def run_m1_demo(config: Config, logger: logging.Logger) -> None:
    """Milestone 1 Demo Mode: framed TCP and UDP multicast ping-pong."""
    logger.info(
        "Starting Milestone 1 Demo (TCP Port: %d, Multicast: %s:%d)",
        config.tcp_port,
        config.mcast_group,
        config.mcast_port,
    )

    discovered_peers: Dict[int, Dict[str, Any]] = {}

    async def on_tcp_message(
        msg: Dict[str, Any],
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        peer = writer.get_extra_info("peername")
        mtype = msg.get("type")
        sender = msg.get("sender_id")
        logger.info("TCP received %s from node %s (%s)", mtype, sender, peer)

        if mtype in (MessageType.TEST_TCP, MessageType.PING):
            reply = create_message(
                MessageType.PONG,
                config.node_id,
                text=f"Hello from node {config.node_id} over framed TCP!",
                original_msg_type=mtype,
            )
            await send_framed_message(writer, reply)
            logger.info("Sent framed TCP PONG response to node %s", sender)

    async def on_udp_message(msg: Dict[str, Any], addr: tuple) -> None:
        sender = msg.get("sender_id")
        mtype = msg.get("type")
        if sender == config.node_id:
            logger.debug("Received own UDP multicast datagram via loopback from %s", addr)
            return

        logger.info("UDP multicast received %s from node %s at %s", mtype, sender, addr)

        if mtype == MessageType.TEST_UDP:
            peer_host = msg.get("tcp_host", addr[0])
            peer_port = msg.get("tcp_port")
            if peer_port and sender not in discovered_peers:
                discovered_peers[sender] = {"host": peer_host, "port": peer_port}
                logger.info("Discovered new peer node %s at %s:%d", sender, peer_host, peer_port)
                asyncio.create_task(test_tcp_connection(peer_host, peer_port, sender))

    tcp_transport = TcpTransport(
        node_id=config.node_id,
        bind_host="0.0.0.0",
        port=config.tcp_port,
        advertised_ip=config.host,
        on_message=on_tcp_message,
        logger=logger,
    )

    mcast_transport = MulticastTransport(
        group=config.mcast_group,
        port=config.mcast_port,
        interface_ip=config.host,
        on_message=on_udp_message,
        logger=logger,
    )

    async def test_tcp_connection(peer_host: str, peer_port: int, target_id: int) -> None:
        await asyncio.sleep(0.5)
        try:
            req = create_message(
                MessageType.TEST_TCP,
                config.node_id,
                payload="Milestone 1 Framed TCP Handshake",
            )
            logger.info("Initiating framed TCP request to node %s at %s:%d", target_id, peer_host, peer_port)
            res = await tcp_transport.send_request(peer_host, peer_port, req, timeout=3.0)
            logger.info("Received framed TCP response from node %s: %s", target_id, res)
        except Exception as e:
            logger.warning("Failed framed TCP handshake with node %s: %s", target_id, e)

    actual_port = await tcp_transport.start()
    config.tcp_port = actual_port
    await mcast_transport.start()

    stop_event = asyncio.Event()

    def signal_handler() -> None:
        logger.info("Shutdown signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            pass

    async def broadcast_loop() -> None:
        while not stop_event.is_set():
            beacon = create_message(
                MessageType.TEST_UDP,
                config.node_id,
                tcp_host=config.host,
                tcp_port=config.tcp_port,
                role=config.role,
            )
            try:
                await mcast_transport.send(beacon)
            except Exception as e:
                logger.warning("Error broadcasting multicast beacon: %s", e)

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=3.0)
            except asyncio.TimeoutError:
                pass

    beacon_task = asyncio.create_task(broadcast_loop())

    try:
        await stop_event.wait()
    finally:
        beacon_task.cancel()
        await mcast_transport.close()
        await tcp_transport.close()
        logger.info("Node %d shut down cleanly", config.node_id)


async def run_node(config: Config) -> None:
    """Run full distributed chat room node (Milestone 2+)."""
    node = Node(
        node_id=config.node_id,
        host=config.host,
        tcp_port=config.tcp_port,
        mcast_group=config.mcast_group,
        mcast_port=config.mcast_port,
        initial_role=config.role,
        heartbeat_interval=config.heartbeat_interval,
        heartbeat_timeout=config.heartbeat_timeout,
        election_timeout=config.election_timeout,
        data_dir=config.data_dir,
    )

    logger = setup_logger(config.node_id, lambda: node.role)
    node.logger = logger
    node.tcp_transport.logger = logger
    node.mcast_transport.logger = logger
    node.membership.logger = logger
    node.election.logger = logger
    node.multicast.logger = logger
    if node.storage:
        node.storage.logger = logger

    await node.start()

    cli = CommandLineInterface(node, logger=logger)

    def signal_handler() -> None:
        logger.info("Shutdown signal caught. Commencing clean leave...")
        cli.stop()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            pass

    try:
        await cli.run()
    finally:
        await node.stop()


def parse_args() -> Tuple[Config, bool]:
    """Parse command line arguments into Config dataclass."""
    parser = argparse.ArgumentParser(
        description="Group 53 Distributed Peer-to-Peer Chat Room Node",
    )
    parser.add_argument(
        "--id",
        type=int,
        required=True,
        help="Unique integer Node ID (e.g., 1, 2, 3)",
    )
    parser.add_argument(
        "--host",
        type=str,
        default=None,
        help="IP address to bind and advertise (default: auto-detected LAN IP)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=6000,
        help="TCP port to listen on (default: 6000, 0 for ephemeral)",
    )
    parser.add_argument(
        "--mcast-group",
        type=str,
        default="239.255.0.1",
        help="UDP Multicast group (default: 239.255.0.1)",
    )
    parser.add_argument(
        "--mcast-port",
        type=int,
        default=5000,
        help="UDP Multicast port (default: 5000)",
    )
    parser.add_argument(
        "--role",
        type=str,
        default="FOLLOWER",
        choices=["FOLLOWER", "LEADER"],
        help="Initial node role (default: FOLLOWER)",
    )
    parser.add_argument(
        "--heartbeat-interval",
        type=float,
        default=0.8,
        help="Multicast heartbeat interval in seconds (default: 0.8)",
    )
    parser.add_argument(
        "--heartbeat-timeout",
        type=float,
        default=2.5,
        help="Heartbeat silence timeout before declaring crash in seconds (default: 2.5)",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=".chat_data",
        help="Local directory for persistent chat history (default: .chat_data)",
    )
    parser.add_argument(
        "--demo-ping",
        action="store_true",
        help="Run Milestone 1 interactive transport test",
    )

    args = parser.parse_args()
    resolved_host = get_lan_ip(args.host)

    cfg = Config(
        node_id=args.id,
        host=resolved_host,
        tcp_port=args.port,
        mcast_group=args.mcast_group,
        mcast_port=args.mcast_port,
        role=args.role,
        heartbeat_interval=args.heartbeat_interval,
        heartbeat_timeout=args.heartbeat_timeout,
        data_dir=args.data_dir,
    )
    return cfg, args.demo_ping


def main() -> None:
    """Synchronous entrypoint."""
    config, is_demo_ping = parse_args()
    if is_demo_ping:
        logger = setup_logger(config.node_id, config.role)
        logger.info("Initializing Node %d on %s:%d in demo-ping mode", config.node_id, config.host, config.tcp_port)
        try:
            asyncio.run(run_m1_demo(config, logger))
        except KeyboardInterrupt:
            logger.info("Exiting on keyboard interrupt")
    else:
        try:
            asyncio.run(run_node(config))
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()

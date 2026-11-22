"""
Transport layer for Group 53 Distributed P2P Chat Room.

Provides:
- Outgoing LAN IPv4 address detection without hardcoding 127.0.0.1.
- Async UDP Multicast transport with SO_REUSEADDR/SO_REUSEPORT and loopback.
- Async TCP server and client with strict 4-byte framing, timeouts, and error handling.
"""

from __future__ import annotations

import asyncio
import logging
import socket
import struct
from typing import Any, Callable, Dict, Optional, Set, Tuple

from chatroom.protocol import (
    FramingError,
    ProtocolError,
    decode_udp_message,
    encode_udp_message,
    read_framed_message,
    send_framed_message,
    validate_envelope,
)


class TransportError(Exception):
    """Base exception for network transport failures."""
    pass


class PeerUnreachableError(TransportError):
    """Raised when a remote peer cannot be reached over TCP."""
    pass


class PeerTimeoutError(TransportError):
    """Raised when a TCP connection, send, or receive operation times out."""
    pass


def get_lan_ip(override: Optional[str] = None) -> str:
    """
    Detect the local machine's primary LAN IPv4 address.
    
    Uses a connectionless UDP routing probe towards an external address.
    Because UDP is connectionless, no packets are actually transmitted.
    Never returns a loopback address unless no LAN interface is present.
    """
    if override and override.strip():
        return override.strip()

    # Attempt 1: External routing probe
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        if ip and not ip.startswith("127."):
            return ip
    except Exception:
        pass
    finally:
        s.close()

    # Attempt 2: Private network routing probe (works when disconnected from public internet)
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("10.255.255.255", 1))
        ip = s.getsockname()[0]
        if ip and not ip.startswith("127."):
            return ip
    except Exception:
        pass
    finally:
        s.close()

    # Attempt 3: Hostname resolution fallback
    try:
        hostname = socket.gethostname()
        ip = socket.gethostbyname(hostname)
        if ip and not ip.startswith("127."):
            return ip
    except Exception:
        pass

    return "127.0.0.1"


class _MulticastProtocol(asyncio.DatagramProtocol):
    """Asyncio DatagramProtocol handling incoming UDP multicast messages."""

    def __init__(
        self,
        on_message: Callable[[Dict[str, Any], Tuple[str, int]], Any],
        logger: logging.Logger,
    ) -> None:
        self.on_message = on_message
        self.logger = logger
        self.transport: Optional[asyncio.DatagramTransport] = None

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:  # type: ignore[override]
        self.transport = transport

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        try:
            msg = decode_udp_message(data)
            if not validate_envelope(msg):
                self.logger.warning("Dropped malformed UDP envelope from %s", addr)
                return

            result = self.on_message(msg, addr)
            if asyncio.iscoroutine(result):
                asyncio.create_task(result)
        except Exception as e:
            self.logger.debug("Failed to process UDP datagram from %s: %s", addr, e)

    def error_received(self, exc: Exception) -> None:
        self.logger.warning("UDP datagram transport error: %s", exc)

    def connection_lost(self, exc: Optional[Exception]) -> None:
        if exc:
            self.logger.debug("UDP multicast connection lost: %s", exc)


class MulticastTransport:
    """
    Manages non-blocking UDP multicast sending and receiving.
    
    Configures SO_REUSEADDR and SO_REUSEPORT (on macOS/BSD/Linux) so multiple
    nodes on the same physical host can bind to the same multicast port.
    Enables IP_MULTICAST_LOOP so nodes on the same host receive each other's datagrams.
    """

    def __init__(
        self,
        group: str = "239.255.0.1",
        port: int = 5000,
        interface_ip: Optional[str] = None,
        on_message: Optional[Callable[[Dict[str, Any], Tuple[str, int]], Any]] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.group = group
        self.port = port
        self.interface_ip = interface_ip or get_lan_ip()
        self.on_message = on_message or (lambda msg, addr: None)
        self.logger = logger or logging.getLogger("MulticastTransport")

        self._transport: Optional[asyncio.DatagramTransport] = None
        self._protocol: Optional[_MulticastProtocol] = None
        self._sock: Optional[socket.socket] = None
        self._is_running = False

    async def start(self) -> None:
        """Create and bind the multicast socket and register with asyncio."""
        if self._is_running:
            return

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)

        # Allow multiple processes on the same host to bind to the multicast port
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except OSError:
                pass

        # Enable multicast loopback so other processes on this machine receive messages
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)

        # Set Multicast TTL (2 = local network)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, struct.pack("b", 2))

        # Explicitly configure outgoing multicast interface (critical on macOS to prevent Errno 65: No route to host)
        target_interface = self.interface_ip if self.interface_ip and self.interface_ip != "0.0.0.0" else "127.0.0.1"
        try:
            sock.setsockopt(
                socket.IPPROTO_IP,
                socket.IP_MULTICAST_IF,
                socket.inet_aton(target_interface),
            )
        except OSError as e:
            self.logger.warning("Could not set IP_MULTICAST_IF on %s: %s", target_interface, e)

        # Bind to INADDR_ANY ("") on macOS and Linux
        sock.bind(("", self.port))

        # Join the multicast group on the target interface
        try:
            mreq = struct.pack("4s4s", socket.inet_aton(self.group), socket.inet_aton(target_interface))
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        except OSError:
            # Fallback to INADDR_ANY if binding to target_interface fails
            mreq = struct.pack("4s4s", socket.inet_aton(self.group), socket.inet_aton("0.0.0.0"))
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

        sock.setblocking(False)

        self._sock = sock
        loop = asyncio.get_running_loop()
        self._protocol = _MulticastProtocol(self.on_message, self.logger)
        self._transport, _ = await loop.create_datagram_endpoint(
            lambda: self._protocol,  # type: ignore[arg-type]
            sock=self._sock,
        )
        self._is_running = True
        self.logger.info(
            "UDP Multicast transport ready on group %s:%d (interface: %s)",
            self.group,
            self.port,
            target_interface,
        )

    async def send(self, msg: Dict[str, Any]) -> None:
        """Broadcast a message dictionary to the multicast group."""
        if not self._is_running or not self._transport or self._transport.is_closing():
            raise TransportError("Multicast transport is not running")

        data = encode_udp_message(msg)
        self._transport.sendto(data, (self.group, self.port))

    async def close(self) -> None:
        """Drop multicast group membership and cleanly close socket and transport."""
        self._is_running = False
        if self._sock and not getattr(self._sock, "_closed", False):
            target_interface = self.interface_ip if self.interface_ip and self.interface_ip != "0.0.0.0" else "127.0.0.1"
            for iface in (target_interface, "0.0.0.0"):
                try:
                    mreq = struct.pack("4s4s", socket.inet_aton(self.group), socket.inet_aton(iface))
                    self._sock.setsockopt(socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP, mreq)
                    break
                except OSError:
                    pass

        if self._transport and not self._transport.is_closing():
            self._transport.close()

        self._transport = None
        self._protocol = None
        self._sock = None


class TcpTransport:
    """
    Manages non-blocking asynchronous TCP server and client connections.
    
    All communication uses 4-byte big-endian length prefix framing.
    Every connection uses explicit timeouts and guarantees clean resource cleanup.
    """

    def __init__(
        self,
        node_id: int,
        bind_host: str = "0.0.0.0",
        port: int = 0,
        advertised_ip: Optional[str] = None,
        on_message: Optional[
            Callable[[Dict[str, Any], asyncio.StreamReader, asyncio.StreamWriter], Any]
        ] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.node_id = node_id
        self.bind_host = bind_host
        self.port = port
        self.advertised_ip = advertised_ip or get_lan_ip()
        self.on_message = on_message or (lambda msg, r, w: None)
        self.logger = logger or logging.getLogger(f"TcpTransport-{node_id}")

        self.server: Optional[asyncio.AbstractServer] = None
        self.bound_port: int = port
        self._active_writers: Set[asyncio.StreamWriter] = set()
        self._is_closing = False

    async def start(self) -> int:
        """
        Start the TCP listening server.
        
        Returns:
          int: The actual bound TCP port (useful when port=0 was passed).
        """
        self._is_closing = False
        self.server = await asyncio.start_server(
            self._client_connection_handler,
            host=self.bind_host,
            port=self.port,
        )

        sockets = self.server.sockets
        if sockets:
            self.bound_port = sockets[0].getsockname()[1]

        self.logger.info(
            "TCP Server listening on %s:%d (advertised address: %s:%d)",
            self.bind_host,
            self.bound_port,
            self.advertised_ip,
            self.bound_port,
        )
        return self.bound_port

    async def _client_connection_handler(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Handle incoming client connection, reading framed messages sequentially."""
        self._active_writers.add(writer)
        peer = writer.get_extra_info("peername")

        try:
            while not self._is_closing:
                msg = await read_framed_message(reader)
                if msg is None:
                    # Clean EOF: peer closed connection
                    break

                result = self.on_message(msg, reader, writer)
                if asyncio.iscoroutine(result):
                    await result

        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError) as e:
            self.logger.debug("TCP connection from %s closed by peer: %s", peer, e)
        except FramingError as e:
            self.logger.warning("Framing error from client %s: %s", peer, e)
        except ProtocolError as e:
            self.logger.warning("Protocol error from client %s: %s", peer, e)
        except Exception as e:
            self.logger.error("Unexpected error in TCP handler for %s: %s", peer, e, exc_info=True)
        finally:
            self._active_writers.discard(writer)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def send_message(
        self,
        host: str,
        port: int,
        msg: Dict[str, Any],
        timeout: float = 3.0,
    ) -> None:
        """
        Connect to a peer, send a 4-byte framed message, and cleanly close the connection.
        """
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=timeout,
            )
        except (ConnectionRefusedError, OSError) as e:
            raise PeerUnreachableError(f"Cannot connect to peer at {host}:{port}: {e}") from e
        except asyncio.TimeoutError as e:
            raise PeerTimeoutError(f"Timed out connecting to peer at {host}:{port}") from e

        try:
            await asyncio.wait_for(
                send_framed_message(writer, msg),
                timeout=timeout,
            )
        except asyncio.TimeoutError as e:
            raise PeerTimeoutError(f"Timed out sending message to peer at {host}:{port}") from e
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def send_request(
        self,
        host: str,
        port: int,
        msg: Dict[str, Any],
        timeout: float = 3.0,
    ) -> Dict[str, Any]:
        """
        Connect to a peer, send a framed request, wait for a framed response, and close cleanly.
        """
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=timeout,
            )
        except (ConnectionRefusedError, OSError) as e:
            raise PeerUnreachableError(f"Cannot connect to peer at {host}:{port}: {e}") from e
        except asyncio.TimeoutError as e:
            raise PeerTimeoutError(f"Timed out connecting to peer at {host}:{port}") from e

        try:
            await asyncio.wait_for(
                send_framed_message(writer, msg),
                timeout=timeout,
            )
            response = await asyncio.wait_for(
                read_framed_message(reader),
                timeout=timeout,
            )
            if response is None:
                raise PeerUnreachableError(f"Peer at {host}:{port} disconnected without replying")
            return response
        except asyncio.TimeoutError as e:
            raise PeerTimeoutError(f"Timed out waiting for response from {host}:{port}") from e
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def close(self) -> None:
        """Stop TCP server and close all active connections cleanly."""
        self._is_closing = True
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.server = None

        for writer in list(self._active_writers):
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        self._active_writers.clear()
        self.logger.info("TCP Transport closed for node %d", self.node_id)

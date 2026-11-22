"""
Protocol layer for Group 53 Distributed P2P Chat Room.

Defines message type constants, the standard JSON envelope contract,
and strict 4-byte big-endian length-prefixed TCP stream framing helpers.
"""

from __future__ import annotations

import asyncio
import json
import struct
import time
from typing import Any, Dict, Optional

# TCP Framing constants: 4-byte unsigned big-endian integer length prefix
HEADER_FORMAT: str = "!I"
HEADER_SIZE: int = struct.calcsize(HEADER_FORMAT)
MAX_PAYLOAD_SIZE: int = 10 * 1024 * 1024  # 10 MB sanity limit to prevent memory exhaustion


class MessageType:
    """Message type constants across all 5 milestones."""

    # Milestone 1 & Diagnostics
    PING = "PING"
    PONG = "PONG"
    TEST_TCP = "TEST_TCP"
    TEST_UDP = "TEST_UDP"

    # Milestone 2: Discovery, Membership & Heartbeats
    ANNOUNCE = "ANNOUNCE"
    LEADER_INFO = "LEADER_INFO"
    JOIN = "JOIN"
    JOIN_ACK = "JOIN_ACK"
    LEAVE = "LEAVE"
    MEMBERSHIP_UPDATE = "MEMBERSHIP_UPDATE"
    HEARTBEAT = "HEARTBEAT"

    # Milestone 3: Bully Election & State Reconciliation
    ELECTION = "ELECTION"
    ANSWER = "ANSWER"
    COORDINATOR = "COORDINATOR"
    STATE_REQ = "STATE_REQ"
    STATE_RES = "STATE_RES"

    # Milestone 4: Reliable Ordered Multicast
    CHAT_CLIENT_TO_LEADER = "CHAT_CLIENT_TO_LEADER"
    CHAT_MULTICAST_ORDERED = "CHAT_MULTICAST_ORDERED"
    CHAT_ACK = "CHAT_ACK"
    RETRANSMIT_REQ = "RETRANSMIT_REQ"
    RETRANSMIT_RES = "RETRANSMIT_RES"


class FramingError(Exception):
    """Raised when stream framing or length header validation fails."""
    pass


class ProtocolError(Exception):
    """Raised when message envelope or JSON serialization validation fails."""
    pass


def create_message(msg_type: str, sender_id: int, **payload: Any) -> Dict[str, Any]:
    """
    Construct a protocol envelope complying with the Group 53 specification.
    
    Required envelope fields:
      - type: str
      - sender_id: int
      - timestamp: float (epoch seconds)
    """
    msg: Dict[str, Any] = {
        "type": msg_type,
        "sender_id": int(sender_id),
        "timestamp": float(time.time()),
    }
    msg.update(payload)
    return msg


def validate_envelope(msg: Any) -> bool:
    """
    Validate that a parsed object is a dict and contains the mandatory envelope fields.
    """
    if not isinstance(msg, dict):
        return False
    if "type" not in msg or not isinstance(msg["type"], str):
        return False
    if "sender_id" not in msg or not isinstance(msg["sender_id"], int):
        return False
    if "timestamp" not in msg or not isinstance(msg["timestamp"], (int, float)):
        return False
    return True


# =====================================================================
# Milestone 2: Discovery, Membership & Heartbeat Message Helpers
# =====================================================================

def create_announce_message(node_id: int, host: str, port: int) -> Dict[str, Any]:
    return create_message(
        MessageType.ANNOUNCE,
        node_id,
        tcp_host=host,
        tcp_port=int(port),
    )


def create_leader_info_message(
    sender_id: int,
    leader_id: int,
    leader_host: str,
    leader_port: int,
) -> Dict[str, Any]:
    return create_message(
        MessageType.LEADER_INFO,
        sender_id,
        leader_id=int(leader_id),
        leader_host=leader_host,
        leader_port=int(leader_port),
    )


def create_join_message(
    node_id: int,
    host: str,
    port: int,
    last_delivered_seq: int = 0,
) -> Dict[str, Any]:
    return create_message(
        MessageType.JOIN,
        node_id,
        tcp_host=host,
        tcp_port=int(port),
        last_delivered_seq=int(last_delivered_seq),
    )


def create_join_ack_message(
    leader_id: int,
    membership: Dict[str, Any],
    history: Optional[list] = None,
) -> Dict[str, Any]:
    return create_message(
        MessageType.JOIN_ACK,
        leader_id,
        leader_id=int(leader_id),
        membership=membership,
        history=history or [],
    )


def create_leave_message(node_id: int) -> Dict[str, Any]:
    return create_message(
        MessageType.LEAVE,
        node_id,
    )


def create_membership_update_message(
    leader_id: int,
    membership: Dict[str, Any],
    reason: str,
    affected_node_id: Optional[int] = None,
) -> Dict[str, Any]:
    return create_message(
        MessageType.MEMBERSHIP_UPDATE,
        leader_id,
        membership=membership,
        reason=reason,
        affected_node_id=affected_node_id,
    )


def create_heartbeat_message(sender_id: int, role: str) -> Dict[str, Any]:
    return create_message(
        MessageType.HEARTBEAT,
        sender_id,
        role=role,
    )


# =====================================================================
# Milestone 3: Bully Election & State Reconciliation Helpers
# =====================================================================

def create_election_message(sender_id: int) -> Dict[str, Any]:
    return create_message(
        MessageType.ELECTION,
        sender_id,
    )


def create_answer_message(sender_id: int) -> Dict[str, Any]:
    return create_message(
        MessageType.ANSWER,
        sender_id,
    )


def create_coordinator_message(sender_id: int, max_seq: int = 0) -> Dict[str, Any]:
    return create_message(
        MessageType.COORDINATOR,
        sender_id,
        leader_id=int(sender_id),
        max_seq=int(max_seq),
    )


def create_state_req_message(sender_id: int) -> Dict[str, Any]:
    return create_message(
        MessageType.STATE_REQ,
        sender_id,
    )


def create_state_res_message(
    sender_id: int,
    max_seq: int,
    history: Optional[list] = None,
) -> Dict[str, Any]:
    return create_message(
        MessageType.STATE_RES,
        sender_id,
        max_seq=int(max_seq),
        history=history or [],
    )


# =====================================================================
# Milestone 4: Reliable Totally Ordered Multicast Helpers
# =====================================================================

def create_chat_client_message(
    sender_id: int,
    msg_id: str,
    text: str,
) -> Dict[str, Any]:
    return create_message(
        MessageType.CHAT_CLIENT_TO_LEADER,
        sender_id,
        msg_id=str(msg_id),
        text=str(text),
    )


def create_chat_ordered_message(
    leader_id: int,
    seq: int,
    sender_id: int,
    msg_id: str,
    text: str,
    orig_timestamp: Optional[float] = None,
) -> Dict[str, Any]:
    return create_message(
        MessageType.CHAT_MULTICAST_ORDERED,
        leader_id,
        seq=int(seq),
        origin_sender_id=int(sender_id),
        msg_id=str(msg_id),
        text=str(text),
        orig_timestamp=float(orig_timestamp if orig_timestamp is not None else time.time()),
    )


def create_retransmit_req_message(
    sender_id: int,
    missing_seqs: list[int],
) -> Dict[str, Any]:
    return create_message(
        MessageType.RETRANSMIT_REQ,
        sender_id,
        missing_seqs=[int(s) for s in missing_seqs],
    )


def create_retransmit_res_message(
    sender_id: int,
    messages: list[Dict[str, Any]],
) -> Dict[str, Any]:
    return create_message(
        MessageType.RETRANSMIT_RES,
        sender_id,
        messages=messages,
    )


def create_chat_ack_message(
    sender_id: int,
    msg_id: str,
    seq: int,
) -> Dict[str, Any]:
    return create_message(
        MessageType.CHAT_ACK,
        sender_id,
        msg_id=str(msg_id),
        seq=int(seq),
    )


def encode_framed_bytes(msg: Dict[str, Any]) -> bytes:
    """
    Serialize a message dictionary to UTF-8 JSON bytes and prepend
    a 4-byte unsigned big-endian length prefix.
    """
    try:
        payload_bytes = json.dumps(msg, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError) as e:
        raise ProtocolError(f"Cannot serialize message to JSON: {e}") from e

    length = len(payload_bytes)
    if length > MAX_PAYLOAD_SIZE:
        raise FramingError(
            f"Payload length {length} exceeds maximum allowed size of {MAX_PAYLOAD_SIZE} bytes"
        )

    header = struct.pack(HEADER_FORMAT, length)
    return header + payload_bytes


def decode_framed_bytes(payload_bytes: bytes) -> Dict[str, Any]:
    """
    Decode UTF-8 JSON payload bytes into a dictionary and validate its envelope.
    """
    try:
        decoded_text = payload_bytes.decode("utf-8")
        msg = json.loads(decoded_text)
    except UnicodeDecodeError as e:
        raise FramingError(f"Corrupt UTF-8 payload: {e}") from e
    except json.JSONDecodeError as e:
        raise ProtocolError(f"Corrupt JSON payload: {e}") from e

    if not validate_envelope(msg):
        raise ProtocolError(f"Message missing required envelope fields: {msg}")

    return msg


async def send_framed_message(writer: asyncio.StreamWriter, msg: Dict[str, Any]) -> None:
    """
    Send a 4-byte length prefixed framed message over an active TCP StreamWriter.
    """
    framed_data = encode_framed_bytes(msg)
    writer.write(framed_data)
    await writer.drain()


async def read_framed_message(reader: asyncio.StreamReader) -> Optional[Dict[str, Any]]:
    """
    Read a single framed message from a TCP StreamReader using strict 4-byte length prefix.

    Returns:
      - dict: Parsed and validated message.
      - None: If the remote peer closed the connection gracefully before any bytes were sent (EOF).
    Raises:
      - FramingError: If header or payload read is incomplete or exceeds max size.
      - ProtocolError: If payload fails JSON or envelope validation.
    """
    try:
        header = await reader.readexactly(HEADER_SIZE)
    except asyncio.IncompleteReadError as e:
        if len(e.partial) == 0:
            # Clean EOF: peer disconnected between messages
            return None
        raise FramingError(
            f"Connection closed during frame header read (received {len(e.partial)} of {HEADER_SIZE} bytes)"
        ) from e

    (length,) = struct.unpack(HEADER_FORMAT, header)
    if length > MAX_PAYLOAD_SIZE:
        raise FramingError(
            f"Frame header specifies length {length} exceeding maximum allowed {MAX_PAYLOAD_SIZE} bytes"
        )

    try:
        payload_bytes = await reader.readexactly(length)
    except asyncio.IncompleteReadError as e:
        raise FramingError(
            f"Connection closed during frame payload read (expected {length} bytes, received {len(e.partial)} bytes)"
        ) from e

    return decode_framed_bytes(payload_bytes)


def encode_udp_message(msg: Dict[str, Any]) -> bytes:
    """
    Serialize a message to UTF-8 JSON bytes for UDP datagram transmission.
    (Datagrams preserve message boundaries; 4-byte length prefix is not required).
    """
    try:
        return json.dumps(msg, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError) as e:
        raise ProtocolError(f"Cannot serialize UDP message to JSON: {e}") from e


def decode_udp_message(raw_bytes: bytes) -> Dict[str, Any]:
    """
    Decode raw UDP datagram bytes into a dictionary and validate its envelope.
    """
    try:
        msg = json.loads(raw_bytes.decode("utf-8"))
    except UnicodeDecodeError as e:
        raise ProtocolError(f"Corrupt UTF-8 in UDP datagram: {e}") from e
    except json.JSONDecodeError as e:
        raise ProtocolError(f"Corrupt JSON in UDP datagram: {e}") from e

    if not validate_envelope(msg):
        raise ProtocolError(f"UDP datagram missing required envelope fields: {msg}")

    return msg

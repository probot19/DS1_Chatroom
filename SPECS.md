# Technical Specification: Distributed Peer-to-Peer Chat Room

- **Course:** Distributed Systems I: Foundations (University of Stuttgart, WS2026)
- **Group:** Group 53 (Pratham Upadhye, Samiksha Chavan, Ritesh Ajit Patil, Florina Mary Peter Sahaya Raj)
- **Runtime:** Python 3.10+ standard library only (`asyncio`, `socket`, `struct`, `json`, `uuid`, `argparse`, `logging`, `pathlib`)
- **Deployment:** Native Python processes across multiple terminals on one machine and across at least two physical machines on a LAN (NO Docker, NO external distributed middleware)

---

## 1. Architectural Model & Assumptions

- **Model:** Hybrid Peer-to-Peer with a dynamically elected leader. Every participant runs the exact same node software (`main.py`). There is no dedicated/fixed server and no permanent single point of failure.
- **Scale:** 3–10 nodes on a local area network (do not hardcode node count limits).
- **Roles:**
  - **Follower:** Sends and displays chat messages, maintains a full replica of the chat history and membership list, sends periodic heartbeats, and participates in elections.
  - **Leader:** Everything a follower does, plus acts as the global message sequencer, owns authoritative membership updates, responds to `JOIN` requests with history catch-up, and serves retransmission requests.
- **Failure Model & Assumptions:**
  - Covered: Crash-stop node failures, lost or delayed messages, temporary disconnections and reconnects.
  - Not covered: Network partitions (split-brain), Byzantine/malicious nodes, simultaneous crash of all nodes.

---

## 2. Five-Layer Software Architecture (Identical on Every Node)

1. **Chat Application + Non-Blocking CLI (`node.py`, `cli.py`)**
   - User input/output, local chat history replica (persisted to disk per node ID for reconnect tests), and demo/debug commands.
2. **Reliable Ordered Multicast (`multicast.py`)**
   - Sequencer logic (when leader), hold-back queue, strict sequential delivery (`expected_seq`), gap detection, retransmission requests/responses, sender retry loop for unconfirmed messages, and `msg_id` deduplication.
3. **Leader Election — Bully Algorithm (`election.py`)**
   - `ELECTION`, `ANSWER`, `COORDINATOR` state machine, preemption when a higher-ID node joins, and post-election history reconciliation (`STATE_REQ` / `STATE_RES`).
4. **Membership & Failure Detection (`membership.py`)**
   - Membership table (`node_id -> {ip, tcp_port, last_seen}`), discovery (`ANNOUNCE` / `LEADER_INFO` / `JOIN` / `JOIN_ACK` / `LEAVE` / `MEMBERSHIP_UPDATE`), periodic UDP multicast heartbeats, and crash detection via timeout.
5. **Communication & Discovery (`transport.py`, `protocol.py`)**
   - UDP Multicast socket for discovery announcements and heartbeats.
   - TCP server & client connections for chat messages, elections, membership updates, history transfer, and retransmissions.

---

## 3. Networking & Wire Protocol

### Transport Rules
- **UDP Multicast:** Default group `239.255.0.1`, port `5000` (configurable via CLI flags). Configure `SO_REUSEADDR`, `SO_REUSEPORT` (where supported by OS), `IP_ADD_MEMBERSHIP`, and `IP_MULTICAST_LOOP=1` so multiple local processes AND multiple LAN machines receive multicast datagrams.
- **TCP:** Bind listener to `0.0.0.0:<tcp_port>`, but advertise the node's actual LAN IPv4 address (auto-detected via a dummy UDP connect to a non-loopback address, overridable via `--host`)—never advertise `127.0.0.1` unless explicitly running in local-only mode.
- **TCP Framing:** Every TCP message MUST be framed with a 4-byte unsigned big-endian integer length prefix (`struct.pack("!I", len(payload_bytes))`) followed by UTF-8 JSON bytes.

### JSON Envelope Contract
Every message (both TCP and UDP) is a JSON object containing at minimum:
```json
{
  "type": "MESSAGE_TYPE",
  "sender_id": 101,
  "timestamp": 1732280000.123
}
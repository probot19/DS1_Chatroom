# Workspace Rules for Group 53 Distributed P2P Chat Room

Always read and follow `SPECS.md` as the single source of truth for system architecture, protocol message types, and algorithms.

## Hard Constraints (Never Violate)
1. **Standard Library Only:** Use Python 3.10+ built-in libraries only (`asyncio`, `socket`, `struct`, `json`, `uuid`, `argparse`, `logging`, `pathlib`, `dataclasses`, `typing`). Never add third-party distributed middleware or messaging frameworks (no ZooKeeper, Redis, ZeroMQ, Kafka, RabbitMQ, gRPC, Flask, FastAPI, etc.).
2. **No Docker:** Do NOT create Dockerfiles, `docker-compose.yml`, or container-dependent test scripts. Everything must run as native Python processes on macOS/Linux/Windows and across multiple physical LAN machines.
3. **Strict TCP Framing:** Every TCP send/receive MUST use the 4-byte big-endian length prefix helpers in `chatroom/protocol.py`. Never assume one `read()` equals one JSON message.
4. **LAN-Safe Networking:** Never hardcode `127.0.0.1` in protocol announcements or peer addresses. Bind TCP servers to `0.0.0.0` and advertise the resolved LAN IP (or `--host` override). Never use hostnames in socket addresses.
5. **Non-Blocking Asyncio:** Never block the `asyncio` event loop (no `time.sleep()`, no raw `input()`, no blocking socket calls). Always attach timeouts to network I/O and close TCP writers cleanly (`writer.close(); await writer.wait_closed()`).
6. **State Safety Invariants:**
   - A newly elected leader MUST complete `STATE_REQ`/`STATE_RES` history reconciliation before assigning any new sequence number or sending `COORDINATOR`.
   - Both the leader (`seen_msg_ids`) and every receiver (`delivered_msg_ids`) MUST deduplicate by `msg_id`.
   - Never deliver sequence number `k` before `k - 1` has been delivered.

## Development Workflow (Plan -> Execute -> Verify)
- **One Milestone at a Time:** Do not implement future milestones ahead of schedule. Keep changes tightly scoped to the requested milestone.
- **Plan Before Coding:** For every milestone, produce an implementation plan listing exact files to create/edit, state transitions, and edge cases before writing code.
- **Clean Modularity:** Keep strict separation across the 5 layers (`protocol.py`, `transport.py`, `membership.py`, `election.py`, `multicast.py`, `node.py`, `cli.py`) so all 4 team members can work independently and explain every module during Q&A.
- **Observable Logs:** Log all key distributed events with timestamps and `[Node <id> | <ROLE>]` prefixes (discovery, joins, leaves, heartbeat timeouts, `ELECTION`/`ANSWER`/`COORDINATOR`, reconciliation, hold-back buffering, and retransmissions) so demos are transparent and easy to follow.
- **Mandatory Verification Gate:** Never declare a milestone finished until you write and run an automated verification script in `tests/` that spawns multiple real node instances, tests both the happy path and the failure/edge case for that milestone, and passes with zero unhandled exceptions or hanging tasks.

## Code Quality Standards
- Add type hints on all function signatures and clear docstrings explaining the distributed-systems purpose of each class/method.
- Handle `ConnectionRefusedError`, `BrokenPipeError`, `asyncio.TimeoutError`, and `OSError` gracefully—a crashed peer must never crash a surviving node.
- Keep configuration timeouts centralized in a single `Config` dataclass so they are easy to tune for multi-machine LAN demos.
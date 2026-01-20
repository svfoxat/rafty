# Raft Implementation Status

This document provides a comprehensive overview of the Raft consensus algorithm implementation within the `internal/raft` package, detailing its current capabilities, limitations, and adherence to the specifications outlined in the original Raft paper.

## Implementation Overview

The Raft library is designed as a modular, gRPC-based consensus engine. It handles leader election, log replication, and safety guarantees while providing a simple interface for state machine integration.

### Core Consensus Engine

The following features represent the foundational components of the Raft protocol:

*   **Leader Election**: Fully implemented with randomized timeouts (500ms - 1000ms) to minimize collisions. Includes term-based voting and candidate transition logic.
*   **Log Replication**: The leader manages log propagation to followers via `AppendEntries` RPCs. Consensus is reached when a majority of nodes acknowledge receipt.
*   **Safety Guarantees**:
    *   **Election Restriction**: Candidates must have a log at least as up-to-date as the voter's log to receive a vote (Section 5.4.1).
    *   **Leader Step Down**: Nodes automatically revert to the follower state upon discovering a higher term in any RPC request or response (Section 5.1).
    *   **Commitment Logic**: The leader updates its `commitIndex` based on `matchIndex` tracking for the majority of the cluster (Section 5.4.2).
*   **Heartbeat Mechanism**: Periodic empty `AppendEntries` calls (100ms interval) maintain leadership and prevent unnecessary elections.
*   **Follower Consistency Check**: Basic log inconsistency handling is implemented through a simple `nextIndex` back-off mechanism (Section 5.3).

### Durability and Recovery

To satisfy the safety requirements of Raft, critical state is persisted to stable storage:

*   **State Persistence**: The `currentTerm`, `votedFor`, and the entire `log` are saved to disk before responding to any RPC that modifies them (Section 5.2).
*   **Atomic Writes**: The storage layer uses temporary files and `fsync` to ensure that state remains consistent even in the event of a power failure during a write.
*   **Crash Recovery**: Nodes automatically reload their persistent state upon initialization, allowing them to resume participation in the cluster without violating safety properties.

### Performance and Reliability

*   **Log Batching**: A dedicated `logBatcher` groups multiple client proposals into a single disk write and network replication cycle (5ms window) to maximize throughput.
*   **Transparent Forwarding**: Follower nodes automatically proxy `Propose` requests to the current leader via gRPC, simplifying client-side logic.
*   **Connection Management**: A robust gRPC client pool handles on-demand connection establishment with support for Kubernetes FQDNs.

## Compliance Gap Analysis

While the core protocol is stable, several advanced features and optimizations from the Raft paper are either partially implemented or currently missing.

| Feature | Paper Section | Status | Description |
| :--- | :--- | :--- | :--- |
| **Commit Restriction** | 5.4.2 | Partial | The leader correctly counts replicas for the current term. Verification is needed for legacy term commitment rules. |
| **Log Matching** | 5.3 | Partial | Basic consistency check is active, but the $O(N)$ back-off is unoptimized compared to the paper's suggestions. |
| **Log Compaction** | 7 | Missing | `InstallSnapshot` RPC and log truncation are not yet implemented. Logs will grow indefinitely. |
| **Membership Changes** | 6 | Missing | The cluster configuration is static. Dynamic node addition/removal via Joint Consensus is not supported. |
| **Linearizable Reads** | 8 | Missing | `GET` operations bypass the Raft log and read from local state, allowing for potentially stale data. |
| **Graceful Shutdown** | - | Incomplete | Goroutines for election timers and log batching do not currently respond to context cancellation. |

## Operational Requirements

The current deployment model assumes a Kubernetes environment:
*   **StatefulSets**: Used to maintain stable network identities (`rafty-0`, `rafty-1`, etc.).
*   **Persistent Volumes**: Required to store the `raft.json` state file and Pebble KV data.
*   **Headless Service**: Facilitates DNS-based peer discovery within the cluster.

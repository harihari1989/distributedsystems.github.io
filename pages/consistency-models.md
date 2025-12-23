---
layout: default
title: Consistency Models
---

# Consistency Models

## Why this problem exists
Distributed systems replicate state for availability and scale, but replication introduces divergence.
Consistency models define what clients can assume about reads relative to writes.
Without explicit guarantees, applications become brittle and correctness bugs are hard to reproduce.

## Core idea / pattern
### Strong consistency
Strong consistency (often linearizability) ensures reads reflect the latest successful write in a single global order.
This model simplifies reasoning but requires coordination and can reduce availability under partition.

### Causal consistency
Causal consistency preserves the order of causally related writes, but allows concurrent writes to be observed in different orders.
It enables low-latency reads while respecting causal relationships such as read-your-writes and session guarantees.

### Eventual consistency
Eventual consistency allows replicas to diverge temporarily, but guarantees convergence when updates stop.
Applications must handle conflicts or use mergeable data types to reconcile divergent histories.

| Model | Guarantee | Typical cost |
| --- | --- | --- |
| Strong | Global order, latest reads | Higher latency, lower availability |
| Causal | Preserves causality | Metadata overhead, session tracking |
| Eventual | Convergence over time | Conflicts, app-level reconciliation |

## Architecture diagram
<pre class="mermaid">
sequenceDiagram
  participant Client
  participant Leader
  participant Replica1
  participant Replica2
  Client->>Leader: Write(x=1)
  Leader->>Replica1: Replicate x=1
  Leader->>Replica2: Replicate x=1
  Client->>Replica2: Read x
  Replica2-->>Client: x=1
</pre>

## Step-by-step flow
1. A client writes a value to the leader or primary replica.
2. The leader replicates the write to followers and tracks acknowledgements.
3. A read is routed to a replica based on the configured consistency level.
4. If strong consistency is required, the system waits for quorum or leader confirmation.
5. If eventual consistency is allowed, the read may return stale data.

## Failure modes
- Stale reads occur when replicas lag or clients read from followers without quorum.
- Concurrent writes create conflicts that require resolution or can cause lost updates.
- Network partitions force a choice between availability and strict ordering.
- Clock skew breaks timestamp-based ordering and can cause write reordering.
- Over-aggressive retries amplify contention and increase conflict rates.

## Trade-offs
- Strong consistency reduces anomalies but increases tail latency and coordination cost.
- Causal consistency offers a balance, but requires tracking causal metadata.
- Eventual consistency maximizes availability and scale, but shifts complexity to the application.
- Quorum reads and writes reduce inconsistency but increase operational cost.
- Read locality improves latency but risks returning stale data.

## Real-world usage
- Spanner and CockroachDB provide strong consistency with distributed transactions.
- Dynamo-style systems and Cassandra often favor eventual consistency with tunable quorums.
- Redis and caching layers frequently use eventual or session consistency for speed.
- Multi-region systems often offer configurable consistency tiers per request.

## Interview tips
- Start by stating the required user-visible guarantees, not the database name.
- Explain the trade-off between latency and correctness for the workload.
- Mention how quorum settings map to consistency and availability.
- Call out conflict resolution strategy when using eventual consistency.
- Tie consistency choices to failure handling and recovery expectations.

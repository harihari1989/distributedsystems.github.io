---
layout: default
title: Storage and Data Patterns
---

# Storage and Data Patterns

## Why this problem exists
Data systems must survive failures, scale throughput, and keep latency predictable under uneven access.
Replication protects availability and durability, while sharding distributes load across nodes.
Partitioning decisions define where data lives, which directly shapes failure behavior and consistency cost.

## Core idea / pattern
### Replication, sharding, partitioning
Replication copies data across nodes to improve availability and read capacity.
Sharding splits a dataset by key or range so no single node holds all data.
Partitioning is the physical placement strategy that binds shards to nodes and zones.

| Technique | Primary goal | Typical risks |
| --- | --- | --- |
| Replication | Availability and durability | Replication lag, write amplification |
| Sharding | Horizontal scale | Hot shards, complex rebalancing |
| Partitioning | Fault isolation | Skewed placement, blast radius overlap |

### SQL vs NoSQL architectures
SQL systems emphasize schema, transactions, and strong consistency within a single logical database.
NoSQL systems trade rigid schemas and cross-entity transactions for scale and flexible access patterns.
The real decision is workload fit, not ideology; use the smallest model that meets the SLA.

| Attribute | SQL | NoSQL |
| --- | --- | --- |
| Consistency | Strong by default | Often tunable |
| Schema | Strict, enforced | Flexible or schema-on-read |
| Scaling | Vertical, or sharded with coordination | Horizontal by design |
| Transactions | Multi-row, ACID | Limited or scoped |

### CAP trade-offs
CAP highlights trade-offs during network partitions, not during healthy operation.
Under partition, you typically pick availability with eventual consistency or strict consistency with reduced availability.
See [consistency models](consistency-models.md) for precise guarantees.

## Architecture diagram
<pre class="mermaid">
flowchart LR
  App[Application] --> Router[Shard Router]
  Router --> ShardA[Shard A Primary]
  Router --> ShardB[Shard B Primary]
  ShardA --> RepA1[Replica A1]
  ShardA --> RepA2[Replica A2]
  ShardB --> RepB1[Replica B1]
  ShardB --> RepB2[Replica B2]
</pre>

## Step-by-step flow
1. The application submits a request with a shard key.
2. The router selects the shard based on the partitioning scheme.
3. Writes go to the shard primary and replicate to secondaries.
4. Reads are served from the primary or a replica based on consistency requirements.
5. Background processes rebalance shards when hot spots or growth thresholds appear.

## Failure modes
- Replication lag causes stale reads when clients read from followers.
- Network partitions create split-brain risk without clear leader election.
- Hot shards dominate CPU or storage, causing tail latency spikes.
- Rebalancing moves large amounts of data and can overload the network.
- Misconfigured quorum sizes allow stale reads or lost writes.

## Trade-offs
- Strong consistency simplifies reasoning but adds write latency and reduces availability under partition.
- Eventually consistent systems scale and survive partitions, but add reconciliation complexity.
- More replicas improve durability but increase write cost and coordination overhead.
- Range sharding enables efficient scans but risks uneven key distribution.
- Hash sharding balances load but complicates range queries and data locality.

## Real-world usage
- SQL systems like PostgreSQL or MySQL are common for transactional workloads with strict correctness.
- NoSQL systems like Cassandra or Dynamo-style stores power high-throughput, multi-region workloads.
- Document stores and wide-column databases are common for flexible schemas and high ingest.
- Cloud-native systems combine strong consistency per shard with global replication.

## Interview tips
- Start with access patterns and consistency requirements before naming a database.
- Call out shard keys early, along with how you handle hot partitions.
- Explain whether reads can be stale and how you communicate that in the product.
- Tie replication factor to durability goals and failure domains.
- Mention backfill and rebalancing strategies for growth.

---
layout: default
title: System Design Interview Patterns
---

# System Design Interview Patterns

## Why this problem exists
System design interviews test whether you can identify core failure modes and apply proven patterns.
These patterns recur because they address coordination, reliability, and scaling constraints that appear in most systems.
Knowing when to apply them is as important as knowing how they work.

## Core idea / pattern
### Leader election
Leader election provides a single authority for coordination tasks like scheduling or primary writes.
It reduces conflict but introduces leadership failure handling and split-brain risk.

### Quorum reads and writes
Quorums require a minimum number of replicas to acknowledge reads or writes.
They balance consistency and availability using R + W > N to ensure overlap.

### Consensus (Raft, Paxos)
Consensus ensures a set of nodes agree on a sequence of values even under failure.
Use it for critical metadata or coordination, not as a general data plane for all writes.

### Idempotency
Idempotency allows retrying requests without unintended side effects.
It is essential for safe retries in unreliable networks.

### Rate limiting and circuit breakers
Rate limiting shapes traffic to protect downstream services.
Circuit breakers fail fast when dependencies are unhealthy to prevent cascading failure.
See [networking patterns](networking-patterns.md) for backpressure context.

| Pattern | Primary goal | Typical risk |
| --- | --- | --- |
| Leader election | Single authority for coordination | Split brain, failover delays |
| Quorums | Bounded consistency | Higher latency and cost |
| Consensus | Agree on ordering | Complexity, performance overhead |
| Idempotency | Safe retries | Storage of dedupe keys |
| Rate limiting | Protect dependencies | Over-throttling users |

## Architecture diagram
<pre class="mermaid">
flowchart LR
  Client[Client] --> API[API Service]
  API --> Limiter[Rate Limiter]
  Limiter --> Queue[Work Queue]
  Queue --> Worker1[Worker A]
  Queue --> Worker2[Worker B]
  Worker1 --> Store[(Primary Store)]
  Worker2 --> Store
  Store --> Replica[(Replica)]
</pre>

## Step-by-step flow
1. The client submits a request with an idempotency key.
2. The API service checks the rate limiter and rejects if over quota.
3. The request enters a queue to smooth bursts and provide backpressure.
4. Workers process the request and write to a replicated store.
5. Reads use quorum policies to balance consistency and latency.

## Failure modes
- Split-brain leaders accept conflicting writes without proper quorum.
- Duplicate processing occurs when retries lack idempotency protection.
- Overly strict rate limits cause self-inflicted denial of service.
- Queues hide saturation and increase latency without clear backpressure signals.
- Consensus paths become a bottleneck when used for all writes.

## Trade-offs
- Leaders simplify coordination but introduce failover complexity and temporary unavailability.
- Quorums improve correctness but increase write latency and cost.
- Idempotency improves reliability but requires storage and lifecycle management for keys.
- Rate limiting protects dependencies but can reduce throughput for bursty workloads.
- Circuit breakers avoid cascading failure but require careful reset and fallback logic.

## Real-world usage
- ZooKeeper, etcd, and Consul provide leader election and consensus.
- Dynamo-style systems use tunable quorums for reads and writes.
- Payment and order processing APIs rely on idempotency keys.
- Gateways enforce rate limits and circuit breakers for shared dependencies.

## Interview tips
- Call out which components require strong coordination versus eventual consistency.
- Explain how retries, idempotency, and rate limits work together.
- Show how you prevent split-brain and handle leader failover.
- Tie quorum sizes to failure domains and SLA requirements.
- Mention how you observe and tune limits in production.

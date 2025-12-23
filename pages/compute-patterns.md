---
layout: default
title: Compute Patterns
---

# Compute Patterns

## Why this problem exists
Distributed systems must serve variable workloads while meeting latency and availability targets.
Compute patterns define how work is placed, how requests are routed, and how state is managed.
Without clear patterns, scaling decisions become ad hoc and failure handling becomes inconsistent.

## Core idea / pattern
### Client-server vs peer-to-peer
Client-server centralizes control and simplifies coordination, but concentrates load and failure domains.
Peer-to-peer distributes load and ownership, but requires more complex discovery and consistency management.
See [networking patterns](networking-patterns.md) for discovery and routing trade-offs.

### Stateless vs stateful services
Stateless services keep request state out of the service process, enabling quick replacement and elastic scaling.
Stateful services keep session or durable state locally, enabling low-latency access at the cost of more complex failover.
State placement interacts with replication and consistency decisions in [database architectures](database-architectures.md).

| Attribute | Stateless services | Stateful services |
| --- | --- | --- |
| Scale-out | Add or remove instances freely | Requires data movement or session affinity |
| Failure recovery | Replace instance with minimal impact | Recover or rebuild local state |
| Deployment | Easier rolling updates | Careful coordination and drain needed |
| Latency | Often higher due to remote state | Lower for local state access |

### Horizontal vs vertical scaling
Horizontal scaling adds more nodes to spread load, while vertical scaling increases resources per node.
Horizontal scaling improves fault tolerance, but adds coordination overhead and more failure modes.
Vertical scaling is simpler to operate, but hits hard limits and larger blast radius.

### Load balancing strategies
Load balancing spreads requests across replicas using round-robin, least-connections, or latency-aware routing.
L7 balancing enables routing by request attributes, but adds CPU cost and state in the proxy layer.
For gateway and proxy details, see [networking patterns](networking-patterns.md).

## Architecture diagram
<pre class="mermaid">
flowchart LR
  Clients[Clients] --> DNS[DNS or Anycast]
  DNS --> LB[Load Balancer]
  LB --> S1[Stateless Service A]
  LB --> S2[Stateless Service B]
  S1 --> Cache[Distributed Cache]
  S2 --> Cache
  Cache --> DB[(Stateful Store)]
</pre>

## Step-by-step flow
1. A client resolves the service endpoint via DNS or anycast.
2. The load balancer selects a healthy backend based on policy.
3. The stateless service validates the request and fetches state from cache or storage.
4. The service computes the response and writes state updates if needed.
5. The response returns through the load balancer to the client.

## Failure modes
- Load balancer misconfiguration or failure causes partial or total outage.
- Hot keys or skewed traffic create uneven load across replicas.
- Stateful instances fail before persisting state, causing data loss or session drops.
- Cache stampedes amplify load on storage during misses or invalidations.
- Horizontal scaling without shard rebalancing creates hot partitions.

## Trade-offs
- Statelessness favors elasticity and fast recovery, but increases dependency on storage and cache tiers.
- Stateful services reduce per-request latency, but complicate failover and rollout processes.
- Horizontal scaling improves resilience, but adds coordination overhead and tail-latency risk.
- L7 routing enables smart traffic shaping, but adds a critical proxy layer and operational complexity.

## Real-world usage
- Web frontends and API layers are typically stateless behind L7 load balancers like Envoy or NGINX.
- Stateful services include databases, queues, and session stores that require replication and sharding.
- Peer-to-peer patterns appear in BitTorrent-style distribution and WebRTC meshes.
- Autoscaling patterns are often implemented with Kubernetes HPA in [Kubernetes patterns](kubernetes-patterns.md).

## Interview tips
- Start by stating the workload shape and whether state can be externalized.
- Call out load balancer policy and health checks early.
- Explain how you would avoid hot partitions and cache stampedes.
- Tie scaling decisions to SLAs and failure domains, not just throughput.

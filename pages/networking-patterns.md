---
layout: default
title: Networking Patterns
---

# Networking Patterns

## Why this problem exists
Distributed systems move across dynamic networks where service instances appear, disappear, and relocate.
Clients and services need stable discovery, safe routing, and policy enforcement without assuming static IPs.
Networking patterns provide repeatable ways to route traffic, manage protocol choices, and prevent cascading failures.

## Core idea / pattern
### Service discovery
Service discovery provides a stable name that resolves to healthy instances as the fleet changes.
Patterns include DNS-based discovery, client-side discovery with a registry, and server-side discovery via a load balancer.
Discovery choices interact with scaling behavior discussed in [compute patterns](compute-patterns.md).

| Pattern | How it works | Strengths | Risks |
| --- | --- | --- | --- |
| DNS-based | DNS returns multiple A/AAAA records | Simple, ubiquitous caching | Slow propagation, coarse health visibility |
| Client-side | Client queries registry then selects instance | Fine-grained routing, fast rebalancing | Client complexity, uneven upgrades |
| Server-side | Load balancer resolves and routes | Centralized policy, simpler clients | Proxy adds latency and failure domain |

### Reverse proxies and API gateways
Reverse proxies terminate client connections and forward requests to upstream services.
API gateways add routing, auth, rate limits, and request shaping at the edge.
These patterns centralize cross-cutting concerns but create a critical dependency layer.

### gRPC vs REST
REST over HTTP is easy to adopt and debuggable, making it a common external interface.
gRPC uses HTTP/2 and strongly typed schemas for efficient internal communication.
Pick interfaces based on client diversity and latency sensitivity, and link protocol choices to your SLA.

| Attribute | REST | gRPC |
| --- | --- | --- |
| Serialization | JSON or text | Protobuf |
| Client support | Broad, browser-friendly | Strong in backend-to-backend |
| Streaming | Limited via SSE/WebSocket | First-class bidi streams |
| Compatibility | Loose versioning | Strict schema evolution |

### Backpressure and flow control
Backpressure protects upstream services when downstream services are saturated.
Mechanisms include queue limits, circuit breakers, and token-bucket rate limiting.
See [system design interview patterns](system-design-patterns.md) for rate limiting and circuit breakers.

## Architecture diagram
<pre class="mermaid">
flowchart LR
  Client[Client] --> DNS[Service DNS]
  DNS --> Edge[API Gateway]
  Edge --> Proxy[Reverse Proxy]
  Proxy --> Mesh[Service Mesh]
  Mesh --> SvcA[Service A]
  Mesh --> SvcB[Service B]
  SvcA --> Registry[(Service Registry)]
  SvcB --> Registry
</pre>

## Step-by-step flow
1. The client resolves the service name through DNS and connects to the gateway.
2. The gateway authenticates the request and applies global rate limits.
3. A reverse proxy forwards the request to a service mesh or load balancer.
4. The mesh routes to a healthy instance based on discovery data.
5. The service replies, and headers or traces are added for observability.

## Failure modes
- Stale discovery data sends traffic to dead instances, causing spikes in retries.
- Overloaded gateways or proxies become bottlenecks and amplify tail latency.
- Misconfigured TLS or routing rules break canary traffic and cause partial outages.
- Retry storms increase load and create feedback loops without backoff or budgets.
- Inconsistent timeouts between layers cause slow drain and resource exhaustion.

## Trade-offs
- Centralized gateways simplify policy enforcement but increase critical-path dependencies.
- Client-side discovery improves responsiveness at scale but increases client complexity.
- gRPC provides efficient internal calls but reduces interoperability with browsers.
- Aggressive retries improve success rates but increase p99 latency and load.
- Service meshes improve observability and mTLS but add operational overhead.

## Real-world usage
- Edge gateways like Envoy, NGINX, or Kong front API traffic and enforce policy.
- Service meshes such as Istio or Linkerd manage mTLS and routing inside clusters.
- DNS-based discovery is common in Kubernetes via CoreDNS; see [Kubernetes patterns](kubernetes-patterns.md).
- gRPC is common for internal RPC while REST remains common for public APIs.

## Interview tips
- Start with the request path and identify where policy and routing should live.
- Call out discovery and health checks before discussing retries and timeouts.
- Explain how you prevent retry storms and protect downstream services.
- Tie protocol choice to client ecosystem and latency requirements.
- Mention how you would instrument traces and metrics at the gateway and mesh.

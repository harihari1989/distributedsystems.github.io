---
layout: default
title: Kubernetes Patterns
---

# Kubernetes Patterns

## Why this problem exists
Modern distributed systems need repeatable deployment, scaling, and recovery without bespoke automation per service.
Kubernetes abstracts the infrastructure layer so teams can standardize scheduling, rollout, and service discovery.
Patterns emerge to make those primitives safe at scale and to avoid fragile, ad hoc operational playbooks.

## Core idea / pattern
### Pods, services, and ingress
Pods are the smallest schedulable unit and encapsulate one or more tightly coupled containers.
Services provide stable virtual IPs and discovery for ephemeral pods.
Ingress exposes services externally and centralizes TLS termination and routing.

| Component | Role | Strengths | Risks |
| --- | --- | --- | --- |
| Pod | Co-scheduled containers with shared network | Tight coupling, shared volumes | Coupled failure domain |
| Service | Stable name + load balancing | Decouples clients from pods | Misconfigured selectors cause blackholes |
| Ingress | External routing and TLS | Centralized policy | Single critical layer if unscaled |

### Sidecar pattern
Sidecars add cross-cutting concerns such as mTLS, logging, or rate limiting without changing app code.
This pattern enables consistent policy but increases resource usage and operational complexity.
See [networking patterns](networking-patterns.md) for gateway and mesh details.

### Operators and controllers
Controllers reconcile desired state to actual state, making systems self-healing.
Operators extend this with domain-specific automation such as backups or failover.
The reconciliation loop is the core reliability mechanic, not an optional detail.

### Autoscaling
Horizontal Pod Autoscaler (HPA) scales replica counts based on metrics like CPU or custom signals.
Vertical Pod Autoscaler (VPA) adjusts resource requests and limits to fit observed usage.
Autoscaling depends on accurate metrics and sane limits to avoid oscillation.

## Architecture diagram
<pre class="mermaid">
flowchart LR
  Client[Client] --> Ingress[Ingress]
  Ingress --> Svc[Service]
  Svc --> PodA[Pod A]
  Svc --> PodB[Pod B]
  PodA --> SidecarA[Sidecar]
  PodB --> SidecarB[Sidecar]
  Controller[Controller] --> Deploy[Deployment]
  Deploy --> PodA
  Deploy --> PodB
</pre>

## Step-by-step flow
1. A client connects to the ingress endpoint over TLS.
2. Ingress routes the request to a service based on hostname and path.
3. The service load balances across healthy pod endpoints.
4. The sidecar enforces mTLS or policy before the app container handles the request.
5. Controllers observe desired state and replace unhealthy pods automatically.

## Failure modes
- Misconfigured readiness probes route traffic to unhealthy pods.
- Ingress or service selectors drift from labels, causing traffic drops.
- Sidecars crash-loop and block app traffic, creating partial outages.
- HPA scales on noisy metrics and causes oscillation or thundering herds.
- Resource limits are too low, leading to OOM kills and instability.

## Trade-offs
- Kubernetes reduces manual ops but increases platform complexity and learning cost.
- Sidecars centralize policy but add CPU and memory overhead.
- Operators automate complex workflows but can hide critical domain assumptions.
- HPA improves responsiveness but can amplify load spikes without backpressure.
- Ingress centralizes routing but can become a bottleneck if underprovisioned.

## Real-world usage
- Production clusters commonly use NGINX or Envoy-based ingress controllers.
- Service meshes such as Istio or Linkerd deploy sidecars for mTLS and routing.
- Operators manage databases like PostgreSQL or Kafka with custom failover logic.
- Autoscaling pairs HPA with cluster autoscalers to add nodes as load grows.

## Interview tips
- Start with pod and service roles before introducing ingress and sidecars.
- Explain how health checks and probes protect user traffic.
- Discuss rollout strategies like canary and blue-green with safe rollback.
- Tie autoscaling to metrics quality and cost constraints.
- Mention how you avoid noisy-neighbor and resource starvation issues.

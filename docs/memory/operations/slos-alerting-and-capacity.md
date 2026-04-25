# SLOs, Alerting, and Capacity

## 1. Service level indicators (SLIs)

| SLI | Definition |
|---|---|
| Retrieval latency | time from retriever start to merged result |
| Retrieval success | percent of requests with usable memory layer |
| Fast consolidation success | completed fast-lane jobs / triggered jobs |
| Deep consolidation throughput | sessions processed per schedule window |
| Forget completion | successful full-store delete/redaction completion |

## 2. SLO targets (example baseline)

| SLO | Target |
|---|---|
| Retrieval latency p95 | <= 40 ms for memory layer |
| Retrieval success | >= 99.5% |
| Fast consolidation success | >= 99.0% |
| Forget completion | >= 99.9% within policy window |

Adjust to deployment profile after baseline measurements.

## 3. Alert classes

### P1 (immediate)

- both retrieval paths failing
- delete/forget pipeline stuck
- sustained index/store write failure

### P2 (urgent)

- fast lane failure spikes
- graphify backlog growth beyond threshold
- large drift between index and vector counts

### P3 (watch)

- rising retrieval latency trend
- reduced acceptance ratio from extractor drift

## 4. Capacity checkpoints

| Store | Watch signals |
|---|---|
| Redis | memory utilization, eviction count, command latency |
| PostgreSQL | partition growth, index bloat, slow query ratio |
| Qdrant | query latency, segment count, RAM/disk growth |
| Neo4j | traversal latency, constraint/index health, tx retries |
| PostgreSQL event tables | write latency, partition growth, job backlog |
| MinIO | object count growth, retrieval latency |

## 5. Dashboard minimums

- retrieval stage timing breakdown
- vector vs graph contribution rates
- consolidation queue depth and job age
- forget pipeline completion funnel
- per-store write/read error rates

<!-- memory-expansion-2026-04-10 -->

## Builder Addendum: Expanded Control Surface

This addendum extends the document with practical implementation controls for the Tony memory runtime.

| Control surface | Default posture | Why it matters |
|---|---|---|
| Candidate precision | threshold-gated writes | reduces low-signal memory pollution |
| Recall diversity | vector + graph blending | improves answer richness and grounding |
| Durability | multi-store receipts + audit trail | prevents silent memory loss |
| Cost efficiency | token-budget fitting and pruning | preserves quality under context limits |

```mermaid
flowchart LR
    A[Input signal] --> B[Memory classification]
    B --> C[Store writes and indexing]
    C --> D[Hybrid retrieval]
    D --> E[Context budget fit]
    E --> F[Response + post-hooks]
```

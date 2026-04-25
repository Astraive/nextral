# Episodic Memory

## Index

1. [Purpose](#purpose)
2. [Primary Stores](#primary-stores)
3. [Consolidation Flow](#consolidation-flow)
4. [Retrieval Characteristics](#retrieval-characteristics)
5. [Failure Mode It Prevents](#failure-mode-it-prevents)
6. [Builder Addendum: Expanded Control Surface](#builder-addendum-expanded-control-surface)


## Purpose

Episodic memory stores what happened and when across sessions, so Tony can reference prior events with timeline context.

## Primary Stores

| Store | Role |
|---|---|
| Qdrant | Embedded conversation episodes for semantic recall |
| PostgreSQL event tables | Event log and temporal timeline rows |
| MinIO S3 | Raw transcript archive for full-fidelity replay |

## Consolidation Flow

1. Session goes idle/closes and queues an async MemoryAgent job.
2. Full transcript is loaded from PostgreSQL.
3. Memory candidates are extracted and scored.
4. Accepted chunks are embedded and written to Qdrant.
5. Events are recorded in PostgreSQL event tables and raw transcripts are archived to MinIO/S3.

## Retrieval Characteristics

- Strong at "what happened before?" and "when did we decide X?"
- Preserves cross-session recall even when hot/warm session windows have rolled over

## Failure Mode It Prevents

Without episodic memory, past decisions and event timing collapse into vague summaries or disappear between sessions.

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

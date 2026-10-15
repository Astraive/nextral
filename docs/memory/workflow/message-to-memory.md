# Workflow: message to memory

For a message such as “Atlas should use PostgreSQL; remind me Friday,” separate the host's reasoning from runtime operations:

1. Authenticate the user in the host and append a scoped session message.
2. Supply a host query vector/entities to assemble session tail, summary, policies and retrieved context.
3. Let the host generate the answer and decide whether this statement should become durable memory.
4. Have the host extract an explicit decision candidate, graph hints and embedding; call ingestion, or supply candidates plus a nonempty summary in fast consolidation.
5. Inspect acceptance/rejection and external receipts; drain durable outbox recovery if needed.
6. If a reminder is intended, resolve Friday/timezone/consent in the host, then explicitly schedule a reminder linked to the accepted owned source memory.
7. Run host-owned due polling and inspect real delivery outcomes.

Appending prose alone guarantees none of extraction, vector indexing, graph enrichment, reminder scheduling or delivery. Deep archival is a separate explicit consolidation lane with validated chunk provenance and an acknowledged S3 body. Stable IDs and versions support replay, not fabricated outcome guarantees.

# Episodic memory

Episodic records preserve events and their source/time context across sessions. Canonical PostgreSQL memory rows retain provenance; Qdrant stores host embeddings for recall; deep consolidation archives the actual transcript to private S3-compatible storage.

The host selects a session and supplies extracted candidates and vectors. Deep candidates must correspond to deterministic contiguous transcript chunks and source IDs; archive acknowledgement is part of the operation. No internal MemoryAgent extracts episodes or fabricates archive success.

Retrieval returns canonical active scoped content, not arbitrary archived transcripts. An archive is replay material with its own retention/access obligations, not an automatic event-analysis or timeline-search service. Preserve source linkage so forget and recovery can identify derived material.

# Seven memory categories

| Type | Purpose | Implemented boundary |
| --- | --- | --- |
| [Working](working/README.md) | Context for one host response | Ephemeral assembly; host owns reasoning/model state |
| [Session](session/README.md) | Conversation continuity | Full PostgreSQL transcript, Redis tail, host summary |
| [Episodic](episodic/README.md) | What happened and when | Canonical episode records, vectors and real transcript archive |
| [Semantic](semantic/README.md) | Stable facts/preferences/decisions | Canonical records and host embeddings |
| [Relational](relational/README.md) | Entity relationships | Host hints/entities merged into scoped graph |
| [Procedural](procedural/README.md) | User-specific instructions | Explicit policy CRUD, PostgreSQL and Redis cache |
| [Prospective](prospective/README.md) | Future commitments | Explicit owned-source reminders and acknowledged delivery |

These categories do not imply seven independent autonomous agents or seven storage engines. Working context is ephemeral; the other categories have durable representations only under `production_stores`. The explicitly selected test backend is process-local. See [architecture](../architecture.md).

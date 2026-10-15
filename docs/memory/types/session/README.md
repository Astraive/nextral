# Session memory

Session append persists each accepted message in PostgreSQL and maintains the last 20 messages in Redis. The full warm transcript is not delayed until the hot tail overflows. Redis retention uses configured `session_ttl_seconds`; no sub-millisecond or 5 ms latency guarantee is implied.

Messages have tenant/user/session scope and stable IDs for replay. Working context reads the tail, a persisted host-supplied summary, policies and relevant memory. A host can initiate idle/close consolidation, but importing Nextral does not schedule automatic summary generation.

A warm retention horizon such as 90 days is a deployment policy, not an automatic package default. Use explicit retention jobs and inspect results. Keep session IDs scoped; never build your own cache keys using only a global `session:{id}` namespace.

See [session-to-memory workflow](../../workflow/message-to-memory.md) and [consolidation](../../pipeline/consolidation-pipeline.md).

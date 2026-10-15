# Procedural memory

Procedural policies are explicit user-scoped instructions, not automatically learned habits, analytics-derived playbooks, or a package-managed prompt directory.

Tools `nextral.policy.upsert`, `nextral.policy.list`, and `nextral.policy.remove` operate on tenant/user scope. Upsert uses `policy_id` (optional), `name`, `body`, `privacy_level`, and optional `enabled`. List supports `privacy_scope` and `include_disabled`. PostgreSQL persists policies and Redis caches policy loading; mutation invalidates cached state.

Working context loads enabled authorized policies with the other context layers and includes their estimated token contribution. The host decides how instructions are applied to the model and must resolve conflicts with its own higher-priority rules.

Policies derived from a forgotten source can be scrubbed by exact owned reference. Unrelated manual policies must survive; a short memory ID appearing as arbitrary text is not a deletion match.

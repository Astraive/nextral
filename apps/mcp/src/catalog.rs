use serde_json::{json, Value};
use std::sync::LazyLock;

/// The executable package surface, shared by MCP discovery and CLI schema output.
pub fn tools() -> &'static [Value] {
    static TOOLS: LazyLock<Vec<Value>> = LazyLock::new(build_tools);
    &TOOLS
}

pub fn tool(name: &str) -> Option<&'static Value> {
    tools().iter().find(|tool| tool["name"] == name)
}

fn object(properties: Value, required: &[&str]) -> Value {
    json!({"type":"object", "properties":properties, "required":required, "additionalProperties":false})
}

fn scoped(mut properties: Value, required: &[&str]) -> Value {
    properties["tenant_id"] =
        json!({"type":"string","minLength":1,"description":"Tenant isolation boundary."});
    properties["user_id"] =
        json!({"type":"string","minLength":1,"description":"User within the tenant."});
    let mut fields = vec!["tenant_id", "user_id"];
    fields.extend_from_slice(required);
    object(properties, &fields)
}

fn strings() -> Value {
    json!({"type":"array","items":{"type":"string"}})
}
fn score() -> Value {
    json!({"type":"number","minimum":0,"maximum":1})
}
fn nullable(schema: Value) -> Value {
    json!({"anyOf":[schema,{"type":"null"}]})
}
fn privacy() -> Value {
    json!({"type":"string","enum":["private","sensitive","shared","restricted"]})
}
fn lane() -> Value {
    json!({"type":"string","enum":["stable","canary","sandbox"]})
}
fn severity() -> Value {
    json!({"type":"string","enum":["info","success","warning","destructive"]})
}
fn policy() -> Value {
    object(
        json!({"min_importance_score":score(),"min_confidence_score":score()}),
        &["min_importance_score", "min_confidence_score"],
    )
}
fn content_type() -> Value {
    json!({"type":"string","enum":["decision","preference","goal","fact","task","event","note","commitment","pattern"]})
}
fn memory_type() -> Value {
    json!({"type":"string","enum":["working","session","episodic","semantic","relational","procedural","prospective"]})
}
fn source_type() -> Value {
    json!({"type":"string","enum":["realtime","fast_lane","deep_lane","import","manual"]})
}
fn hints() -> Value {
    json!({"type":"array","items":object(json!({
        "from_label":{"type":"string"},"from_name":{"type":"string"},
        "relationship_type":{"type":"string"},"to_label":{"type":"string"},
        "to_name":{"type":"string"},"confidence":score()
    }), &["from_label","from_name","relationship_type","to_label","to_name","confidence"])})
}

fn graph_replay() -> Value {
    let mut extraction = hints();
    extraction["maxItems"] = json!(256);
    scoped(
        json!({"memory_id":{"type":"string","minLength":1},"hints":extraction}),
        &["memory_id"],
    )
}
fn vector() -> Value {
    nullable(
        json!({"type":"array","minItems":1,"items":{"type":"number"},"description":"Host-provided finite embedding matching the configured production dimension."}),
    )
}

fn ingest() -> Value {
    scoped(
        json!({
            "id":nullable(json!({"type":"string"})),"session_id":nullable(json!({"type":"string"})),
            "content":{"type":"string","minLength":1},"content_type":content_type(),
            "memory_type":memory_type(),"source_type":source_type(),"source_message_ids":strings(),
            "importance_score":score(),"confidence_score":nullable(score()),"entities":strings(),"tags":strings(),
            "privacy_level":privacy(),"graph_hints":hints(),"policy":policy(),
            "trace_id":nullable(json!({"type":"string"})),"embedding":vector()
        }),
        &[
            "content",
            "content_type",
            "memory_type",
            "source_type",
            "source_message_ids",
            "importance_score",
            "entities",
            "tags",
            "privacy_level",
            "graph_hints",
            "policy",
        ],
    )
}

fn retrieval() -> Value {
    scoped(
        json!({
            "session_id":nullable(json!({"type":"string"})),"query_text":{"type":"string","minLength":1},
            "entities":strings(),"intent_topic":nullable(json!({"type":"string"})),
            "token_budget":{"type":"integer","minimum":1},"privacy_scope":{"type":"array","items":privacy()},
            "top_k_vector":{"type":"integer","minimum":0},"max_graph_hops":{"type":"integer","minimum":0,"maximum":10},
            "lane":nullable(lane()),"policy_version":nullable(json!({"type":"string"})),
            "trace_id":nullable(json!({"type":"string"})),"query_vector":vector(),
            "scoring_weights":nullable(object(json!({"semantic_similarity":score(),"recency":score(),"importance":score(),"access":score()}), &["semantic_similarity","recency","importance","access"]))
        }),
        &[
            "query_text",
            "entities",
            "token_budget",
            "privacy_scope",
            "top_k_vector",
            "max_graph_hops",
        ],
    )
}

fn forget() -> Value {
    scoped(
        json!({"memory_id":{"type":"string","minLength":1},"actor":{"type":"string","minLength":1},"reason":{"type":"string","minLength":1},"redact":{"type":"boolean"}}),
        &["memory_id", "actor", "reason", "redact"],
    )
}

fn definition(
    name: &str,
    description: &str,
    schema: Value,
    read_only: bool,
    destructive: bool,
    idempotent: bool,
    open_world: bool,
) -> Value {
    json!({"name":name,"description":description,"inputSchema":schema,
        "annotations":{"readOnlyHint":read_only,"destructiveHint":destructive,"idempotentHint":idempotent,"openWorldHint":open_world}})
}

fn consolidation() -> Value {
    scoped(
        json!({
            "session_id":{"type":"string","minLength":1},
            "lane":{"type":"string","enum":["fast","deep"]},"policy":policy(),
            "version":{"type":"string","default":"v1"},
            "summary":nullable(json!({"type":"string","description":"Host-produced session summary; required for fast consolidation."})),
            "candidates":{"type":"array","items":ingest(),"description":"Host-extracted memories including scores and production embeddings. Deep candidates match deterministic transcript chunks."},
            "chunk_messages":{"type":"integer","minimum":1,"default":100}
        }),
        &["session_id", "lane", "policy", "candidates"],
    )
}

fn reminder_schedule() -> Value {
    scoped(
        json!({
            "source_memory_id":{"type":"string","minLength":1},
            "kind":{"type":"string","enum":["follow_up","commitment","task"]},
            "title":{"type":"string","minLength":1},"due_at":{"type":"string"},"timezone":{"type":"string"},
            "trace_id":nullable(json!({"type":"string"})),
            "priority":nullable(json!({"type":"string","enum":["low","normal","high"]})),
            "channel":{"type":"string","default":"webhook"},
            "allowed_channels":strings(),
            "quiet_hours":nullable(object(json!({"start_hour":{"type":"integer","minimum":0,"maximum":23},"end_hour":{"type":"integer","minimum":0,"maximum":23}}), &["start_hour","end_hour"])),
            "expires_at":nullable(json!({"type":"string"}))
        }),
        &["source_memory_id", "kind", "title", "due_at", "timezone"],
    )
}

fn reminder_due() -> Value {
    scoped(
        json!({
            "due_at_or_before":{"type":"string"},"actor":{"type":"string","minLength":1},
            "retry_delay_seconds":{"type":"integer","minimum":0},
            "max_retries":nullable(json!({"type":"integer","minimum":0})),
            "dispatch_policy_version":nullable(json!({"type":"string"})),
            "retry_strategy_id":nullable(json!({"type":"string"})),
            "trace_id":nullable(json!({"type":"string"})),
            "privacy_scope":{"type":"array","items":privacy()},
            "lease_seconds":{"type":"integer","minimum":1,"default":300}
        }),
        &["due_at_or_before", "actor", "retry_delay_seconds"],
    )
}

fn reembed_execution() -> Value {
    let plan = object(
        json!({
            "id":{"type":"string","minLength":1},"tenant_id":{"type":"string","minLength":1},
            "source_collection":{"type":"string","minLength":1},"shadow_collection":{"type":"string","minLength":1},
            "target_embedding_provider":{"type":"string","minLength":1},"target_embedding_model":{"type":"string","minLength":1},
            "status":{"type":"string","description":"Use the plan returned by nextral.reembed.plan."}
        }),
        &[
            "id",
            "tenant_id",
            "source_collection",
            "shadow_collection",
            "target_embedding_provider",
            "target_embedding_model",
            "status",
        ],
    );
    object(
        json!({
            "plan":plan,"user_id":{"type":"string","minLength":1},
            "vectors":{"type":"array","items":object(json!({"memory_id":{"type":"string","minLength":1},"vector":{"type":"array","minItems":1,"items":{"type":"number"}}}), &["memory_id","vector"])},
            "target_dimension":{"type":"integer","minimum":1},
            "cutover":{"type":"boolean","default":false},"rollback":{"type":"boolean","default":false}
        }),
        &["plan", "user_id", "target_dimension"],
    )
}

fn enqueue_job() -> Value {
    scoped(
        json!({
            "session_id":{"type":"string","minLength":1},
            "trigger":{"type":"string","enum":["idle","close","deep","retention"]},
            "run_at":{"type":"string","description":"Epoch seconds or RFC3339."},
            "idle_seconds":nullable(json!({"type":"integer","minimum":1})),
            "consolidation":nullable(consolidation()),"memory_ids":strings(),
            "redact":{"type":"boolean","default":false},
            "max_attempts":{"type":"integer","minimum":1,"default":5}
        }),
        &["session_id", "trigger", "run_at"],
    )
}

fn build_tools() -> Vec<Value> {
    let mut tools = vec![
        definition("nextral.memory.ingest", "Validate and persist a memory with host-provided embedding in production. Returns record_id and write receipts.", ingest(), false, false, false, true),
        definition("nextral.memory.retrieve", "Retrieve ranked memories within tenant, user, privacy scope and token budget; updates access metadata.", retrieval(), false, false, false, true),
        definition("nextral.memory.forget", "Forget a memory, optionally redact content, and propagate removal to configured stores.", forget(), false, true, true, true),
        definition("nextral.memory.get", "Read one scoped memory by ID; returns null when absent.", scoped(json!({"memory_id":{"type":"string","minLength":1}}), &["memory_id"]), true, false, true, true),
        definition("nextral.memory.list", "List scoped memory records, optionally including inactive records.", scoped(json!({"include_inactive":{"type":"boolean"},"privacy_scope":{"type":"array","items":privacy()}}), &[]), true, false, true, true),
        definition("nextral.memory.stats", "Count scoped memories, tags and entities and summarize importance.", scoped(json!({}), &[]), true, false, true, true),
        definition("nextral.memory.update", "Patch memory content, scores, privacy, tags or entities in configured stores.", scoped(json!({
            "memory_id":{"type":"string","minLength":1},"content":{"type":"string"},"content_type":content_type(),
            "importance_score":score(),"confidence_score":nullable(score()),"privacy_level":privacy(),
            "add_tags":strings(),"remove_tags":strings(),"add_entities":strings(),"remove_entities":strings(),"embedding":vector()
        }), &["memory_id"]), false, true, true, true),
        definition("nextral.memory.search_by_tag", "List active scoped memories carrying a tag.", scoped(json!({"tag":{"type":"string","minLength":1}}), &["tag"]), true, false, true, true),
        definition("nextral.memory.search_by_entity", "List active scoped memories mentioning an entity.", scoped(json!({"entity":{"type":"string","minLength":1}}), &["entity"]), true, false, true, true),
        definition("nextral.graph.query", "Retrieve graph-supported memories using the scoped retrieval request.", retrieval(), false, false, false, true),
        definition("nextral.graph.graphify", "Durably replay scoped host graph hints against an active canonical memory ID without rewriting its body.", graph_replay(), false, true, true, true),
        definition("nextral.session.append", "Append a session message; idempotency_key deduplicates host retries.", scoped(json!({"session_id":{"type":"string","minLength":1},"role":{"type":"string","minLength":1},"content":{"type":"string","minLength":1},"idempotency_key":nullable(json!({"type":"string"})),"privacy_level":privacy()}), &["session_id","role","content"]), false, false, true, true),
        definition("nextral.session.eligible", "List scoped sessions whose latest message is at or before the cutoff, oldest first then session ID; returns session_id, UTC last_message_at and message_count for host-selected deep jobs.", scoped(json!({"inactive_at_or_before":{"type":"string","format":"date-time"},"limit":{"type":"integer","minimum":1,"maximum":1000,"default":100}}), &["inactive_at_or_before"]), true, false, true, true),
        definition("nextral.session.context", "Assemble session tail, summary and retrieved long-term memory within a token budget.", retrieval(), false, false, false, true),
        definition("nextral.policy.upsert", "Persist scoped procedural instructions without requiring an embedding.", scoped(json!({"policy_id":{"type":"string","minLength":1},"name":{"type":"string","minLength":1},"body":{"type":"string","minLength":1},"privacy_level":privacy(),"enabled":{"type":"boolean","default":true}}), &["name","body","privacy_level"]), false, true, true, true),
        definition("nextral.policy.list", "List procedural policies within the caller's scope and privacy policy.", scoped(json!({"privacy_scope":{"type":"array","items":privacy()},"include_disabled":{"type":"boolean","default":false}}), &[]), true, false, true, true),
        definition("nextral.policy.remove", "Remove scoped procedural instructions and invalidate cached policy.", scoped(json!({"policy_id":{"type":"string","minLength":1}}), &["policy_id"]), false, true, true, true),
        definition("nextral.consolidation.run", "Consolidate host-extracted candidates and summary into durable session memory using a fast or deep lane.", consolidation(), false, false, false, true),
        definition("nextral.reminders.schedule", "Schedule a source-linked reminder with delivery channel, quiet hours and expiry.", reminder_schedule(), false, false, true, true),
        definition("nextral.reminders.due", "Dispatch due reminders through the configured delivery seam and persist retry outcomes.", reminder_due(), false, false, false, true),
        definition("nextral.reminders.list", "List reminder records for one tenant and user.", scoped(json!({}), &[]), true, false, true, true),
        definition("nextral.reembed.plan", "Plan migration to a shadow vector collection without claiming execution or promotion.", object(json!({"tenant_id":{"type":"string","minLength":1},"source_collection":{"type":"string"},"shadow_collection":{"type":"string"},"target_embedding_provider":{"type":"string"},"target_embedding_model":{"type":"string"}}), &["tenant_id","source_collection","shadow_collection","target_embedding_provider","target_embedding_model"]), true, false, true, false),
        definition("nextral.jobs.reembed.execute", "Write and validate host-provided shadow embeddings, checkpoint progress, and optionally cut over or roll back collections.", reembed_execution(), false, true, false, true),
        definition("nextral.jobs.enqueue", "Enqueue durable idle, close, deep-consolidation or retention work with host outputs.", enqueue_job(), false, false, true, true),
        definition("nextral.jobs.run", "Claim and execute due lifecycle jobs with persisted retry outcomes.", scoped(json!({"now":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":1000,"default":100},"lease_seconds":{"type":"integer","minimum":1,"default":300}}), &["now"]), false, true, false, true),
        definition("nextral.jobs.list", "List durable lifecycle jobs for a tenant and user.", scoped(json!({}), &[]), true, false, true, true),
        definition("nextral.jobs.get", "Read one durable scoped job by ID; returns null when absent.", scoped(json!({"job_id":{"type":"string","minLength":1}}), &["job_id"]), true, false, true, true),
        definition("nextral.outbox.run", "Retry pending tenant outbox effects and report completed and failed counts.", object(json!({"tenant_id":{"type":"string","minLength":1}}), &["tenant_id"]), false, true, false, true),
        definition("nextral.database.migrate", "Apply the configured production database migrations.", object(json!({}), &[]), false, true, true, true),
        definition("nextral.health", "Report configured backend readiness; fail_fast marks an unavailable store.", object(json!({}), &[]), true, false, true, true),
        definition("nextral.runtime.scored_search", "Retrieve scoped memories with score breakdown and telemetry.", retrieval(), false, false, false, true),
        definition("experiments.create", "Create an experiment candidate for a runtime lane and policy version.", object(json!({"lane":lane(),"policy_version":{"type":"string"},"description":{"type":"string"}}), &["lane","policy_version","description"]), false, false, false, false),
        definition("experiments.promote", "Promote an experiment using comparable measured release-build baseline and candidate replay evidence.", crate::promote_schema::schema(), false, true, false, false),
        definition("experiments.rollback", "Restore the active experiment's previous lane and policy and record the rollback reason.", object(json!({"experiment_id":{"type":"string","minLength":1},"reason":{"type":"string","minLength":1}}), &["experiment_id","reason"]), false, true, false, false),
        definition("experiments.status", "Read experiment registry status, optionally for one experiment.", object(json!({"experiment_id":{"type":"string"}}), &[]), true, false, true, false),
        definition("safety.policy.get", "Read safety policy severity-to-action mappings.", object(json!({}), &[]), true, false, true, false),
        definition("safety.policy.set", "Set the decision action for a safety severity.", object(json!({"severity":severity(),"action":{"type":"string","enum":["continue","record_improvement","constrain","auto_rollback_and_quarantine"]}}), &["severity","action"]), false, true, true, false),
    ];
    for (name, key, item_schema, description, destructive) in [
        (
            "nextral.batch.ingest",
            "items",
            ingest(),
            "Ingest up to 1000 memories and return per-item results and errors.",
            false,
        ),
        (
            "nextral.batch.retrieve",
            "queries",
            retrieval(),
            "Retrieve up to 1000 scoped queries and return per-item results and errors.",
            false,
        ),
        (
            "nextral.batch.forget",
            "items",
            forget(),
            "Forget up to 1000 scoped memories and return per-item results and errors.",
            true,
        ),
    ] {
        let mut properties = json!({"idempotency_key":nullable(json!({"type":"string"}))});
        properties[key] = json!({"type":"array","maxItems":1000,"items":item_schema});
        tools.push(definition(
            name,
            description,
            object(properties, &[key]),
            false,
            destructive,
            false,
            true,
        ));
    }
    tools
}

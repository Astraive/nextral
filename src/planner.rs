use crate::topology::{profile, ReadPath, StoreRole, WriteLane};
use crate::{memory::MemoryType, topology::MemoryTypeProfile};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum MemoryOperation {
    Ingest,
    Retrieve,
    Consolidate,
    Forget,
    Redact,
    Archive,
    Schedule,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperationStep {
    pub name: String,
    pub store: Option<StoreRole>,
    pub required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryOperationPlan {
    pub memory_type: MemoryType,
    pub operation: MemoryOperation,
    pub write_lanes: Vec<WriteLane>,
    pub read_paths: Vec<ReadPath>,
    pub steps: Vec<OperationStep>,
}

pub fn operation_plan(memory_type: &MemoryType, operation: MemoryOperation) -> MemoryOperationPlan {
    let type_profile = profile(memory_type);
    let steps = steps_for(&type_profile, &operation);
    MemoryOperationPlan {
        memory_type: memory_type.clone(),
        operation,
        write_lanes: type_profile.write_lanes,
        read_paths: type_profile.read_paths,
        steps,
    }
}

pub fn all_operation_plans() -> Vec<MemoryOperationPlan> {
    let memory_types = [
        MemoryType::Working,
        MemoryType::Session,
        MemoryType::Episodic,
        MemoryType::Semantic,
        MemoryType::Relational,
        MemoryType::Procedural,
        MemoryType::Prospective,
    ];
    let operations = [
        MemoryOperation::Ingest,
        MemoryOperation::Retrieve,
        MemoryOperation::Consolidate,
        MemoryOperation::Forget,
        MemoryOperation::Redact,
        MemoryOperation::Archive,
        MemoryOperation::Schedule,
    ];

    memory_types
        .iter()
        .flat_map(|memory_type| {
            operations
                .iter()
                .map(|operation| operation_plan(memory_type, operation.clone()))
        })
        .collect()
}

fn steps_for(type_profile: &MemoryTypeProfile, operation: &MemoryOperation) -> Vec<OperationStep> {
    match operation {
        MemoryOperation::Ingest => ingest_steps(type_profile),
        MemoryOperation::Retrieve => retrieve_steps(type_profile),
        MemoryOperation::Consolidate => consolidate_steps(type_profile),
        MemoryOperation::Forget => forget_steps(type_profile, false),
        MemoryOperation::Redact => forget_steps(type_profile, true),
        MemoryOperation::Archive => archive_steps(type_profile),
        MemoryOperation::Schedule => schedule_steps(type_profile),
    }
}

fn ingest_steps(type_profile: &MemoryTypeProfile) -> Vec<OperationStep> {
    if !type_profile.durable {
        return vec![step(
            "keep in model context",
            Some(StoreRole::ModelContext),
            true,
        )];
    }

    let mut steps = vec![
        step("validate contract and policy", None, true),
        step("write canonical index", Some(StoreRole::Postgres), true),
    ];
    if type_profile.required_stores.contains(&StoreRole::Redis) {
        steps.push(step(
            "update hot cache or queue",
            Some(StoreRole::Redis),
            true,
        ));
    }
    if type_profile.required_stores.contains(&StoreRole::Qdrant) {
        steps.push(step(
            "embed and upsert vector",
            Some(StoreRole::Qdrant),
            true,
        ));
    }
    if type_profile.required_stores.contains(&StoreRole::Neo4j) {
        steps.push(step("merge graph evidence", Some(StoreRole::Neo4j), true));
    }
    if type_profile.required_stores.contains(&StoreRole::S3) {
        steps.push(step("archive source payload", Some(StoreRole::S3), true));
    }
    steps.push(step("write audit event", Some(StoreRole::Postgres), true));
    steps
}

fn retrieve_steps(type_profile: &MemoryTypeProfile) -> Vec<OperationStep> {
    let mut steps = Vec::new();
    if type_profile.read_paths.contains(&ReadPath::ContextWindow) {
        steps.push(step(
            "read in-flight context",
            Some(StoreRole::ModelContext),
            true,
        ));
    }
    if type_profile.read_paths.contains(&ReadPath::HotTail) {
        steps.push(step("read session hot tail", Some(StoreRole::Redis), true));
    }
    if type_profile.read_paths.contains(&ReadPath::WarmSummary) {
        steps.push(step("read warm summary", Some(StoreRole::Postgres), true));
    }
    if type_profile.read_paths.contains(&ReadPath::Vector) {
        steps.push(step("run vector retrieval", Some(StoreRole::Qdrant), true));
    }
    if type_profile.read_paths.contains(&ReadPath::GraphTraversal) {
        steps.push(step("run graph traversal", Some(StoreRole::Neo4j), true));
    }
    if type_profile.read_paths.contains(&ReadPath::PolicyLoad) {
        steps.push(step(
            "load procedural policy",
            Some(StoreRole::Postgres),
            true,
        ));
        steps.push(step("read policy cache", Some(StoreRole::Redis), false));
    }
    if type_profile.read_paths.contains(&ReadPath::DueQueue) {
        steps.push(step("read due queue", Some(StoreRole::Redis), true));
        steps.push(step(
            "load reminder record",
            Some(StoreRole::Postgres),
            true,
        ));
    }
    if type_profile.read_paths.contains(&ReadPath::Archive) {
        steps.push(step(
            "read archive metadata",
            Some(StoreRole::Postgres),
            true,
        ));
        steps.push(step("read archived object", Some(StoreRole::S3), false));
    }
    steps
}

fn consolidate_steps(type_profile: &MemoryTypeProfile) -> Vec<OperationStep> {
    if !type_profile.durable {
        return Vec::new();
    }
    let mut steps = vec![
        step("lease consolidation job", Some(StoreRole::Redis), true),
        step(
            "load source transcript or records",
            Some(StoreRole::Postgres),
            true,
        ),
    ];
    if type_profile.required_stores.contains(&StoreRole::Qdrant) {
        steps.push(step(
            "embed accepted candidates",
            Some(StoreRole::Qdrant),
            true,
        ));
    }
    if type_profile.required_stores.contains(&StoreRole::Neo4j) {
        steps.push(step(
            "enqueue graphify work",
            Some(StoreRole::Postgres),
            true,
        ));
    }
    if type_profile.required_stores.contains(&StoreRole::S3) {
        steps.push(step(
            "archive consolidation snapshot",
            Some(StoreRole::S3),
            true,
        ));
    }
    steps
}

fn forget_steps(type_profile: &MemoryTypeProfile, redact: bool) -> Vec<OperationStep> {
    if !type_profile.durable {
        return vec![step(
            "drop in-flight context",
            Some(StoreRole::ModelContext),
            true,
        )];
    }
    let action = if redact { "redact" } else { "forget" };
    let mut steps = vec![step(
        &format!("{action} canonical index row"),
        Some(StoreRole::Postgres),
        true,
    )];
    for store in &type_profile.required_stores {
        match store {
            StoreRole::Redis => steps.push(step(
                "invalidate cache and queues",
                Some(StoreRole::Redis),
                true,
            )),
            StoreRole::Qdrant => steps.push(step(
                "delete or redact vector payload",
                Some(StoreRole::Qdrant),
                true,
            )),
            StoreRole::Neo4j => {
                steps.push(step("unlink graph evidence", Some(StoreRole::Neo4j), true))
            }
            StoreRole::S3 => steps.push(step(
                "apply archive retention policy",
                Some(StoreRole::S3),
                true,
            )),
            StoreRole::Postgres | StoreRole::ModelContext => {}
        }
    }
    steps.push(step(
        "write governance audit event",
        Some(StoreRole::Postgres),
        true,
    ));
    steps
}

fn archive_steps(type_profile: &MemoryTypeProfile) -> Vec<OperationStep> {
    if type_profile.required_stores.contains(&StoreRole::S3) {
        vec![
            step("build immutable archive object", None, true),
            step("write archive object", Some(StoreRole::S3), true),
            step("write archive metadata", Some(StoreRole::Postgres), true),
        ]
    } else {
        Vec::new()
    }
}

fn schedule_steps(type_profile: &MemoryTypeProfile) -> Vec<OperationStep> {
    if type_profile.memory_type == MemoryType::Prospective {
        vec![
            step("write reminder record", Some(StoreRole::Postgres), true),
            step("enqueue due reminder", Some(StoreRole::Redis), true),
            step("attach retrieval context", Some(StoreRole::Qdrant), false),
            step(
                "write scheduling audit event",
                Some(StoreRole::Postgres),
                true,
            ),
        ]
    } else {
        Vec::new()
    }
}

fn step(name: &str, store: Option<StoreRole>, required: bool) -> OperationStep {
    OperationStep {
        name: name.to_string(),
        store,
        required,
    }
}

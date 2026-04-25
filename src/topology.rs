use crate::memory::MemoryType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum StoreRole {
    ModelContext,
    Postgres,
    Redis,
    Qdrant,
    Neo4j,
    S3,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum WriteLane {
    Ephemeral,
    Realtime,
    FastLane,
    DeepLane,
    Graphify,
    Policy,
    Scheduled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ReadPath {
    ContextWindow,
    HotTail,
    WarmSummary,
    Vector,
    GraphTraversal,
    PolicyLoad,
    DueQueue,
    Archive,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryTypeProfile {
    pub memory_type: MemoryType,
    pub write_lanes: Vec<WriteLane>,
    pub read_paths: Vec<ReadPath>,
    pub required_stores: Vec<StoreRole>,
    pub durable: bool,
    pub retrievable: bool,
}

pub fn profile(memory_type: &MemoryType) -> MemoryTypeProfile {
    match memory_type {
        MemoryType::Working => MemoryTypeProfile {
            memory_type: MemoryType::Working,
            write_lanes: vec![WriteLane::Ephemeral],
            read_paths: vec![ReadPath::ContextWindow],
            required_stores: vec![StoreRole::ModelContext],
            durable: false,
            retrievable: false,
        },
        MemoryType::Session => MemoryTypeProfile {
            memory_type: MemoryType::Session,
            write_lanes: vec![WriteLane::Realtime, WriteLane::FastLane],
            read_paths: vec![ReadPath::HotTail, ReadPath::WarmSummary],
            required_stores: vec![StoreRole::Redis, StoreRole::Postgres],
            durable: true,
            retrievable: true,
        },
        MemoryType::Episodic => MemoryTypeProfile {
            memory_type: MemoryType::Episodic,
            write_lanes: vec![WriteLane::FastLane, WriteLane::DeepLane],
            read_paths: vec![ReadPath::Vector, ReadPath::Archive],
            required_stores: vec![StoreRole::Qdrant, StoreRole::Postgres, StoreRole::S3],
            durable: true,
            retrievable: true,
        },
        MemoryType::Semantic => MemoryTypeProfile {
            memory_type: MemoryType::Semantic,
            write_lanes: vec![
                WriteLane::Realtime,
                WriteLane::FastLane,
                WriteLane::DeepLane,
            ],
            read_paths: vec![ReadPath::Vector],
            required_stores: vec![StoreRole::Qdrant, StoreRole::Postgres],
            durable: true,
            retrievable: true,
        },
        MemoryType::Relational => MemoryTypeProfile {
            memory_type: MemoryType::Relational,
            write_lanes: vec![WriteLane::Graphify],
            read_paths: vec![ReadPath::GraphTraversal],
            required_stores: vec![StoreRole::Neo4j, StoreRole::Postgres],
            durable: true,
            retrievable: true,
        },
        MemoryType::Procedural => MemoryTypeProfile {
            memory_type: MemoryType::Procedural,
            write_lanes: vec![WriteLane::Policy],
            read_paths: vec![ReadPath::PolicyLoad],
            required_stores: vec![StoreRole::Postgres, StoreRole::Redis],
            durable: true,
            retrievable: true,
        },
        MemoryType::Prospective => MemoryTypeProfile {
            memory_type: MemoryType::Prospective,
            write_lanes: vec![WriteLane::Scheduled],
            read_paths: vec![ReadPath::DueQueue, ReadPath::Vector],
            required_stores: vec![StoreRole::Postgres, StoreRole::Redis, StoreRole::Qdrant],
            durable: true,
            retrievable: true,
        },
    }
}

pub fn all_profiles() -> Vec<MemoryTypeProfile> {
    vec![
        profile(&MemoryType::Working),
        profile(&MemoryType::Session),
        profile(&MemoryType::Episodic),
        profile(&MemoryType::Semantic),
        profile(&MemoryType::Relational),
        profile(&MemoryType::Procedural),
        profile(&MemoryType::Prospective),
    ]
}

pub fn requires_store(memory_type: &MemoryType, store: StoreRole) -> bool {
    profile(memory_type).required_stores.contains(&store)
}

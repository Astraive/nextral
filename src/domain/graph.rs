use crate::{
    contracts::CoreResult,
    memory::{now_timestamp, validate_score, MemoryRecord},
    store::GraphStore,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphNode {
    pub user_id: String,
    pub label: String,
    pub name: String,
    pub canonical_name: String,
    pub key: String,
    pub confidence: f32,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphEdge {
    pub user_id: String,
    pub from_key: String,
    pub relationship_type: String,
    pub to_key: String,
    pub confidence: f32,
    pub source_memory_ids: Vec<String>,
    pub created_at: String,
    pub last_confirmed_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphHint {
    pub from_label: String,
    pub from_name: String,
    pub relationship_type: String,
    pub to_label: String,
    pub to_name: String,
    pub confidence: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphifyInput {
    pub memory_id: String,
    pub user_id: String,
    pub content: String,
    pub content_type: String,
    pub created_at: String,
    pub entities: Vec<String>,
    pub hints: Vec<GraphHint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphifyOutput {
    pub nodes: Vec<GraphNode>,
    pub relationships: Vec<GraphEdge>,
}

impl GraphNode {
    pub fn new(user_id: &str, label: &str, name: &str, confidence: f32) -> CoreResult<Self> {
        validate_score("confidence", confidence)?;
        let canonical_name = canonicalize(name);
        Ok(Self {
            user_id: user_id.to_string(),
            label: label.to_string(),
            name: name.trim().to_string(),
            key: format!("{}:{}", label.trim(), canonical_name),
            canonical_name,
            confidence,
            created_at: now_timestamp(),
        })
    }
}

impl GraphEdge {
    pub fn new(
        user_id: &str,
        from_key: &str,
        relationship_type: &str,
        to_key: &str,
        confidence: f32,
        source_memory_id: &str,
    ) -> CoreResult<Self> {
        validate_score("confidence", confidence)?;
        let now = now_timestamp();
        Ok(Self {
            user_id: user_id.to_string(),
            from_key: from_key.to_string(),
            relationship_type: relationship_type.trim().to_uppercase(),
            to_key: to_key.to_string(),
            confidence,
            source_memory_ids: vec![source_memory_id.to_string()],
            created_at: now.clone(),
            last_confirmed_at: now,
        })
    }
}

pub fn graphify_record(record: &MemoryRecord, hints: &[GraphHint]) -> CoreResult<GraphifyOutput> {
    let mut nodes = Vec::new();
    for entity in &record.entities {
        if !entity.trim().is_empty() {
            nodes.push(GraphNode::new(&record.user_id, "Entity", entity, 0.8)?);
        }
    }

    let mut relationships = Vec::new();
    for hint in hints {
        let from = GraphNode::new(
            &record.user_id,
            &hint.from_label,
            &hint.from_name,
            hint.confidence,
        )?;
        let to = GraphNode::new(
            &record.user_id,
            &hint.to_label,
            &hint.to_name,
            hint.confidence,
        )?;
        relationships.push(GraphEdge::new(
            &record.user_id,
            &from.key,
            &hint.relationship_type,
            &to.key,
            hint.confidence,
            &record.id,
        )?);
        nodes.push(from);
        nodes.push(to);
    }

    dedup_nodes(&mut nodes);
    Ok(GraphifyOutput {
        nodes,
        relationships,
    })
}

pub fn merge_graph(store: &mut impl GraphStore, output: GraphifyOutput) -> CoreResult<()> {
    for node in output.nodes {
        store.merge_node(node)?;
    }
    for edge in output.relationships {
        store.merge_edge(edge)?;
    }
    Ok(())
}

fn dedup_nodes(nodes: &mut Vec<GraphNode>) {
    nodes.sort_by(|left, right| left.key.cmp(&right.key));
    nodes.dedup_by(|left, right| left.user_id == right.user_id && left.key == right.key);
}

pub fn canonicalize(name: &str) -> String {
    name.trim().to_lowercase().replace(' ', "_")
}

pub fn connect(edges: &mut Vec<GraphEdge>, from: impl Into<String>, to: impl Into<String>) {
    let from = from.into();
    let to = to.into();
    if let Ok(edge) = GraphEdge::new("default", &from, "RELATED_TO", &to, 0.5, "legacy") {
        edges.push(edge);
    }
}

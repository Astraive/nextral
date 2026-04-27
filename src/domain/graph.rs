use crate::{
    contracts::{CoreError, CoreResult},
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
    pub trace_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphifyOutput {
    pub trace_id: String,
    pub nodes: Vec<GraphNode>,
    pub relationships: Vec<GraphEdge>,
    pub contradictions: Vec<String>,
    pub confidence_calibration_version: String,
    pub contradiction_class: String,
    pub evidence_count: u32,
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
    let mut contradictions = Vec::new();
    for entity in &record.entities {
        if !entity.trim().is_empty() {
            nodes.push(GraphNode::new(&record.user_id, infer_label(entity), entity, 0.8)?);
        }
    }

    let inferred = heuristic_hints(record);
    let all_hints: Vec<GraphHint> = hints.iter().cloned().chain(inferred).collect();
    let mut relationships = Vec::new();
    for hint in &all_hints {
        validate_graph_hint(hint)?;
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
    contradictions.extend(find_contradictions(&relationships));

    dedup_nodes(&mut nodes);
    Ok(GraphifyOutput {
        trace_id: crate::memory::deterministic_id(&[&record.id, &record.user_id, "graphify_trace"]),
        evidence_count: (nodes.len() + relationships.len()) as u32,
        contradiction_class: if contradictions.is_empty() {
            "none".to_string()
        } else if contradictions.len() == 1 {
            "single_conflict".to_string()
        } else {
            "multi_conflict".to_string()
        },
        nodes,
        relationships,
        contradictions,
        confidence_calibration_version: "native-v2".to_string(),
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

fn infer_label(entity: &str) -> &'static str {
    let lowered = entity.to_lowercase();
    if lowered.contains("postgres")
        || lowered.contains("redis")
        || lowered.contains("neo4j")
        || lowered.contains("qdrant")
    {
        "Technology"
    } else if entity
        .chars()
        .next()
        .map(|first| first.is_uppercase())
        .unwrap_or(false)
    {
        "Entity"
    } else {
        "Concept"
    }
}

fn heuristic_hints(record: &MemoryRecord) -> Vec<GraphHint> {
    let mut hints = Vec::new();
    let content = record.content.to_lowercase();
    let has_uses = content.contains(" use ") || content.contains(" using ");
    let has_owns = content.contains(" owns ") || content.contains(" owner ");
    let has_decides = content.contains(" decide") || content.contains(" chose ");
    if record.entities.len() >= 2 {
        for pair in record.entities.windows(2) {
            let relationship_type = if has_uses {
                "USES"
            } else if has_owns {
                "OWNS"
            } else if has_decides {
                "DECIDED"
            } else {
                "RELATED_TO"
            };
            hints.push(GraphHint {
                from_label: infer_label(&pair[0]).to_string(),
                from_name: pair[0].clone(),
                relationship_type: relationship_type.to_string(),
                to_label: infer_label(&pair[1]).to_string(),
                to_name: pair[1].clone(),
                confidence: 0.65,
            });
        }
    }
    hints
}

fn validate_graph_hint(hint: &GraphHint) -> CoreResult<()> {
    let allowed_labels = [
        "Person",
        "Project",
        "Technology",
        "Organization",
        "Entity",
        "Concept",
    ];
    let allowed_relationships = [
        "WORKS_ON",
        "USES",
        "OWNS",
        "DECIDED",
        "RELATED_TO",
        "DEPENDS_ON",
    ];
    if !allowed_labels.contains(&hint.from_label.trim()) {
        return Err(CoreError::InvalidInput(format!(
            "unsupported from_label {}",
            hint.from_label
        )));
    }
    if !allowed_labels.contains(&hint.to_label.trim()) {
        return Err(CoreError::InvalidInput(format!(
            "unsupported to_label {}",
            hint.to_label
        )));
    }
    let relationship = hint.relationship_type.trim().to_uppercase();
    if !allowed_relationships.contains(&relationship.as_str()) {
        return Err(CoreError::InvalidInput(format!(
            "unsupported relationship_type {}",
            hint.relationship_type
        )));
    }
    validate_score("confidence", hint.confidence)?;
    Ok(())
}

fn find_contradictions(edges: &[GraphEdge]) -> Vec<String> {
    let mut conflicts = Vec::new();
    for (idx, left) in edges.iter().enumerate() {
        for right in edges.iter().skip(idx + 1) {
            if left.from_key == right.from_key
                && left.to_key == right.to_key
                && left.relationship_type != right.relationship_type
            {
                conflicts.push(format!(
                    "conflicting relationship types for {} -> {}: {} vs {}",
                    left.from_key, left.to_key, left.relationship_type, right.relationship_type
                ));
            }
        }
    }
    conflicts
}

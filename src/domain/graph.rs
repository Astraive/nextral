use crate::{
    contracts::{CoreError, CoreResult},
    memory::{now_timestamp, validate_score, MemoryRecord},
    store::GraphStore,
};
use serde::{Deserialize, Serialize};

const NODE_LABELS: &[&str] = &[
    "Person",
    "Project",
    "Technology",
    "Organization",
    "Entity",
    "Concept",
];
const RELATIONSHIP_TYPES: &[&str] = &[
    "WORKS_ON",
    "USES",
    "OWNS",
    "DECIDED",
    "RELATED_TO",
    "DEPENDS_ON",
    "NOT_WORKS_ON",
    "NOT_USES",
    "NOT_OWNS",
    "NOT_DECIDED",
    "NOT_RELATED_TO",
    "NOT_DEPENDS_ON",
];

/// A knowledge graph entity node with tenant isolation and confidence scoring.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphNode {
    pub tenant_id: String,
    pub user_id: String,
    pub label: String,
    pub name: String,
    pub canonical_name: String,
    pub key: String,
    pub confidence: f32,
    pub created_at: String,
}

/// A typed relationship between two graph nodes, tracking provenance via source memory IDs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphEdge {
    pub tenant_id: String,
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
    pub fn new(
        tenant_id: &str,
        user_id: &str,
        label: &str,
        name: &str,
        confidence: f32,
    ) -> CoreResult<Self> {
        validate_score("confidence", confidence)?;
        crate::store::validate_scope_component(tenant_id, "tenant_id")?;
        crate::store::validate_scope_component(user_id, "user_id")?;
        if !NODE_LABELS.contains(&label.trim()) {
            return Err(CoreError::InvalidInput(
                "unsupported graph node label".into(),
            ));
        }
        if name.trim().is_empty() {
            return Err(CoreError::InvalidInput(
                "graph entity name is required".into(),
            ));
        }
        let canonical_name = canonicalize(name);
        Ok(Self {
            tenant_id: tenant_id.to_string(),
            user_id: user_id.to_string(),
            label: label.trim().to_string(),
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
        tenant_id: &str,
        user_id: &str,
        from_key: &str,
        relationship_type: &str,
        to_key: &str,
        confidence: f32,
        source_memory_id: &str,
    ) -> CoreResult<Self> {
        validate_score("confidence", confidence)?;
        crate::store::validate_scope_component(tenant_id, "tenant_id")?;
        crate::store::validate_scope_component(user_id, "user_id")?;
        crate::store::validate_scope_component(source_memory_id, "source_memory_id")?;
        let relationship_type = relationship_type.trim().to_uppercase();
        if from_key.trim().is_empty()
            || to_key.trim().is_empty()
            || !RELATIONSHIP_TYPES.contains(&relationship_type.as_str())
        {
            return Err(CoreError::InvalidInput(
                "invalid graph endpoints or relationship type".into(),
            ));
        }
        let now = now_timestamp();
        Ok(Self {
            tenant_id: tenant_id.to_string(),
            user_id: user_id.to_string(),
            from_key: from_key.to_string(),
            relationship_type,
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
            nodes.push(GraphNode::new(
                &record.tenant_id,
                &record.user_id,
                infer_label(entity),
                entity,
                0.8,
            )?);
        }
    }

    let inferred = if hints.is_empty() {
        heuristic_hints(record)
    } else {
        Vec::new()
    };
    let all_hints = hints.iter().chain(inferred.iter());
    let mut relationships = Vec::new();
    for hint in all_hints {
        validate_graph_hint(hint)?;
        let from = GraphNode::new(
            &record.tenant_id,
            &record.user_id,
            &hint.from_label,
            &hint.from_name,
            hint.confidence,
        )?;
        let to = GraphNode::new(
            &record.tenant_id,
            &record.user_id,
            &hint.to_label,
            &hint.to_name,
            hint.confidence,
        )?;
        relationships.push(GraphEdge::new(
            &record.tenant_id,
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

    relationships.sort_by(|left, right| {
        left.from_key
            .cmp(&right.from_key)
            .then(left.relationship_type.cmp(&right.relationship_type))
            .then(left.to_key.cmp(&right.to_key))
            .then(right.confidence.total_cmp(&left.confidence))
    });
    relationships.dedup_by(|left, right| {
        left.from_key == right.from_key
            && left.relationship_type == right.relationship_type
            && left.to_key == right.to_key
    });
    contradictions.extend(find_contradictions(&relationships));
    dedup_nodes(&mut nodes);
    Ok(GraphifyOutput {
        trace_id: crate::memory::deterministic_id(&[&record.id, &record.user_id, "graphify_trace"]),
        evidence_count: u32::try_from(nodes.len() + relationships.len()).unwrap_or(u32::MAX),
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
    nodes.sort_by(|left, right| {
        left.tenant_id
            .cmp(&right.tenant_id)
            .then(left.user_id.cmp(&right.user_id))
            .then(left.key.cmp(&right.key))
            .then(right.confidence.total_cmp(&left.confidence))
    });
    nodes.dedup_by(|left, right| {
        left.tenant_id == right.tenant_id && left.user_id == right.user_id && left.key == right.key
    });
}

pub fn canonicalize(name: &str) -> String {
    let mut canonical = String::with_capacity(name.len());
    for (index, word) in name.split_whitespace().enumerate() {
        if index > 0 {
            canonical.push('_');
        }
        canonical.extend(word.chars().flat_map(char::to_lowercase));
    }
    canonical
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
    let allowed_labels = NODE_LABELS;
    let allowed_relationships = RELATIONSHIP_TYPES;
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

pub(crate) fn validate_node(node: &GraphNode) -> CoreResult<()> {
    crate::store::validate_scope_component(&node.tenant_id, "tenant_id")?;
    crate::store::validate_scope_component(&node.user_id, "user_id")?;
    validate_score("graph confidence", node.confidence)?;
    if !NODE_LABELS.contains(&node.label.as_str())
        || node.name.trim().is_empty()
        || node.canonical_name != canonicalize(&node.name)
        || node
            .key
            .strip_prefix(node.label.as_str())
            .and_then(|suffix| suffix.strip_prefix(':'))
            != Some(node.canonical_name.as_str())
    {
        return Err(CoreError::InvalidInput(
            "invalid canonical graph node".into(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_edge(edge: &GraphEdge) -> CoreResult<()> {
    crate::store::validate_scope_component(&edge.tenant_id, "tenant_id")?;
    crate::store::validate_scope_component(&edge.user_id, "user_id")?;
    validate_score("graph confidence", edge.confidence)?;
    if !RELATIONSHIP_TYPES.contains(&edge.relationship_type.as_str())
        || edge.from_key.trim().is_empty()
        || edge.to_key.trim().is_empty()
        || edge.source_memory_ids.is_empty()
    {
        return Err(CoreError::InvalidInput(
            "invalid graph edge or provenance".into(),
        ));
    }
    for source in &edge.source_memory_ids {
        crate::store::validate_scope_component(source, "source_memory_id")?;
    }
    Ok(())
}

pub(crate) fn validate_source_graph(
    tenant_id: &str,
    user_id: &str,
    memory_id: &str,
    nodes: &[GraphNode],
    edges: &[GraphEdge],
) -> CoreResult<()> {
    crate::store::validate_scope_component(tenant_id, "tenant_id")?;
    crate::store::validate_scope_component(user_id, "user_id")?;
    crate::store::validate_scope_component(memory_id, "memory_id")?;
    for node in nodes {
        validate_node(node)?;
        if node.tenant_id != tenant_id || node.user_id != user_id {
            return Err(CoreError::InvalidInput("graph node scope mismatch".into()));
        }
    }
    for edge in edges {
        validate_edge(edge)?;
        if edge.tenant_id != tenant_id
            || edge.user_id != user_id
            || edge.source_memory_ids.len() != 1
            || edge.source_memory_ids.first().map(String::as_str) != Some(memory_id)
            || !nodes.iter().any(|node| node.key == edge.from_key)
            || !nodes.iter().any(|node| node.key == edge.to_key)
        {
            return Err(CoreError::InvalidInput(
                "graph source/endpoints mismatch".into(),
            ));
        }
    }
    Ok(())
}

fn find_contradictions(edges: &[GraphEdge]) -> Vec<String> {
    let mut conflicts = Vec::new();
    let mut seen = std::collections::HashMap::with_capacity(edges.len());
    for edge in edges {
        let (relation, polarity) = match edge.relationship_type.strip_prefix("NOT_") {
            Some(relation) => (relation, 2u8),
            None => (edge.relationship_type.as_str(), 1u8),
        };
        let key = (
            edge.tenant_id.as_str(),
            edge.user_id.as_str(),
            edge.from_key.as_str(),
            relation,
            edge.to_key.as_str(),
        );
        let evidence = seen.entry(key).or_insert(0u8);
        if *evidence != 0 && *evidence & polarity == 0 {
            conflicts.push(format!(
                "opposed {relation} evidence for {} -> {}",
                edge.from_key, edge.to_key
            ));
        }
        *evidence |= polarity;
    }
    conflicts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalization_merges_duplicate_names_and_preserves_distinct_evidence() {
        let mut record = MemoryRecord::new(
            "source",
            "tenant",
            "owner",
            "Canonical content",
            crate::memory::ContentType::Fact,
            crate::memory::MemoryType::Semantic,
            crate::memory::SourceType::Manual,
        );
        record.entities = vec!["Aster".into(), "Aster".into()];
        let hint = GraphHint {
            from_label: "Project".into(),
            from_name: " Aster   Team ".into(),
            relationship_type: "USES".into(),
            to_label: "Technology".into(),
            to_name: "Redis".into(),
            confidence: 0.9,
        };
        let mut duplicate = hint.clone();
        duplicate.from_name = "aster team".into();
        duplicate.confidence = 0.7;
        let mut ownership = hint.clone();
        ownership.relationship_type = "OWNS".into();
        let output = graphify_record(&record, &[hint, duplicate, ownership]).unwrap();
        assert_eq!(
            output
                .nodes
                .iter()
                .filter(|node| node.key == "Project:aster_team")
                .count(),
            1
        );
        assert_eq!(
            output
                .relationships
                .iter()
                .filter(|edge| edge.from_key == "Project:aster_team"
                    && edge.relationship_type == "USES")
                .count(),
            1
        );
        assert!(output
            .relationships
            .iter()
            .all(|edge| edge.source_memory_ids == ["source"]));
    }

    #[test]
    fn ontology_and_provenance_are_validated_at_construction() {
        assert!(GraphNode::new("tenant", "owner", "Unapproved", "Aster", 0.8).is_err());
        assert!(GraphNode::new("tenant", "owner", "Project", "   ", 0.8).is_err());
        assert!(GraphNode::new("", "owner", "Project", "Aster", 0.8).is_err());
        assert!(GraphEdge::new("tenant", "owner", "a", "UNAPPROVED", "b", 0.8, "source").is_err());
        assert!(GraphEdge::new("tenant", "owner", "a", "USES", "b", 0.8, "").is_err());
    }

    #[test]
    fn compatible_graph_facts_do_not_contradict() {
        let record = MemoryRecord::new(
            "source",
            "tenant",
            "owner",
            "Aster owns and uses Redis.",
            crate::memory::ContentType::Fact,
            crate::memory::MemoryType::Relational,
            crate::memory::SourceType::Manual,
        );
        let uses = GraphHint {
            from_label: "Project".into(),
            from_name: "Aster".into(),
            relationship_type: "USES".into(),
            to_label: "Technology".into(),
            to_name: "Redis".into(),
            confidence: 0.9,
        };
        let mut owns = uses.clone();
        owns.relationship_type = "OWNS".into();
        assert!(graphify_record(&record, &[uses, owns])
            .unwrap()
            .contradictions
            .is_empty());
    }

    #[test]
    fn explicit_negation_conflicts_only_for_the_same_scoped_proposition() {
        let record = MemoryRecord::new(
            "source",
            "tenant",
            "owner",
            "Host-supplied evidence",
            crate::memory::ContentType::Fact,
            crate::memory::MemoryType::Relational,
            crate::memory::SourceType::Manual,
        );
        let positive = GraphHint {
            from_label: "Project".into(),
            from_name: "Aster".into(),
            relationship_type: "USES".into(),
            to_label: "Technology".into(),
            to_name: "Redis".into(),
            confidence: 0.9,
        };
        let mut negative = positive.clone();
        negative.relationship_type = "NOT_USES".into();
        let mut other_target = negative.clone();
        other_target.to_name = "PostgreSQL".into();
        let output = graphify_record(&record, &[positive, negative, other_target]).unwrap();
        assert_eq!(output.contradictions.len(), 1);
    }
}

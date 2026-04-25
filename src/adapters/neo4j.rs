use crate::{
    adapters::AdapterHealth,
    contracts::{CoreError, CoreResult},
    graph::{GraphEdge, GraphNode},
    ports::{Neo4jPort, TenantUserScope},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Neo4jAdapter {
    pub url: String,
}

impl Neo4jAdapter {
    pub fn new(url: impl Into<String>) -> CoreResult<Self> {
        let url = url.into();
        if url.trim().is_empty() {
            return Err(CoreError::InvalidInput("neo4j_url is required".to_string()));
        }
        Ok(Self { url })
    }

    pub fn schema_cypher(&self) -> &'static str {
        include_str!("../../migrations/neo4j/0001_graph_schema.cypher")
    }

    pub fn health(&self) -> AdapterHealth {
        AdapterHealth::configured("neo4j")
    }
}

impl Neo4jPort for Neo4jAdapter {
    fn merge_node(&self, _node: &GraphNode) -> CoreResult<()> {
        Err(CoreError::InvalidInput(
            "neo4j driver execution is not enabled in this build".to_string(),
        ))
    }

    fn merge_edge(&self, _edge: &GraphEdge) -> CoreResult<()> {
        Err(CoreError::InvalidInput(
            "neo4j driver execution is not enabled in this build".to_string(),
        ))
    }

    fn related_memory_ids(
        &self,
        _scope: &TenantUserScope,
        _query_entities: &[String],
        _max_hops: u8,
    ) -> CoreResult<Vec<String>> {
        Err(CoreError::InvalidInput(
            "neo4j driver execution is not enabled in this build".to_string(),
        ))
    }

    fn redact_memory_edges(&self, _scope: &TenantUserScope, _memory_id: &str) -> CoreResult<()> {
        Err(CoreError::InvalidInput(
            "neo4j driver execution is not enabled in this build".to_string(),
        ))
    }
}

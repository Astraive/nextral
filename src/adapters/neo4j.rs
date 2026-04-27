use crate::{
    adapters::{
        transport::{maybe_add_bearer_auth, validate_transport_url, TransportHardeningProfile},
        AdapterHealth,
    },
    contracts::{CoreError, CoreResult},
    graph::{GraphEdge, GraphNode},
    ports::{Neo4jPort, TenantUserScope},
};
use reqwest::blocking::Client;
use serde_json::{json, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Neo4jAdapter {
    pub url: String,
    pub hardening: TransportHardeningProfile,
}

impl Neo4jAdapter {
    pub fn new(url: impl Into<String>) -> CoreResult<Self> {
        let url = url.into();
        if url.trim().is_empty() {
            return Err(CoreError::InvalidInput("neo4j_url is required".to_string()));
        }
        let hardening = TransportHardeningProfile::baseline(Some("NEXTRAL_NEO4J_API_KEY"));
        validate_transport_url(
            &url.replace("neo4j://", "http://").replace("bolt://", "http://"),
            hardening.require_tls,
        )
        .map_err(CoreError::InvalidInput)?;
        Ok(Self { url, hardening })
    }

    pub fn schema_cypher(&self) -> &'static str {
        include_str!("../../migrations/neo4j/0001_graph_schema.cypher")
    }

    pub fn health(&self) -> AdapterHealth {
        AdapterHealth::configured("neo4j")
    }

    pub fn readiness(&self) -> CoreResult<Value> {
        self.cypher("RETURN 1 as ok", json!({}))
    }

    fn http_url(&self) -> String {
        self.url
            .replace("neo4j://", "http://")
            .replace("bolt://", "http://")
    }

    fn cypher(&self, statement: &str, params: Value) -> CoreResult<Value> {
        let payload = json!({
            "statements": [{
                "statement": statement,
                "parameters": params
            }]
        });
        let response = maybe_add_bearer_auth(
            Client::new().post(format!(
                "{}/db/neo4j/tx/commit",
                self.http_url().trim_end_matches('/')
            )),
            self.hardening.token_env.as_deref(),
        )
        .json(&payload)
            .send()
            .map_err(|error| CoreError::Io(error.to_string()))?;
        if !response.status().is_success() {
            return Err(CoreError::Io(format!(
                "neo4j request failed: {}",
                response.status()
            )));
        }
        response
            .json::<Value>()
            .map_err(|error| CoreError::Serialization(error.to_string()))
    }
}

impl Neo4jPort for Neo4jAdapter {
    fn merge_node(&self, node: &GraphNode) -> CoreResult<()> {
        let statement = r#"
            MERGE (n:NextralEntity {tenant_id:$tenant_id, user_id:$user_id, label:$label, canonical_name:$canonical_name})
            ON CREATE SET n.name=$name, n.key=$key, n.confidence=$confidence, n.created_at=$created_at
            ON MATCH SET n.name=$name, n.key=$key, n.confidence=CASE WHEN n.confidence > $confidence THEN n.confidence ELSE $confidence END
            RETURN n.key as key
        "#;
        self.cypher(
            statement,
            json!({
                "tenant_id": "configured-by-user",
                "user_id": node.user_id,
                "label": node.label,
                "canonical_name": node.canonical_name,
                "name": node.name,
                "key": node.key,
                "confidence": node.confidence,
                "created_at": node.created_at,
            }),
        )?;
        Ok(())
    }

    fn merge_edge(&self, edge: &GraphEdge) -> CoreResult<()> {
        let statement = r#"
            MATCH (a:NextralEntity {user_id:$user_id, key:$from_key})
            MATCH (b:NextralEntity {user_id:$user_id, key:$to_key})
            MERGE (a)-[r:NEXTRAL_RELATES_TO {tenant_id:$tenant_id, user_id:$user_id, relationship_type:$relationship_type, from_key:$from_key, to_key:$to_key}]->(b)
            ON CREATE SET r.confidence=$confidence, r.source_memory_ids=$source_memory_ids, r.created_at=$created_at, r.last_confirmed_at=$last_confirmed_at
            ON MATCH SET r.confidence=CASE WHEN r.confidence > $confidence THEN r.confidence ELSE $confidence END,
                         r.last_confirmed_at=$last_confirmed_at,
                         r.source_memory_ids=$source_memory_ids
            RETURN r.relationship_type as relationship_type
        "#;
        self.cypher(
            statement,
            json!({
                "tenant_id": "configured-by-user",
                "user_id": edge.user_id,
                "from_key": edge.from_key,
                "to_key": edge.to_key,
                "relationship_type": edge.relationship_type,
                "confidence": edge.confidence,
                "source_memory_ids": edge.source_memory_ids,
                "created_at": edge.created_at,
                "last_confirmed_at": edge.last_confirmed_at,
            }),
        )?;
        Ok(())
    }

    fn related_memory_ids(
        &self,
        scope: &TenantUserScope,
        query_entities: &[String],
        max_hops: u8,
    ) -> CoreResult<Vec<String>> {
        if query_entities.is_empty() {
            return Ok(Vec::new());
        }
        let statement = format!(
            r#"
            MATCH (n:NextralEntity {{user_id:$user_id}})
            WHERE any(term in $query_entities WHERE toLower(n.name) CONTAINS toLower(term))
            MATCH p=(n)-[r:NEXTRAL_RELATES_TO*1..{}]-()
            UNWIND relationships(p) as rel
            UNWIND rel.source_memory_ids as memory_id
            RETURN DISTINCT memory_id
            LIMIT 64
        "#,
            max_hops.max(1)
        );
        let body = self.cypher(
            &statement,
            json!({
                "user_id": scope.user_id,
                "query_entities": query_entities,
            }),
        )?;
        let rows = body["results"][0]["data"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let ids = rows
            .into_iter()
            .filter_map(|row| row["row"][0].as_str().map(|value| value.to_string()))
            .collect();
        Ok(ids)
    }

    fn redact_memory_edges(&self, scope: &TenantUserScope, memory_id: &str) -> CoreResult<()> {
        let statement = r#"
            MATCH ()-[r:NEXTRAL_RELATES_TO {user_id:$user_id}]-()
            WHERE any(id in r.source_memory_ids WHERE id = $memory_id)
            SET r.source_memory_ids = [id IN r.source_memory_ids WHERE id <> $memory_id]
            WITH r
            WHERE size(r.source_memory_ids) = 0
            DELETE r
        "#;
        self.cypher(
            statement,
            json!({
                "user_id": scope.user_id,
                "memory_id": memory_id
            }),
        )?;
        Ok(())
    }
}

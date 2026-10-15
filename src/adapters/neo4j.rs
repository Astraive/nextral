use crate::{
    adapters::{
        transport::{
            env_secret, hardened_client, store_failure, validate_transport_url,
            TransportHardeningProfile,
        },
        AdapterHealth,
    },
    contracts::{CoreError, CoreResult},
    graph::{GraphEdge, GraphNode},
    ports::{Neo4jPort, TenantUserScope},
    runtime::observability::StoreBackend,
};
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
        let hardening = TransportHardeningProfile::baseline(None);
        let is_tls = url.starts_with("neo4j+s://") || url.starts_with("bolt+s://");
        let http_url = url
            .replace("neo4j+s://", "https://")
            .replace("bolt+s://", "https://")
            .replace("neo4j://", "http://")
            .replace("bolt://", "http://");
        validate_transport_url(&http_url, hardening.require_tls || is_tls)
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
        let body = self.cypher("RETURN 1 as ok", json!({}))?;
        if response_rows(&body)?.first().map(|row| &row["row"][0]) != Some(&json!(1)) {
            return Err(CoreError::Io(
                "Neo4j readiness query returned no result".into(),
            ));
        }
        Ok(body)
    }
    /// Atomically replace one source contribution while retaining other sources.
    pub fn replace_memory_graph(
        &self,
        scope: &TenantUserScope,
        memory_id: &str,
        nodes: &[GraphNode],
        edges: &[GraphEdge],
    ) -> CoreResult<()> {
        crate::graph::validate_source_graph(
            &scope.tenant_id,
            &scope.user_id,
            memory_id,
            nodes,
            edges,
        )?;
        let body = self.cypher(r#"
            CALL {
                MATCH (:NextralEntity {tenant_id:$tenant_id,user_id:$user_id})-[r:NEXTRAL_RELATES_TO {tenant_id:$tenant_id,user_id:$user_id}]->(:NextralEntity {tenant_id:$tenant_id,user_id:$user_id})
                WHERE $memory_id IN r.source_memory_ids
                WITH r, [i IN range(0,size(r.source_memory_ids)-1) WHERE r.source_memory_ids[i] <> $memory_id] AS keep
                SET r.source_confidences=[i IN keep | coalesce(r.source_confidences[i],r.confidence)],
                    r.source_confirmations=[i IN keep | coalesce(r.source_confirmations[i],r.last_confirmed_at)],
                    r.source_memory_ids=[i IN keep | r.source_memory_ids[i]]
                SET r.confidence=reduce(best=0.0, score IN r.source_confidences | CASE WHEN score>best THEN score ELSE best END),
                    r.last_confirmed_at=reduce(latest='', stamp IN r.source_confirmations | CASE WHEN stamp>latest THEN stamp ELSE latest END)
                FOREACH (_ IN CASE WHEN size(r.source_memory_ids)=0 THEN [1] ELSE [] END | DELETE r)
                RETURN count(*) AS removed
            }
            CALL {
                UNWIND $nodes AS node
                MERGE (n:NextralEntity {tenant_id:$tenant_id,user_id:$user_id,label:node.label,canonical_name:node.canonical_name})
                ON CREATE SET n.created_at=node.created_at
                SET n.name=node.name,n.key=node.key,n.confidence=node.confidence
                RETURN count(*) AS node_count
            }
            CALL {
                UNWIND $edges AS edge
                MATCH (a:NextralEntity {tenant_id:$tenant_id,user_id:$user_id,key:edge.from_key})
                MATCH (b:NextralEntity {tenant_id:$tenant_id,user_id:$user_id,key:edge.to_key})
                MERGE (a)-[r:NEXTRAL_RELATES_TO {tenant_id:$tenant_id,user_id:$user_id,relationship_type:edge.relationship_type,from_key:edge.from_key,to_key:edge.to_key}]->(b)
                ON CREATE SET r.created_at=edge.created_at,r.source_memory_ids=[],r.source_confidences=[],r.source_confirmations=[]
                WITH r,edge,coalesce(r.source_memory_ids,[]) AS ids
                SET r.source_confidences=[i IN range(0,size(ids)-1) | coalesce(r.source_confidences[i],r.confidence)] + [edge.confidence],
                    r.source_confirmations=[i IN range(0,size(ids)-1) | coalesce(r.source_confirmations[i],r.last_confirmed_at)] + [edge.last_confirmed_at],
                    r.source_memory_ids=ids + [$memory_id]
                SET r.confidence=reduce(best=0.0, score IN r.source_confidences | CASE WHEN score>best THEN score ELSE best END),
                    r.last_confirmed_at=reduce(latest='', stamp IN r.source_confirmations | CASE WHEN stamp>latest THEN stamp ELSE latest END)
                RETURN count(*) AS edge_count
            }
            RETURN node_count,edge_count
        "#, json!({"tenant_id":scope.tenant_id,"user_id":scope.user_id,"memory_id":memory_id,"nodes":nodes,"edges":edges}))?;
        let rows = response_rows(&body)?;
        if rows.first().map(|row| &row["row"][1]) != Some(&json!(edges.len())) {
            return Err(CoreError::Io(
                "graph replacement acknowledgement missing".into(),
            ));
        }
        Ok(())
    }

    fn http_url(&self) -> CoreResult<reqwest::Url> {
        let converted = self
            .url
            .replacen("neo4j+s://", "https://", 1)
            .replacen("bolt+s://", "https://", 1)
            .replacen("neo4j://", "http://", 1)
            .replacen("bolt://", "http://", 1);
        let mut url =
            reqwest::Url::parse(&converted).map_err(|e| CoreError::InvalidInput(e.to_string()))?;
        if url.port() == Some(7687) {
            let port = if url.scheme() == "https" { 7473 } else { 7474 };
            url.set_port(Some(port))
                .map_err(|_| CoreError::InvalidInput("invalid Neo4j HTTP port".into()))?;
        }
        // Credentials belong in environment variables, never in logged endpoints.
        validate_transport_url(url.as_str(), self.hardening.require_tls)
            .map_err(CoreError::InvalidInput)?;
        Ok(url)
    }

    fn cypher(&self, statement: &str, params: Value) -> CoreResult<Value> {
        let url = self.http_url()?;
        let username = std::env::var("NEXTRAL_NEO4J_USER").unwrap_or_else(|_| "neo4j".into());
        let password = env_secret("NEXTRAL_NEO4J_PASSWORD")?;
        if username.is_empty() {
            return Err(CoreError::InvalidInput("Neo4j username is empty".into()));
        }
        let payload = json!({"statements":[{"statement":statement,"parameters":params}]});
        let response = hardened_client(&self.hardening)?
            .post(format!(
                "{}/db/neo4j/tx/commit",
                url.as_str().trim_end_matches('/')
            ))
            .basic_auth(username, Some(password))
            .json(&payload)
            .send()
            .map_err(|e| {
                store_failure(
                    StoreBackend::Neo4j,
                    CoreError::Io(format!("Neo4j request failed: {e}")),
                )
            })?;
        if !response.status().is_success() {
            return Err(store_failure(
                StoreBackend::Neo4j,
                CoreError::Io(format!("Neo4j request failed: {}", response.status())),
            ));
        }
        let body = response.json::<Value>().map_err(|e| {
            store_failure(StoreBackend::Neo4j, CoreError::Serialization(e.to_string()))
        })?;
        validate_response(body).map_err(|error| store_failure(StoreBackend::Neo4j, error))
    }
}

impl Neo4jPort for Neo4jAdapter {
    fn merge_node(&self, node: &GraphNode) -> CoreResult<()> {
        crate::graph::validate_node(node)?;
        let statement = r#"
            MERGE (n:NextralEntity {tenant_id:$tenant_id, user_id:$user_id, label:$label, canonical_name:$canonical_name})
            ON CREATE SET n.name=$name, n.key=$key, n.confidence=$confidence, n.created_at=$created_at
            ON MATCH SET n.name=$name, n.key=$key, n.confidence=CASE WHEN n.confidence > $confidence THEN n.confidence ELSE $confidence END
            RETURN n.key as key
        "#;
        self.cypher(
            statement,
            json!({
                "tenant_id": node.tenant_id,
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
        crate::graph::validate_edge(edge)?;
        let statement = r#"
            MATCH (a:NextralEntity {tenant_id:$tenant_id, user_id:$user_id, key:$from_key})
            MATCH (b:NextralEntity {tenant_id:$tenant_id, user_id:$user_id, key:$to_key})
            MERGE (a)-[r:NEXTRAL_RELATES_TO {tenant_id:$tenant_id, user_id:$user_id, relationship_type:$relationship_type, from_key:$from_key, to_key:$to_key}]->(b)
            ON CREATE SET r.source_memory_ids=[],r.source_confidences=[],r.source_confirmations=[],r.created_at=$created_at
            WITH r,coalesce(r.source_memory_ids,[]) AS ids
            WITH r,ids,reduce(added=[],id IN $source_memory_ids | CASE WHEN id IN ids OR id IN added THEN added ELSE added+[id] END) AS added
            SET r.source_confidences=[i IN range(0,size(ids)-1) | CASE WHEN ids[i] IN $source_memory_ids THEN $confidence ELSE coalesce(r.source_confidences[i],r.confidence) END] + [id IN added | $confidence],
                r.source_confirmations=[i IN range(0,size(ids)-1) | CASE WHEN ids[i] IN $source_memory_ids THEN $last_confirmed_at ELSE coalesce(r.source_confirmations[i],r.last_confirmed_at) END] + [id IN added | $last_confirmed_at],
                r.source_memory_ids=ids+added
            SET r.confidence=reduce(best=0.0,score IN r.source_confidences | CASE WHEN score>best THEN score ELSE best END),
                r.last_confirmed_at=reduce(latest='',stamp IN r.source_confirmations | CASE WHEN stamp>latest THEN stamp ELSE latest END)
            RETURN r.relationship_type as relationship_type
        "#;
        self.cypher(
            statement,
            json!({
                "tenant_id": edge.tenant_id,
                "user_id": edge.user_id,
                "from_key": edge.from_key,
                "to_key": edge.to_key,
                "relationship_type": edge.relationship_type,
                "confidence": edge.confidence,
                "source_memory_ids": edge.source_memory_ids,
                "created_at": edge.created_at,
                "last_confirmed_at": edge.last_confirmed_at,
            }),
        )
        .and_then(|body| {
            let rows = response_rows(&body)?;
            if rows.is_empty() {
                return Err(CoreError::NotFound(
                    "graph edge endpoints do not exist in scope".into(),
                ));
            }
            Ok(())
        })
    }

    fn related_memory_ids(
        &self,
        scope: &TenantUserScope,
        query_entities: &[String],
        max_hops: u8,
    ) -> CoreResult<Vec<String>> {
        validate_scope(scope)?;
        if query_entities.is_empty() || max_hops == 0 {
            return Ok(Vec::new());
        }
        if max_hops > 10 {
            return Err(CoreError::InvalidInput(
                "graph traversal exceeds 10 hops".into(),
            ));
        }
        let statement = format!(
            r#"
            MATCH (n:NextralEntity {{tenant_id:$tenant_id, user_id:$user_id}})
            WHERE any(term in $query_entities WHERE toLower(n.name) CONTAINS toLower(term))
            MATCH p=(n)-[:NEXTRAL_RELATES_TO*1..{}]-()
            WHERE all(node IN nodes(p) WHERE node.tenant_id=$tenant_id AND node.user_id=$user_id)
              AND all(rel IN relationships(p) WHERE rel.tenant_id=$tenant_id AND rel.user_id=$user_id)
            UNWIND relationships(p) as rel
            UNWIND rel.source_memory_ids as memory_id
            RETURN DISTINCT memory_id
            LIMIT 64
        "#,
            max_hops
        );
        let body = self.cypher(
            &statement,
            json!({
                "tenant_id": scope.tenant_id,
                "user_id": scope.user_id,
                "query_entities": query_entities,
            }),
        )?;
        response_rows(&body)?
            .iter()
            .map(|row| {
                row["row"][0]
                    .as_str()
                    .map(str::to_string)
                    .ok_or_else(|| CoreError::Serialization("Neo4j memory ID missing".into()))
            })
            .collect()
    }

    fn redact_memory_edges(&self, scope: &TenantUserScope, memory_id: &str) -> CoreResult<()> {
        validate_scope(scope)?;
        if memory_id.is_empty() {
            return Err(CoreError::InvalidInput("memory ID required".into()));
        }
        let statement = r#"
            MATCH (a:NextralEntity {tenant_id:$tenant_id,user_id:$user_id})-[r:NEXTRAL_RELATES_TO {tenant_id:$tenant_id, user_id:$user_id}]->(b:NextralEntity {tenant_id:$tenant_id,user_id:$user_id})
            WHERE any(id in r.source_memory_ids WHERE id = $memory_id)
            WITH r,[i IN range(0,size(r.source_memory_ids)-1) WHERE r.source_memory_ids[i] <> $memory_id] AS keep
            SET r.source_confidences=[i IN keep | coalesce(r.source_confidences[i],r.confidence)],
                r.source_confirmations=[i IN keep | coalesce(r.source_confirmations[i],r.last_confirmed_at)],
                r.source_memory_ids=[i IN keep | r.source_memory_ids[i]]
            SET r.confidence=reduce(best=0.0,score IN r.source_confidences | CASE WHEN score>best THEN score ELSE best END),
                r.last_confirmed_at=reduce(latest='',stamp IN r.source_confirmations | CASE WHEN stamp>latest THEN stamp ELSE latest END)
            FOREACH (_ IN CASE WHEN size(r.source_memory_ids)=0 THEN [1] ELSE [] END | DELETE r)
        "#;
        self.cypher(
            statement,
            json!({
                "tenant_id": scope.tenant_id,
                "user_id": scope.user_id,
                "memory_id": memory_id
            }),
        )?;
        Ok(())
    }
}

fn validate_scope(scope: &TenantUserScope) -> CoreResult<()> {
    validate_identity(&scope.tenant_id, &scope.user_id)
}
fn validate_identity(tenant: &str, user: &str) -> CoreResult<()> {
    if tenant.is_empty() || user.is_empty() {
        return Err(CoreError::InvalidInput(
            "graph tenant and user required".into(),
        ));
    }
    Ok(())
}
fn validate_response(body: Value) -> CoreResult<Value> {
    let errors = body["errors"]
        .as_array()
        .ok_or_else(|| CoreError::Serialization("Neo4j errors field missing".into()))?;
    if !errors.is_empty() {
        return Err(CoreError::Io(format!(
            "Neo4j transaction failed: {errors:?}"
        )));
    }
    if !body["results"].is_array() {
        return Err(CoreError::Serialization("Neo4j results missing".into()));
    }
    Ok(body)
}
fn response_rows(body: &Value) -> CoreResult<&Vec<Value>> {
    body["results"][0]["data"]
        .as_array()
        .ok_or_else(|| CoreError::Serialization("Neo4j data missing".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn successful_http_with_cypher_error_is_not_success() {
        assert!(validate_response(json!({"results":[],"errors":[{"code":"Neo.ClientError.Security.Unauthorized","message":"denied"}]})).is_err());
        assert!(validate_response(json!({"results":[{"data":[]}],"errors":[]})).is_ok());
    }
    #[test]
    fn bolt_endpoint_maps_to_authenticated_http_port() {
        let adapter = Neo4jAdapter::new("neo4j://localhost:7687").unwrap();
        assert_eq!(
            adapter.http_url().unwrap().as_str(),
            "http://localhost:7474/"
        );
        let mut adapter = adapter;
        adapter.hardening.require_tls = true;
        assert!(adapter.http_url().is_err());
    }
    #[test]
    fn source_replacement_rejects_cross_scope_and_wrong_provenance_before_io() {
        let adapter = Neo4jAdapter::new("http://127.0.0.1:1").unwrap();
        let scope = TenantUserScope {
            tenant_id: "tenant".into(),
            user_id: "owner".into(),
        };
        let node = GraphNode::new("tenant", "peer", "Entity", "Aster", 0.8).unwrap();
        assert!(adapter
            .replace_memory_graph(&scope, "source", &[node], &[])
            .is_err());
        let node = GraphNode::new("tenant", "owner", "Entity", "Aster", 0.8).unwrap();
        let edge = GraphEdge::new(
            "tenant",
            "owner",
            &node.key,
            "USES",
            &node.key,
            0.8,
            "another-source",
        )
        .unwrap();
        assert!(adapter
            .replace_memory_graph(&scope, "source", &[node], &[edge])
            .is_err());
    }
    #[test]
    #[ignore = "requires Neo4j with NEXTRAL_NEO4J_URL, NEXTRAL_NEO4J_USER and NEXTRAL_NEO4J_PASSWORD"]
    fn real_graph_unions_evidence_and_redacts_only_its_tenant() {
        let adapter = Neo4jAdapter::new(std::env::var("NEXTRAL_NEO4J_URL").unwrap()).unwrap();
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let tenant_a = format!("adapter-test-a-{unique}");
        let tenant_b = format!("adapter-test-b-{unique}");
        let scope_a = TenantUserScope {
            tenant_id: tenant_a.clone(),
            user_id: "user".into(),
        };
        let scope_b = TenantUserScope {
            tenant_id: tenant_b.clone(),
            user_id: "user".into(),
        };
        for tenant in [&tenant_a, &tenant_b] {
            let a = GraphNode::new(tenant, "user", "Entity", "AdapterOrigin", 0.8).unwrap();
            let b = GraphNode::new(tenant, "user", "Entity", "AdapterTarget", 0.8).unwrap();
            adapter.merge_node(&a).unwrap();
            adapter.merge_node(&b).unwrap();
            adapter
                .merge_edge(
                    &GraphEdge::new(
                        tenant,
                        "user",
                        &a.key,
                        "RELATED_TO",
                        &b.key,
                        0.8,
                        "memory-1",
                    )
                    .unwrap(),
                )
                .unwrap();
            if tenant == &tenant_a {
                adapter
                    .merge_edge(
                        &GraphEdge::new(
                            tenant,
                            "user",
                            &a.key,
                            "RELATED_TO",
                            &b.key,
                            0.9,
                            "memory-2",
                        )
                        .unwrap(),
                    )
                    .unwrap();
            }
        }
        let terms = vec!["AdapterOrigin".into()];
        let before = adapter.related_memory_ids(&scope_a, &terms, 2).unwrap();
        assert!(before.contains(&"memory-1".into()) && before.contains(&"memory-2".into()));
        adapter
            .replace_memory_graph(&scope_a, "memory-1", &[], &[])
            .unwrap();
        adapter
            .replace_memory_graph(&scope_a, "memory-1", &[], &[])
            .unwrap();
        assert_eq!(
            adapter.related_memory_ids(&scope_a, &terms, 2).unwrap(),
            vec!["memory-2"]
        );
        assert_eq!(
            adapter.related_memory_ids(&scope_b, &terms, 2).unwrap(),
            vec!["memory-1"]
        );
        adapter.redact_memory_edges(&scope_a, "memory-2").unwrap();
        adapter.redact_memory_edges(&scope_b, "memory-1").unwrap();
    }
}

#[cfg(test)]
mod replacement_tests {
    use super::*;

    #[test]
    #[ignore = "requires Neo4j with NEXTRAL_NEO4J_URL, NEXTRAL_NEO4J_USER and NEXTRAL_NEO4J_PASSWORD"]
    fn real_source_replacement_preserves_peer_confidence_and_confirmation() {
        let adapter = Neo4jAdapter::new(std::env::var("NEXTRAL_NEO4J_URL").unwrap()).unwrap();
        let tenant = format!(
            "graph-replacement-{}",
            crate::runtime::lifecycle::worker_owner()
        );
        let scope = TenantUserScope {
            tenant_id: tenant.clone(),
            user_id: "owner".into(),
        };
        let origin = GraphNode::new(&tenant, "owner", "Project", "Aster", 0.95).unwrap();
        let old_target =
            GraphNode::new(&tenant, "owner", "Technology", "PostgreSQL", 0.95).unwrap();
        let new_target = GraphNode::new(&tenant, "owner", "Technology", "Redis", 0.2).unwrap();
        let mut peer = GraphEdge::new(
            &tenant,
            "owner",
            &origin.key,
            "USES",
            &old_target.key,
            0.4,
            "peer",
        )
        .unwrap();
        peer.last_confirmed_at = "1700000000".into();
        adapter
            .replace_memory_graph(
                &scope,
                "peer",
                &[origin.clone(), old_target.clone()],
                &[peer],
            )
            .unwrap();
        let source = GraphEdge::new(
            &tenant,
            "owner",
            &origin.key,
            "USES",
            &old_target.key,
            0.95,
            "source",
        )
        .unwrap();
        adapter
            .replace_memory_graph(
                &scope,
                "source",
                &[origin.clone(), old_target.clone()],
                &[source],
            )
            .unwrap();
        let changed = GraphEdge::new(
            &tenant,
            "owner",
            &origin.key,
            "USES",
            &new_target.key,
            0.2,
            "source",
        )
        .unwrap();
        adapter
            .replace_memory_graph(&scope, "source", &[origin, new_target], &[changed])
            .unwrap();
        let body = adapter.cypher(
            "MATCH ()-[r:NEXTRAL_RELATES_TO {tenant_id:$tenant_id,user_id:$user_id,to_key:$target}]->() RETURN r.source_memory_ids,r.confidence,r.last_confirmed_at,r.source_confirmations",
            json!({"tenant_id":tenant,"user_id":"owner","target":old_target.key}),
        ).unwrap();
        let rows = response_rows(&body).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["row"][0], json!(["peer"]));
        assert!((rows[0]["row"][1].as_f64().unwrap() - 0.4).abs() < 0.000001);
        assert_eq!(rows[0]["row"][2], json!("1700000000"));
        assert_eq!(rows[0]["row"][3], json!(["1700000000"]));
        adapter
            .replace_memory_graph(&scope, "source", &[], &[])
            .unwrap();
        assert_eq!(
            adapter
                .related_memory_ids(&scope, &["Aster".into()], 2)
                .unwrap(),
            vec!["peer"]
        );
        adapter.redact_memory_edges(&scope, "peer").unwrap();
    }
}

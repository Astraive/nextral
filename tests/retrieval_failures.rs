use nextral::{
    config::IngestionPolicy,
    domain::{AuditEvent, GraphEdge, GraphNode},
    graph::GraphHint,
    ingestion::{ingest_memory, IngestMemoryRequest, IngestStatus},
    memory::{ContentType, MemoryRecord, MemoryType, PrivacyLevel, SourceType},
    retrieval::{retrieve, RetrievalRequest, RetrievalResponse, RetrievalStatus, SourcePath},
    store::{GraphStore, MemoryIndexStore, TestMemoryStore},
    CoreError, CoreResult,
};
use std::{
    sync::{mpsc, Mutex},
    time::Duration,
};

const TENANT: &str = "retrieval-failure-tenant";
const USER: &str = "atlas-owner";
const MEMORY_ID: &str = "atlas-storage-decision";
const CONTENT: &str = "Project Atlas uses PostgreSQL for durable customer records.";
const VECTOR_FAILURE: &str = "vector_path_failed";
const GRAPH_FAILURE: &str = "graph_path_failed";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Path {
    Vector,
    Graph,
}

struct EntryGate {
    entered: mpsc::Sender<Path>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl EntryGate {
    fn enter(&self, path: Path) -> CoreResult<()> {
        self.entered
            .send(path)
            .map_err(|error| CoreError::Io(format!("entry observer disconnected: {error}")))?;
        self.release
            .lock()
            .expect("release receiver lock")
            .recv_timeout(Duration::from_secs(15))
            .map_err(|error| CoreError::Io(format!("entry gate was not released: {error}")))
    }
}

struct FaultStore {
    inner: TestMemoryStore,
    fail_vector: bool,
    fail_graph: bool,
    vector_gate: Option<EntryGate>,
    graph_gate: Option<EntryGate>,
}

impl FaultStore {
    fn new(fail_vector: bool, fail_graph: bool) -> Self {
        let mut inner = TestMemoryStore::new();
        let mut request = IngestMemoryRequest::new(
            TENANT,
            USER,
            CONTENT,
            ContentType::Fact,
            MemoryType::Semantic,
            SourceType::Manual,
            IngestionPolicy {
                min_importance_score: 0.2,
                min_confidence_score: 0.2,
            },
        );
        request.id = Some(MEMORY_ID.into());
        request.importance_score = 0.9;
        request.confidence_score = Some(0.95);
        request.entities = vec!["Project Atlas".into(), "PostgreSQL".into()];
        request.graph_hints = vec![GraphHint {
            from_label: "Project".into(),
            from_name: "Project Atlas".into(),
            relationship_type: "USES".into(),
            to_label: "Technology".into(),
            to_name: "PostgreSQL".into(),
            confidence: 0.95,
        }];
        let ingested = ingest_memory(&mut inner, request).expect("ingest canonical fixture");
        assert_eq!(ingested.status, IngestStatus::Accepted);
        assert_eq!(ingested.record_id.as_deref(), Some(MEMORY_ID));
        Self {
            inner,
            fail_vector,
            fail_graph,
            vector_gate: None,
            graph_gate: None,
        }
    }

    fn access_count(&self) -> u64 {
        self.inner
            .get_memory(TENANT, USER, MEMORY_ID)
            .expect("load canonical fixture")
            .expect("canonical fixture exists")
            .access_count
    }
}

// Only candidate-path faults and entry gates differ from the existing test store.
impl MemoryIndexStore for FaultStore {
    fn prepare_retrieval(&self, request: &mut RetrievalRequest) -> CoreResult<()> {
        self.inner.prepare_retrieval(request)
    }

    fn prepare_embedding(
        &self,
        record: &mut MemoryRecord,
        embedding: Option<&[f32]>,
    ) -> CoreResult<()> {
        self.inner.prepare_embedding(record, embedding)
    }

    fn ingestion_policy(&self, requested: &IngestionPolicy) -> IngestionPolicy {
        self.inner.ingestion_policy(requested)
    }

    fn vector_candidates(
        &self,
        tenant_id: &str,
        user_id: &str,
        query: &str,
        query_vector: Option<&[f32]>,
        top_k: usize,
        privacy_scope: &[PrivacyLevel],
        filters: Option<&nextral::memory::MemoryFilters>,
    ) -> CoreResult<Vec<(String, f32)>> {
        if let Some(gate) = &self.vector_gate {
            gate.enter(Path::Vector)?;
        }
        if self.fail_vector {
            return Err(CoreError::Io("injected vector outage".into()));
        }
        self.inner.vector_candidates(
            tenant_id,
            user_id,
            query,
            query_vector,
            top_k,
            privacy_scope,
            filters,
        )
    }

    fn commit_ingestion(
        &mut self,
        record: MemoryRecord,
        nodes: Vec<GraphNode>,
        edges: Vec<GraphEdge>,
        audit: AuditEvent,
    ) -> CoreResult<Vec<String>> {
        self.inner.commit_ingestion(record, nodes, edges, audit)
    }

    fn commit_graph_replay(
        &mut self,
        record: &MemoryRecord,
        nodes: &[GraphNode],
        edges: &[GraphEdge],
        audit: &AuditEvent,
    ) -> CoreResult<Vec<String>> {
        self.inner.commit_graph_replay(record, nodes, edges, audit)
    }

    fn upsert_memory(&mut self, record: MemoryRecord) -> CoreResult<()> {
        self.inner.upsert_memory(record)
    }

    fn get_memory(
        &self,
        tenant_id: &str,
        user_id: &str,
        id: &str,
    ) -> CoreResult<Option<MemoryRecord>> {
        self.inner.get_memory(tenant_id, user_id, id)
    }

    fn get_active_memories(
        &self,
        tenant_id: &str,
        user_id: &str,
        ids: &[&str],
    ) -> CoreResult<Vec<MemoryRecord>> {
        self.inner.get_active_memories(tenant_id, user_id, ids)
    }

    fn query_memories(
        &self,
        request: &nextral::memory::MemoryQuery,
    ) -> CoreResult<nextral::memory::MemoryPage> {
        self.inner.query_memories(request)
    }

    fn list_memories(
        &self,
        tenant_id: &str,
        user_id: &str,
        privacy_scope: &[PrivacyLevel],
        include_inactive: bool,
    ) -> CoreResult<Vec<MemoryRecord>> {
        self.inner
            .list_memories(tenant_id, user_id, privacy_scope, include_inactive)
    }

    fn update_memory(&mut self, record: MemoryRecord) -> CoreResult<()> {
        self.inner.update_memory(record)
    }

    fn record_accesses(&mut self, tenant_id: &str, user_id: &str, ids: &[&str]) -> CoreResult<()> {
        self.inner.record_accesses(tenant_id, user_id, ids)
    }

    fn count_memories(
        &self,
        tenant_id: &str,
        user_id: &str,
        include_inactive: bool,
    ) -> CoreResult<usize> {
        self.inner
            .count_memories(tenant_id, user_id, include_inactive)
    }

    fn list_entities(&self, tenant_id: &str, user_id: &str) -> CoreResult<Vec<String>> {
        self.inner.list_entities(tenant_id, user_id)
    }

    fn list_tags(&self, tenant_id: &str, user_id: &str) -> CoreResult<Vec<String>> {
        self.inner.list_tags(tenant_id, user_id)
    }
}

impl GraphStore for FaultStore {
    fn merge_node(&mut self, node: GraphNode) -> CoreResult<()> {
        self.inner.merge_node(node)
    }

    fn merge_edge(&mut self, edge: GraphEdge) -> CoreResult<()> {
        self.inner.merge_edge(edge)
    }

    fn graph_memory_ids(
        &self,
        tenant_id: &str,
        user_id: &str,
        query: &str,
        max_hops: u8,
        privacy_scope: &[PrivacyLevel],
    ) -> CoreResult<Vec<String>> {
        if let Some(gate) = &self.graph_gate {
            gate.enter(Path::Graph)?;
        }
        if self.fail_graph {
            return Err(CoreError::Io("injected graph outage".into()));
        }
        self.inner
            .graph_memory_ids(tenant_id, user_id, query, max_hops, privacy_scope)
    }
}

fn request() -> RetrievalRequest {
    RetrievalRequest::test(TENANT, USER, "PostgreSQL")
}

fn assert_failed_paths(response: &RetrievalResponse, expected: &[&str]) {
    let paths = response.telemetry.degraded_reasons.iter().map(|reason| {
        reason
            .split_once(':')
            .map_or(reason.as_str(), |(path, _)| path)
    });
    assert!(
        paths.eq(expected.iter().copied()),
        "wrong failed paths: {:?}",
        response.telemetry.degraded_reasons
    );
}

fn assert_canonical_item(response: &RetrievalResponse, source: SourcePath) {
    let [item] = response.items.as_slice() else {
        panic!(
            "expected exactly the canonical memory, got {:?}",
            response.items
        );
    };
    assert_eq!(item.memory_id, MEMORY_ID);
    assert_eq!(item.content, CONTENT);
    assert_eq!(item.source_path, source);
    assert!(item.retrieval_score > 0.0);
    assert_eq!(response.telemetry.merged_candidates, 1);
    assert_eq!(response.telemetry.selected_candidates, 1);
    assert_eq!(response.telemetry.token_estimate, item.token_estimate);
}

fn assert_explicit_failure(response: &RetrievalResponse, reasons: &[&str]) {
    assert_eq!(response.status, RetrievalStatus::Error);
    assert_eq!(response.quality_score, 0.0);
    assert!(response.items.is_empty());
    assert_failed_paths(response, reasons);
    assert_eq!(
        (
            response.telemetry.vector_candidates,
            response.telemetry.graph_candidates,
            response.telemetry.merged_candidates,
            response.telemetry.selected_candidates,
            response.telemetry.token_estimate,
        ),
        (0, 0, 0, 0, 0)
    );
}

#[test]
fn vector_failure_preserves_canonical_graph_result_as_degraded() {
    let mut store = FaultStore::new(true, false);
    let response = retrieve(&mut store, request()).expect("graph path remains available");

    assert_canonical_item(&response, SourcePath::Graph);
    assert_eq!(response.status, RetrievalStatus::Degraded);
    assert_failed_paths(&response, &[VECTOR_FAILURE]);
    assert_eq!(response.telemetry.vector_candidates, 0);
    assert_eq!(response.telemetry.graph_candidates, 1);
    assert_eq!(store.access_count(), 1);
}

#[test]
fn graph_failure_preserves_canonical_vector_result_as_degraded() {
    let mut store = FaultStore::new(false, true);
    let response = retrieve(&mut store, request()).expect("vector path remains available");

    assert_canonical_item(&response, SourcePath::Vector);
    assert_eq!(response.status, RetrievalStatus::Degraded);
    assert_failed_paths(&response, &[GRAPH_FAILURE]);
    assert_eq!(response.telemetry.vector_candidates, 1);
    assert_eq!(response.telemetry.graph_candidates, 0);
    assert_eq!(store.access_count(), 1);
}

#[test]
fn both_path_failures_return_explicit_error_without_false_success() {
    let mut store = FaultStore::new(true, true);
    let response = retrieve(&mut store, request()).expect("structured retrieval failure");

    assert_explicit_failure(&response, &[VECTOR_FAILURE, GRAPH_FAILURE]);
    assert_eq!(store.access_count(), 0);
}

#[test]
fn vector_failure_with_graph_disabled_returns_explicit_error() {
    let mut store = FaultStore::new(true, false);
    let mut request = request();
    request.max_graph_hops = 0;
    let response = retrieve(&mut store, request).expect("structured retrieval failure");

    assert_explicit_failure(&response, &[VECTOR_FAILURE]);
    assert_eq!(store.access_count(), 0);
}

#[test]
fn vector_and_graph_enter_before_either_is_released() {
    let mut store = FaultStore::new(false, false);
    let (entered, entries) = mpsc::channel();
    let (release_vector, vector_release) = mpsc::channel();
    let (release_graph, graph_release) = mpsc::channel();
    store.vector_gate = Some(EntryGate {
        entered: entered.clone(),
        release: Mutex::new(vector_release),
    });
    store.graph_gate = Some(EntryGate {
        entered,
        release: Mutex::new(graph_release),
    });
    let worker = std::thread::spawn(move || {
        let response = retrieve(&mut store, request());
        (response, store)
    });

    // Both paths must enter while neither can finish; timeouts only bound deadlocks.
    let first = entries.recv_timeout(Duration::from_secs(5));
    let second = entries.recv_timeout(Duration::from_secs(5));
    let vector_released = release_vector.send(());
    let graph_released = release_graph.send(());
    let (response, store) = worker.join().expect("retrieval worker joins");
    let first = first.expect("first candidate path entered");
    let second = second.expect("second candidate path entered before either was released");
    assert_ne!(first, second, "both distinct candidate paths must enter");
    vector_released.expect("vector path was waiting for release");
    graph_released.expect("graph path was waiting for release");

    let response = response.expect("concurrent retrieval succeeds");
    assert_canonical_item(&response, SourcePath::Both);
    assert_eq!(response.status, RetrievalStatus::Ok);
    assert!(response.telemetry.degraded_reasons.is_empty());
    assert_eq!(response.telemetry.vector_candidates, 1);
    assert_eq!(response.telemetry.graph_candidates, 1);
    assert_eq!(response.telemetry.dedupe_ratio, 0.5);
    assert_eq!(store.access_count(), 1);
}

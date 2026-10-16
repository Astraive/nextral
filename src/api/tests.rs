use super::*;
use grpc::proto::{
    self, memory_service_client::MemoryServiceClient,
    reminder_service_client::ReminderServiceClient,
};

fn test_config() -> NextralConfig {
    serde_json::from_value(json!({
        "backend":"test_memory", "stores":null,
        "embedding":{"kind":"test","model":"test","dimension":3,"endpoint":null,"api_key_env":null},
        "extraction":{"kind":"test","model":"test","endpoint":null,"api_key_env":null}, "reranker":null,
        "ingestion_policy":{"min_importance_score":0.0,"min_confidence_score":0.0},
        "retrieval_policy":{"privacy_scope":["private"],"token_budget":1800,"top_k_vector":12,"max_graph_hops":2,"scoring_weights":{"semantic_similarity":0.5,"recency":0.2,"importance":0.2,"access":0.1}},
        "cache":{"key_prefix":"protocol-test","session_ttl_seconds":7200,"retrieval_ttl_seconds":120,"policy_ttl_seconds":300},
        "service":{"http_bind":"127.0.0.1:0","grpc_bind":"127.0.0.1:0","graphql_bind":"127.0.0.1:0"},
        "auth":{"issuer":null,"audience":null,"jwks_url":null,"service_token_env":null},
        "observability":{"enabled":false,"otlp_endpoint":null,"prometheus_bind":null}
    })).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_protocols_share_runtime_and_execute_every_declared_operation() {
    let config = test_config();
    let state = ServiceState::new(config).unwrap();
    let http_socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let grpc_socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let graphql_socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_url = format!("http://{}", http_socket.local_addr().unwrap());
    let grpc_url = format!("http://{}", grpc_socket.local_addr().unwrap());
    let graphql_url = format!("http://{}/graphql", graphql_socket.local_addr().unwrap());
    let (stop, _) = watch::channel(false);
    let h = tokio::spawn(http::serve(
        http_socket,
        http::router(state.clone()),
        stop.subscribe(),
    ));
    let g = tokio::spawn(grpc::serve(grpc_socket, state.clone(), stop.subscribe()));
    let q = tokio::spawn(http::serve(
        graphql_socket,
        graphql::router(state, graphql::build_schema().unwrap()),
        stop.subscribe(),
    ));
    let client = reqwest::Client::new();
    assert_eq!(
        client
            .get(format!("{http_url}/healthz"))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        client
            .get(format!("{http_url}/readyz"))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let ingest = json!({"tenant_id":"protocol-tenant","user_id":"protocol-user","content":"protocol client shared memory","content_type":"fact","memory_type":"semantic","privacy_level":"private","importance_score":0.8,"confidence_score":0.9});
    let accepted = client
        .post(format!("{http_url}/v1/memories"))
        .json(&ingest)
        .send()
        .await
        .unwrap();
    assert_eq!(accepted.status(), 202);
    let first: Value = accepted.json().await.unwrap();
    let first_id = first["memory_id"].as_str().unwrap();
    let mut memories = MemoryServiceClient::connect(grpc_url.clone())
        .await
        .unwrap();
    let second = memories
        .ingest(proto::IngestMemoryRequest {
            tenant_id: "protocol-tenant".into(),
            user_id: "protocol-user".into(),
            content: "another protocol client shared memory".into(),
            content_type: "fact".into(),
            memory_type: "semantic".into(),
            privacy_level: "private".into(),
            importance_score: Some(0.8),
            confidence_score: Some(0.9),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let retrieved = memories
        .retrieve(proto::RetrieveMemoryRequest {
            tenant_id: "protocol-tenant".into(),
            user_id: "protocol-user".into(),
            query_text: "protocol client".into(),
            token_budget: 1000,
            privacy_scope: vec!["private".into()],
            top_k_vector: 12,
            max_graph_hops: 2,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(retrieved.items.iter().any(|m| m.memory_id == first_id));
    let mut reminders = ReminderServiceClient::connect(grpc_url).await.unwrap();
    let reminder = reminders
        .schedule(proto::ScheduleReminderRequest {
            tenant_id: "protocol-tenant".into(),
            user_id: "protocol-user".into(),
            source_memory_id: first_id.into(),
            kind: "task".into(),
            title: "protocol task".into(),
            due_at: "2030-01-01T00:00:00Z".into(),
            timezone: "UTC".into(),
            priority: Some("high".into()),
        })
        .await
        .unwrap()
        .into_inner();
    let due = reminders
        .list_due(proto::ListDueRemindersRequest {
            tenant_id: "protocol-tenant".into(),
            user_id: "protocol-user".into(),
            due_at_or_before: "2031-01-01T00:00:00Z".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(due.reminder_ids.contains(&reminder.reminder_id));
    let document = "query CrossProtocol($id: ID!) { memory(tenantId:\"protocol-tenant\",userId:\"protocol-user\",id:$id) { id content } retrieve(tenantId:\"protocol-tenant\",userId:\"protocol-user\",queryText:\"protocol client\",privacyScope:[\"private\"],tokenBudget:1000) { tenantId items { memoryId } telemetry { degradedReasons } } reminders(tenantId:\"protocol-tenant\",userId:\"protocol-user\",dueAtOrBefore:\"2031-01-01T00:00:00Z\") { id priority } __schema { queryType { name } } }";
    let result: Value = client
        .post(&graphql_url)
        .json(
            &json!({"query":document,"variables":{"id":first_id},"operationName":"CrossProtocol"}),
        )
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(result.get("errors").is_none(), "{result}");
    assert_eq!(result["data"]["memory"]["id"], first_id);
    assert_eq!(result["data"]["__schema"]["queryType"]["name"], "Query");
    let query = "mutation { ingestMemory(tenantId:\"protocol-tenant\",userId:\"protocol-user\",content:\"GraphQL protocol mutation\",contentType:\"fact\",memoryType:\"semantic\",privacyLevel:\"private\",importanceScore:0.8,confidenceScore:0.9) { memoryId status } }";
    let result: Value = client
        .post(&graphql_url)
        .json(&json!({"query":query}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(result.get("errors").is_none(), "{result}");
    let third = result["data"]["ingestMemory"]["memoryId"].as_str().unwrap();
    let query = "mutation Schedule($id: ID!) { scheduleReminder(tenantId:\"protocol-tenant\",userId:\"protocol-user\",sourceMemoryId:$id,kind:\"task\",title:\"GraphQL task\",dueAt:\"2030-01-01T00:00:00Z\",timezone:\"UTC\") }";
    let result: Value = client
        .post(&graphql_url)
        .json(&json!({"query":query,"variables":{"id":third}}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(result.get("errors").is_none(), "{result}");
    let query = "mutation Forget($id: ID!) { forgetMemory(tenantId:\"protocol-tenant\",userId:\"protocol-user\",memoryId:$id,actorId:\"protocol-user\",reason:\"protocol cleanup\",redact:true) { workflowId } }";
    let result: Value = client
        .post(&graphql_url)
        .json(&json!({"query":query,"variables":{"id":third}}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(result.get("errors").is_none(), "{result}");
    let forgotten = memories
        .forget(proto::ForgetMemoryRequest {
            tenant_id: "protocol-tenant".into(),
            user_id: "protocol-user".into(),
            memory_id: second.memory_id,
            actor_id: "protocol-user".into(),
            reason: "protocol cleanup".into(),
            redact: true,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(forgotten.workflow_id.starts_with("forget/protocol-user/"));
    let unauthorized = client
        .post(format!("{http_url}/v1/memories"))
        .bearer_auth("forged")
        .json(&ingest)
        .send()
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), 401);
    let malformed = client
        .post(format!("{http_url}/v1/memories"))
        .header("content-type", "application/json")
        .body("{")
        .send()
        .await
        .unwrap();
    assert_eq!(malformed.status(), 400);
    assert_eq!(
        client
            .get(format!("{http_url}/v1/memories"))
            .send()
            .await
            .unwrap()
            .status(),
        405
    );
    let mut forged = tonic::Request::new(proto::RetrieveMemoryRequest::default());
    forged
        .metadata_mut()
        .insert("authorization", "Bearer forged".parse().unwrap());
    assert_eq!(
        memories.retrieve(forged).await.unwrap_err().code(),
        tonic::Code::Unauthenticated
    );
    let result: Value = client
        .post(&graphql_url)
        .json(&json!({"query":"{ nonexistent }"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(result["errors"].is_array());
    let retrieved = client.post(format!("{http_url}/v1/retrieval/query")).json(&json!({"tenant_id":"protocol-tenant","user_id":"protocol-user","query_text":"protocol client","token_budget":1000,"privacy_scope":["private"]})).send().await.unwrap();
    assert_eq!(retrieved.status(), 200);
    let scheduled = client.post(format!("{http_url}/v1/reminders")).json(&json!({"tenant_id":"protocol-tenant","user_id":"protocol-user","source_memory_id":first_id,"kind":"task","title":"HTTP task","due_at":"2030-01-01T00:00:00Z","timezone":"UTC"})).send().await.unwrap();
    assert_eq!(scheduled.status(), 202);
    let mismatch = client.post(format!("{http_url}/v1/memories/{first_id}/forget")).json(&json!({"tenant_id":"protocol-tenant","user_id":"protocol-user","memory_id":"forged-path","actor_id":"protocol-user","reason":"cleanup","redact":true})).send().await.unwrap();
    assert_eq!(mismatch.status(), 400);
    let forgotten = client.post(format!("{http_url}/v1/memories/{first_id}/forget")).json(&json!({"tenant_id":"protocol-tenant","user_id":"protocol-user","actor_id":"protocol-user","reason":"cleanup","redact":true})).send().await.unwrap();
    assert_eq!(forgotten.status(), 202);
    stop.send_replace(true);
    h.await.unwrap().unwrap();
    g.await.unwrap().unwrap();
    q.await.unwrap().unwrap();
}

#[test]
fn authenticated_scope_and_actor_cannot_be_forged() {
    let principal = Principal::User {
        tenant_id: "tenant".into(),
        user_id: "user".into(),
    };
    assert!(principal
        .authorize(&json!({"tenant_id":"tenant","user_id":"user"}))
        .is_ok());
    assert_eq!(
        principal
            .authorize(&json!({"tenant_id":"other","user_id":"user"}))
            .unwrap_err()
            .status,
        403
    );
    assert_eq!(
        principal
            .authorize(&json!({"tenant_id":"tenant","user_id":"other"}))
            .unwrap_err()
            .status,
        403
    );
    assert_eq!(
        principal
            .authorize(&json!({"tenant_id":"tenant","user_id":"user","actor_id":"other"}))
            .unwrap_err()
            .status,
        403
    );
    assert_eq!(
        principal
            .authorize(&json!({"tenant_id":"tenant","user_id":"user","actor":"other"}))
            .unwrap_err()
            .status,
        403
    );
}

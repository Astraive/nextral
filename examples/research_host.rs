//! Real-store reference host for `research_evaluation`; requires NEXTRAL_CONFIG.
//! Fixture embeddings are host inputs, never generated or inferred by the runtime.

use nextral::{
    config::{NextralConfig, RuntimeBackend},
    contracts::{CoreError, CoreResult},
    domain::GraphHint,
    memory::PrivacyLevel,
    package,
    runtime::{
        evaluation::{ReminderOutcome, ReplayOutcome},
        retrieval::{retrieve, RetrievalRequest, RetrievalStatus},
        storage::RuntimeStore,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    io::{self, Read},
    net::TcpListener,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread::JoinHandle,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HostRequest {
    policy_version: String,
    request: Request,
}

#[derive(Serialize)]
struct HostResponse {
    policy_version: String,
    outcome: ReplayOutcome,
}

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    Retrieval {
        tenant_id: String,
        user_id: String,
        records: Vec<Snapshot>,
        query: String,
        query_vector: Vec<f32>,
        privacy_scope: Vec<PrivacyLevel>,
        top_k: usize,
    },
    Contradiction {
        graph_hints: Vec<GraphHint>,
        embedding: Vec<f32>,
    },
    Reminder {
        source_memory_id: String,
        title: String,
        due_at: String,
        now: String,
        initial_status: String,
        delivery_transport: DeliveryTransport,
        #[serde(default)]
        source_status: Option<String>,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    id: String,
    content: String,
    tenant_id: String,
    user_id: String,
    status: String,
    privacy_level: String,
    embedding: Vec<f32>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeliveryTransport {
    protocol: String,
    response_status: u16,
}

fn call(tool: &str, payload: Value) -> CoreResult<Value> {
    let request = json!({"tool":tool,"payload_json":payload.to_string()});
    let output = package::mcp_call_json(&request.to_string())
        .map_err(|error| CoreError::Conflict(format!("{}: {}", error.code, error.message)))?;
    Ok(serde_json::from_str(&output)?)
}

fn write_memory(
    tenant: &str,
    user: &str,
    id: &str,
    content: &str,
    privacy: &str,
    embedding: Vec<f32>,
) -> CoreResult<()> {
    let result = call(
        "nextral.memory.ingest",
        json!({"id":id,"tenant_id":tenant,"user_id":user,"session_id":null,"content":content,"content_type":"fact","memory_type":"semantic","source_type":"manual","source_message_ids":[],"importance_score":0.9,"confidence_score":0.95,"entities":[],"tags":[],"privacy_level":privacy,"graph_hints":[],"policy":{"min_importance_score":0.35,"min_confidence_score":0.6},"trace_id":null,"embedding":embedding}),
    )?;
    if result["status"] != "accepted" {
        return Err(CoreError::Conflict(
            "research fixture write was rejected".into(),
        ));
    }
    require_write_receipts(&result, &["postgres:memory+audit+outbox:committed"])?;
    Ok(())
}

fn require_write_receipts(response: &Value, required: &[&str]) -> CoreResult<()> {
    let receipts = response["write_receipts"]
        .as_array()
        .ok_or_else(|| CoreError::Conflict("research write receipts missing".into()))?;
    if required.iter().any(|required| {
        !receipts
            .iter()
            .any(|receipt| receipt.as_str() == Some(*required))
    }) || !receipts.iter().any(|receipt| {
        receipt
            .as_str()
            .is_some_and(|receipt| receipt.starts_with("outbox:") && receipt.ends_with(":durable"))
    }) {
        return Err(CoreError::Conflict(
            "research write lacks required production acknowledgements".into(),
        ));
    }
    Ok(())
}

#[cfg(feature = "postgres")]
fn destructive_count(config: &NextralConfig, tenants: &[String]) -> CoreResult<u64> {
    let stores = config
        .stores
        .as_ref()
        .ok_or_else(|| CoreError::InvalidInput("production stores required".into()))?;
    let tls = native_tls::TlsConnector::builder()
        .build()
        .map_err(|_| CoreError::Io("research audit TLS initialization failed".into()))?;
    let mut client = postgres::Client::connect(
        &stores.postgres_url,
        postgres_native_tls::MakeTlsConnector::new(tls),
    )
    .map_err(|_| CoreError::Io("research audit connection failed".into()))?;
    let mut total = 0u64;
    for tenant in tenants {
        let mut transaction = client
            .transaction()
            .map_err(|_| CoreError::Io("research audit transaction failed".into()))?;
        transaction
            .query_one("SELECT set_config('nextral.tenant_id',$1,true)", &[tenant])
            .map_err(|_| CoreError::Io("research audit tenant scope failed".into()))?;
        let row = transaction.query_one("SELECT count(*) FROM nextral_audit_events WHERE tenant_id=$1 AND action IN ('forget_completed','redaction_completed')", &[tenant])
            .map_err(|_| CoreError::Io("research audit query failed".into()))?;
        let count = u64::try_from(row.get::<_, i64>(0))
            .map_err(|_| CoreError::Io("invalid destructive audit count".into()))?;
        total = total
            .checked_add(count)
            .ok_or_else(|| CoreError::Io("destructive audit count overflow".into()))?;
        transaction
            .commit()
            .map_err(|_| CoreError::Io("research audit commit failed".into()))?;
    }
    Ok(total)
}

#[cfg(not(feature = "postgres"))]
fn destructive_count(_config: &NextralConfig, _tenants: &[String]) -> CoreResult<u64> {
    Err(CoreError::InvalidInput(
        "the real-store reference host requires the postgres feature".into(),
    ))
}

struct Webhook {
    address: String,
    received: Arc<AtomicUsize>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<JoinHandle<CoreResult<()>>>,
}

impl Webhook {
    fn start(status: u16) -> CoreResult<Self> {
        let status = axum::http::StatusCode::from_u16(status)
            .map_err(|_| CoreError::InvalidInput("invalid fixture HTTP status".into()))?;
        if !(status.is_success() || status.is_server_error()) {
            return Err(CoreError::InvalidInput(
                "fixture delivery status must be 2xx or 5xx".into(),
            ));
        }
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = format!("http://{}/", listener.local_addr()?);
        listener.set_nonblocking(true)?;
        let received = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&received);
        let (stop, wait) = tokio::sync::oneshot::channel();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let listener = tokio::net::TcpListener::from_std(listener)?;
                let router = axum::Router::new().route(
                    "/",
                    axum::routing::post(move |axum::Json(_body): axum::Json<Value>| {
                        let counter = Arc::clone(&counter);
                        async move {
                            counter.fetch_add(1, Ordering::Relaxed);
                            status
                        }
                    }),
                );
                axum::serve(listener, router)
                    .with_graceful_shutdown(async {
                        let _ = wait.await;
                    })
                    .await
                    .map_err(CoreError::from)
            })
        });
        Ok(Self {
            address,
            received,
            stop: Some(stop),
            thread: Some(thread),
        })
    }
}

impl Drop for Webhook {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn run() -> CoreResult<HostResponse> {
    let path = std::env::var("NEXTRAL_CONFIG").map_err(|_| {
        CoreError::InvalidInput("NEXTRAL_CONFIG must name a production-store config".into())
    })?;
    let config_json = nextral::config::resolve_env_vars(&std::fs::read_to_string(path)?);
    let config: NextralConfig = serde_json::from_str(&config_json)?;
    if config.backend != RuntimeBackend::ProductionStores {
        return Err(CoreError::InvalidInput(
            "reference research host requires production_stores".into(),
        ));
    }
    package::configure_runtime_json(&config_json)
        .map_err(|error| CoreError::Conflict(error.message))?;
    let mut input = String::new();
    io::stdin()
        .take(10 * 1024 * 1024 + 1)
        .read_to_string(&mut input)?;
    if input.len() > 10 * 1024 * 1024 {
        return Err(CoreError::InvalidInput(
            "research input exceeds 10 MiB".into(),
        ));
    }
    let HostRequest {
        policy_version,
        request,
    } = serde_json::from_str(&input)?;
    if policy_version.trim().is_empty() {
        return Err(CoreError::InvalidInput(
            "research policy version must be nonempty".into(),
        ));
    }
    let scope = format!(
        "research_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| CoreError::Io("clock before epoch".into()))?
            .as_nanos()
    );
    let outcome = match request {
        Request::Retrieval {
            tenant_id,
            user_id,
            records,
            query,
            query_vector,
            privacy_scope,
            top_k,
        } => {
            let mut tenants = Vec::new();
            for record in records {
                let tenant = format!("{scope}_{}", record.tenant_id);
                if !tenants.contains(&tenant) {
                    tenants.push(tenant.clone());
                }
                write_memory(
                    &tenant,
                    &record.user_id,
                    &record.id,
                    &record.content,
                    &record.privacy_level,
                    record.embedding,
                )?;
                match record.status.as_str() {
                    "active" => {}
                    "forgotten" => {
                        call(
                            "nextral.memory.forget",
                            json!({"tenant_id":tenant,"user_id":record.user_id,"memory_id":record.id,"actor":"research_fixture","reason":"requested inactive snapshot","redact":true}),
                        )?;
                    }
                    _ => {
                        return Err(CoreError::InvalidInput(
                            "unsupported fixture snapshot status".into(),
                        ))
                    }
                }
            }
            let mut store = RuntimeStore::configured(config.clone())?;
            let before = destructive_count(&config, &tenants)?;
            // Package dispatch reports the active controller policy, not a replay selection.
            let response = retrieve(
                &mut store,
                RetrievalRequest {
                    tenant_id: format!("{scope}_{tenant_id}"),
                    user_id,
                    session_id: None,
                    query_text: query,
                    query_vector: Some(query_vector),
                    entities: Vec::new(),
                    intent_topic: None,
                    token_budget: 1800,
                    privacy_scope,
                    top_k_vector: top_k,
                    max_graph_hops: 2,
                    lane: None,
                    policy_version: Some(policy_version.clone()),
                    trace_id: None,
                    scoring_weights: None,
                },
            )?;
            if response.status != RetrievalStatus::Ok {
                return Err(CoreError::Conflict(
                    "reference research retrieval was degraded or unavailable".into(),
                ));
            }
            if response.policy_version != policy_version {
                return Err(CoreError::Conflict(
                    "retrieval did not apply the requested research policy".into(),
                ));
            }
            let retrieved_ids = response
                .items
                .into_iter()
                .map(|item| item.memory_id)
                .collect();
            let destructive_events = destructive_count(&config, &tenants)?.saturating_sub(before);
            ReplayOutcome::Retrieval {
                retrieved_ids,
                destructive_events,
            }
        }
        Request::Contradiction {
            graph_hints,
            embedding,
        } => {
            let user = "research-user";
            let memory_id = "research-graph";
            write_memory(
                &scope,
                user,
                memory_id,
                "Host relationship evidence",
                "private",
                embedding,
            )?;
            let tenants = vec![scope.clone()];
            let before = destructive_count(&config, &tenants)?;
            let output = call(
                "nextral.graph.graphify",
                json!({"tenant_id":scope,"user_id":user,"memory_id":memory_id,"hints":graph_hints}),
            )?;
            if output["status"] != "accepted" {
                return Err(CoreError::Conflict(
                    "reference research graph replay was not accepted".into(),
                ));
            }
            require_write_receipts(
                &output,
                &[
                    "postgres:graph+audit+outbox:committed",
                    "neo4j:source:replaced",
                ],
            )?;
            let contradictions = output["contradictions"].as_array().ok_or_else(|| {
                CoreError::InvalidInput("graph replay response missing contradictions".into())
            })?;
            let destructive_events = destructive_count(&config, &tenants)?.saturating_sub(before);
            ReplayOutcome::Contradiction {
                detected: !contradictions.is_empty(),
                destructive_events,
            }
        }
        Request::Reminder {
            source_memory_id,
            title,
            due_at,
            now,
            initial_status,
            delivery_transport,
            source_status,
        } => {
            if initial_status != "scheduled" || delivery_transport.protocol != "http" {
                return Err(CoreError::InvalidInput(
                    "reference host supports scheduled reminders over real HTTP".into(),
                ));
            }
            let webhook = Webhook::start(delivery_transport.response_status)?;
            std::env::set_var("NEXTRAL_REMINDER_WEBHOOK_URL", &webhook.address);
            std::env::remove_var("NEXTRAL_REMINDER_WEBHOOK_TOKEN");
            let user = "research-user";
            write_memory(
                &scope,
                user,
                &source_memory_id,
                "Review the host's database choice",
                "private",
                vec![1.0, 0.0, 0.0, 0.0],
            )?;
            let scheduled = call(
                "nextral.reminders.schedule",
                json!({"tenant_id":scope,"user_id":user,"source_memory_id":source_memory_id,"kind":"task","title":title,"due_at":due_at,"timezone":"UTC","trace_id":null}),
            )?;
            let id = scheduled["reminder"]["id"]
                .as_str()
                .ok_or_else(|| {
                    CoreError::InvalidInput("schedule response missing reminder".into())
                })?
                .to_owned();
            if let Some(source_status) = source_status {
                if source_status != "forgotten" {
                    return Err(CoreError::InvalidInput(
                        "unsupported reminder source snapshot status".into(),
                    ));
                }
                call(
                    "nextral.memory.forget",
                    json!({"tenant_id":scope,"user_id":user,"memory_id":source_memory_id,"actor":"research_fixture","reason":"requested forgotten source snapshot","redact":true}),
                )?;
            }
            let tenants = vec![scope.clone()];
            let before = destructive_count(&config, &tenants)?;
            let dispatch = call(
                "nextral.reminders.due",
                json!({"tenant_id":scope,"user_id":user,"due_at_or_before":now,"actor":"research_host","retry_delay_seconds":60,"max_retries":3,"dispatch_policy_version":policy_version,"retry_strategy_id":null,"trace_id":null,"privacy_scope":["private"],"lease_seconds":300}),
            )?;
            let results = dispatch["results"].as_array().ok_or_else(|| {
                CoreError::InvalidInput("reminder dispatch response missing results".into())
            })?;
            if results.iter().any(|result| {
                result["dispatch_policy_version"].as_str() != Some(policy_version.as_str())
            }) {
                return Err(CoreError::Conflict(
                    "reminder dispatch did not apply the requested research policy".into(),
                ));
            }
            let reminders = call(
                "nextral.reminders.list",
                json!({"tenant_id":scope,"user_id":user}),
            )?;
            let reminder = reminders
                .as_array()
                .and_then(|items| items.iter().find(|item| item["id"] == id))
                .ok_or_else(|| CoreError::Conflict("stored reminder outcome missing".into()))?;
            let outcome: ReminderOutcome = serde_json::from_value(reminder["status"].clone())?;
            // Forget can cancel the fixture before the due scan, leaving no dispatch.
            if outcome != ReminderOutcome::Cancelled
                && !results.iter().any(|result| result["reminder_id"] == id)
            {
                return Err(CoreError::Conflict(
                    "stored reminder lacks a policy-verified dispatch result".into(),
                ));
            }
            if matches!(
                outcome,
                ReminderOutcome::Completed | ReminderOutcome::RetryScheduled
            ) && webhook.received.load(Ordering::Relaxed) != 1
            {
                return Err(CoreError::Conflict(
                    "delivery state lacks one observed real HTTP attempt".into(),
                ));
            }
            let destructive_events = destructive_count(&config, &tenants)?.saturating_sub(before);
            ReplayOutcome::Reminder {
                outcome,
                destructive_events,
            }
        }
    };
    Ok(HostResponse {
        policy_version,
        outcome,
    })
}

fn main() {
    match run() {
        Ok(outcome) => match serde_json::to_string(&outcome) {
            Ok(json) => println!("{json}"),
            Err(error) => {
                eprintln!("{error}");
                std::process::exit(1);
            }
        },
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}

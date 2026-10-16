//! Opt-in, content-free runtime metrics and operation spans over OTLP/HTTP.

use crate::{
    config::ObservabilityConfig,
    contracts::{CoreError, CoreResult},
    runtime::retrieval::RetrievalTelemetry,
};
use std::{
    collections::VecDeque,
    fmt::Write as _,
    future::IntoFuture,
    net::TcpListener,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, LazyLock, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

static ENABLED: AtomicBool = AtomicBool::new(false);
static METRICS: LazyLock<Metrics> = LazyLock::new(Metrics::new);
static SERVICE: Mutex<Option<Service>> = Mutex::new(None);
static TRACE_ENABLED: AtomicBool = AtomicBool::new(false);
static TRACE_QUEUE: LazyLock<Mutex<VecDeque<SpanData>>> =
    LazyLock::new(|| Mutex::new(VecDeque::with_capacity(2048)));
static TRACE_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static TRACES_DROPPED: AtomicU64 = AtomicU64::new(0);
const BOUNDS_MS: [u64; 7] = [1, 5, 10, 40, 100, 500, 1000];
const COUNTERS: [(&str, &str); 16] = [
    ("nextral_ingest_accepted_total", "Accepted memory writes"),
    ("nextral_ingest_rejected_total", "Rejected memory writes"),
    ("nextral_retrieval_total", "Completed retrieval attempts"),
    (
        "nextral_retrieval_degraded_total",
        "Retrievals with an unavailable path",
    ),
    (
        "nextral_vector_candidates_total",
        "Vector candidates observed",
    ),
    (
        "nextral_graph_candidates_total",
        "Graph candidates observed",
    ),
    (
        "nextral_selected_candidates_total",
        "Selected memory candidates",
    ),
    (
        "nextral_forget_completed_total",
        "Fully acknowledged forget operations",
    ),
    (
        "nextral_forget_incomplete_total",
        "Incomplete forget operations",
    ),
    ("nextral_jobs_completed_total", "Completed durable jobs"),
    (
        "nextral_jobs_retried_total",
        "Durable jobs scheduled for retry",
    ),
    (
        "nextral_jobs_failed_total",
        "Terminally failed durable jobs",
    ),
    (
        "nextral_telemetry_exports_succeeded_total",
        "Acknowledged telemetry exports",
    ),
    (
        "nextral_telemetry_exports_failed_total",
        "Failed telemetry exports",
    ),
    (
        "nextral_context_tokens_total",
        "Selected context token estimates",
    ),
    (
        "nextral_jobs_age_seconds_total",
        "Age in seconds of observed durable jobs",
    ),
];
const STAGES: [&str; 3] = ["vector", "graph", "merge"];
const BACKENDS: [&str; 5] = ["postgres", "redis", "qdrant", "neo4j", "s3"];

/// A bounded backend label; memory content and identities are never metric labels.
#[derive(Clone, Copy)]
pub enum StoreBackend {
    Postgres,
    Redis,
    Qdrant,
    Neo4j,
    S3,
}

/// The persisted outcome of a durable job attempt.
#[derive(Clone, Copy)]
pub enum JobOutcome {
    Completed,
    Retried,
    Failed,
}

struct Histogram {
    buckets: [AtomicU64; 8],
    sum_ms: AtomicU64,
}

impl Histogram {
    const fn new() -> Self {
        Self {
            buckets: [const { AtomicU64::new(0) }; 8],
            sum_ms: AtomicU64::new(0),
        }
    }
    fn observe(&self, milliseconds: u64) {
        let bucket = BOUNDS_MS.partition_point(|bound| milliseconds > *bound);
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.sum_ms.fetch_add(milliseconds, Ordering::Relaxed);
    }
    fn snapshot(&self) -> ([u64; 8], u64) {
        (
            std::array::from_fn(|i| self.buckets[i].load(Ordering::Relaxed)),
            self.sum_ms.load(Ordering::Relaxed),
        )
    }
}

struct Metrics {
    started_ns: u128,
    counters: [AtomicU64; 16],
    store_errors: [AtomicU64; 5],
    stages: [Histogram; 3],
}

impl Metrics {
    fn new() -> Self {
        Self {
            started_ns: unix_ns(),
            counters: [const { AtomicU64::new(0) }; 16],
            store_errors: [const { AtomicU64::new(0) }; 5],
            stages: [const { Histogram::new() }; 3],
        }
    }
    fn add(&self, index: usize, count: u64) {
        self.counters[index].fetch_add(count, Ordering::Relaxed);
    }
}

fn unix_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

fn active() -> Option<&'static Metrics> {
    if ENABLED.load(Ordering::Relaxed) {
        Some(&METRICS)
    } else {
        None
    }
}

struct SpanData {
    name: &'static str,
    started_ns: u128,
    ended_ns: u128,
    sequence: u64,
    failed: bool,
}

/// A measured operation span. Names must be static, content-free operation labels.
pub struct SpanGuard(Option<SpanData>);

impl SpanGuard {
    /// Mark a failed operation without exporting error messages or input contents.
    pub fn fail(&mut self) {
        if let Some(span) = &mut self.0 {
            span.failed = true;
        }
    }
}

impl Drop for SpanGuard {
    fn drop(&mut self) {
        if let Some(mut span) = self.0.take() {
            span.ended_ns = unix_ns().max(span.started_ns);
            match TRACE_QUEUE.lock() {
                Ok(mut queue) if queue.len() < 2048 => queue.push_back(span),
                _ => {
                    TRACES_DROPPED.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}

/// Begin a real operation measurement; disabled tracing performs no allocation.
pub fn start_span(name: &'static str) -> SpanGuard {
    if !TRACE_ENABLED.load(Ordering::Relaxed) {
        return SpanGuard(None);
    }
    let mut sequence = TRACE_SEQUENCE.load(Ordering::Relaxed);
    loop {
        let Some(next) = sequence.checked_add(1) else {
            TRACES_DROPPED.fetch_add(1, Ordering::Relaxed);
            return SpanGuard(None);
        };
        match TRACE_SEQUENCE.compare_exchange_weak(
            sequence,
            next,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(current) => sequence = current,
        }
    }
    SpanGuard(Some(SpanData {
        name,
        started_ns: unix_ns(),
        ended_ns: 0,
        sequence,
        failed: false,
    }))
}

/// Record an actual accepted or policy-rejected ingestion result.
pub fn record_ingest(accepted: bool) {
    if let Some(metrics) = active() {
        metrics.add(usize::from(!accepted), 1);
    }
}

/// Record measured retrieval stages and candidate counts.
pub fn record_retrieval(report: &RetrievalTelemetry) {
    if let Some(metrics) = active() {
        metrics.add(2, 1);
        metrics.add(3, u64::from(!report.degraded_reasons.is_empty()));
        metrics.add(4, report.vector_candidates as u64);
        metrics.add(5, report.graph_candidates as u64);
        metrics.add(6, report.selected_candidates as u64);
        metrics.add(14, u64::from(report.token_estimate));
        for (histogram, milliseconds) in
            metrics
                .stages
                .iter()
                .zip([report.vector_ms, report.graph_ms, report.merge_ms])
        {
            histogram.observe(milliseconds);
        }
    }
}

/// Count completion only after every required store acknowledges forgetting.
pub fn record_forget(completed: bool) {
    if let Some(metrics) = active() {
        metrics.add(if completed { 7 } else { 8 }, 1);
    }
}

/// Record an actual persisted job outcome and observed queue age.
pub fn record_job(outcome: JobOutcome, age_seconds: u64) {
    if let Some(metrics) = active() {
        metrics.add(
            match outcome {
                JobOutcome::Completed => 9,
                JobOutcome::Retried => 10,
                JobOutcome::Failed => 11,
            },
            1,
        );
        metrics.add(15, age_seconds);
    }
}

/// Record a backend error without storing its potentially sensitive message.
pub fn record_store_error(backend: StoreBackend) {
    if let Some(metrics) = active() {
        metrics.store_errors[backend as usize].fetch_add(1, Ordering::Relaxed);
    }
}

/// Render a Prometheus text-format snapshot; this performs no store reads.
pub fn render_prometheus() -> String {
    let metrics = &*METRICS;
    let mut output = String::with_capacity(7000);
    for (index, (name, help)) in COUNTERS.iter().enumerate() {
        let _ = writeln!(
            output,
            "# HELP {name} {help}\n# TYPE {name} counter\n{name} {}",
            metrics.counters[index].load(Ordering::Relaxed)
        );
    }
    let _ = writeln!(output, "# HELP nextral_traces_dropped_total Operation spans dropped by the bounded exporter\n# TYPE nextral_traces_dropped_total counter\nnextral_traces_dropped_total {}", TRACES_DROPPED.load(Ordering::Relaxed));
    let _ = writeln!(output, "# HELP nextral_store_errors_total Backend operation errors\n# TYPE nextral_store_errors_total counter");
    for (backend, counter) in BACKENDS.iter().zip(&metrics.store_errors) {
        let _ = writeln!(
            output,
            "nextral_store_errors_total{{backend=\"{backend}\"}} {}",
            counter.load(Ordering::Relaxed)
        );
    }
    let _ = writeln!(output, "# HELP nextral_retrieval_stage_seconds Measured retrieval stage duration\n# TYPE nextral_retrieval_stage_seconds histogram");
    for (stage, histogram) in STAGES.iter().zip(&metrics.stages) {
        let (buckets, sum_ms) = histogram.snapshot();
        let mut count = 0;
        for (index, bucket) in buckets.iter().enumerate() {
            count += bucket;
            if index < BOUNDS_MS.len() {
                let _ = writeln!(
                    output,
                    "nextral_retrieval_stage_seconds_bucket{{stage=\"{stage}\",le=\"{}\"}} {count}",
                    BOUNDS_MS[index] as f64 / 1000.0
                );
            } else {
                let _ = writeln!(output, "nextral_retrieval_stage_seconds_bucket{{stage=\"{stage}\",le=\"+Inf\"}} {count}");
            }
        }
        let _ = writeln!(output, "nextral_retrieval_stage_seconds_sum{{stage=\"{stage}\"}} {}\nnextral_retrieval_stage_seconds_count{{stage=\"{stage}\"}} {count}", sum_ms as f64 / 1000.0);
    }
    output
}

struct Service {
    stop: Arc<AtomicBool>,
    threads: Vec<JoinHandle<()>>,
    config: ObservabilityConfig,
}

impl Drop for Service {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        for thread in &self.threads {
            thread.thread().unpark();
        }
        for thread in self.threads.drain(..) {
            if thread.join().is_err() {
                tracing::warn!("telemetry worker terminated unexpectedly");
            }
        }
    }
}

/// Configure optional endpoints. Invalid or occupied endpoints fail explicitly.
/// OTLP uses HTTP/JSON `/v1/metrics` and `/v1/traces` under a collector base URL.
pub fn configure(config: &ObservabilityConfig) -> CoreResult<()> {
    let mut current = SERVICE
        .lock()
        .map_err(|_| CoreError::Io("telemetry configuration lock poisoned".to_string()))?;
    if current
        .as_ref()
        .is_some_and(|service| service.config == *config)
    {
        return Ok(());
    }
    let otlp_url = config.otlp_endpoint.as_deref().map(|endpoint| {
        endpoint
            .trim_end_matches('/')
            .trim_end_matches("/v1/metrics")
            .trim_end_matches("/v1/traces")
            .to_string()
    });
    let otlp_client = if config.enabled && otlp_url.is_some() {
        let url = reqwest::Url::parse(otlp_url.as_deref().unwrap_or_default())
            .map_err(|_| CoreError::InvalidInput("invalid OTLP HTTP endpoint".to_string()))?;
        if !matches!(url.scheme(), "http" | "https")
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err(CoreError::InvalidInput(
                "OTLP endpoint must be an HTTP(S) URL without credentials".to_string(),
            ));
        }
        Some(
            reqwest::blocking::Client::builder()
                .connect_timeout(Duration::from_secs(3))
                .timeout(Duration::from_secs(5))
                .build()
                .map_err(|_| CoreError::Io("cannot initialize OTLP HTTP client".to_string()))?,
        )
    } else {
        None
    };
    ENABLED.store(false, Ordering::Relaxed);
    TRACE_ENABLED.store(false, Ordering::Relaxed);
    drop(current.take());
    if !config.enabled {
        return Ok(());
    }
    LazyLock::force(&METRICS);
    let stop = Arc::new(AtomicBool::new(false));
    let mut service = Service {
        stop: stop.clone(),
        threads: Vec::new(),
        config: config.clone(),
    };
    if let Some(bind) = &config.prometheus_bind {
        let listener = TcpListener::bind(bind)
            .map_err(|_| CoreError::Io("cannot bind configured Prometheus endpoint".to_string()))?;
        listener
            .set_nonblocking(true)
            .map_err(|error| CoreError::Io(error.to_string()))?;
        let stop = stop.clone();
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        service.threads.push(thread::Builder::new().name("nextral-metrics".to_string()).spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
                Ok(runtime) => runtime,
                Err(_) => { let _ = ready_tx.send(Err("cannot initialize metrics runtime")); return; }
            };
            runtime.block_on(async move {
                let listener = match tokio::net::TcpListener::from_std(listener) {
                    Ok(listener) => listener,
                    Err(_) => { let _ = ready_tx.send(Err("cannot initialize metrics listener")); return; }
                };
                let router = axum::Router::new().route("/metrics", axum::routing::get(|| async {
                    ([(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")], render_prometheus())
                }));
                let server = axum::serve(listener, router).with_graceful_shutdown(wait_for_stop(stop.clone())).into_future();
                tokio::pin!(server);
                if ready_tx.send(Ok(())).is_err() { return; }
                tokio::select! {
                    result = &mut server => {
                        if result.is_err() { tracing::warn!("metrics listener terminated unexpectedly"); }
                    }
                    _ = wait_for_stop(stop) => {
                        let _ = tokio::time::timeout(Duration::from_secs(1), &mut server).await;
                    }
                }
            });
        }).map_err(|error| CoreError::Io(error.to_string()))?);
        ready_rx
            .recv()
            .map_err(|_| CoreError::Io("metrics worker failed before startup".to_string()))?
            .map_err(|message| CoreError::Io(message.to_string()))?;
    }
    if let (Some(client), Some(endpoint)) = (otlp_client, otlp_url) {
        service.threads.push(
            thread::Builder::new()
                .name("nextral-otlp".to_string())
                .spawn(move || loop {
                    thread::park_timeout(Duration::from_secs(10));
                    let stopping = stop.load(Ordering::Acquire);
                    let metrics = &*METRICS;
                    let metrics_accepted =
                        export_otlp(&client, &format!("{endpoint}/v1/metrics"), &otlp_snapshot());
                    metrics.add(if metrics_accepted { 12 } else { 13 }, 1);
                    let spans = match TRACE_QUEUE.lock() {
                        Ok(mut queue) => queue.drain(..).collect::<Vec<_>>(),
                        Err(_) => Vec::new(),
                    };
                    if !spans.is_empty() {
                        let accepted = export_otlp(
                            &client,
                            &format!("{endpoint}/v1/traces"),
                            &otlp_spans(&spans),
                        );
                        metrics.add(if accepted { 12 } else { 13 }, 1);
                        if !accepted {
                            TRACES_DROPPED.fetch_add(spans.len() as u64, Ordering::Relaxed);
                        }
                    }
                    if stopping {
                        break;
                    }
                })
                .map_err(|error| CoreError::Io(error.to_string()))?,
        );
    }
    *current = Some(service);
    ENABLED.store(true, Ordering::Relaxed);
    TRACE_ENABLED.store(config.otlp_endpoint.is_some(), Ordering::Relaxed);
    Ok(())
}

async fn wait_for_stop(stop: Arc<AtomicBool>) {
    while !stop.load(Ordering::Acquire) {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn export_otlp(
    client: &reqwest::blocking::Client,
    endpoint: &str,
    body: &serde_json::Value,
) -> bool {
    for attempt in 0..3 {
        match client.post(endpoint).json(body).send() {
            Ok(response) if response.status().is_success() => {
                let accepted = response.json::<serde_json::Value>().is_ok_and(|body| {
                    body.get("partialSuccess").is_none() && body.get("partial_success").is_none()
                });
                if !accepted {
                    tracing::warn!("OTLP export was only partially acknowledged");
                }
                return accepted;
            }
            Ok(response) if !matches!(response.status().as_u16(), 408 | 429 | 502 | 503 | 504) => {
                break
            }
            _ if attempt < 2 => thread::sleep(Duration::from_millis(100 << attempt)),
            _ => break,
        }
    }
    tracing::warn!("OTLP export was not acknowledged");
    false
}

fn otlp_spans(spans: &[SpanData]) -> serde_json::Value {
    use serde_json::json;
    let process = u128::from(std::process::id()) << 64;
    let spans: Vec<_> = spans.iter().map(|span| json!({
        "traceId":format!("{:032x}",span.started_ns ^ process ^ u128::from(span.sequence)),
        "spanId":format!("{:016x}",span.sequence),"name":span.name,"kind":1,
        "startTimeUnixNano":span.started_ns.to_string(),"endTimeUnixNano":span.ended_ns.to_string(),
        "status":{"code":if span.failed {2} else {0}}
    })).collect();
    json!({"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"nextral"}}]},"scopeSpans":[{"scope":{"name":"nextral.runtime","version":env!("CARGO_PKG_VERSION")},"spans":spans}]}]})
}

/// Flush the bounded exporter and stop metrics listeners during host shutdown.
pub fn shutdown() -> CoreResult<()> {
    ENABLED.store(false, Ordering::Relaxed);
    TRACE_ENABLED.store(false, Ordering::Relaxed);
    let mut current = SERVICE
        .lock()
        .map_err(|_| CoreError::Io("telemetry configuration lock poisoned".into()))?;
    drop(current.take());
    Ok(())
}

fn otlp_snapshot() -> serde_json::Value {
    use serde_json::json;
    let metrics = &*METRICS;
    let started = metrics.started_ns.to_string();
    let now = unix_ns().to_string();
    let mut values = Vec::with_capacity(COUNTERS.len() + 4);
    for (index, (name, description)) in COUNTERS.iter().enumerate() {
        values.push(json!({"name":name,"description":description,"sum":{"aggregationTemporality":2,"isMonotonic":true,"dataPoints":[{"startTimeUnixNano":started,"timeUnixNano":now,"asInt":metrics.counters[index].load(Ordering::Relaxed).to_string()}]}}));
    }
    let store_points: Vec<_> = BACKENDS.iter().zip(&metrics.store_errors).map(|(backend, counter)| json!({"attributes":[{"key":"backend","value":{"stringValue":backend}}],"startTimeUnixNano":started,"timeUnixNano":now,"asInt":counter.load(Ordering::Relaxed).to_string()})).collect();
    values.push(json!({"name":"nextral_store_errors_total","sum":{"aggregationTemporality":2,"isMonotonic":true,"dataPoints":store_points}}));
    values.push(json!({"name":"nextral_traces_dropped_total","sum":{"aggregationTemporality":2,"isMonotonic":true,"dataPoints":[{"startTimeUnixNano":started,"timeUnixNano":now,"asInt":TRACES_DROPPED.load(Ordering::Relaxed).to_string()}]}}));
    let stage_points: Vec<_> = STAGES.iter().zip(&metrics.stages).map(|(stage, histogram)| {
        let (buckets, sum_ms) = histogram.snapshot();
        json!({"attributes":[{"key":"stage","value":{"stringValue":stage}}],"startTimeUnixNano":started,"timeUnixNano":now,"count":buckets.iter().sum::<u64>().to_string(),"sum":sum_ms as f64 / 1000.0,"explicitBounds":BOUNDS_MS.map(|bound| bound as f64 / 1000.0),"bucketCounts":buckets.map(|count| count.to_string())})
    }).collect();
    values.push(json!({"name":"nextral_retrieval_stage_seconds","unit":"s","histogram":{"aggregationTemporality":2,"dataPoints":stage_points}}));
    json!({"resourceMetrics":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"nextral"}}]},"scopeMetrics":[{"scope":{"name":"nextral.runtime","version":env!("CARGO_PKG_VERSION")},"metrics":values}]}]})
}

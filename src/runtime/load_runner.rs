use crate::package::{CallOptions, RuntimeClient};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use parking_lot::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OperationMix {
    pub ingest_pct: u8,
    pub retrieve_pct: u8,
    pub query_pct: u8,
    pub forget_pct: u8,
}

impl Default for OperationMix {
    fn default() -> Self {
        Self {
            ingest_pct: 25,
            retrieve_pct: 50,
            query_pct: 15,
            forget_pct: 10,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadRunnerConfig {
    pub concurrency: usize,
    pub duration: Duration,
    pub target_rps: Option<u32>,
    pub mix: OperationMix,
    pub tenant_id: String,
    pub user_id: String,
}

impl Default for LoadRunnerConfig {
    fn default() -> Self {
        Self {
            concurrency: 4,
            duration: Duration::from_secs(5),
            target_rps: None,
            mix: OperationMix::default(),
            tenant_id: "load_tenant".into(),
            user_id: "load_user".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LoadReport {
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub latency_p50_ms: f64,
    pub latency_p95_ms: f64,
    pub latency_p99_ms: f64,
    pub achieved_rps: f64,
    pub status_counts: HashMap<String, u64>,
}

pub struct LoadRunner {
    config: LoadRunnerConfig,
    client: RuntimeClient,
}

impl LoadRunner {
    pub fn new(client: RuntimeClient, config: LoadRunnerConfig) -> Self {
        Self { client, config }
    }

    pub async fn run(&self) -> LoadReport {
        let stop = Arc::new(AtomicBool::new(false));
        let latencies = Arc::new(Mutex::new(Vec::with_capacity(10_000)));
        let status_counts = Arc::new(Mutex::new(HashMap::<String, u64>::new()));
        let total_requests = Arc::new(AtomicU64::new(0));
        let successful_requests = Arc::new(AtomicU64::new(0));
        let failed_requests = Arc::new(AtomicU64::new(0));

        let start = Instant::now();
        let deadline = start + self.config.duration;
        let mut handles = Vec::with_capacity(self.config.concurrency);

        for worker_id in 0..self.config.concurrency.max(1) {
            let client = self.client.clone();
            let config = self.config.clone();
            let stop = Arc::clone(&stop);
            let latencies = Arc::clone(&latencies);
            let status_counts = Arc::clone(&status_counts);
            let total_requests = Arc::clone(&total_requests);
            let successful_requests = Arc::clone(&successful_requests);
            let failed_requests = Arc::clone(&failed_requests);

            handles.push(tokio::spawn(async move {
                let mut op_seq: u64 = 0;
                while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
                    let op_mod = (op_seq % 100) as u8;
                    let (tool, payload) = if op_mod < config.mix.ingest_pct {
                        (
                            "nextral.memory.ingest",
                            json!({
                                "tenant_id": config.tenant_id,
                                "user_id": config.user_id,
                                "content": format!("load worker {} fact {}", worker_id, op_seq),
                                "content_type": "fact",
                                "memory_type": "semantic",
                                "privacy_level": "private",
                                "importance_score": 0.5,
                                "confidence_score": 0.5
                            }),
                        )
                    } else if op_mod < config.mix.ingest_pct + config.mix.retrieve_pct {
                        (
                            "nextral.memory.retrieve",
                            json!({
                                "tenant_id": config.tenant_id,
                                "user_id": config.user_id,
                                "query_text": format!("load query {}", op_seq),
                                "privacy_scope": ["private"]
                            }),
                        )
                    } else if op_mod < config.mix.ingest_pct + config.mix.retrieve_pct + config.mix.query_pct {
                        (
                            "nextral.memory.list",
                            json!({
                                "tenant_id": config.tenant_id,
                                "user_id": config.user_id,
                                "limit": 10
                            }),
                        )
                    } else {
                        (
                            "nextral.memory.forget",
                            json!({
                                "tenant_id": config.tenant_id,
                                "user_id": config.user_id,
                                "memory_id": format!("mem_{}_{}", worker_id, op_seq % 50),
                                "actor": config.user_id,
                                "reason": "load test forget",
                                "redact": false
                            }),
                        )
                    };

                    op_seq = op_seq.wrapping_add(1);
                    total_requests.fetch_add(1, Ordering::Relaxed);
                    let op_start = Instant::now();

                    let res = client
                        .call_json(tool, payload.to_string(), CallOptions::default())
                        .await;

                    let elapsed_ms = op_start.elapsed().as_secs_f64() * 1000.0;
                    {
                        let mut l = latencies.lock();
                        if l.len() < 100_000 {
                            l.push(elapsed_ms);
                        }
                    }

                    match res {
                        Ok(_) => {
                            successful_requests.fetch_add(1, Ordering::Relaxed);
                            let mut sc = status_counts.lock();
                            *sc.entry("200_ok".to_string()).or_insert(0) += 1;
                        }
                        Err(err) => {
                            failed_requests.fetch_add(1, Ordering::Relaxed);
                            let mut sc = status_counts.lock();
                            *sc.entry(err.code).or_insert(0) += 1;
                        }
                    }

                    if let Some(target_rps) = config.target_rps {
                        if target_rps > 0 {
                            let interval_micros = (1_000_000 * config.concurrency as u64) / (target_rps as u64);
                            tokio::time::sleep(Duration::from_micros(interval_micros)).await;
                        }
                    }
                }
            }));
        }

        // Wait for duration
        let remaining = deadline.saturating_duration_since(Instant::now());
        if !remaining.is_zero() {
            tokio::time::sleep(remaining).await;
        }
        stop.store(true, Ordering::Relaxed);

        for handle in handles {
            let _ = handle.await;
        }

        let elapsed_secs = start.elapsed().as_secs_f64().max(0.001);
        let total = total_requests.load(Ordering::Relaxed);
        let succ = successful_requests.load(Ordering::Relaxed);
        let fail = failed_requests.load(Ordering::Relaxed);

        let mut lat_vec = latencies.lock().clone();
        lat_vec.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let p50 = if lat_vec.is_empty() { 0.0 } else { lat_vec[(lat_vec.len() as f64 * 0.50) as usize] };
        let p95 = if lat_vec.is_empty() { 0.0 } else { lat_vec[(lat_vec.len() as f64 * 0.95).min((lat_vec.len() - 1) as f64) as usize] };
        let p99 = if lat_vec.is_empty() { 0.0 } else { lat_vec[(lat_vec.len() as f64 * 0.99).min((lat_vec.len() - 1) as f64) as usize] };

        let final_counts = status_counts.lock().clone();

        LoadReport {
            total_requests: total,
            successful_requests: succ,
            failed_requests: fail,
            latency_p50_ms: p50,
            latency_p95_ms: p95,
            latency_p99_ms: p99,
            achieved_rps: total as f64 / elapsed_secs,
            status_counts: final_counts,
        }
    }
}

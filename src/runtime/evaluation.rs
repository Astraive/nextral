use crate::contracts::{CoreError, CoreResult};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

const REPORT_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReminderOutcome {
    Completed,
    RetryScheduled,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExpectedOutcome {
    Retrieval {
        relevant_ids: Vec<String>,
        forbidden_ids: Vec<String>,
    },
    Contradiction {
        detected: bool,
    },
    Reminder {
        outcome: ReminderOutcome,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReplayCase {
    pub id: String,
    /// Host-owned input. Inputs and returned memory IDs are never copied into reports.
    pub request: serde_json::Value,
    pub expected: ExpectedOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GoldenSuite {
    pub id: String,
    pub revision: String,
    pub cases: Vec<ReplayCase>,
}

impl GoldenSuite {
    pub fn representative() -> CoreResult<Self> {
        Ok(serde_json::from_str(include_str!(
            "../../fixtures/evaluation/representative.json"
        ))?)
    }

    pub fn fingerprint(&self) -> CoreResult<String> {
        self.validate()?;
        Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(self)?)))
    }

    fn validate(&self) -> CoreResult<()> {
        if !label(&self.id) || !label(&self.revision) || self.cases.is_empty() {
            return invalid("suite requires labels, a revision and cases");
        }
        let mut ids = HashSet::new();
        let mut coverage = [false; 3];
        for case in &self.cases {
            if !label(&case.id) || !ids.insert(&case.id) || case.request.is_null() {
                return invalid("case labels must be unique and inputs must be present");
            }
            match &case.expected {
                ExpectedOutcome::Retrieval {
                    relevant_ids,
                    forbidden_ids,
                } => {
                    let relevant: HashSet<_> = relevant_ids.iter().collect();
                    let forbidden: HashSet<_> = forbidden_ids.iter().collect();
                    if relevant.is_empty()
                        || relevant.len() != relevant_ids.len()
                        || forbidden.len() != forbidden_ids.len()
                        || relevant.intersection(&forbidden).next().is_some()
                        || relevant
                            .iter()
                            .chain(forbidden.iter())
                            .any(|id| id.trim().is_empty())
                    {
                        return invalid("retrieval goldens require distinct nonempty relevant and forbidden IDs");
                    }
                    coverage[0] = true;
                }
                ExpectedOutcome::Contradiction { .. } => coverage[1] = true,
                ExpectedOutcome::Reminder { .. } => coverage[2] = true,
            }
        }
        if coverage.iter().any(|covered| !covered) {
            return invalid("suite must measure retrieval, contradiction and reminder outcomes");
        }
        Ok(())
    }
}

/// Actual host results. Destructive events must come from audited state changes,
/// not from severity labels; hosts must explicitly supply the counter in every result.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReplayOutcome {
    Retrieval {
        retrieved_ids: Vec<String>,
        destructive_events: u64,
    },
    Contradiction {
        detected: bool,
        destructive_events: u64,
    },
    Reminder {
        outcome: ReminderOutcome,
        destructive_events: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OutcomeObservation {
    Retrieval {
        expected: u64,
        matched: u64,
        returned: u64,
        forbidden: u64,
    },
    Contradiction {
        expected: bool,
        detected: bool,
    },
    Reminder {
        expected: ReminderOutcome,
        observed: ReminderOutcome,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CaseObservation {
    pub case_id: String,
    pub elapsed_ns: u64,
    pub destructive_events: u64,
    pub outcome: OutcomeObservation,
}

/// Identifiers must be non-sensitive labels, not memory contents, credentials or paths.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RunMetadata {
    pub run_id: String,
    pub implementation_revision: String,
    pub policy_version: String,
    pub host_id: String,
    pub latency_scope: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReportMetadata {
    pub schema_version: u32,
    pub suite_id: String,
    pub suite_revision: String,
    pub suite_sha256: String,
    pub run: RunMetadata,
    pub started_at_unix_ms: u64,
    pub build_profile: String,
    pub target_os: String,
    pub target_arch: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configuration_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct LatencySummary {
    pub samples: u64,
    pub mean_ns: f64,
    pub p95_ns: u64,
    pub max_ns: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EvaluationReport {
    pub metadata: ReportMetadata,
    pub observations: Vec<CaseObservation>,
    pub golden_recall_score: f32,
    pub contradiction_score: f32,
    pub reminder_outcome_score: f32,
    /// A caller-selected budget, not an assertion of a production SLO.
    pub latency_budget_ns: u64,
    pub latency: LatencySummary,
    pub latency_slo_passed: bool,
    pub destructive_events: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CanaryGateConfig {
    pub min_golden_recall: f32,
    pub min_contradiction_score: f32,
    pub min_reminder_outcome: f32,
    pub max_destructive_events: u64,
}

impl Default for CanaryGateConfig {
    fn default() -> Self {
        Self {
            min_golden_recall: 0.7,
            min_contradiction_score: 0.7,
            min_reminder_outcome: 0.7,
            max_destructive_events: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PromotionEvidence {
    pub baseline: EvaluationReport,
    pub candidate: EvaluationReport,
}

/// Replay each input once against a real host. Setup belongs outside this call unless
/// intentionally included in `latency_scope`. Host errors abort without a success report.
/// An observer can export these content-free measured counters to host telemetry.
pub fn run_golden_replay(
    suite: &GoldenSuite,
    metadata: RunMetadata,
    latency_budget_ns: u64,
    host: impl FnMut(&ReplayCase) -> CoreResult<ReplayOutcome>,
) -> CoreResult<EvaluationReport> {
    run_golden_replay_with_observer(suite, metadata, latency_budget_ns, host, |_| {})
}

pub fn run_golden_replay_with_observer(
    suite: &GoldenSuite,
    metadata: RunMetadata,
    latency_budget_ns: u64,
    mut host: impl FnMut(&ReplayCase) -> CoreResult<ReplayOutcome>,
    mut observer: impl FnMut(&CaseObservation),
) -> CoreResult<EvaluationReport> {
    let fingerprint = suite.fingerprint()?;
    validate_run_metadata(&metadata)?;
    if latency_budget_ns == 0 {
        return invalid("latency budget must be positive");
    }
    let started_at_unix_ms = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| CoreError::InvalidInput("system clock predates Unix epoch".into()))?
            .as_millis(),
    )
    .map_err(|_| CoreError::InvalidInput("system clock exceeds report range".into()))?;
    let mut observations = Vec::with_capacity(suite.cases.len());
    for case in &suite.cases {
        let start = Instant::now();
        let result = host(case)?;
        let elapsed_ns = u64::try_from(start.elapsed().as_nanos())
            .map_err(|_| CoreError::InvalidInput("elapsed time exceeds report range".into()))?;
        let observation = observe(case, result, elapsed_ns)?;
        observer(&observation);
        observations.push(observation);
    }
    let metadata = ReportMetadata {
        schema_version: REPORT_VERSION,
        suite_id: suite.id.clone(),
        suite_revision: suite.revision.clone(),
        suite_sha256: fingerprint,
        run: metadata,
        started_at_unix_ms,
        build_profile: if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        }
        .into(),
        target_os: std::env::consts::OS.into(),
        target_arch: std::env::consts::ARCH.into(),
        configuration_digest: None,
        stage: None,
    };
    summarize(metadata, observations, latency_budget_ns)
}

fn observe(
    case: &ReplayCase,
    result: ReplayOutcome,
    elapsed_ns: u64,
) -> CoreResult<CaseObservation> {
    let (outcome, destructive_events) = match (&case.expected, result) {
        (
            ExpectedOutcome::Retrieval {
                relevant_ids,
                forbidden_ids,
            },
            ReplayOutcome::Retrieval {
                retrieved_ids,
                destructive_events,
            },
        ) => {
            let returned: HashSet<_> = retrieved_ids.iter().collect();
            if returned.len() != retrieved_ids.len()
                || returned.iter().any(|id| id.trim().is_empty())
            {
                return invalid("host returned duplicate or empty retrieval IDs");
            }
            let matched = relevant_ids
                .iter()
                .filter(|id| returned.contains(id))
                .count() as u64;
            let forbidden = forbidden_ids
                .iter()
                .filter(|id| returned.contains(id))
                .count() as u64;
            (
                OutcomeObservation::Retrieval {
                    expected: relevant_ids.len() as u64,
                    matched,
                    returned: returned.len() as u64,
                    forbidden,
                },
                destructive_events,
            )
        }
        (
            ExpectedOutcome::Contradiction { detected: expected },
            ReplayOutcome::Contradiction {
                detected,
                destructive_events,
            },
        ) => (
            OutcomeObservation::Contradiction {
                expected: *expected,
                detected,
            },
            destructive_events,
        ),
        (
            ExpectedOutcome::Reminder { outcome: expected },
            ReplayOutcome::Reminder {
                outcome,
                destructive_events,
            },
        ) => (
            OutcomeObservation::Reminder {
                expected: expected.clone(),
                observed: outcome,
            },
            destructive_events,
        ),
        _ => return invalid("host result kind does not match golden case"),
    };
    Ok(CaseObservation {
        case_id: case.id.clone(),
        elapsed_ns,
        destructive_events,
        outcome,
    })
}

struct MeasuredMetrics {
    recall: f32,
    contradiction: f32,
    reminder: f32,
    latency: LatencySummary,
    destructive_events: u64,
}

pub fn summarize(
    metadata: ReportMetadata,
    observations: Vec<CaseObservation>,
    latency_budget_ns: u64,
) -> CoreResult<EvaluationReport> {
    let metrics = measured_metrics(&observations, latency_budget_ns)?;
    Ok(EvaluationReport {
        metadata,
        observations,
        golden_recall_score: metrics.recall,
        contradiction_score: metrics.contradiction,
        reminder_outcome_score: metrics.reminder,
        latency_slo_passed: metrics.latency.max_ns <= latency_budget_ns,
        latency_budget_ns,
        latency: metrics.latency,
        destructive_events: metrics.destructive_events,
    })
}

fn measured_metrics(
    observations: &[CaseObservation],
    latency_budget_ns: u64,
) -> CoreResult<MeasuredMetrics> {
    let mut expected_total = 0_u64;
    let mut matched_total = 0_u64;
    let mut contradictions = (0_u64, 0_u64);
    let mut reminders = (0_u64, 0_u64);
    let mut destructive_events = 0_u64;
    let mut samples = Vec::with_capacity(observations.len());
    let mut ids = HashSet::new();
    for observation in observations {
        if !label(&observation.case_id) || !ids.insert(&observation.case_id) {
            return invalid("observed case labels must be unique");
        }
        destructive_events = checked_add(destructive_events, observation.destructive_events)?;
        samples.push(observation.elapsed_ns);
        match &observation.outcome {
            OutcomeObservation::Retrieval {
                expected,
                matched,
                returned,
                forbidden,
            } => {
                if *expected == 0
                    || matched > expected
                    || matched > returned
                    || forbidden > returned
                    || *forbidden > returned.saturating_sub(*matched)
                {
                    return invalid("inconsistent retrieval counters");
                }
                expected_total = checked_add(expected_total, *expected)?;
                matched_total = checked_add(matched_total, *matched)?;
            }
            OutcomeObservation::Contradiction { expected, detected } => {
                contradictions.0 += u64::from(expected == detected);
                contradictions.1 += 1;
            }
            OutcomeObservation::Reminder { expected, observed } => {
                reminders.0 += u64::from(expected == observed);
                reminders.1 += 1;
            }
        }
    }
    if expected_total == 0 || contradictions.1 == 0 || reminders.1 == 0 || latency_budget_ns == 0 {
        return invalid("measured evidence must cover every outcome and a positive latency budget");
    }
    samples.sort_unstable();
    let sum: u128 = samples.iter().map(|sample| u128::from(*sample)).sum();
    let max_ns = samples[samples.len() - 1];
    let p95_index = samples.len() - samples.len() / 20 - 1;
    let latency = LatencySummary {
        samples: samples.len() as u64,
        mean_ns: sum as f64 / samples.len() as f64,
        p95_ns: samples[p95_index],
        max_ns,
    };
    Ok(MeasuredMetrics {
        recall: matched_total as f32 / expected_total as f32,
        contradiction: contradictions.0 as f32 / contradictions.1 as f32,
        reminder: reminders.0 as f32 / reminders.1 as f32,
        latency,
        destructive_events,
    })
}

impl EvaluationReport {
    /// Recompute all aggregates; missing, nonfinite or inconsistent evidence is invalid.
    pub fn validate(&self) -> CoreResult<()> {
        validate_run_metadata(&self.metadata.run)?;
        let metadata = &self.metadata;
        if metadata.schema_version != REPORT_VERSION
            || !label(&metadata.suite_id)
            || !label(&metadata.suite_revision)
            || metadata.suite_sha256.len() != 64
            || !metadata
                .suite_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || metadata.started_at_unix_ms == 0
            || !matches!(metadata.build_profile.as_str(), "debug" | "release")
            || !label(&metadata.target_os)
            || !label(&metadata.target_arch)
        {
            return invalid("invalid report metadata");
        }
        let metrics = measured_metrics(&self.observations, self.latency_budget_ns)?;
        if self.golden_recall_score != metrics.recall
            || self.contradiction_score != metrics.contradiction
            || self.reminder_outcome_score != metrics.reminder
            || self.latency != metrics.latency
            || self.destructive_events != metrics.destructive_events
            || self.latency_slo_passed != (metrics.latency.max_ns <= self.latency_budget_ns)
        {
            return invalid("report metrics do not match measured observations");
        }
        Ok(())
    }
}

pub fn canary_replay_gate(report: &EvaluationReport) -> bool {
    canary_replay_gate_with_config(report, &CanaryGateConfig::default())
}

pub fn canary_replay_gate_with_config(
    report: &EvaluationReport,
    config: &CanaryGateConfig,
) -> bool {
    report.validate().is_ok() && meets_thresholds(report, config)
}

fn meets_thresholds(report: &EvaluationReport, config: &CanaryGateConfig) -> bool {
    let valid_threshold = |value: f32| value.is_finite() && (0.0..=1.0).contains(&value);
    valid_threshold(config.min_golden_recall)
        && valid_threshold(config.min_contradiction_score)
        && valid_threshold(config.min_reminder_outcome)
        && report.destructive_events <= config.max_destructive_events
        && report.observations.iter().all(|observation|
            !matches!(&observation.outcome, OutcomeObservation::Retrieval { forbidden, .. } if *forbidden > 0))
        && report.golden_recall_score >= config.min_golden_recall
        && report.contradiction_score >= config.min_contradiction_score
        && report.reminder_outcome_score >= config.min_reminder_outcome
        && report.latency_slo_passed
}

/// Promotion requires comparable measured baseline and candidate reports, with no
/// outcome, destructive-event or tail-latency regression. Debug timings are not promotion evidence.
pub fn promotion_replay_gate(evidence: &PromotionEvidence) -> CoreResult<()> {
    let baseline = &evidence.baseline;
    let candidate = &evidence.candidate;
    baseline.validate()?;
    candidate.validate()?;
    let left = &baseline.metadata;
    let right = &candidate.metadata;
    if left.suite_sha256 != right.suite_sha256
        || left.suite_id != right.suite_id
        || left.suite_revision != right.suite_revision
        || left.run.host_id != right.run.host_id
        || left.run.latency_scope != right.run.latency_scope
        || left.target_os != right.target_os
        || left.target_arch != right.target_arch
        || left.build_profile != "release"
        || right.build_profile != "release"
        || left.run.run_id == right.run.run_id
        || baseline.latency_budget_ns != candidate.latency_budget_ns
        || baseline.observations.len() != candidate.observations.len()
    {
        return invalid(
            "promotion reports must be distinct comparable release replays of the same suite",
        );
    }
    if let (Some(stage_left), Some(stage_right)) = (&left.stage, &right.stage) {
        if stage_left == "calibration" || stage_right == "calibration" {
            return invalid(
                "calibration replay cannot be used as promotion qualification evidence; qualification stage required",
            );
        }
    }
    if let (Some(conf_left), Some(conf_right)) =
        (&left.configuration_digest, &right.configuration_digest)
    {
        if conf_left != conf_right {
            return invalid(
                "promotion evidence reports must match the same measured system configuration",
            );
        }
    }
    if !meets_thresholds(candidate, &CanaryGateConfig::default())
        || candidate.golden_recall_score < baseline.golden_recall_score
        || candidate.contradiction_score < baseline.contradiction_score
        || candidate.reminder_outcome_score < baseline.reminder_outcome_score
        || candidate.destructive_events > baseline.destructive_events
        || candidate.latency.p95_ns > baseline.latency.p95_ns
        || candidate.latency.max_ns > baseline.latency.max_ns
    {
        return Err(CoreError::Conflict(
            "measured replay safety, quality or latency gate rejected candidate".into(),
        ));
    }
    for (before, after) in baseline.observations.iter().zip(&candidate.observations) {
        let no_regression = match (&before.outcome, &after.outcome) {
            (
                OutcomeObservation::Retrieval {
                    expected: a,
                    matched: b,
                    ..
                },
                OutcomeObservation::Retrieval {
                    expected: c,
                    matched: d,
                    ..
                },
            ) => a == c && d >= b,
            (
                OutcomeObservation::Contradiction {
                    expected: a,
                    detected: b,
                },
                OutcomeObservation::Contradiction {
                    expected: c,
                    detected: d,
                },
            ) => a == c && (a != b || c == d),
            (
                OutcomeObservation::Reminder {
                    expected: a,
                    observed: b,
                },
                OutcomeObservation::Reminder {
                    expected: c,
                    observed: d,
                },
            ) => a == c && (a != b || c == d),
            _ => false,
        };
        if before.case_id != after.case_id
            || !no_regression
            || after.destructive_events > before.destructive_events
        {
            return Err(CoreError::Conflict(
                "measured replay case regressed or changed its golden expectation".into(),
            ));
        }
    }
    Ok(())
}

fn validate_run_metadata(metadata: &RunMetadata) -> CoreResult<()> {
    if [
        &metadata.run_id,
        &metadata.implementation_revision,
        &metadata.policy_version,
        &metadata.host_id,
        &metadata.latency_scope,
    ]
    .iter()
    .any(|value| !label(value))
    {
        return invalid(
            "run metadata requires non-sensitive ASCII labels (letters, digits, '.', '_' or '-')",
        );
    }
    Ok(())
}

fn label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn checked_add(left: u64, right: u64) -> CoreResult<u64> {
    left.checked_add(right)
        .ok_or_else(|| CoreError::InvalidInput("measured counter overflow".into()))
}

fn invalid<T>(message: &str) -> CoreResult<T> {
    Err(CoreError::InvalidInput(message.into()))
}

#[cfg(test)]
pub(crate) fn deterministic_test_evidence() -> PromotionEvidence {
    // Fixed observations exercise gate arithmetic, never performance claims.
    let observations = vec![
        CaseObservation {
            case_id: "recall".into(),
            elapsed_ns: 100,
            destructive_events: 0,
            outcome: OutcomeObservation::Retrieval {
                expected: 4,
                matched: 4,
                returned: 4,
                forbidden: 0,
            },
        },
        CaseObservation {
            case_id: "contradiction".into(),
            elapsed_ns: 200,
            destructive_events: 0,
            outcome: OutcomeObservation::Contradiction {
                expected: true,
                detected: true,
            },
        },
        CaseObservation {
            case_id: "reminder".into(),
            elapsed_ns: 300,
            destructive_events: 0,
            outcome: OutcomeObservation::Reminder {
                expected: ReminderOutcome::Completed,
                observed: ReminderOutcome::Completed,
            },
        },
    ];
    let metadata = ReportMetadata {
        schema_version: REPORT_VERSION,
        suite_id: "gate-fixture".into(),
        suite_revision: "v1".into(),
        suite_sha256: format!("{:x}", Sha256::digest(b"deterministic-gate-fixture")),
        run: RunMetadata {
            run_id: "baseline".into(),
            implementation_revision: "revision-a".into(),
            policy_version: "policy-v1".into(),
            host_id: "fixture-host".into(),
            latency_scope: "fixture".into(),
        },
        started_at_unix_ms: 1,
        build_profile: "release".into(),
        target_os: "test".into(),
        target_arch: "test".into(),
        configuration_digest: None,
        stage: None,
    };
    let baseline = summarize(metadata, observations, 1_000).unwrap();
    let mut candidate = baseline.clone();
    candidate.metadata.run.run_id = "candidate".into();
    candidate.metadata.run.policy_version = "policy-v2".into();
    PromotionEvidence {
        baseline,
        candidate,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rebuild(report: EvaluationReport) -> EvaluationReport {
        summarize(
            report.metadata,
            report.observations,
            report.latency_budget_ns,
        )
        .unwrap()
    }

    #[test]
    fn golden_replay_measures_runtime_results_and_content_free_observations() {
        use crate::{
            domain::{graphify_record, GraphHint, ReminderKind, ReminderRecord, ReminderStatus},
            memory::{ContentType, MemoryRecord, MemoryType, SourceType},
            runtime::retrieval::{retrieve, RetrievalRequest},
            store::MemoryIndexStore,
            testkit::TestMemoryStore,
        };
        let suite = GoldenSuite {
            id: "runtime".into(),
            revision: "v1".into(),
            cases: vec![
                ReplayCase {
                    id: "recall".into(),
                    request: serde_json::json!({"query":"PostgreSQL"}),
                    expected: ExpectedOutcome::Retrieval {
                        relevant_ids: vec!["golden-hit".into()],
                        forbidden_ids: vec!["golden-miss".into()],
                    },
                },
                ReplayCase {
                    id: "contradiction".into(),
                    request: serde_json::json!({"operation":"graphify"}),
                    expected: ExpectedOutcome::Contradiction { detected: true },
                },
                ReplayCase {
                    id: "reminder".into(),
                    request: serde_json::json!({"operation":"transition"}),
                    expected: ExpectedOutcome::Reminder {
                        outcome: ReminderOutcome::Completed,
                    },
                },
            ],
        };
        let mut store = TestMemoryStore::new();
        store
            .upsert_memory(MemoryRecord::new(
                "golden-hit",
                "tenant",
                "user",
                "PostgreSQL durable memory",
                ContentType::Fact,
                MemoryType::Semantic,
                SourceType::Manual,
            ))
            .unwrap();
        store
            .upsert_memory(MemoryRecord::new(
                "golden-miss",
                "tenant",
                "user",
                "Water garden",
                ContentType::Fact,
                MemoryType::Semantic,
                SourceType::Manual,
            ))
            .unwrap();
        let metadata = RunMetadata {
            run_id: "runtime-test".into(),
            implementation_revision: "test".into(),
            policy_version: "policy-v1".into(),
            host_id: "test".into(),
            latency_scope: "runtime-call".into(),
        };
        let mut exported = Vec::new();
        let report = run_golden_replay_with_observer(
            &suite,
            metadata,
            u64::MAX,
            |case| match case.id.as_str() {
                "recall" => {
                    let response = retrieve(
                        &mut store,
                        RetrievalRequest::test(
                            "tenant",
                            "user",
                            case.request["query"].as_str().unwrap(),
                        ),
                    )?;
                    Ok(ReplayOutcome::Retrieval {
                        retrieved_ids: response
                            .items
                            .into_iter()
                            .map(|item| item.memory_id)
                            .collect(),
                        destructive_events: store
                            .memories
                            .iter()
                            .filter(|record| record.status != crate::memory::MemoryStatus::Active)
                            .count() as u64,
                    })
                }
                "contradiction" => {
                    let record = &store.memories[0];
                    let hints = ["USES", "NOT_USES"].map(|relation| GraphHint {
                        from_label: "Project".into(),
                        from_name: "Atlas".into(),
                        relationship_type: relation.into(),
                        to_label: "Technology".into(),
                        to_name: "PostgreSQL".into(),
                        confidence: 1.0,
                    });
                    let result = graphify_record(record, &hints)?;
                    Ok(ReplayOutcome::Contradiction {
                        detected: !result.contradictions.is_empty(),
                        destructive_events: 0,
                    })
                }
                "reminder" => {
                    let mut reminder = ReminderRecord::new(
                        "tenant",
                        "user",
                        "golden-hit",
                        ReminderKind::Task,
                        "Review database",
                        "2000000000",
                        "UTC",
                    )?;
                    for status in [
                        ReminderStatus::Due,
                        ReminderStatus::Dispatched,
                        ReminderStatus::Completed,
                    ] {
                        reminder.transition(status, "test-host", "observed transition")?;
                    }
                    Ok(ReplayOutcome::Reminder {
                        outcome: if reminder.status == ReminderStatus::Completed {
                            ReminderOutcome::Completed
                        } else {
                            ReminderOutcome::Failed
                        },
                        destructive_events: 0,
                    })
                }
                _ => unreachable!(),
            },
            |observation| exported.push(observation.clone()),
        )
        .unwrap();
        assert_eq!(
            (
                report.golden_recall_score,
                report.contradiction_score,
                report.reminder_outcome_score,
                report.latency.samples,
                exported.len()
            ),
            (1.0, 1.0, 1.0, 3, 3)
        );
        assert!(report.validate().is_ok());
        let json = serde_json::to_string(&report).unwrap();
        assert!(
            !json.contains("PostgreSQL")
                && !json.contains("golden-hit")
                && !json.contains("Water garden")
        );
    }

    #[test]
    fn replay_aborts_on_host_failure_without_inventing_results() {
        let suite = GoldenSuite::representative().unwrap();
        let metadata = deterministic_test_evidence().baseline.metadata.run;
        assert!(run_golden_replay(&suite, metadata, 100, |_| invalid("host unavailable")).is_err());
    }

    #[test]
    fn replay_rejects_wrong_outcome_kind() {
        let suite = GoldenSuite::representative().unwrap();
        let metadata = deterministic_test_evidence().baseline.metadata.run;
        assert!(run_golden_replay(&suite, metadata, 100, |_| Ok(
            ReplayOutcome::Contradiction {
                detected: true,
                destructive_events: 0
            }
        ))
        .is_err());
    }

    #[test]
    fn gates_reject_nonfinite_or_fabricated_aggregates() {
        for value in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY, -0.1, 1.1, 0.8] {
            let mut report = deterministic_test_evidence().candidate;
            report.golden_recall_score = value;
            assert!(
                !canary_replay_gate(&report),
                "invalid aggregate accepted: {value}"
            );
        }
    }

    #[test]
    fn gates_reject_invalid_thresholds() {
        let report = deterministic_test_evidence().candidate;
        let config = CanaryGateConfig {
            min_golden_recall: f32::NAN,
            ..CanaryGateConfig::default()
        };
        assert!(!canary_replay_gate_with_config(&report, &config));
    }

    #[test]
    fn gates_reject_missing_measurements() {
        let mut report = deterministic_test_evidence().candidate;
        report.observations.clear();
        assert!(!canary_replay_gate(&report));
    }

    #[test]
    fn gates_reject_inconsistent_observation_counters() {
        let mut report = deterministic_test_evidence().candidate;
        report.observations[0].outcome = OutcomeObservation::Retrieval {
            expected: 4,
            matched: 5,
            returned: 4,
            forbidden: 0,
        };
        assert!(!canary_replay_gate(&report));
    }

    #[test]
    fn gates_reject_unsafe_retrieval_even_when_audit_counter_is_zero() {
        let mut report = deterministic_test_evidence().candidate;
        report.observations[0].outcome = OutcomeObservation::Retrieval {
            expected: 4,
            matched: 4,
            returned: 5,
            forbidden: 1,
        };
        assert!(!canary_replay_gate(&rebuild(report)));
    }

    #[test]
    fn gates_use_actual_latency_budget() {
        let mut report = deterministic_test_evidence().candidate;
        report.latency_budget_ns = 299;
        assert!(!canary_replay_gate(&rebuild(report)));
    }

    #[test]
    fn gate_accepts_safe_nonregressing_measured_evidence() {
        assert!(promotion_replay_gate(&deterministic_test_evidence()).is_ok());
    }

    #[test]
    fn promotion_rejects_recall_regression_even_above_absolute_threshold() {
        let mut evidence = deterministic_test_evidence();
        evidence.candidate.observations[0].outcome = OutcomeObservation::Retrieval {
            expected: 4,
            matched: 3,
            returned: 3,
            forbidden: 0,
        };
        evidence.candidate = rebuild(evidence.candidate);
        assert!(promotion_replay_gate(&evidence).is_err());
    }

    #[test]
    fn promotion_rejects_tail_latency_regression() {
        let mut evidence = deterministic_test_evidence();
        evidence.candidate.observations[2].elapsed_ns += 1;
        evidence.candidate = rebuild(evidence.candidate);
        assert!(promotion_replay_gate(&evidence).is_err());
    }

    #[test]
    fn promotion_rejects_destructive_events() {
        let mut evidence = deterministic_test_evidence();
        evidence.candidate.observations[0].destructive_events = 1;
        evidence.candidate = rebuild(evidence.candidate);
        assert!(promotion_replay_gate(&evidence).is_err());
    }

    #[test]
    fn promotion_rejects_noncomparable_suites() {
        let mut evidence = deterministic_test_evidence();
        evidence.candidate.metadata.suite_revision = "v2".into();
        assert!(promotion_replay_gate(&evidence).is_err());
    }

    #[test]
    fn promotion_rejects_debug_timings() {
        let mut evidence = deterministic_test_evidence();
        evidence.candidate.metadata.build_profile = "debug".into();
        assert!(promotion_replay_gate(&evidence).is_err());
    }

    #[test]
    fn deserialization_rejects_unmeasured_legacy_reports() {
        assert!(
            serde_json::from_value::<EvaluationReport>(serde_json::json!({
                "golden_recall_score": 0.9, "contradiction_score": 0.9,
                "reminder_outcome_score": 0.9, "latency_slo_passed": true, "destructive_events": 0
            }))
            .is_err()
        );
    }
}

#[cfg(test)]
mod unsafe_evidence_tests {
    use super::*;

    #[test]
    fn promotion_rejects_contradiction_regression() {
        let mut evidence = deterministic_test_evidence();
        evidence.candidate.observations[1].outcome = OutcomeObservation::Contradiction {
            expected: true,
            detected: false,
        };
        evidence.candidate = summarize(
            evidence.candidate.metadata,
            evidence.candidate.observations,
            evidence.candidate.latency_budget_ns,
        )
        .unwrap();
        assert!(promotion_replay_gate(&evidence).is_err());
    }

    #[test]
    fn promotion_rejects_reminder_regression() {
        let mut evidence = deterministic_test_evidence();
        evidence.candidate.observations[2].outcome = OutcomeObservation::Reminder {
            expected: ReminderOutcome::Completed,
            observed: ReminderOutcome::RetryScheduled,
        };
        evidence.candidate = summarize(
            evidence.candidate.metadata,
            evidence.candidate.observations,
            evidence.candidate.latency_budget_ns,
        )
        .unwrap();
        assert!(promotion_replay_gate(&evidence).is_err());
    }

    #[test]
    fn report_validation_rejects_nonfinite_latency() {
        let mut report = deterministic_test_evidence().candidate;
        report.latency.mean_ns = f64::INFINITY;
        assert!(report.validate().is_err());
    }

    #[test]
    fn report_validation_rejects_counter_overflow() {
        let mut report = deterministic_test_evidence().candidate;
        report.observations[0].destructive_events = u64::MAX;
        report.observations[1].destructive_events = 1;
        assert!(report.validate().is_err());
    }

    #[test]
    fn report_validation_rejects_duplicate_case_labels() {
        let mut report = deterministic_test_evidence().candidate;
        report.observations[1].case_id = report.observations[0].case_id.clone();
        assert!(report.validate().is_err());
    }

    #[test]
    fn replay_rejects_duplicate_retrieval_ids() {
        let case = ReplayCase {
            id: "recall".into(),
            request: serde_json::json!({}),
            expected: ExpectedOutcome::Retrieval {
                relevant_ids: vec!["record".into()],
                forbidden_ids: vec![],
            },
        };
        let result = ReplayOutcome::Retrieval {
            retrieved_ids: vec!["record".into(), "record".into()],
            destructive_events: 0,
        };
        assert!(observe(&case, result, 1).is_err());
    }
}

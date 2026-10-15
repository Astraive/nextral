use crate::runtime::intelligence::Severity;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EvaluationReport {
    pub golden_recall_score: f32,
    pub contradiction_score: f32,
    pub reminder_outcome_score: f32,
    pub latency_slo_passed: bool,
    pub destructive_events: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CanaryGateConfig {
    pub min_golden_recall: f32,
    pub min_contradiction_score: f32,
    pub min_reminder_outcome: f32,
    pub max_destructive_events: usize,
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

pub fn canary_replay_gate(report: &EvaluationReport) -> bool {
    canary_replay_gate_with_config(report, &CanaryGateConfig::default())
}

pub fn canary_replay_gate_with_config(
    report: &EvaluationReport,
    config: &CanaryGateConfig,
) -> bool {
    report.destructive_events <= config.max_destructive_events
        && report.golden_recall_score >= config.min_golden_recall
        && report.contradiction_score >= config.min_contradiction_score
        && report.reminder_outcome_score >= config.min_reminder_outcome
        && report.latency_slo_passed
}

pub fn report_from_severity(severity: &Severity) -> EvaluationReport {
    match severity {
        Severity::Destructive => EvaluationReport {
            golden_recall_score: 0.2,
            contradiction_score: 0.1,
            reminder_outcome_score: 0.3,
            latency_slo_passed: false,
            destructive_events: 1,
        },
        Severity::Warning => EvaluationReport {
            golden_recall_score: 0.6,
            contradiction_score: 0.6,
            reminder_outcome_score: 0.6,
            latency_slo_passed: true,
            destructive_events: 0,
        },
        Severity::Success => EvaluationReport {
            golden_recall_score: 0.9,
            contradiction_score: 0.9,
            reminder_outcome_score: 0.9,
            latency_slo_passed: true,
            destructive_events: 0,
        },
        Severity::Info => EvaluationReport {
            golden_recall_score: 0.75,
            contradiction_score: 0.75,
            reminder_outcome_score: 0.75,
            latency_slo_passed: true,
            destructive_events: 0,
        },
    }
}

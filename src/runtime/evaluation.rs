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

pub fn canary_replay_gate(report: &EvaluationReport) -> bool {
    report.destructive_events == 0
        && report.golden_recall_score >= 0.7
        && report.contradiction_score >= 0.7
        && report.reminder_outcome_score >= 0.7
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

use crate::memory::deterministic_id;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeLane {
    Stable,
    Canary,
    Sandbox,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Info,
    Success,
    Warning,
    Destructive,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum DecisionAction {
    Continue,
    RecordImprovement,
    Constrain,
    AutoRollbackAndQuarantine,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemoryQualitySnapshot {
    pub write_precision: f32,
    pub retrieval_usefulness: f32,
    pub contradiction_rate: f32,
    pub recall_stability: f32,
    pub overall: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExperimentStatus {
    Created,
    Promoted,
    RolledBack,
    Quarantined,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExperimentCandidate {
    pub id: String,
    pub lane: RuntimeLane,
    pub policy_version: String,
    pub description: String,
    pub confidence: f32,
    pub status: ExperimentStatus,
    pub rollback_id: Option<String>,
    pub last_severity: Severity,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExperimentRegistry {
    pub current_lane: RuntimeLane,
    pub active_policy_version: String,
    pub experiments: HashMap<String, ExperimentCandidate>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SafetyPolicy {
    pub actions: HashMap<Severity, DecisionAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemoryQualityController;

impl MemoryQualityController {
    pub fn score(
        write_precision: f32,
        retrieval_usefulness: f32,
        contradiction_rate: f32,
        recall_stability: f32,
    ) -> MemoryQualitySnapshot {
        let contradiction_component = (1.0 - contradiction_rate).clamp(0.0, 1.0);
        let overall = (0.35 * write_precision)
            + (0.35 * retrieval_usefulness)
            + (0.2 * contradiction_component)
            + (0.1 * recall_stability);
        MemoryQualitySnapshot {
            write_precision,
            retrieval_usefulness,
            contradiction_rate,
            recall_stability,
            overall: overall.clamp(0.0, 1.0),
        }
    }
}

impl Default for SafetyPolicy {
    fn default() -> Self {
        let mut actions = HashMap::new();
        actions.insert(Severity::Info, DecisionAction::Continue);
        actions.insert(Severity::Success, DecisionAction::RecordImprovement);
        actions.insert(Severity::Warning, DecisionAction::Constrain);
        actions.insert(
            Severity::Destructive,
            DecisionAction::AutoRollbackAndQuarantine,
        );
        Self { actions }
    }
}

impl Default for ExperimentRegistry {
    fn default() -> Self {
        Self {
            current_lane: RuntimeLane::Stable,
            active_policy_version: "policy-v1".to_string(),
            experiments: HashMap::new(),
        }
    }
}

impl ExperimentRegistry {
    pub fn create(
        &mut self,
        lane: RuntimeLane,
        policy_version: String,
        description: String,
    ) -> ExperimentCandidate {
        let id = deterministic_id(&[&policy_version, &description, "experiment"]);
        let candidate = ExperimentCandidate {
            id: id.clone(),
            lane,
            policy_version,
            description,
            confidence: 0.5,
            status: ExperimentStatus::Created,
            rollback_id: None,
            last_severity: Severity::Info,
        };
        self.experiments.insert(id.clone(), candidate.clone());
        candidate
    }

    pub fn promote(
        &mut self,
        experiment_id: &str,
        severity: Severity,
    ) -> Option<ExperimentCandidate> {
        let candidate = self.experiments.get_mut(experiment_id)?;
        candidate.last_severity = severity.clone();
        if severity == Severity::Destructive {
            candidate.status = ExperimentStatus::Quarantined;
            candidate.rollback_id = Some(deterministic_id(&[experiment_id, "rollback"]));
            return Some(candidate.clone());
        }
        candidate.status = ExperimentStatus::Promoted;
        self.active_policy_version = candidate.policy_version.clone();
        self.current_lane = candidate.lane.clone();
        candidate.confidence = (candidate.confidence + 0.2).clamp(0.0, 1.0);
        Some(candidate.clone())
    }

    pub fn rollback(&mut self, experiment_id: &str, reason: &str) -> Option<ExperimentCandidate> {
        let candidate = self.experiments.get_mut(experiment_id)?;
        candidate.status = ExperimentStatus::RolledBack;
        candidate.last_severity = Severity::Destructive;
        candidate.rollback_id = Some(deterministic_id(&[experiment_id, reason, "rollback"]));
        candidate.confidence = (candidate.confidence - 0.3).clamp(0.0, 1.0);
        self.current_lane = RuntimeLane::Stable;
        Some(candidate.clone())
    }

    pub fn status(&self, experiment_id: Option<&str>) -> serde_json::Value {
        if let Some(id) = experiment_id {
            return serde_json::to_value(self.experiments.get(id)).unwrap_or_else(|_| serde_json::json!(null));
        }
        serde_json::json!({
            "current_lane": self.current_lane,
            "active_policy_version": self.active_policy_version,
            "experiments": self.experiments,
        })
    }
}

pub fn classify_severity(
    quality_score: f32,
    contradiction_rate: f32,
    degraded: bool,
) -> Severity {
    if contradiction_rate > 0.4 || quality_score < 0.25 {
        Severity::Destructive
    } else if degraded || quality_score < 0.5 {
        Severity::Warning
    } else if quality_score > 0.8 {
        Severity::Success
    } else {
        Severity::Info
    }
}

pub fn decision_for(policy: &SafetyPolicy, severity: &Severity) -> DecisionAction {
    policy
        .actions
        .get(severity)
        .cloned()
        .unwrap_or(DecisionAction::Constrain)
}

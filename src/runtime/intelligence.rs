use crate::{
    contracts::{CoreError, CoreResult},
    memory::deterministic_id,
    runtime::evaluation::{promotion_replay_gate, PromotionEvidence},
    scoring::normalize_score,
};
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeSnapshot {
    pub lane: RuntimeLane,
    pub policy_version: String,
    pub experiment_id: Option<String>,
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
    #[serde(default)]
    pub promoted_report_run_id: Option<String>,
    pub rollback_target: Option<RuntimeSnapshot>,
    pub rollback_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExperimentRegistry {
    pub current_lane: RuntimeLane,
    pub active_policy_version: String,
    pub active_experiment_id: Option<String>,
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
        let write_precision = normalize_score(write_precision);
        let retrieval_usefulness = normalize_score(retrieval_usefulness);
        let contradiction_rate = if contradiction_rate.is_finite() {
            normalize_score(contradiction_rate)
        } else {
            1.0
        };
        let recall_stability = normalize_score(recall_stability);
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
            active_experiment_id: None,
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
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let id = deterministic_id(&[
            &policy_version,
            &description,
            &crate::memory::now_timestamp(),
            &seq.to_string(),
            "experiment",
        ]);
        let candidate = ExperimentCandidate {
            id: id.clone(),
            lane,
            policy_version,
            description,
            confidence: 0.5,
            status: ExperimentStatus::Created,
            rollback_id: None,
            last_severity: Severity::Info,
            promoted_report_run_id: None,
            rollback_target: None,
            rollback_reason: None,
        };
        self.experiments.insert(id.clone(), candidate.clone());
        candidate
    }

    /// Promote only with comparable measured evidence for this candidate policy.
    pub fn promote(
        &mut self,
        experiment_id: &str,
        evidence: &PromotionEvidence,
    ) -> CoreResult<ExperimentCandidate> {
        let candidate = self
            .experiments
            .get(experiment_id)
            .ok_or_else(|| CoreError::NotFound(format!("experiment {experiment_id} not found")))?;
        if candidate.status != ExperimentStatus::Created {
            return Err(CoreError::Conflict(
                "only a newly created experiment can be promoted".into(),
            ));
        }
        if evidence.candidate.metadata.run.policy_version != candidate.policy_version
            || evidence.baseline.metadata.run.policy_version != self.active_policy_version
        {
            return Err(CoreError::Conflict(
                "replay policy versions do not match candidate and active policy".into(),
            ));
        }
        promotion_replay_gate(evidence)?;
        let candidate = self
            .experiments
            .get_mut(experiment_id)
            .ok_or_else(|| CoreError::NotFound(format!("experiment {experiment_id} not found")))?;
        candidate.last_severity = Severity::Success;
        candidate.status = ExperimentStatus::Promoted;
        candidate.promoted_report_run_id = Some(evidence.candidate.metadata.run.run_id.clone());
        candidate.confidence = evidence
            .candidate
            .golden_recall_score
            .min(evidence.candidate.contradiction_score)
            .min(evidence.candidate.reminder_outcome_score);
        candidate.rollback_target = Some(RuntimeSnapshot {
            lane: std::mem::replace(&mut self.current_lane, candidate.lane.clone()),
            policy_version: std::mem::replace(
                &mut self.active_policy_version,
                candidate.policy_version.clone(),
            ),
            experiment_id: self.active_experiment_id.replace(candidate.id.clone()),
        });
        Ok(candidate.clone())
    }

    pub fn rollback(
        &mut self,
        experiment_id: &str,
        reason: &str,
    ) -> CoreResult<ExperimentCandidate> {
        if reason.trim().is_empty() {
            return Err(CoreError::InvalidInput(
                "rollback reason is required".into(),
            ));
        }
        let candidate = self
            .experiments
            .get_mut(experiment_id)
            .ok_or_else(|| CoreError::NotFound(format!("experiment {experiment_id} not found")))?;
        if candidate.status != ExperimentStatus::Promoted
            || self.active_experiment_id.as_deref() != Some(experiment_id)
        {
            return Err(CoreError::Conflict(
                "only the active promoted experiment can be rolled back".into(),
            ));
        }
        let target = candidate
            .rollback_target
            .as_ref()
            .ok_or_else(|| CoreError::Conflict("experiment has no rollback point".into()))?;
        self.current_lane.clone_from(&target.lane);
        self.active_policy_version
            .clone_from(&target.policy_version);
        self.active_experiment_id.clone_from(&target.experiment_id);
        candidate.status = ExperimentStatus::RolledBack;
        candidate.last_severity = Severity::Destructive;
        candidate.rollback_id = Some(deterministic_id(&[experiment_id, reason, "rollback"]));
        candidate.rollback_reason = Some(reason.to_owned());
        candidate.confidence = (candidate.confidence - 0.3).clamp(0.0, 1.0);
        Ok(candidate.clone())
    }

    pub fn status(&self, experiment_id: Option<&str>) -> serde_json::Value {
        if let Some(id) = experiment_id {
            return serde_json::to_value(self.experiments.get(id))
                .unwrap_or(serde_json::Value::Null);
        }
        serde_json::json!({
            "current_lane": self.current_lane,
            "active_policy_version": self.active_policy_version,
            "active_experiment_id": self.active_experiment_id,
            "experiments": self.experiments,
        })
    }
}

pub fn classify_severity(quality_score: f32, contradiction_rate: f32, degraded: bool) -> Severity {
    if !quality_score.is_finite()
        || !contradiction_rate.is_finite()
        || !(0.0..=1.0).contains(&quality_score)
        || !(0.0..=1.0).contains(&contradiction_rate)
    {
        return Severity::Destructive;
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::evaluation::deterministic_test_evidence;

    #[test]
    fn promotion_records_measured_report_and_policy() {
        let mut registry = ExperimentRegistry::default();
        let candidate = registry.create(
            RuntimeLane::Canary,
            "policy-v2".into(),
            "Measured ranking change".into(),
        );
        let promoted = registry
            .promote(&candidate.id, &deterministic_test_evidence())
            .unwrap();
        assert_eq!(
            (
                promoted.status,
                promoted.promoted_report_run_id,
                registry.active_policy_version
            ),
            (
                ExperimentStatus::Promoted,
                Some("candidate".into()),
                "policy-v2".into()
            )
        );
    }

    #[test]
    fn promotion_rejects_report_for_another_policy_without_changing_active_policy() {
        let mut registry = ExperimentRegistry::default();
        let candidate = registry.create(
            RuntimeLane::Canary,
            "policy-v3".into(),
            "Unmeasured policy".into(),
        );
        assert!(registry
            .promote(&candidate.id, &deterministic_test_evidence())
            .is_err());
        assert_eq!(registry.active_policy_version, "policy-v1");
    }

    #[test]
    fn promotion_rejects_unsafe_evidence_without_changing_experiment_status() {
        let mut registry = ExperimentRegistry::default();
        let candidate = registry.create(
            RuntimeLane::Canary,
            "policy-v2".into(),
            "Unsafe policy".into(),
        );
        let mut evidence = deterministic_test_evidence();
        evidence.candidate.golden_recall_score = f32::NAN;
        assert!(registry.promote(&candidate.id, &evidence).is_err());
        assert_eq!(
            registry.experiments[&candidate.id].status,
            ExperimentStatus::Created
        );
    }

    #[test]
    fn promotion_rejects_unknown_experiment() {
        let mut registry = ExperimentRegistry::default();
        assert!(matches!(
            registry.promote("missing", &deterministic_test_evidence()),
            Err(CoreError::NotFound(_))
        ));
    }

    #[test]
    fn invalid_quality_is_destructive_not_successful() {
        assert_eq!(
            classify_severity(f32::NAN, 0.0, false),
            Severity::Destructive
        );
    }

    #[test]
    fn rollback_restores_the_previous_active_policy_and_lane() {
        let mut registry = ExperimentRegistry::default();
        let previous = (
            registry.active_policy_version.clone(),
            registry.current_lane.clone(),
        );
        let candidate = registry.create(
            RuntimeLane::Canary,
            "policy-v2".into(),
            "Measured candidate".into(),
        );
        registry
            .promote(&candidate.id, &deterministic_test_evidence())
            .unwrap();
        registry
            .rollback(&candidate.id, "Restore prior policy")
            .unwrap();
        assert_eq!(
            (registry.active_policy_version, registry.current_lane),
            previous
        );
    }

    #[test]
    fn rollback_of_an_unpromoted_candidate_cannot_change_the_active_runtime() {
        let mut registry = ExperimentRegistry::default();
        let active = registry.create(
            RuntimeLane::Canary,
            "policy-v2".into(),
            "Measured candidate".into(),
        );
        registry
            .promote(&active.id, &deterministic_test_evidence())
            .unwrap();
        let previous = (
            registry.active_policy_version.clone(),
            registry.current_lane.clone(),
        );
        let pending = registry.create(
            RuntimeLane::Sandbox,
            "unpromoted-policy".into(),
            "Unmeasured candidate".into(),
        );
        let _ = registry.rollback(&pending.id, "Cannot roll back an inactive candidate");
        assert_eq!(
            (registry.active_policy_version, registry.current_lane),
            previous
        );
        assert_eq!(
            registry.experiments[&pending.id].status,
            ExperimentStatus::Created
        );
    }
}

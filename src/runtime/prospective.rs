use crate::{
    contracts::CoreResult,
    runtime::reminders::{execute_due_reminders, ExecuteDueRemindersRequest, ExecuteDueRemindersResponse},
    store::ReminderStore,
};
use std::sync::Arc;
use tokio::{
    sync::{watch, Mutex},
    task::JoinHandle,
    time::{self, Duration},
};

pub struct ProspectiveScheduler<S>
where
    S: ReminderStore + Send + 'static,
{
    store: Arc<Mutex<S>>,
    tenant_id: String,
    user_id: String,
    actor: String,
    retry_delay_seconds: u64,
    poll_interval: Duration,
    stop_tx: watch::Sender<bool>,
}

impl<S> ProspectiveScheduler<S>
where
    S: ReminderStore + Send + 'static,
{
    pub fn new(
        store: Arc<Mutex<S>>,
        tenant_id: impl Into<String>,
        user_id: impl Into<String>,
        actor: impl Into<String>,
        retry_delay_seconds: u64,
        poll_interval_seconds: u64,
    ) -> Self {
        let (stop_tx, _stop_rx) = watch::channel(false);
        Self {
            store,
            tenant_id: tenant_id.into(),
            user_id: user_id.into(),
            actor: actor.into(),
            retry_delay_seconds,
            poll_interval: Duration::from_secs(poll_interval_seconds.max(1)),
            stop_tx,
        }
    }

    pub fn start(&self) -> JoinHandle<()> {
        let store = Arc::clone(&self.store);
        let tenant_id = self.tenant_id.clone();
        let user_id = self.user_id.clone();
        let actor = self.actor.clone();
        let retry_delay_seconds = self.retry_delay_seconds;
        let poll_interval = self.poll_interval;
        let mut stop_rx = self.stop_tx.subscribe();
        tokio::spawn(async move {
            let mut ticker = time::interval(poll_interval);
            loop {
                tokio::select! {
                    changed = stop_rx.changed() => {
                        if changed.is_ok() && *stop_rx.borrow() {
                            break;
                        }
                    }
                    _ = ticker.tick() => {
                        let due_now = crate::memory::now_timestamp();
                        let mut guard = store.lock().await;
                        let _ = execute_due_reminders(
                            &mut *guard,
                            ExecuteDueRemindersRequest {
                                tenant_id: tenant_id.clone(),
                                user_id: user_id.clone(),
                                due_at_or_before: due_now,
                                actor: actor.clone(),
                                retry_delay_seconds,
                                dispatch_policy_version: None,
                                retry_strategy_id: None,
                                trace_id: None,
                            },
                        );
                    }
                }
            }
        })
    }

    pub fn stop(&self) {
        let _ = self.stop_tx.send(true);
    }

    pub async fn tick_once(&self, due_at_or_before: &str) -> CoreResult<ExecuteDueRemindersResponse> {
        let mut guard = self.store.lock().await;
        execute_due_reminders(
            &mut *guard,
            ExecuteDueRemindersRequest {
                tenant_id: self.tenant_id.clone(),
                user_id: self.user_id.clone(),
                due_at_or_before: due_at_or_before.to_string(),
                actor: self.actor.clone(),
                retry_delay_seconds: self.retry_delay_seconds,
                dispatch_policy_version: None,
                retry_strategy_id: None,
                trace_id: None,
            },
        )
    }
}

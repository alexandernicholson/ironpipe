use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rebar::agent::{AgentRef, start_agent};
use rebar::runtime::Runtime;

use crate::task_id::TaskId;
use crate::xcom::XComStore;

/// Handle to a running `XCom` Agent.
///
/// Replaces the previous `GenServer`-based `XComActor` with Rebar's `Agent`,
/// which is purpose-built for shared state (no custom message types needed).
#[derive(Clone)]
pub struct XComAgent {
    agent: AgentRef,
}

impl XComAgent {
    /// Start a new `XCom` agent.
    pub async fn start(runtime: Arc<Runtime>) -> Self {
        let agent = start_agent(Arc::clone(&runtime), XComStore::new).await;
        Self { agent }
    }

    /// Push a value for a task.
    pub fn push(&self, task_id: TaskId, key: String, value: serde_json::Value) {
        self.agent
            .cast(move |store: &mut XComStore| {
                store.push(&task_id, &key, value);
            })
            .ok();
    }

    /// Pull a value for a task by key.
    pub async fn pull(
        &self,
        task_id: TaskId,
        key: String,
        timeout: Duration,
    ) -> Option<serde_json::Value> {
        self.agent
            .get(
                move |store: &XComStore| store.pull(&task_id, &key).cloned(),
                timeout,
            )
            .await
            .ok()
            .flatten()
    }

    /// Pull all values for a task.
    pub async fn pull_all(
        &self,
        task_id: TaskId,
        timeout: Duration,
    ) -> Option<HashMap<String, serde_json::Value>> {
        self.agent
            .get(
                move |store: &XComStore| store.pull_all(&task_id).cloned(),
                timeout,
            )
            .await
            .ok()
            .flatten()
    }

    /// Clear all data for a task.
    pub fn clear_task(&self, task_id: TaskId) {
        self.agent
            .cast(move |store: &mut XComStore| {
                store.clear_task(&task_id);
            })
            .ok();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn xcom_agent_push_and_pull() {
        let rt = Arc::new(Runtime::new(1));
        let xcom = XComAgent::start(Arc::clone(&rt)).await;

        xcom.push(TaskId::new("task_a"), "result".to_string(), json!(42));
        tokio::time::sleep(Duration::from_millis(10)).await;

        let val = xcom
            .pull(TaskId::new("task_a"), "result".to_string(), Duration::from_secs(1))
            .await;
        assert_eq!(val, Some(json!(42)));
    }

    #[tokio::test]
    async fn xcom_agent_pull_nonexistent() {
        let rt = Arc::new(Runtime::new(1));
        let xcom = XComAgent::start(Arc::clone(&rt)).await;

        let val = xcom
            .pull(TaskId::new("missing"), "x".to_string(), Duration::from_secs(1))
            .await;
        assert_eq!(val, None);
    }

    #[tokio::test]
    async fn xcom_agent_pull_all() {
        let rt = Arc::new(Runtime::new(1));
        let xcom = XComAgent::start(Arc::clone(&rt)).await;

        xcom.push(TaskId::new("task_a"), "k1".to_string(), json!("v1"));
        xcom.push(TaskId::new("task_a"), "k2".to_string(), json!("v2"));
        tokio::time::sleep(Duration::from_millis(10)).await;

        let all = xcom
            .pull_all(TaskId::new("task_a"), Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all.get("k1"), Some(&json!("v1")));
    }

    #[tokio::test]
    async fn xcom_agent_clear_task() {
        let rt = Arc::new(Runtime::new(1));
        let xcom = XComAgent::start(Arc::clone(&rt)).await;

        xcom.push(TaskId::new("task_a"), "k".to_string(), json!(1));
        tokio::time::sleep(Duration::from_millis(10)).await;

        xcom.clear_task(TaskId::new("task_a"));
        tokio::time::sleep(Duration::from_millis(10)).await;

        let val = xcom
            .pull(TaskId::new("task_a"), "k".to_string(), Duration::from_secs(1))
            .await;
        assert_eq!(val, None);
    }
}

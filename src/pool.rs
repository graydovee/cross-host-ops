use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use parking_lot::Mutex;
use tokio::sync::{Mutex as AsyncMutex, Notify, RwLock, mpsc::UnboundedSender};
use tracing::info;

use crate::config::AppConfig;
use crate::connection::{Connection, CopySpec, ResolvedTarget, connect};
use crate::protocol::PoolStatus;
use crate::protocol::ServerEvent;

#[derive(Clone)]
pub struct ConnectionPool {
    config: Arc<RwLock<AppConfig>>,
    pools: Arc<Mutex<HashMap<String, Arc<TargetPool>>>>,
}

impl ConnectionPool {
    pub fn new(config: Arc<RwLock<AppConfig>>) -> Self {
        Self {
            config,
            pools: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn execute(
        &self,
        target: ResolvedTarget,
        argv: Vec<String>,
        sender: UnboundedSender<ServerEvent>,
    ) -> Result<i32> {
        let pool = self.get_or_create_pool(target.clone());
        let slot = pool.acquire(self.config.clone()).await?;
        let result = async {
            let mut guard = slot.connection.connection.lock().await;
            if guard.is_none() {
                let config = self.config.read().await.clone();
                info!(target = %target.key, "opening new pooled SSH connection");
                *guard = Some(connect(&target, &config).await?);
            }
            let config = self.config.read().await.clone();
            let first_result = guard
                .as_mut()
                .expect("connection initialized")
                .execute(&argv, &sender, &config)
                .await;
            match first_result {
                Ok(code) => Ok(code),
                Err(error) if should_reconnect(&error.to_string()) => {
                    info!(target = %target.key, error = %error, "reopening stale pooled SSH connection");
                    *guard = None;
                    let config = self.config.read().await.clone();
                    *guard = Some(connect(&target, &config).await?);
                    guard
                        .as_mut()
                        .expect("connection reinitialized")
                        .execute(&argv, &sender, &config)
                        .await
                }
                Err(error) => Err(error),
            }
        }
        .await;
        pool.release(slot.id);
        result
    }

    pub async fn copy(&self, target: ResolvedTarget, spec: CopySpec) -> Result<()> {
        let pool = self.get_or_create_pool(target.clone());
        let slot = pool.acquire(self.config.clone()).await?;
        let result = async {
            let mut guard = slot.connection.connection.lock().await;
            if guard.is_none() {
                let config = self.config.read().await.clone();
                info!(target = %target.key, "opening new pooled SSH connection");
                *guard = Some(connect(&target, &config).await?);
            }
            let config = self.config.read().await.clone();
            guard
                .as_mut()
                .expect("connection initialized")
                .copy(&spec, &config)
                .await
        }
        .await;
        pool.release(slot.id);
        result
    }

    pub async fn prune_idle(&self) {
        let config = self.config.read().await.clone();
        let mut remove_keys = Vec::new();
        let pools = self.pools.lock();
        for (key, pool) in pools.iter() {
            pool.prune_idle(config.ssh.max_idle_time);
            if pool.is_empty() {
                remove_keys.push(key.clone());
            }
        }
        drop(pools);
        if !remove_keys.is_empty() {
            let mut pools = self.pools.lock();
            for key in remove_keys {
                pools.remove(&key);
            }
        }
    }

    pub fn status(&self) -> Vec<PoolStatus> {
        let pools = self.pools.lock();
        pools.values().map(|pool| pool.status()).collect()
    }

    fn get_or_create_pool(&self, target: ResolvedTarget) -> Arc<TargetPool> {
        let mut pools = self.pools.lock();
        pools
            .entry(target.key.clone())
            .or_insert_with(|| Arc::new(TargetPool::new(target)))
            .clone()
    }
}

struct TargetPool {
    target: ResolvedTarget,
    state: Mutex<TargetPoolState>,
    notify: Notify,
}

impl TargetPool {
    fn new(target: ResolvedTarget) -> Self {
        Self {
            target,
            state: Mutex::new(TargetPoolState {
                slots: Vec::new(),
                waiters: 0,
                next_id: 1,
            }),
            notify: Notify::new(),
        }
    }

    async fn acquire(&self, config: Arc<RwLock<AppConfig>>) -> Result<Lease> {
        loop {
            if let Some(lease) = self.try_acquire(&config).await? {
                return Ok(lease);
            }
            let waiter = {
                let mut state = self.state.lock();
                state.waiters += 1;
                self.notify.notified()
            };
            waiter.await;
            let mut state = self.state.lock();
            state.waiters = state.waiters.saturating_sub(1);
        }
    }

    async fn try_acquire(&self, config: &Arc<RwLock<AppConfig>>) -> Result<Option<Lease>> {
        let cfg = config.read().await.clone();
        self.prune_idle(cfg.ssh.max_idle_time);
        let mut create = None;
        {
            let mut state = self.state.lock();
            for slot in &mut state.slots {
                if !slot.busy {
                    slot.busy = true;
                    return Ok(Some(Lease {
                        id: slot.id,
                        connection: slot.connection.clone(),
                    }));
                }
            }
            if state.slots.len() < cfg.ssh.max_connections_per_ip {
                let id = state.next_id;
                state.next_id += 1;
                let connection = Arc::new(PooledConnection {
                    connection: AsyncMutex::new(None),
                });
                state.slots.push(SlotState {
                    id,
                    busy: true,
                    last_idle: Instant::now(),
                    connection: connection.clone(),
                });
                create = Some(Lease { id, connection });
            }
        }
        Ok(create)
    }

    fn release(&self, id: usize) {
        let mut state = self.state.lock();
        if let Some(slot) = state.slots.iter_mut().find(|slot| slot.id == id) {
            slot.busy = false;
            slot.last_idle = Instant::now();
        }
        self.notify.notify_one();
    }

    fn prune_idle(&self, max_idle_time: std::time::Duration) {
        let now = Instant::now();
        let mut state = self.state.lock();
        let target_key = self.target.key.clone();
        state.slots.retain(|slot| {
            let expired = !slot.busy && now.duration_since(slot.last_idle) >= max_idle_time;
            if expired {
                info!(target = %target_key, slot_id = slot.id, "closing idle pooled SSH connection");
            }
            !expired
        });
    }

    fn status(&self) -> PoolStatus {
        let state = self.state.lock();
        let total = state.slots.len();
        let busy = state.slots.iter().filter(|slot| slot.busy).count();
        let idle = total.saturating_sub(busy);
        PoolStatus {
            key: self.target.key.clone(),
            total,
            busy,
            idle,
            queued: state.waiters,
        }
    }

    fn is_empty(&self) -> bool {
        self.state.lock().slots.is_empty()
    }
}

struct TargetPoolState {
    slots: Vec<SlotState>,
    waiters: usize,
    next_id: usize,
}

struct SlotState {
    id: usize,
    busy: bool,
    last_idle: Instant,
    connection: Arc<PooledConnection>,
}

struct PooledConnection {
    connection: AsyncMutex<Option<Box<dyn Connection>>>,
}

struct Lease {
    id: usize,
    connection: Arc<PooledConnection>,
}

fn should_reconnect(error: &str) -> bool {
    let lowered = error.to_ascii_lowercase();
    lowered.contains("channel closed")
        || lowered.contains("closed unexpectedly")
        || lowered.contains("broken pipe")
        || lowered.contains("connection reset")
        || lowered.contains("connection aborted")
}

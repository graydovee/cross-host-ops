use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::{Mutex as AsyncMutex, Notify, OwnedSemaphorePermit, Semaphore, TryAcquireError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionStage {
    Connect,
    Prepare,
    Started,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetryDecision {
    Retry,
    DoNotRetry,
}

impl ConnectionStage {
    pub(crate) fn retry_decision(self) -> RetryDecision {
        match self {
            Self::Connect | Self::Prepare => RetryDecision::Retry,
            Self::Started => RetryDecision::DoNotRetry,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectionStatusSnapshot {
    pub(crate) key: String,
    pub(crate) generation: u64,
    pub(crate) active: usize,
    pub(crate) idle: usize,
    pub(crate) capacity: usize,
}

pub(crate) struct ManagedSingleton<T> {
    inner: AsyncMutex<Option<SingletonEntry<T>>>,
    next_generation: Mutex<u64>,
}

struct SingletonEntry<T> {
    resource: Arc<T>,
    generation: u64,
    created_at: Instant,
    last_used: Instant,
}

pub(crate) struct SingletonLease<T> {
    resource: Arc<T>,
    generation: u64,
}

// Clone does not require `T: Clone` — only the `Arc` is duplicated.
impl<T> Clone for SingletonLease<T> {
    fn clone(&self) -> Self {
        Self {
            resource: self.resource.clone(),
            generation: self.generation,
        }
    }
}

impl<T> ManagedSingleton<T> {
    pub(crate) fn new() -> Self {
        Self {
            inner: AsyncMutex::new(None),
            next_generation: Mutex::new(1),
        }
    }

    pub(crate) async fn checkout_or_insert_with<F, Fut, E>(
        &self,
        create: F,
    ) -> Result<SingletonLease<T>, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        let mut guard = self.inner.lock().await;
        if let Some(entry) = guard.as_mut() {
            entry.last_used = Instant::now();
            return Ok(SingletonLease {
                resource: entry.resource.clone(),
                generation: entry.generation,
            });
        }

        let resource = Arc::new(create().await?);
        let generation = self.allocate_generation();
        let now = Instant::now();
        *guard = Some(SingletonEntry {
            resource: resource.clone(),
            generation,
            created_at: now,
            last_used: now,
        });
        Ok(SingletonLease {
            resource,
            generation,
        })
    }

    pub(crate) async fn invalidate_generation(&self, generation: u64) -> bool {
        let mut guard = self.inner.lock().await;
        let should_clear = guard
            .as_ref()
            .map(|entry| entry.generation == generation)
            .unwrap_or(false);
        if should_clear {
            *guard = None;
        }
        should_clear
    }

    pub(crate) async fn current_generation(&self) -> Option<u64> {
        let guard = self.inner.lock().await;
        guard.as_ref().map(|entry| entry.generation)
    }

    pub(crate) async fn checkout_generation(&self, generation: u64) -> Option<SingletonLease<T>> {
        let mut guard = self.inner.lock().await;
        let entry = guard.as_mut()?;
        if entry.generation != generation {
            return None;
        }
        entry.last_used = Instant::now();
        Some(SingletonLease {
            resource: entry.resource.clone(),
            generation: entry.generation,
        })
    }

    pub(crate) async fn prune_idle(&self, max_idle: Duration) -> bool {
        let mut guard = self.inner.lock().await;
        let should_clear = guard
            .as_ref()
            .map(|entry| {
                Arc::strong_count(&entry.resource) == 1 && entry.last_used.elapsed() > max_idle
            })
            .unwrap_or(false);
        if should_clear {
            *guard = None;
        }
        should_clear
    }

    pub(crate) async fn status_snapshot(&self, key: impl Into<String>) -> ConnectionStatusSnapshot {
        let guard = self.inner.lock().await;
        if let Some(entry) = guard.as_ref() {
            ConnectionStatusSnapshot {
                key: key.into(),
                generation: entry.generation,
                active: Arc::strong_count(&entry.resource).saturating_sub(1),
                idle: 1,
                capacity: 1,
            }
        } else {
            ConnectionStatusSnapshot {
                key: key.into(),
                generation: 0,
                active: 0,
                idle: 0,
                capacity: 1,
            }
        }
    }

    fn allocate_generation(&self) -> u64 {
        let mut next = self.next_generation.lock();
        let generation = *next;
        *next = next.saturating_add(1);
        generation
    }
}

impl<T> SingletonLease<T> {
    pub(crate) fn resource(&self) -> Arc<T> {
        self.resource.clone()
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }
}

pub(crate) struct ManagedPool<K, T> {
    inner: Arc<Mutex<HashMap<K, PoolState<T>>>>,
    next_generation: Mutex<u64>,
    max_idle: Duration,
    capacity: usize,
}

struct PoolState<T> {
    semaphore: Arc<Semaphore>,
    notify: Arc<Notify>,
    idle: Vec<PoolEntry<T>>,
    active: usize,
    latest_generation: u64,
}

struct PoolEntry<T> {
    resource: T,
    generation: u64,
    created_at: Instant,
    last_used: Instant,
    permit: OwnedSemaphorePermit,
}

pub(crate) struct PoolLease<K: Eq + Hash, T> {
    key: K,
    resource: Option<T>,
    generation: u64,
    permit: Option<OwnedSemaphorePermit>,
    inner: Arc<Mutex<HashMap<K, PoolState<T>>>>,
}

impl<K, T> ManagedPool<K, T>
where
    K: Clone + Eq + Hash,
{
    pub(crate) fn new(capacity: usize, max_idle: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            next_generation: Mutex::new(1),
            max_idle,
            capacity: capacity.max(1),
        }
    }

    pub(crate) async fn checkout_or_create_with<F, Fut, E>(
        &self,
        key: K,
        create: F,
    ) -> Result<PoolLease<K, T>, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        let mut create = Some(create);
        loop {
            if let Some(lease) = self.checkout_idle(&key) {
                return Ok(lease);
            }

            let (semaphore, notify) = self.pool_handles(&key);
            match semaphore.clone().try_acquire_owned() {
                Ok(permit) => {
                    self.mark_active(&key);
                    let create = create
                        .take()
                        .expect("managed pool create callback consumed before use");
                    match create().await {
                        Ok(resource) => {
                            let generation = self.allocate_generation();
                            self.record_generation(&key, generation);
                            return Ok(PoolLease {
                                key,
                                resource: Some(resource),
                                generation,
                                permit: Some(permit),
                                inner: self.inner.clone(),
                            });
                        }
                        Err(error) => {
                            self.unmark_active_and_notify(&key);
                            drop(permit);
                            return Err(error);
                        }
                    }
                }
                Err(TryAcquireError::NoPermits) => {
                    let notified = notify.notified();
                    if let Some(lease) = self.checkout_idle(&key) {
                        return Ok(lease);
                    }
                    if semaphore.available_permits() > 0 {
                        continue;
                    }
                    notified.await;
                }
                Err(TryAcquireError::Closed) => {
                    unreachable!("managed pool semaphore is never closed");
                }
            }
        }
    }

    pub(crate) fn return_healthy(&self, mut lease: PoolLease<K, T>) {
        let Some(resource) = lease.resource.take() else {
            return;
        };
        let permit = lease
            .permit
            .take()
            .expect("managed pool lease missing capacity permit");
        let now = Instant::now();
        let mut inner = self.inner.lock();
        let state = inner
            .entry(lease.key.clone())
            .or_insert_with(|| PoolState::new(self.capacity));
        state.active = state.active.saturating_sub(1);
        state.latest_generation = state.latest_generation.max(lease.generation);
        state.idle.push(PoolEntry {
            resource,
            generation: lease.generation,
            created_at: now,
            last_used: now,
            permit,
        });
        state.notify.notify_one();
    }

    pub(crate) fn invalidate_generation(&self, key: &K, generation: u64) -> bool {
        let mut inner = self.inner.lock();
        let Some(state) = inner.get_mut(key) else {
            return false;
        };
        let before = state.idle.len();
        state.idle.retain(|entry| entry.generation != generation);
        let changed = before != state.idle.len();
        if changed {
            state.notify.notify_waiters();
        }
        changed
    }

    pub(crate) fn discard_idle_where<F>(&self, should_discard: F) -> usize
    where
        F: Fn(&T) -> bool,
    {
        let mut inner = self.inner.lock();
        let mut discarded = 0usize;
        for state in inner.values_mut() {
            let before = state.idle.len();
            state.idle.retain(|entry| !should_discard(&entry.resource));
            let removed = before.saturating_sub(state.idle.len());
            if removed > 0 {
                discarded += removed;
                state.notify.notify_waiters();
            }
        }
        inner.retain(|_, state| state.active > 0 || !state.idle.is_empty());
        discarded
    }

    pub(crate) fn discard(&self, mut lease: PoolLease<K, T>) {
        lease.resource.take();
        lease.permit.take();
        let mut inner = self.inner.lock();
        let remove_key = if let Some(state) = inner.get_mut(&lease.key) {
            state.active = state.active.saturating_sub(1);
            state.notify.notify_one();
            state.active == 0 && state.idle.is_empty()
        } else {
            false
        };
        if remove_key {
            inner.remove(&lease.key);
        }
    }

    pub(crate) fn prune_idle_with<F>(&self, is_alive: F)
    where
        F: Fn(&T) -> bool,
    {
        let mut inner = self.inner.lock();
        for state in inner.values_mut() {
            let before = state.idle.len();
            state.idle.retain(|entry| {
                entry.last_used.elapsed() < self.max_idle && is_alive(&entry.resource)
            });
            if before != state.idle.len() {
                state.notify.notify_waiters();
            }
        }
        inner.retain(|_, state| state.active > 0 || !state.idle.is_empty());
    }

    pub(crate) fn total_entries(&self) -> usize {
        let inner = self.inner.lock();
        inner
            .values()
            .map(|state| state.active + state.idle.len())
            .sum()
    }

    pub(crate) fn status_snapshot_with<F>(&self, key_label: F) -> Vec<ConnectionStatusSnapshot>
    where
        F: Fn(&K) -> String,
    {
        let inner = self.inner.lock();
        inner
            .iter()
            .map(|(key, state)| ConnectionStatusSnapshot {
                key: key_label(key),
                generation: state.latest_generation,
                active: state.active,
                idle: state.idle.len(),
                capacity: self.capacity,
            })
            .collect()
    }

    fn checkout_idle(&self, key: &K) -> Option<PoolLease<K, T>> {
        let mut inner = self.inner.lock();
        let state = inner.get_mut(key)?;
        let entry = state.idle.pop()?;
        state.active += 1;
        state.latest_generation = state.latest_generation.max(entry.generation);
        Some(PoolLease {
            key: key.clone(),
            resource: Some(entry.resource),
            generation: entry.generation,
            permit: Some(entry.permit),
            inner: self.inner.clone(),
        })
    }

    fn pool_handles(&self, key: &K) -> (Arc<Semaphore>, Arc<Notify>) {
        let mut inner = self.inner.lock();
        let state = inner
            .entry(key.clone())
            .or_insert_with(|| PoolState::new(self.capacity));
        (state.semaphore.clone(), state.notify.clone())
    }

    fn mark_active(&self, key: &K) {
        let mut inner = self.inner.lock();
        let state = inner
            .entry(key.clone())
            .or_insert_with(|| PoolState::new(self.capacity));
        state.active += 1;
    }

    fn unmark_active_and_notify(&self, key: &K) {
        let mut inner = self.inner.lock();
        if let Some(state) = inner.get_mut(key) {
            state.active = state.active.saturating_sub(1);
            state.notify.notify_one();
        }
    }

    fn record_generation(&self, key: &K, generation: u64) {
        let mut inner = self.inner.lock();
        let state = inner
            .entry(key.clone())
            .or_insert_with(|| PoolState::new(self.capacity));
        state.latest_generation = state.latest_generation.max(generation);
    }

    fn allocate_generation(&self) -> u64 {
        let mut next = self.next_generation.lock();
        let generation = *next;
        *next = next.saturating_add(1);
        generation
    }
}

impl<T> PoolState<T> {
    fn new(capacity: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(capacity.max(1))),
            notify: Arc::new(Notify::new()),
            idle: Vec::new(),
            active: 0,
            latest_generation: 0,
        }
    }
}

impl<K, T> PoolLease<K, T>
where
    K: Eq + Hash,
{
    pub(crate) fn resource(&self) -> &T {
        self.resource
            .as_ref()
            .expect("managed pool lease resource already consumed")
    }

    pub(crate) fn resource_mut(&mut self) -> &mut T {
        self.resource
            .as_mut()
            .expect("managed pool lease resource already consumed")
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }
}

impl<K, T> Drop for PoolLease<K, T>
where
    K: Eq + Hash,
{
    fn drop(&mut self) {
        let had_resource = self.resource.take().is_some();
        let had_permit = self.permit.take().is_some();
        if !had_resource && !had_permit {
            return;
        }

        let mut inner = self.inner.lock();
        let remove_key = if let Some(state) = inner.get_mut(&self.key) {
            state.active = state.active.saturating_sub(1);
            state.notify.notify_one();
            state.active == 0 && state.idle.is_empty()
        } else {
            false
        };
        if remove_key {
            inner.remove(&self.key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn singleton_generation_invalidation_does_not_clear_newer_entry() {
        let manager = ManagedSingleton::new();
        let first = manager
            .checkout_or_insert_with(|| async { Ok::<_, ()>(1usize) })
            .await
            .unwrap();
        assert!(manager.invalidate_generation(first.generation()).await);

        let second = manager
            .checkout_or_insert_with(|| async { Ok::<_, ()>(2usize) })
            .await
            .unwrap();
        assert!(!manager.invalidate_generation(first.generation()).await);
        assert_eq!(
            manager.current_generation().await,
            Some(second.generation())
        );
        assert_eq!(
            manager
                .checkout_generation(second.generation())
                .await
                .map(|lease| *lease.resource()),
            Some(2)
        );
        assert!(
            manager
                .checkout_generation(first.generation())
                .await
                .is_none()
        );
        assert_eq!(*second.resource(), 2);
    }

    #[tokio::test]
    async fn singleton_prune_keeps_active_lease() {
        let manager = ManagedSingleton::new();
        let lease = manager
            .checkout_or_insert_with(|| async { Ok::<_, ()>(1usize) })
            .await
            .unwrap();

        assert!(!manager.prune_idle(Duration::ZERO).await);
        assert_eq!(manager.current_generation().await, Some(lease.generation()));

        drop(lease);
        assert!(manager.prune_idle(Duration::ZERO).await);
        assert_eq!(manager.current_generation().await, None);
    }

    #[tokio::test]
    async fn managed_pool_reuses_idle_entries() {
        let manager = ManagedPool::new(2, Duration::from_secs(60));
        let creates = AtomicUsize::new(0);
        let lease = manager
            .checkout_or_create_with("a", || async {
                creates.fetch_add(1, Ordering::SeqCst);
                Ok::<_, ()>(10usize)
            })
            .await
            .unwrap();
        manager.return_healthy(lease);

        let lease = manager
            .checkout_or_create_with("a", || async {
                creates.fetch_add(1, Ordering::SeqCst);
                Ok::<_, ()>(20usize)
            })
            .await
            .unwrap();
        assert_eq!(*lease.resource.as_ref().unwrap(), 10);
        assert_eq!(creates.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn managed_pool_counts_active_and_idle_capacity() {
        let manager = ManagedPool::new(1, Duration::from_secs(60));
        let lease = manager
            .checkout_or_create_with("a", || async { Ok::<_, ()>(10usize) })
            .await
            .unwrap();
        let snapshot = manager.status_snapshot_with(|key| key.to_string());
        assert_eq!(snapshot[0].active, 1);
        assert_eq!(snapshot[0].idle, 0);
        assert_eq!(snapshot[0].capacity, 1);

        manager.return_healthy(lease);
        let snapshot = manager.status_snapshot_with(|key| key.to_string());
        assert_eq!(snapshot[0].active, 0);
        assert_eq!(snapshot[0].idle, 1);
    }

    #[tokio::test]
    async fn managed_pool_drop_active_lease_discards_and_decrements_active() {
        let manager = ManagedPool::new(1, Duration::from_secs(60));
        let lease = manager
            .checkout_or_create_with("a", || async { Ok::<_, ()>(10usize) })
            .await
            .unwrap();

        drop(lease);

        let snapshot = manager.status_snapshot_with(|key| key.to_string());
        assert!(snapshot.is_empty());
    }

    #[tokio::test]
    async fn managed_pool_drop_active_lease_wakes_waiter_at_capacity() {
        let manager = Arc::new(ManagedPool::new(1, Duration::from_secs(60)));
        let lease = manager
            .checkout_or_create_with("a", || async { Ok::<_, ()>(10usize) })
            .await
            .unwrap();

        let creates = Arc::new(AtomicUsize::new(0));
        let waiter_manager = manager.clone();
        let waiter_creates = creates.clone();
        let waiter = tokio::spawn(async move {
            waiter_manager
                .checkout_or_create_with("a", || async {
                    waiter_creates.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, ()>(20usize)
                })
                .await
                .unwrap()
        });

        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        drop(lease);

        let lease = waiter.await.unwrap();
        assert_eq!(*lease.resource.as_ref().unwrap(), 20);
        assert_eq!(creates.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn managed_pool_returned_lease_drop_is_noop() {
        let manager = ManagedPool::new(1, Duration::from_secs(60));
        let lease = manager
            .checkout_or_create_with("a", || async { Ok::<_, ()>(10usize) })
            .await
            .unwrap();

        manager.return_healthy(lease);

        let snapshot = manager.status_snapshot_with(|key| key.to_string());
        assert_eq!(snapshot[0].active, 0);
        assert_eq!(snapshot[0].idle, 1);
    }

    #[tokio::test]
    async fn managed_pool_discarded_lease_drop_is_noop() {
        let manager = ManagedPool::new(1, Duration::from_secs(60));
        let lease = manager
            .checkout_or_create_with("a", || async { Ok::<_, ()>(10usize) })
            .await
            .unwrap();

        manager.discard(lease);

        let snapshot = manager.status_snapshot_with(|key| key.to_string());
        assert!(snapshot.is_empty());
    }

    #[tokio::test]
    async fn managed_pool_waiter_reuses_returned_idle_entry_at_capacity() {
        let manager = Arc::new(ManagedPool::new(1, Duration::from_secs(60)));
        let lease = manager
            .checkout_or_create_with("a", || async { Ok::<_, ()>(10usize) })
            .await
            .unwrap();

        let waiter_manager = manager.clone();
        let waiter = tokio::spawn(async move {
            waiter_manager
                .checkout_or_create_with("a", || async { Ok::<_, ()>(20usize) })
                .await
                .unwrap()
        });

        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        manager.return_healthy(lease);
        let lease = waiter.await.unwrap();
        assert_eq!(*lease.resource.as_ref().unwrap(), 10);
    }

    #[tokio::test]
    async fn managed_pool_discards_idle_entries_by_predicate() {
        let manager = ManagedPool::new(2, Duration::from_secs(60));
        let keep = manager
            .checkout_or_create_with("a", || async { Ok::<_, ()>(10usize) })
            .await
            .unwrap();
        let discard = manager
            .checkout_or_create_with("b", || async { Ok::<_, ()>(20usize) })
            .await
            .unwrap();
        manager.return_healthy(keep);
        manager.return_healthy(discard);

        assert_eq!(manager.total_entries(), 2);
        assert_eq!(manager.discard_idle_where(|value| *value == 20), 1);
        assert_eq!(manager.total_entries(), 1);

        let snapshot = manager.status_snapshot_with(|key| key.to_string());
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].key, "a");
    }

    #[test]
    fn retry_decision_tracks_started_boundary() {
        assert_eq!(
            ConnectionStage::Connect.retry_decision(),
            RetryDecision::Retry
        );
        assert_eq!(
            ConnectionStage::Prepare.retry_decision(),
            RetryDecision::Retry
        );
        assert_eq!(
            ConnectionStage::Started.retry_decision(),
            RetryDecision::DoNotRetry
        );
    }
}

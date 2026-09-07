//! Browser-session capacity and ownership policy.
//!
//! This module owns the process-local browser capacity limit and the durable
//! account-lease capability required by authenticated work. It exposes two
//! non-interchangeable session types through [`super::webdriver`]: a clean
//! public session for article pages and an authenticated session tied to one
//! fenced WeRead account lease.
//!
//! Public sessions are obtained from a Tokio semaphore and carry no account
//! identity, credentials, cookies, or lease guard. Authenticated sessions are
//! obtained only after an [`AccountLeaseGuard`] is acquired. A failed heartbeat
//! marks that guard unusable, so the authenticated adapter can stop before its
//! next upstream request. The guard is intentionally non-cloneable; callers
//! must explicitly release it when work finishes.
//!
//! High availability: the semaphore limits only one process. PostgreSQL job
//! leases prevent duplicate source work, while the repository-backed account
//! lease serializes authenticated use of one account across replicas. The
//! browser pool is not a distributed lock and must not replace either durable
//! lease. Authenticated browser sessions schedule best-effort durable lease
//! cleanup when cancellation drops them; callers should still explicitly
//! release sessions on normal completion so remote browser cleanup is awaited.
//!
//! Non-responsibilities: WebDriver commands, navigation, article parsing,
//! source scheduling, credential persistence, or RSS caching. Sidecar health
//! monitoring belongs to `application::browser_health`; browser-visible
//! timezone validation belongs to the WebDriver adapter, while pacing and
//! scroll orchestration belong to [`super::pacing`] and [`super::article_page`].

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

#[cfg(test)]
use std::time::Duration as StdDuration;

use chrono::Duration;
use thiserror::Error;
use tokio::{
    sync::{watch, OwnedSemaphorePermit, Semaphore, TryAcquireError},
    task::JoinHandle,
    time::{self, MissedTickBehavior},
};

use crate::domain::credentials::{AccountLease, AccountLeaseToken, WeReadAccountId};

use super::webdriver::{AuthenticatedBrowserSession, PublicBrowserSession};

/// Errors exposed by the storage-neutral account-lease port.
#[derive(Debug, Error)]
pub enum AccountLeaseError {
    /// A nil UUID cannot identify a configured account.
    #[error("account id must not be nil")]
    InvalidAccountId,
    /// Lease ownership must identify an application instance.
    #[error("account lease owner must not be empty")]
    EmptyOwner,
    /// A lease must be positive and representable by the lease backend.
    #[error("account lease duration must be positive and fit in milliseconds")]
    InvalidLeaseDuration,
    /// The current owner or fencing token no longer controls the lease.
    #[error("account lease for {account_id} is no longer owned by this claim")]
    LeaseLost { account_id: WeReadAccountId },
    /// The lease backend could not complete the operation.
    #[error("account lease backend error: {0}")]
    Backend(String),
}

/// Storage-neutral account-lease operations required by authenticated work.
///
/// PostgreSQL, the deterministic test repository, and a future alternative
/// lease backend can implement this port. Production implementations must use
/// their authoritative clock for expiry-sensitive operations; callers never
/// provide a wall-clock timestamp.
#[async_trait::async_trait]
pub trait AccountLeaseStore: Send + Sync {
    /// Acquires a lease or returns `None` when another live owner holds it.
    async fn acquire(
        &self,
        account_id: WeReadAccountId,
        owner: &str,
        lease_for: Duration,
    ) -> Result<Option<AccountLease>, AccountLeaseError>;

    /// Extends a lease only when owner, token, and lease liveness all match.
    async fn heartbeat(
        &self,
        account_id: WeReadAccountId,
        owner: &str,
        token: AccountLeaseToken,
        lease_for: Duration,
    ) -> Result<AccountLease, AccountLeaseError>;

    /// Releases a live lease only for its current owner and fencing token.
    async fn release(
        &self,
        account_id: WeReadAccountId,
        owner: &str,
        token: AccountLeaseToken,
    ) -> Result<(), AccountLeaseError>;
}

/// Errors raised while acquiring local browser capacity or an account lease.
#[derive(Debug, Error)]
pub enum BrowserPoolError {
    /// The configured capacity cannot be zero.
    #[error("browser pool capacity must be greater than zero")]
    InvalidCapacity,
    /// The semaphore was closed before a session could be acquired.
    #[error("browser pool is closed")]
    Closed,
    /// The distributed account lease could not be acquired.
    #[error(transparent)]
    AccountLease(#[from] AccountLeaseError),
}

/// A fenced, non-cloneable lease for one authenticated WeRead account.
///
/// The repository controls ownership and expiry. This guard only carries the
/// returned account ID, owner, fencing token, and a local cancellation bit.
/// `heartbeat` must be called before its lease expires. Any heartbeat error
/// cancels the guard, including a transient storage error, because the caller
/// can no longer prove that it owns the account.
pub struct AccountLeaseGuard<R> {
    repository: R,
    lease: AccountLease,
    cancelled: Arc<AtomicBool>,
}

/// Handle for the background heartbeat attached to one authenticated session.
///
/// The task stops before the session releases its durable lease. A heartbeat
/// failure marks the shared guard unusable and is returned when the handle is
/// stopped, so the caller cannot mistake a lost lease for a clean operation.
pub(crate) struct AccountLeaseHeartbeat {
    stop: watch::Sender<bool>,
    task: JoinHandle<Result<(), AccountLeaseError>>,
}

impl AccountLeaseHeartbeat {
    pub(crate) async fn stop(&mut self) -> Result<(), AccountLeaseError> {
        let _ = self.stop.send(true);
        (&mut self.task).await.map_err(|error| {
            AccountLeaseError::Backend(format!("account lease heartbeat task failed: {error}"))
        })?
    }
}

impl Drop for AccountLeaseHeartbeat {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        self.task.abort();
    }
}

impl<R> AccountLeaseGuard<R>
where
    R: AccountLeaseStore,
{
    /// Acquires an account lease and returns `None` when another owner is live.
    pub async fn acquire(
        repository: R,
        account_id: WeReadAccountId,
        owner: &str,
        lease_for: Duration,
    ) -> Result<Option<Self>, AccountLeaseError> {
        tracing::trace!(account_id = %account_id, "acquiring WeRead account lease");
        let acquired = match repository.acquire(account_id, owner, lease_for).await {
            Ok(acquired) => acquired,
            Err(error) => {
                tracing::warn!(account_id = %account_id, error = %error, "unable to acquire WeRead account lease");
                return Err(error);
            }
        };
        let Some(lease) = acquired else {
            tracing::debug!(account_id = %account_id, "WeRead account lease is held by another worker");
            return Ok(None);
        };
        tracing::debug!(account_id = %account_id, "acquired WeRead account lease");
        Ok(Some(Self {
            repository,
            lease,
            cancelled: Arc::new(AtomicBool::new(false)),
        }))
    }

    /// Returns the stable account identity protected by this guard.
    pub const fn account_id(&self) -> WeReadAccountId {
        self.lease.account_id()
    }

    /// Returns the application instance that owns the lease.
    pub fn owner(&self) -> &str {
        self.lease.owner()
    }

    /// Returns the fencing token for this lease incarnation.
    pub const fn token(&self) -> AccountLeaseToken {
        self.lease.token()
    }

    /// Reports whether a failed heartbeat has made the guard unusable.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Fails if the lease was already lost or a heartbeat previously failed.
    pub fn ensure_usable(&self) -> Result<(), AccountLeaseError> {
        if self.is_cancelled() {
            Err(AccountLeaseError::LeaseLost {
                account_id: self.account_id(),
            })
        } else {
            Ok(())
        }
    }

    /// Extends the lease using the repository's authoritative clock.
    ///
    /// A failed heartbeat permanently cancels this guard. The authenticated
    /// adapter must call [`Self::ensure_usable`] before each request and stop
    /// when it reports lease loss.
    pub async fn heartbeat(&mut self, lease_for: Duration) -> Result<(), AccountLeaseError> {
        self.ensure_usable()?;
        let account_id = self.account_id();
        let owner = self.owner().to_owned();
        let token = self.token();
        match self
            .repository
            .heartbeat(account_id, &owner, token, lease_for)
            .await
        {
            Ok(lease) => {
                self.lease = lease;
                tracing::trace!(account_id = %account_id, "heartbeated WeRead account lease");
                Ok(())
            }
            Err(error) => {
                self.cancelled.store(true, Ordering::Release);
                tracing::warn!(account_id = %account_id, error = %error, "lost WeRead account lease heartbeat");
                Err(error)
            }
        }
    }

    /// Starts periodic authoritative-clock heartbeats for an authenticated
    /// operation. The returned handle must be stopped before releasing the
    /// guard; [`super::webdriver::AuthenticatedBrowserSession`] owns that
    /// lifecycle for runtime callers.
    pub(crate) fn start_heartbeat(
        &self,
        heartbeat_for: Duration,
        lease_for: Duration,
    ) -> Result<AccountLeaseHeartbeat, AccountLeaseError>
    where
        R: Clone + 'static,
    {
        self.ensure_usable()?;
        if heartbeat_for <= Duration::zero()
            || lease_for <= Duration::zero()
            || heartbeat_for >= lease_for
        {
            return Err(AccountLeaseError::InvalidLeaseDuration);
        }
        let heartbeat_period = heartbeat_for
            .to_std()
            .map_err(|_| AccountLeaseError::InvalidLeaseDuration)?;
        let repository = self.repository.clone();
        let account_id = self.account_id();
        let owner = self.owner().to_owned();
        let token = self.token();
        let cancelled = Arc::clone(&self.cancelled);
        let (stop, mut stop_receiver) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut ticker =
                time::interval_at(time::Instant::now() + heartbeat_period, heartbeat_period);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    changed = stop_receiver.changed() => {
                        if changed.is_err() || *stop_receiver.borrow() {
                            return Ok(());
                        }
                    }
                    _ = ticker.tick() => {
                        if let Err(error) = repository
                            .heartbeat(account_id, &owner, token, lease_for)
                            .await
                        {
                            cancelled.store(true, Ordering::Release);
                            return Err(error);
                        }
                    }
                }
            }
        });
        Ok(AccountLeaseHeartbeat { stop, task })
    }

    /// Releases this lease through the repository.
    ///
    /// This is explicit because Rust `Drop` cannot await a database call. If a
    /// guard is dropped without release, its durable lease remains protected
    /// by its expiry and can be recovered by another worker after expiration.
    /// Borrowing the guard keeps its lease metadata available if a caller's
    /// future is cancelled while the release request is in flight.
    pub async fn release(&self) -> Result<(), AccountLeaseError> {
        self.cancelled.store(true, Ordering::Release);
        let account_id = self.account_id();
        let result = self
            .repository
            .release(
                self.lease.account_id(),
                self.lease.owner(),
                self.lease.token(),
            )
            .await;
        match &result {
            Ok(()) => tracing::debug!(account_id = %account_id, "released WeRead account lease"),
            Err(error) => {
                tracing::warn!(account_id = %account_id, error = %error, "unable to release WeRead account lease")
            }
        }
        result
    }

    pub(crate) fn into_session(self, permit: OwnedSemaphorePermit) -> AuthenticatedBrowserSession<R>
    where
        R: 'static,
    {
        AuthenticatedBrowserSession::from_permit(permit, self)
    }
}

impl<R> std::fmt::Debug for AccountLeaseGuard<R>
where
    R: AccountLeaseStore,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AccountLeaseGuard")
            .field("account_id", &self.account_id())
            .field("owner", &self.owner())
            .field("token", &"<fencing token>")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

/// Process-local limit for browser sessions.
#[derive(Clone, Debug)]
pub struct BrowserPool {
    permits: Arc<Semaphore>,
    capacity: usize,
}

impl BrowserPool {
    /// Creates a pool with a positive maximum number of sessions.
    pub fn new(capacity: usize) -> Result<Self, BrowserPoolError> {
        if capacity == 0 {
            return Err(BrowserPoolError::InvalidCapacity);
        }
        Ok(Self {
            permits: Arc::new(Semaphore::new(capacity)),
            capacity,
        })
    }

    /// Returns the configured process-local session capacity.
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Acquires a clean, unauthenticated public browser capability.
    pub async fn open_public(&self) -> Result<PublicBrowserSession, BrowserPoolError> {
        tracing::trace!("acquiring public browser capacity");
        let permit = match self.permits.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(error) => {
                tracing::warn!(error = %error, "public browser capacity is closed");
                return Err(BrowserPoolError::Closed);
            }
        };
        tracing::debug!("acquired public browser capacity");
        Ok(PublicBrowserSession::from_permit(permit))
    }

    /// Tries to acquire a clean public browser capability without waiting.
    ///
    /// A missing permit means the configured process-local capacity is busy,
    /// not that the browser sidecar is unhealthy. This is used by health
    /// probes that share the runtime browser pool with real work.
    pub async fn try_open_public(&self) -> Result<Option<PublicBrowserSession>, BrowserPoolError> {
        tracing::trace!("trying to acquire public browser capacity");
        let permit = match self.permits.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => return Ok(None),
            Err(TryAcquireError::Closed) => {
                tracing::warn!("public browser capacity is closed");
                return Err(BrowserPoolError::Closed);
            }
        };
        tracing::debug!("acquired public browser capacity without waiting");
        Ok(Some(PublicBrowserSession::from_permit(permit)))
    }

    /// Acquires local browser capacity and a distributed account lease.
    ///
    /// Local capacity is acquired before the account lease. This prevents a
    /// worker from holding a durable account lock while blocked on its own
    /// browser pool. If account acquisition fails or another owner is live,
    /// the local permit is released with the returned result.
    pub async fn open_authenticated<R>(
        &self,
        repository: R,
        account_id: WeReadAccountId,
        owner: &str,
        lease_for: Duration,
    ) -> Result<Option<AuthenticatedBrowserSession<R>>, BrowserPoolError>
    where
        R: AccountLeaseStore + 'static,
    {
        tracing::trace!(account_id = %account_id, "acquiring authenticated browser capacity");
        let permit = match self.permits.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(error) => {
                tracing::warn!(account_id = %account_id, error = %error, "authenticated browser capacity is closed");
                return Err(BrowserPoolError::Closed);
            }
        };
        let Some(guard) =
            AccountLeaseGuard::acquire(repository, account_id, owner, lease_for).await?
        else {
            tracing::debug!(account_id = %account_id, "authenticated browser capacity acquired but account lease is unavailable");
            return Ok(None);
        };
        tracing::debug!(account_id = %account_id, "acquired authenticated browser capacity");
        Ok(Some(guard.into_session(permit)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use chrono::{DateTime, TimeZone, Utc};
    use tokio::sync::Notify;
    use uuid::Uuid;

    use super::*;
    use crate::persistence::repositories::account_lease_repository::MemoryAccountLeaseRepository;

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0)
            .single()
            .expect("test timestamp should be valid")
    }

    fn account_id() -> WeReadAccountId {
        WeReadAccountId::from_uuid(Uuid::from_u128(1))
    }

    #[derive(Clone)]
    struct CountingLeaseRepository {
        inner: MemoryAccountLeaseRepository,
        heartbeats: Arc<AtomicUsize>,
        heartbeat_seen: Arc<Notify>,
        fail_heartbeat: bool,
    }

    #[async_trait::async_trait]
    impl AccountLeaseStore for CountingLeaseRepository {
        async fn acquire(
            &self,
            account_id: WeReadAccountId,
            owner: &str,
            lease_for: Duration,
        ) -> Result<Option<AccountLease>, AccountLeaseError> {
            self.inner.acquire(account_id, owner, lease_for).await
        }

        async fn heartbeat(
            &self,
            account_id: WeReadAccountId,
            owner: &str,
            token: AccountLeaseToken,
            lease_for: Duration,
        ) -> Result<AccountLease, AccountLeaseError> {
            self.heartbeats.fetch_add(1, Ordering::Relaxed);
            self.heartbeat_seen.notify_waiters();
            if self.fail_heartbeat {
                return Err(AccountLeaseError::Backend("heartbeat failed".to_owned()));
            }
            self.inner
                .heartbeat(account_id, owner, token, lease_for)
                .await
        }

        async fn release(
            &self,
            account_id: WeReadAccountId,
            owner: &str,
            token: AccountLeaseToken,
        ) -> Result<(), AccountLeaseError> {
            self.inner.release(account_id, owner, token).await
        }
    }

    fn counting_repository(fail_heartbeat: bool) -> CountingLeaseRepository {
        CountingLeaseRepository {
            inner: MemoryAccountLeaseRepository::new(at(0)),
            heartbeats: Arc::new(AtomicUsize::new(0)),
            heartbeat_seen: Arc::new(Notify::new()),
            fail_heartbeat,
        }
    }

    #[test]
    fn rejects_zero_capacity() {
        assert!(matches!(
            BrowserPool::new(0),
            Err(BrowserPoolError::InvalidCapacity)
        ));
    }

    #[tokio::test]
    async fn public_session_holds_one_permit_until_drop() {
        let pool = BrowserPool::new(1).unwrap();
        let first = pool.open_public().await.unwrap();
        assert_eq!(pool.capacity(), 1);

        let blocked =
            tokio::time::timeout(std::time::Duration::from_millis(10), pool.open_public()).await;
        assert!(
            blocked.is_err(),
            "the first session should consume capacity"
        );

        drop(first);
        let second = pool.open_public().await.unwrap();
        assert_ne!(second.session_id(), Uuid::nil());
    }

    #[tokio::test]
    async fn try_public_session_reports_busy_without_waiting_for_capacity() {
        let pool = BrowserPool::new(1).unwrap();
        let first = pool.open_public().await.unwrap();

        assert!(pool
            .try_open_public()
            .await
            .expect("a busy pool should be queryable")
            .is_none());

        drop(first);
        assert!(pool
            .try_open_public()
            .await
            .expect("an available pool should be queryable")
            .is_some());
    }

    #[tokio::test]
    async fn failed_heartbeat_cancels_authenticated_capability() {
        let repository = MemoryAccountLeaseRepository::new(at(0));
        let mut guard = AccountLeaseGuard::acquire(
            repository.clone(),
            account_id(),
            "worker-a",
            Duration::seconds(10),
        )
        .await
        .unwrap()
        .unwrap();
        repository.set_now(at(11)).await;

        assert!(matches!(
            guard.heartbeat(Duration::seconds(10)).await,
            Err(AccountLeaseError::LeaseLost { .. })
        ));
        assert!(guard.is_cancelled());
        assert!(matches!(
            guard.ensure_usable(),
            Err(AccountLeaseError::LeaseLost { .. })
        ));
    }

    #[tokio::test]
    async fn background_heartbeat_renews_until_stopped() {
        let repository = counting_repository(false);
        let heartbeat_seen = Arc::clone(&repository.heartbeat_seen);
        let heartbeats = Arc::clone(&repository.heartbeats);
        let guard = AccountLeaseGuard::acquire(
            repository.clone(),
            account_id(),
            "worker-a",
            Duration::seconds(30),
        )
        .await
        .unwrap()
        .unwrap();
        let heartbeat_started = heartbeat_seen.notified();
        let mut heartbeat = guard
            .start_heartbeat(Duration::milliseconds(5), Duration::seconds(30))
            .unwrap();

        tokio::time::timeout(StdDuration::from_secs(1), heartbeat_started)
            .await
            .expect("heartbeat should run before the timeout");
        heartbeat.stop().await.unwrap();
        assert!(heartbeats.load(Ordering::Relaxed) >= 1);
        guard.release().await.unwrap();
    }

    #[tokio::test]
    async fn dropping_background_heartbeat_aborts_future_renewals() {
        let repository = counting_repository(false);
        let heartbeat_seen = Arc::clone(&repository.heartbeat_seen);
        let heartbeats = Arc::clone(&repository.heartbeats);
        let guard =
            AccountLeaseGuard::acquire(repository, account_id(), "worker-a", Duration::seconds(30))
                .await
                .unwrap()
                .unwrap();
        let heartbeat_started = heartbeat_seen.notified();
        let heartbeat = guard
            .start_heartbeat(Duration::milliseconds(5), Duration::seconds(30))
            .unwrap();

        tokio::time::timeout(StdDuration::from_secs(1), heartbeat_started)
            .await
            .expect("heartbeat should run before the timeout");
        let count_at_drop = heartbeats.load(Ordering::Relaxed);
        drop(heartbeat);
        tokio::time::sleep(StdDuration::from_millis(30)).await;

        assert_eq!(heartbeats.load(Ordering::Relaxed), count_at_drop);
        guard.release().await.unwrap();
    }

    #[tokio::test]
    async fn failed_background_heartbeat_cancels_the_guard() {
        let repository = counting_repository(true);
        let heartbeat_seen = Arc::clone(&repository.heartbeat_seen);
        let guard =
            AccountLeaseGuard::acquire(repository, account_id(), "worker-a", Duration::seconds(30))
                .await
                .unwrap()
                .unwrap();
        let heartbeat_started = heartbeat_seen.notified();
        let mut heartbeat = guard
            .start_heartbeat(Duration::milliseconds(5), Duration::seconds(30))
            .unwrap();

        tokio::time::timeout(StdDuration::from_secs(1), heartbeat_started)
            .await
            .expect("failed heartbeat should run before the timeout");
        assert!(matches!(
            heartbeat.stop().await,
            Err(AccountLeaseError::Backend(message)) if message == "heartbeat failed"
        ));
        assert!(matches!(
            guard.ensure_usable(),
            Err(AccountLeaseError::LeaseLost { .. })
        ));
    }
}

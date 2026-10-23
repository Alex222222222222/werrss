//! Non-interactive WeRead authentication lifecycle.
//!
//! This service implements the safe part of account authentication for the
//! first usable runtime: an operator or the QR-login adapter can provision
//! credentials, the service encrypts them before persistence, and
//! expired access credentials are refreshed once under the same distributed
//! account lease used by source synchronization. QR rendering, polling, and
//! browser interaction remain outside this boundary.
//!
//! Tokens are never returned by status values, debug output, or errors. A
//! refresh response may rotate the refresh token; when it does not, the prior
//! refresh token is retained. Risk-control and lease-loss errors are terminal
//! and never cause an automatic refresh retry.

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use ring::{
    aead::{Aad, LessSafeKey, Nonce, UnboundKey, CHACHA20_POLY1305},
    digest::{digest, SHA256},
    rand::{SecureRandom, SystemRandom},
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    acquisition::browser_pool::{AccountLeaseError, AccountLeaseGuard, AccountLeaseStore},
    domain::credentials::{CredentialError, WeReadAccount, WeReadAccountId, WeReadCredentials},
    persistence::repositories::credential_repository::{
        CredentialRecord, CredentialReplacement, CredentialRepository, CredentialRepositoryError,
    },
};

use super::worker::{JobExecution, JobHandler};
use crate::persistence::repositories::job_repository::JobLease;

const CREDENTIAL_BLOB_VERSION: u8 = 1;
const NONCE_LENGTH: usize = 12;

/// Result of a refresh transport call.
#[derive(Debug, Clone)]
pub struct RefreshedCredentials {
    access_token: SecretString,
    refresh_token: Option<SecretString>,
    access_expires_at: DateTime<Utc>,
}

impl RefreshedCredentials {
    /// Creates a refresh result without exposing its secrets to callers.
    pub fn new(
        access_token: impl Into<String>,
        refresh_token: Option<String>,
        access_expires_at: DateTime<Utc>,
    ) -> Result<Self, CredentialError> {
        let access_token = access_token.into();
        if access_token.trim().is_empty()
            || refresh_token
                .as_deref()
                .is_some_and(|token| token.trim().is_empty())
        {
            return Err(CredentialError::EmptyToken);
        }
        if access_expires_at <= DateTime::<Utc>::UNIX_EPOCH {
            return Err(CredentialError::ExpiryNotAfterIssue);
        }
        Ok(Self {
            access_token: SecretString::new(access_token.into_boxed_str()),
            refresh_token: refresh_token.map(|token| SecretString::new(token.into_boxed_str())),
            access_expires_at,
        })
    }
}

/// Boundary for the upstream refresh-token exchange.
#[async_trait]
pub trait CredentialRefresher: Send + Sync {
    /// Exchanges one refresh token for new access credentials.
    async fn refresh(
        &self,
        account_id: WeReadAccountId,
        refresh_token: &str,
    ) -> Result<RefreshedCredentials, CredentialRefreshError>;
}

/// Refresh boundary used when credentials are entered manually by an
/// administrator but no upstream refresh transport has been configured.
///
/// Keeping this as an explicit implementation lets the admin panel reuse the
/// same authentication service without pretending that refresh is available.
#[derive(Debug, Clone, Copy, Default)]
pub struct ManualCredentialRefresher;

#[async_trait]
impl CredentialRefresher for ManualCredentialRefresher {
    async fn refresh(
        &self,
        _account_id: WeReadAccountId,
        _refresh_token: &str,
    ) -> Result<RefreshedCredentials, CredentialRefreshError> {
        Err(CredentialRefreshError::AuthenticationRequired)
    }
}

#[async_trait]
impl<T> CredentialRefresher for Arc<T>
where
    T: CredentialRefresher + ?Sized,
{
    async fn refresh(
        &self,
        account_id: WeReadAccountId,
        refresh_token: &str,
    ) -> Result<RefreshedCredentials, CredentialRefreshError> {
        (**self).refresh(account_id, refresh_token).await
    }
}

/// Typed refresh failures. Variants intentionally contain no upstream body or
/// token, preventing secret leakage through application errors.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CredentialRefreshError {
    /// The refresh token is no longer accepted and interactive login is needed.
    #[error("credential refresh requires interactive login")]
    AuthenticationRequired,
    /// The upstream rejected the operation for risk-control reasons.
    #[error("credential refresh was risk-controlled")]
    RiskControlled,
    /// The upstream or its transport was temporarily unavailable.
    #[error("credential refresh is temporarily unavailable")]
    Transient,
    /// The refresh response failed the local credential contract.
    #[error("credential refresh returned invalid credentials")]
    InvalidResponse,
}

/// Boundary for encrypting credential material before repository persistence.
pub trait CredentialCipher: Clone + Send + Sync + 'static {
    /// Encrypts credentials using the account ID as additional authenticated data.
    fn encrypt(
        &self,
        account_id: WeReadAccountId,
        credentials: &WeReadCredentials,
    ) -> Result<Vec<u8>, CredentialCipherError>;

    /// Decrypts and validates credentials for one account.
    fn decrypt(
        &self,
        account_id: WeReadAccountId,
        ciphertext: &[u8],
    ) -> Result<WeReadCredentials, CredentialCipherError>;
}

/// Errors raised by credential encryption or authenticated decryption.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CredentialCipherError {
    /// The configured encryption key cannot be used.
    #[error("credential encryption key is invalid")]
    InvalidKey,
    /// The encrypted blob was truncated, forged, or from an unsupported version.
    #[error("credential ciphertext is invalid")]
    InvalidCiphertext,
    /// The decrypted payload does not satisfy the credential contract.
    #[error("decrypted credential payload is invalid")]
    InvalidPayload,
}

/// Authenticated credential encryption using ChaCha20-Poly1305.
///
/// The configured secret is reduced to a 256-bit key with SHA-256. Each blob
/// contains a version byte and a random nonce; the account ID is authenticated
/// as associated data, so ciphertext cannot be moved between accounts.
#[derive(Clone)]
pub struct RingCredentialCipher {
    key: [u8; 32],
}

impl fmt::Debug for RingCredentialCipher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RingCredentialCipher")
            .field("key", &"<secret>")
            .finish()
    }
}

impl RingCredentialCipher {
    /// Derives the encryption key from the validated application secret.
    pub fn new(key: &SecretString) -> Result<Self, CredentialCipherError> {
        if key.expose_secret().trim().is_empty() {
            return Err(CredentialCipherError::InvalidKey);
        }
        let digest = digest(&SHA256, key.expose_secret().as_bytes());
        let mut derived = [0_u8; 32];
        derived.copy_from_slice(digest.as_ref());
        Ok(Self { key: derived })
    }

    fn key(&self) -> Result<LessSafeKey, CredentialCipherError> {
        UnboundKey::new(&CHACHA20_POLY1305, &self.key)
            .map(LessSafeKey::new)
            .map_err(|_| CredentialCipherError::InvalidKey)
    }
}

#[derive(Serialize, Deserialize)]
struct CredentialPayload {
    access_token: String,
    refresh_token: String,
    web_cookie: Option<String>,
    access_expires_at: DateTime<Utc>,
}

impl CredentialCipher for RingCredentialCipher {
    fn encrypt(
        &self,
        account_id: WeReadAccountId,
        credentials: &WeReadCredentials,
    ) -> Result<Vec<u8>, CredentialCipherError> {
        let payload = CredentialPayload {
            access_token: credentials.access_token().to_owned(),
            refresh_token: credentials.refresh_token().to_owned(),
            web_cookie: credentials.web_cookie().map(ToOwned::to_owned),
            access_expires_at: credentials.access_expires_at(),
        };
        let mut bytes =
            serde_json::to_vec(&payload).map_err(|_| CredentialCipherError::InvalidPayload)?;
        let mut nonce_bytes = [0_u8; NONCE_LENGTH];
        SystemRandom::new()
            .fill(&mut nonce_bytes)
            .map_err(|_| CredentialCipherError::InvalidKey)?;
        let nonce = Nonce::assume_unique_for_key(nonce_bytes);
        self.key()?
            .seal_in_place_append_tag(
                nonce,
                Aad::from(account_id.as_uuid().as_bytes()),
                &mut bytes,
            )
            .map_err(|_| CredentialCipherError::InvalidCiphertext)?;
        let mut encrypted = Vec::with_capacity(1 + NONCE_LENGTH + bytes.len());
        encrypted.push(CREDENTIAL_BLOB_VERSION);
        encrypted.extend_from_slice(&nonce_bytes);
        encrypted.extend(bytes);
        Ok(encrypted)
    }

    fn decrypt(
        &self,
        account_id: WeReadAccountId,
        ciphertext: &[u8],
    ) -> Result<WeReadCredentials, CredentialCipherError> {
        if ciphertext.len() <= 1 + NONCE_LENGTH || ciphertext[0] != CREDENTIAL_BLOB_VERSION {
            return Err(CredentialCipherError::InvalidCiphertext);
        }
        let nonce_bytes: [u8; NONCE_LENGTH] = ciphertext[1..1 + NONCE_LENGTH]
            .try_into()
            .map_err(|_| CredentialCipherError::InvalidCiphertext)?;
        let mut payload = ciphertext[1 + NONCE_LENGTH..].to_vec();
        let nonce = Nonce::assume_unique_for_key(nonce_bytes);
        let plaintext = self
            .key()?
            .open_in_place(
                nonce,
                Aad::from(account_id.as_uuid().as_bytes()),
                &mut payload,
            )
            .map_err(|_| CredentialCipherError::InvalidCiphertext)?;
        let payload: CredentialPayload =
            serde_json::from_slice(plaintext).map_err(|_| CredentialCipherError::InvalidPayload)?;
        let credentials = WeReadCredentials::new(
            payload.access_token,
            payload.refresh_token,
            payload.access_expires_at,
            DateTime::<Utc>::UNIX_EPOCH,
        )
        .map_err(|_| CredentialCipherError::InvalidPayload)?;
        match payload.web_cookie {
            Some(cookie) => credentials
                .with_web_cookie(cookie)
                .map_err(|_| CredentialCipherError::InvalidPayload),
            None => Ok(credentials),
        }
    }
}

/// Inputs needed to provision an account without an interactive login.
pub struct CredentialProvision {
    /// Stable account identity.
    pub account_id: WeReadAccountId,
    /// Operator-facing non-secret label.
    pub display_name: String,
    /// Initial refreshable credentials.
    pub credentials: WeReadCredentials,
}

/// Timing and lease policy for authentication lifecycle operations.
#[derive(Debug, Clone, Copy)]
pub struct AuthServiceConfig {
    /// Refresh when access expiry is this close.
    pub refresh_before: Duration,
    /// Duration of the distributed refresh lease.
    pub lease_for: Duration,
    /// Heartbeat interval for the refresh lease.
    pub lease_heartbeat: Duration,
}

impl AuthServiceConfig {
    /// Validates a refresh policy.
    pub fn new(
        refresh_before: Duration,
        lease_for: Duration,
        lease_heartbeat: Duration,
    ) -> Result<Self, AuthServiceConfigError> {
        if refresh_before < Duration::zero() {
            return Err(AuthServiceConfigError::NegativeRefreshWindow);
        }
        if lease_for <= Duration::zero() || lease_heartbeat <= Duration::zero() {
            return Err(AuthServiceConfigError::InvalidLease);
        }
        if lease_heartbeat >= lease_for {
            return Err(AuthServiceConfigError::HeartbeatNotBeforeLeaseExpiry);
        }
        Ok(Self {
            refresh_before,
            lease_for,
            lease_heartbeat,
        })
    }
}

/// Invalid authentication lifecycle policy.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AuthServiceConfigError {
    /// Refresh windows cannot move the effective deadline backwards.
    #[error("credential refresh window must not be negative")]
    NegativeRefreshWindow,
    /// Lease durations must be positive.
    #[error("credential refresh lease durations must be positive")]
    InvalidLease,
    /// Heartbeats must occur before lease expiry.
    #[error("credential refresh heartbeat must be shorter than the lease")]
    HeartbeatNotBeforeLeaseExpiry,
}

/// Outcome of a refresh check, containing only safe account metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthRefreshOutcome {
    /// Existing credentials remain valid at the repository's authoritative time.
    Unchanged(WeReadAccount),
    /// Credentials were atomically replaced after a successful refresh.
    Refreshed(WeReadAccount),
}

/// Errors raised by authentication orchestration.
#[derive(Debug, Error)]
pub enum AuthServiceError {
    /// No credentials have been provisioned for the account.
    #[error("credentials are not provisioned for account {account_id}")]
    AccountNotFound { account_id: WeReadAccountId },
    /// Disabled accounts cannot be used or refreshed.
    #[error("account {account_id} is disabled")]
    AccountDisabled { account_id: WeReadAccountId },
    /// Imported account settings cannot authenticate until credentials are enrolled.
    #[error("account {account_id} requires credential setup")]
    SetupRequired { account_id: WeReadAccountId },
    /// Another replica currently owns the refresh lease.
    #[error("account {account_id} is busy")]
    AccountBusy { account_id: WeReadAccountId },
    /// Account lease acquisition, heartbeat, or release failed.
    #[error(transparent)]
    Lease(#[from] AccountLeaseError),
    /// Credential persistence failed.
    #[error(transparent)]
    Repository(#[from] CredentialRepositoryError),
    /// Credential encryption or decryption failed.
    #[error(transparent)]
    Cipher(#[from] CredentialCipherError),
    /// Credential payload validation failed.
    #[error(transparent)]
    Credentials(#[from] CredentialError),
    /// Upstream refresh failed without exposing upstream response data.
    #[error(transparent)]
    Refresh(#[from] CredentialRefreshError),
}

/// Dependencies for [`AuthService`].
#[derive(Clone)]
pub struct AuthServiceDependencies<S, L, R, C> {
    /// Encrypted account repository.
    pub accounts: S,
    /// Distributed account lease store.
    pub leases: L,
    /// Upstream refresh exchange.
    pub refresher: R,
    /// At-rest credential cipher.
    pub cipher: C,
}

/// Coordinates provisioning and one-shot, lease-serialized credential refresh.
#[derive(Clone)]
pub struct AuthService<S, L, R, C> {
    dependencies: AuthServiceDependencies<S, L, R, C>,
    config: AuthServiceConfig,
}

impl<S, L, R, C> AuthService<S, L, R, C> {
    /// Creates the authentication lifecycle service.
    pub fn new(
        dependencies: AuthServiceDependencies<S, L, R, C>,
        config: AuthServiceConfig,
    ) -> Self {
        Self {
            dependencies,
            config,
        }
    }
}

impl<S, L, R, C> AuthService<S, L, R, C>
where
    S: CredentialRepository,
    L: AccountLeaseStore + Clone + 'static,
    R: CredentialRefresher,
    C: CredentialCipher,
{
    /// Encrypts and stores credentials received from an operator or an
    /// interactive-login adapter.
    #[tracing::instrument(skip_all, fields(account_id = %provision.account_id))]
    pub async fn provision(
        &self,
        provision: CredentialProvision,
    ) -> Result<WeReadAccount, AuthServiceError> {
        let ciphertext = self
            .dependencies
            .cipher
            .encrypt(provision.account_id, &provision.credentials)?;
        let record = self
            .dependencies
            .accounts
            .insert(
                provision.account_id,
                &provision.display_name,
                &ciphertext,
                provision.credentials.access_expires_at(),
            )
            .await?;
        tracing::info!(
            account_id = %record.account().account_id(),
            access_expires_at = ?record.account().access_expires_at(),
            "WeRead account credentials provisioned"
        );
        Ok(record.account().clone())
    }

    /// Returns non-secret metadata for an enrolled account.
    pub async fn account(
        &self,
        account_id: WeReadAccountId,
    ) -> Result<WeReadAccount, AuthServiceError> {
        let account = self.load(account_id).await?.account().clone();
        tracing::debug!(account_id = %account_id, "loaded WeRead account metadata");
        Ok(account)
    }

    /// Returns non-secret metadata for every enrolled account, including
    /// disabled accounts for administrative status views.
    pub async fn list_accounts(&self) -> Result<Vec<WeReadAccount>, AuthServiceError> {
        let accounts = self
            .dependencies
            .accounts
            .list_all()
            .await?
            .into_iter()
            .map(|record| record.account().clone())
            .collect::<Vec<_>>();
        tracing::debug!(accounts = accounts.len(), "listed WeRead account metadata");
        Ok(accounts)
    }

    /// Enables or disables an enrolled account without exposing credentials.
    pub async fn set_disabled(
        &self,
        account_id: WeReadAccountId,
        disabled: bool,
    ) -> Result<WeReadAccount, AuthServiceError> {
        let result = match self
            .dependencies
            .accounts
            .set_disabled(account_id, disabled)
            .await
        {
            Ok(record) => Ok(record.account().clone()),
            Err(CredentialRepositoryError::NotFound { .. }) => {
                Err(AuthServiceError::AccountNotFound { account_id })
            }
            Err(error) => Err(error.into()),
        };
        match &result {
            Ok(_) => tracing::info!(
                account_id = %account_id,
                disabled,
                "updated WeRead account enabled state"
            ),
            Err(error) => tracing::warn!(
                account_id = %account_id,
                error = %error,
                "unable to update WeRead account enabled state"
            ),
        }
        result
    }

    /// Permanently deletes an enrolled account and any account lease row.
    pub async fn delete(&self, account_id: WeReadAccountId) -> Result<(), AuthServiceError> {
        let result = match self.dependencies.accounts.delete(account_id).await {
            Ok(()) => Ok(()),
            Err(CredentialRepositoryError::NotFound { .. }) => {
                Err(AuthServiceError::AccountNotFound { account_id })
            }
            Err(error) => Err(error.into()),
        };
        match &result {
            Ok(()) => tracing::info!(account_id = %account_id, "deleted WeRead account"),
            Err(error) => tracing::warn!(
                account_id = %account_id,
                error = %error,
                "unable to delete WeRead account"
            ),
        }
        result
    }

    /// Loads and decrypts credentials for the authenticated acquisition
    /// boundary. Callers receive the value only for the duration of a single
    /// upstream operation; it is never suitable for API responses or logs.
    pub async fn credentials(
        &self,
        account_id: WeReadAccountId,
    ) -> Result<WeReadCredentials, AuthServiceError> {
        tracing::trace!(account_id = %account_id, "loading WeRead credentials for acquisition");
        let record = self.load(account_id).await?;
        if record.account().setup_required() {
            return Err(AuthServiceError::SetupRequired { account_id });
        }
        if record.account().disabled() {
            return Err(AuthServiceError::AccountDisabled { account_id });
        }
        self.decrypt(&record)
    }

    /// Replaces manually supplied credentials under the account lease.
    ///
    /// This is the operator re-authentication path. It uses the same fenced,
    /// version-checked repository operation as automatic refresh, so a manual
    /// update cannot overwrite a concurrent refresh or a lease takeover.
    #[tracing::instrument(skip_all, fields(account_id = %provision.account_id))]
    pub async fn replace(
        &self,
        provision: CredentialProvision,
        owner: &str,
    ) -> Result<WeReadAccount, AuthServiceError> {
        tracing::debug!("replacing WeRead account credentials");
        let Some(lease) = AccountLeaseGuard::acquire(
            self.dependencies.leases.clone(),
            provision.account_id,
            owner,
            self.config.lease_for,
        )
        .await?
        else {
            return Err(AuthServiceError::AccountBusy {
                account_id: provision.account_id,
            });
        };
        let mut heartbeat = lease
            .start_heartbeat(self.config.lease_heartbeat, self.config.lease_for)
            .map_err(AuthServiceError::Lease)?;

        let result = async {
            let current = self.load(provision.account_id).await?;
            let ciphertext = self
                .dependencies
                .cipher
                .encrypt(provision.account_id, &provision.credentials)?;
            lease.ensure_usable().map_err(AuthServiceError::Lease)?;
            if current.account().setup_required() {
                return Ok(self
                    .dependencies
                    .accounts
                    .enroll_placeholder(
                        provision.account_id,
                        &provision.display_name,
                        &ciphertext,
                        provision.credentials.access_expires_at(),
                        lease.owner(),
                        lease.token(),
                    )
                    .await?
                    .account()
                    .clone());
            }
            let updated = self
                .dependencies
                .accounts
                .replace(CredentialReplacement {
                    account_id: provision.account_id,
                    display_name: provision.display_name.clone(),
                    expected_version: current.account().credential_version(),
                    ciphertext,
                    access_expires_at: provision.credentials.access_expires_at(),
                    lease_owner: lease.owner().to_owned(),
                    lease_token: lease.token(),
                })
                .await?;
            Ok(updated.account().clone())
        }
        .await;
        let heartbeat_result = heartbeat.stop().await;
        let release_result = lease.release().await;
        heartbeat_result.map_err(AuthServiceError::Lease)?;
        release_result.map_err(AuthServiceError::Lease)?;
        match &result {
            Ok(account) => tracing::info!(
                account_id = %account.account_id(),
                access_expires_at = ?account.access_expires_at(),
                "WeRead account credentials replaced"
            ),
            Err(error) => tracing::warn!(
                account_id = %provision.account_id,
                error = %error,
                "unable to replace WeRead account credentials"
            ),
        }
        result
    }

    /// Refreshes credentials only when they are within the configured window.
    ///
    /// The repository supplies the authoritative timestamp, so a skewed
    /// worker cannot suppress refreshes or write a future `updated_at`. The
    /// account lease is acquired before the second read and refresh, closing
    /// the race where two replicas observe the same expiry.
    #[tracing::instrument(skip_all, fields(account_id = %account_id))]
    pub async fn refresh_if_needed(
        &self,
        account_id: WeReadAccountId,
        owner: &str,
    ) -> Result<AuthRefreshOutcome, AuthServiceError> {
        let now = self.dependencies.accounts.database_now().await?;
        let initial = self.load(account_id).await?;
        if initial.account().setup_required() {
            return Err(AuthServiceError::SetupRequired { account_id });
        }
        if initial.account().disabled() {
            return Err(AuthServiceError::AccountDisabled { account_id });
        }
        let initial_credentials = self.decrypt(&initial)?;
        if !initial_credentials.needs_refresh(now, self.config.refresh_before) {
            tracing::debug!(account_id = %account_id, "WeRead credentials do not need refresh");
            return Ok(AuthRefreshOutcome::Unchanged(initial.account().clone()));
        }

        let Some(mut lease) = AccountLeaseGuard::acquire(
            self.dependencies.leases.clone(),
            account_id,
            owner,
            self.config.lease_for,
        )
        .await?
        else {
            return Err(AuthServiceError::AccountBusy { account_id });
        };
        let mut heartbeat = lease
            .start_heartbeat(self.config.lease_heartbeat, self.config.lease_for)
            .map_err(AuthServiceError::Lease)?;

        let result = self.refresh_under_lease(account_id, &mut lease).await;
        let heartbeat_result = heartbeat.stop().await;
        let release_result = lease.release().await;
        heartbeat_result.map_err(AuthServiceError::Lease)?;
        release_result.map_err(AuthServiceError::Lease)?;
        match &result {
            Ok(AuthRefreshOutcome::Unchanged(_)) => {
                tracing::debug!(account_id = %account_id, "WeRead credentials remained unchanged")
            }
            Ok(AuthRefreshOutcome::Refreshed(account)) => tracing::info!(
                account_id = %account.account_id(),
                access_expires_at = ?account.access_expires_at(),
                "refreshed WeRead credentials"
            ),
            Err(error) => tracing::warn!(
                account_id = %account_id,
                error = %error,
                "unable to refresh WeRead credentials"
            ),
        }
        result
    }

    async fn refresh_under_lease(
        &self,
        account_id: WeReadAccountId,
        lease: &mut AccountLeaseGuard<L>,
    ) -> Result<AuthRefreshOutcome, AuthServiceError> {
        let now = self.dependencies.accounts.database_now().await?;
        let current = self.load(account_id).await?;
        if current.account().setup_required() {
            return Err(AuthServiceError::SetupRequired { account_id });
        }
        if current.account().disabled() {
            return Err(AuthServiceError::AccountDisabled { account_id });
        }
        let current_credentials = self.decrypt(&current)?;
        if !current_credentials.needs_refresh(now, self.config.refresh_before) {
            return Ok(AuthRefreshOutcome::Unchanged(current.account().clone()));
        }
        lease.ensure_usable().map_err(AuthServiceError::Lease)?;
        let refreshed = self
            .dependencies
            .refresher
            .refresh(account_id, current_credentials.refresh_token())
            .await?;
        lease.ensure_usable().map_err(AuthServiceError::Lease)?;
        let credentials = current_credentials.refreshed(
            refreshed.access_token.expose_secret(),
            refreshed
                .refresh_token
                .map(|token| token.expose_secret().to_owned()),
            refreshed.access_expires_at,
            now,
        )?;
        let ciphertext = self.dependencies.cipher.encrypt(account_id, &credentials)?;
        let updated = self
            .dependencies
            .accounts
            .replace(CredentialReplacement {
                account_id,
                display_name: current.account().display_name().to_owned(),
                expected_version: current.account().credential_version(),
                ciphertext,
                access_expires_at: credentials.access_expires_at(),
                lease_owner: lease.owner().to_owned(),
                lease_token: lease.token(),
            })
            .await?;
        Ok(AuthRefreshOutcome::Refreshed(updated.account().clone()))
    }

    async fn load(
        &self,
        account_id: WeReadAccountId,
    ) -> Result<CredentialRecord, AuthServiceError> {
        self.dependencies
            .accounts
            .find(account_id)
            .await?
            .ok_or(AuthServiceError::AccountNotFound { account_id })
    }

    fn decrypt(&self, record: &CredentialRecord) -> Result<WeReadCredentials, AuthServiceError> {
        let account_id = record.account().account_id();
        if record.account().setup_required() {
            return Err(AuthServiceError::SetupRequired { account_id });
        }
        let ciphertext = record
            .ciphertext()
            .ok_or(AuthServiceError::SetupRequired { account_id })?;
        let credentials = self
            .dependencies
            .cipher
            .decrypt(account_id, ciphertext)
            .map_err(AuthServiceError::Cipher)?;
        if Some(credentials.access_expires_at()) != record.account().access_expires_at() {
            return Err(AuthServiceError::Cipher(
                CredentialCipherError::InvalidPayload,
            ));
        }
        Ok(credentials)
    }
}

/// Worker adapter for queued non-interactive credential refresh jobs.
///
/// The refresh transport is injected by runtime composition because the
/// upstream protocol is deployment-specific. The adapter owns only job-shape
/// validation, safe error classification, and the service call; it never
/// places tokens in a job outcome.
pub struct CredentialRefreshJobHandler<S, L, R, C> {
    service: AuthService<S, L, R, C>,
    owner: String,
    retry_after: Duration,
}

impl<S, L, R, C> CredentialRefreshJobHandler<S, L, R, C> {
    /// Creates a handler for one worker owner and bounded retry delay.
    pub fn new(
        service: AuthService<S, L, R, C>,
        owner: impl Into<String>,
        retry_after: Duration,
    ) -> Result<Self, AuthServiceError> {
        let owner = owner.into();
        if owner.trim().is_empty() || retry_after <= Duration::zero() {
            return Err(AuthServiceError::Refresh(
                CredentialRefreshError::InvalidResponse,
            ));
        }
        Ok(Self {
            service,
            owner,
            retry_after,
        })
    }
}

impl<S, L, R, C> CredentialRefreshJobHandler<S, L, R, C>
where
    S: CredentialRepository,
    L: AccountLeaseStore + Clone + 'static,
    R: CredentialRefresher,
    C: CredentialCipher,
{
    #[tracing::instrument(skip_all, level = "debug", fields(job_id = %lease.job.id()))]
    pub(crate) async fn execute_job(&self, lease: &JobLease, now: DateTime<Utc>) -> JobExecution {
        if lease.job.job_type() != crate::domain::job::JobType::CredentialRefresh {
            tracing::error!(job_id = %lease.job.id(), "credential refresh handler received an unsupported job type");
            return JobExecution::Failed {
                error: "credential-refresh handler received an unsupported job type".to_owned(),
            };
        }
        let Some(account_id) = lease
            .job
            .payload()
            .get("account_id")
            .and_then(serde_json::Value::as_str)
            .and_then(|value| uuid::Uuid::parse_str(value).ok())
            .map(WeReadAccountId::from_uuid)
        else {
            tracing::error!(job_id = %lease.job.id(), "credential refresh job has no valid account id");
            return JobExecution::Failed {
                error: "credential-refresh job payload has no valid account id".to_owned(),
            };
        };

        let result = self
            .service
            .refresh_if_needed(account_id, &self.owner)
            .await;
        let execution = match result {
            Ok(AuthRefreshOutcome::Unchanged(_) | AuthRefreshOutcome::Refreshed(_)) => {
                tracing::info!(account_id = %account_id, "credential refresh job completed");
                JobExecution::Succeeded
            }
            Err(AuthServiceError::AccountBusy { .. })
            | Err(AuthServiceError::Lease(_))
            | Err(AuthServiceError::Repository(CredentialRepositoryError::Conflict { .. }))
            | Err(AuthServiceError::Repository(CredentialRepositoryError::Backend(_)))
            | Err(AuthServiceError::Refresh(CredentialRefreshError::Transient)) => {
                tracing::warn!(account_id = %account_id, "credential refresh is temporarily unavailable");
                retry_at(now, self.retry_after)
            }
            Err(AuthServiceError::Refresh(CredentialRefreshError::AuthenticationRequired))
            | Err(AuthServiceError::Refresh(CredentialRefreshError::RiskControlled))
            | Err(AuthServiceError::Refresh(CredentialRefreshError::InvalidResponse))
            | Err(AuthServiceError::AccountNotFound { .. })
            | Err(AuthServiceError::AccountDisabled { .. })
            | Err(AuthServiceError::SetupRequired { .. })
            | Err(AuthServiceError::Repository(_))
            | Err(AuthServiceError::Cipher(_))
            | Err(AuthServiceError::Credentials(_)) => {
                tracing::error!(account_id = %account_id, "credential refresh reached a terminal failure");
                JobExecution::Failed {
                    error: "credential refresh cannot complete for this account".to_owned(),
                }
            }
        };
        execution
    }
}

#[async_trait]
impl<S, L, R, C> JobHandler for CredentialRefreshJobHandler<S, L, R, C>
where
    S: CredentialRepository,
    L: AccountLeaseStore + Clone + 'static,
    R: CredentialRefresher,
    C: CredentialCipher,
{
    async fn execute(&self, lease: &JobLease, now: DateTime<Utc>) -> JobExecution {
        self.execute_job(lease, now).await
    }
}

fn retry_at(now: DateTime<Utc>, retry_after: Duration) -> JobExecution {
    now.checked_add_signed(retry_after).map_or_else(
        || JobExecution::Failed {
            error: "credential refresh retry time overflowed".to_owned(),
        },
        |retry_at| JobExecution::Retry {
            retry_at,
            error: "credential refresh temporarily unavailable".to_owned(),
        },
    )
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use chrono::{TimeZone, Utc};
    use secrecy::SecretString;
    use tokio::sync::Mutex;
    use uuid::Uuid;

    use super::*;
    use crate::domain::{
        credentials::AccountLeaseToken,
        job::{JobType, NewJob},
    };
    use crate::persistence::repositories::{
        account_lease_repository::MemoryAccountLeaseRepository,
        credential_repository::{CredentialRepository, MemoryCredentialRepository},
        job_repository::{JobQueue, MemoryJobRepository},
    };

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0)
            .single()
            .expect("test timestamp should be valid")
    }

    fn account_id() -> WeReadAccountId {
        WeReadAccountId::from_uuid(Uuid::from_u128(1))
    }

    fn credentials(expires_at: DateTime<Utc>) -> WeReadCredentials {
        WeReadCredentials::new("access-old", "refresh-old", expires_at, at(0))
            .expect("test credentials should be valid")
    }

    fn cipher() -> RingCredentialCipher {
        RingCredentialCipher::new(&SecretString::new("test-encryption-key".into())).unwrap()
    }

    fn service(
        accounts: MemoryCredentialRepository,
        leases: MemoryAccountLeaseRepository,
        refresher: RecordingRefresher,
    ) -> AuthService<
        MemoryCredentialRepository,
        MemoryAccountLeaseRepository,
        RecordingRefresher,
        RingCredentialCipher,
    > {
        AuthService::new(
            AuthServiceDependencies {
                accounts: accounts.with_lease_store(leases.clone()),
                leases,
                refresher,
                cipher: cipher(),
            },
            AuthServiceConfig::new(
                Duration::seconds(30),
                Duration::seconds(30),
                Duration::seconds(5),
            )
            .unwrap(),
        )
    }

    #[derive(Clone)]
    struct ConflictRepository(MemoryCredentialRepository);

    #[async_trait]
    impl CredentialRepository for ConflictRepository {
        async fn database_now(&self) -> Result<DateTime<Utc>, CredentialRepositoryError> {
            self.0.database_now().await
        }

        async fn list(&self) -> Result<Vec<CredentialRecord>, CredentialRepositoryError> {
            self.0.list().await
        }

        async fn list_all(&self) -> Result<Vec<CredentialRecord>, CredentialRepositoryError> {
            self.0.list_all().await
        }

        async fn find(
            &self,
            account_id: WeReadAccountId,
        ) -> Result<Option<CredentialRecord>, CredentialRepositoryError> {
            self.0.find(account_id).await
        }

        async fn insert(
            &self,
            account_id: WeReadAccountId,
            display_name: &str,
            ciphertext: &[u8],
            access_expires_at: DateTime<Utc>,
        ) -> Result<CredentialRecord, CredentialRepositoryError> {
            self.0
                .insert(account_id, display_name, ciphertext, access_expires_at)
                .await
        }

        async fn replace(
            &self,
            replacement: CredentialReplacement,
        ) -> Result<CredentialRecord, CredentialRepositoryError> {
            Err(CredentialRepositoryError::Conflict {
                account_id: replacement.account_id,
            })
        }

        async fn enroll_placeholder(
            &self,
            _account_id: WeReadAccountId,
            _display_name: &str,
            _ciphertext: &[u8],
            _access_expires_at: DateTime<Utc>,
            _lease_owner: &str,
            _lease_token: AccountLeaseToken,
        ) -> Result<CredentialRecord, CredentialRepositoryError> {
            unreachable!("test only exercises replacement conflicts")
        }

        async fn set_disabled(
            &self,
            account_id: WeReadAccountId,
            disabled: bool,
        ) -> Result<CredentialRecord, CredentialRepositoryError> {
            self.0.set_disabled(account_id, disabled).await
        }

        async fn delete(
            &self,
            account_id: WeReadAccountId,
        ) -> Result<(), CredentialRepositoryError> {
            self.0.delete(account_id).await
        }
    }

    #[derive(Clone, Default)]
    struct RecordingRefresher {
        calls: Arc<AtomicUsize>,
        received_refresh_token: Arc<Mutex<Option<String>>>,
        result: Option<CredentialRefreshError>,
    }

    #[async_trait]
    impl CredentialRefresher for RecordingRefresher {
        async fn refresh(
            &self,
            _account_id: WeReadAccountId,
            refresh_token: &str,
        ) -> Result<RefreshedCredentials, CredentialRefreshError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            *self.received_refresh_token.lock().await = Some(refresh_token.to_owned());
            if let Some(error) = &self.result {
                return Err(error.clone());
            }
            RefreshedCredentials::new("access-new", None, at(3_600))
                .map_err(|_| CredentialRefreshError::InvalidResponse)
        }
    }

    #[test]
    fn cipher_is_authenticated_to_account_and_hides_tokens() {
        let cipher = cipher();
        let encrypted = cipher
            .encrypt(account_id(), &credentials(at(3_600)))
            .unwrap();
        assert!(!String::from_utf8_lossy(&encrypted).contains("access-old"));
        assert_eq!(
            cipher
                .decrypt(account_id(), &encrypted)
                .unwrap()
                .access_token(),
            "access-old"
        );
        assert!(matches!(
            cipher.decrypt(WeReadAccountId::from_uuid(Uuid::from_u128(2)), &encrypted,),
            Err(CredentialCipherError::InvalidCiphertext)
        ));
        let mut truncated = encrypted;
        truncated.pop();
        assert!(matches!(
            cipher.decrypt(account_id(), &truncated),
            Err(CredentialCipherError::InvalidCiphertext)
        ));
    }

    #[tokio::test]
    async fn fresh_credentials_do_not_call_refresh_transport() {
        let accounts = MemoryCredentialRepository::new(at(100));
        let leases = MemoryAccountLeaseRepository::new(at(100));
        let refresher = RecordingRefresher::default();
        let calls = Arc::clone(&refresher.calls);
        let service = service(accounts.clone(), leases, refresher);
        service
            .provision(CredentialProvision {
                account_id: account_id(),
                display_name: "primary".to_owned(),
                credentials: credentials(at(3_600))
                    .with_web_cookie("wr_vid=vid; wr_skey=access-old; wr_rt=refresh-old")
                    .unwrap(),
            })
            .await
            .unwrap();
        let result = service
            .refresh_if_needed(account_id(), "worker-a")
            .await
            .unwrap();
        assert!(matches!(result, AuthRefreshOutcome::Unchanged(_)));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        let stored = accounts.find(account_id()).await.unwrap().unwrap();
        assert!(!stored
            .ciphertext()
            .expect("enrolled account should have ciphertext")
            .windows("access-old".len())
            .any(|window| window == b"access-old"));
    }

    #[tokio::test]
    async fn expired_credentials_refresh_and_rotate_metadata_atomically() {
        let accounts = MemoryCredentialRepository::new(at(100));
        let leases = MemoryAccountLeaseRepository::new(at(100));
        let refresher = RecordingRefresher::default();
        let calls = Arc::clone(&refresher.calls);
        let received = Arc::clone(&refresher.received_refresh_token);
        let service = service(accounts.clone(), leases.clone(), refresher);
        service
            .provision(CredentialProvision {
                account_id: account_id(),
                display_name: "primary".to_owned(),
                credentials: credentials(at(110)),
            })
            .await
            .unwrap();
        accounts.set_now(at(200)).await;
        leases.set_now(at(200)).await;

        let result = service
            .refresh_if_needed(account_id(), "worker-a")
            .await
            .unwrap();
        let AuthRefreshOutcome::Refreshed(account) = result else {
            panic!("expired credentials should be refreshed");
        };
        assert_eq!(account.credential_version(), 2);
        assert_eq!(account.access_expires_at(), Some(at(3_600)));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(*received.lock().await, Some("refresh-old".to_owned()));
        assert!(leases
            .acquire(account_id(), "worker-b", Duration::seconds(10))
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn refresh_failure_releases_lease_without_overwriting_credentials() {
        let accounts = MemoryCredentialRepository::new(at(100));
        let leases = MemoryAccountLeaseRepository::new(at(100));
        let refresher = RecordingRefresher {
            result: Some(CredentialRefreshError::AuthenticationRequired),
            ..RecordingRefresher::default()
        };
        let service = service(accounts.clone(), leases.clone(), refresher);
        service
            .provision(CredentialProvision {
                account_id: account_id(),
                display_name: "primary".to_owned(),
                credentials: credentials(at(110)),
            })
            .await
            .unwrap();

        assert_eq!(
            service
                .refresh_if_needed(account_id(), "worker-a")
                .await
                .unwrap_err()
                .to_string(),
            "credential refresh requires interactive login"
        );
        assert_eq!(
            accounts
                .find(account_id())
                .await
                .unwrap()
                .unwrap()
                .account()
                .credential_version(),
            1
        );
        assert!(leases
            .acquire(account_id(), "worker-b", Duration::seconds(10))
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn a_live_account_lease_prevents_refresh_without_calling_upstream() {
        let accounts = MemoryCredentialRepository::new(at(100));
        let leases = MemoryAccountLeaseRepository::new(at(100));
        let refresher = RecordingRefresher::default();
        let calls = Arc::clone(&refresher.calls);
        let service = service(accounts, leases.clone(), refresher);
        service
            .provision(CredentialProvision {
                account_id: account_id(),
                display_name: "primary".to_owned(),
                credentials: credentials(at(110)),
            })
            .await
            .unwrap();
        let held = leases
            .acquire(account_id(), "worker-b", Duration::seconds(30))
            .await
            .unwrap()
            .expect("the competing worker should hold the lease");

        assert!(matches!(
            service
                .refresh_if_needed(account_id(), "worker-a")
                .await,
            Err(AuthServiceError::AccountBusy { account_id: busy }) if busy == account_id()
        ));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        leases
            .release(account_id(), "worker-b", held.token())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn refresh_job_retries_transient_transport_failures_without_leaking_account_lease() {
        let accounts = MemoryCredentialRepository::new(at(100));
        let leases = MemoryAccountLeaseRepository::new(at(100));
        let refresher = RecordingRefresher {
            result: Some(CredentialRefreshError::Transient),
            ..RecordingRefresher::default()
        };
        let service = service(accounts, leases.clone(), refresher);
        service
            .provision(CredentialProvision {
                account_id: account_id(),
                display_name: "primary".to_owned(),
                credentials: credentials(at(110)),
            })
            .await
            .unwrap();
        let handler =
            CredentialRefreshJobHandler::new(service, "auth-worker", Duration::seconds(5)).unwrap();

        let queue = MemoryJobRepository::new();
        queue
            .enqueue(NewJob {
                job_type: JobType::CredentialRefresh,
                source_id: None,
                priority: 1,
                run_after: at(100),
                max_attempts: 2,
                payload: serde_json::json!({"account_id": account_id().as_uuid()}),
                dedupe_key: "credential-refresh-test".to_owned(),
                now: at(100),
            })
            .await
            .unwrap();
        let job = queue
            .claim_next(
                "job-worker",
                at(100),
                Duration::seconds(30),
                &[JobType::CredentialRefresh],
            )
            .await
            .unwrap()
            .unwrap();

        assert!(matches!(
            handler.execute_job(&job, at(100)).await,
            JobExecution::Retry { retry_at, .. } if retry_at == at(105)
        ));
        assert!(leases
            .acquire(account_id(), "another-worker", Duration::seconds(10))
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn refresh_job_retries_a_fenced_replacement_conflict() {
        let backing = MemoryCredentialRepository::new(at(100));
        let leases = MemoryAccountLeaseRepository::new(at(100));
        let accounts = ConflictRepository(backing);
        let service = AuthService::new(
            AuthServiceDependencies {
                accounts: accounts.clone(),
                leases: leases.clone(),
                refresher: RecordingRefresher::default(),
                cipher: cipher(),
            },
            AuthServiceConfig::new(
                Duration::seconds(30),
                Duration::seconds(30),
                Duration::seconds(5),
            )
            .unwrap(),
        );
        service
            .provision(CredentialProvision {
                account_id: account_id(),
                display_name: "primary".to_owned(),
                credentials: credentials(at(110)),
            })
            .await
            .unwrap();
        let handler =
            CredentialRefreshJobHandler::new(service, "auth-worker", Duration::seconds(5)).unwrap();

        let queue = MemoryJobRepository::new();
        queue
            .enqueue(NewJob {
                job_type: JobType::CredentialRefresh,
                source_id: None,
                priority: 1,
                run_after: at(100),
                max_attempts: 2,
                payload: serde_json::json!({"account_id": account_id().as_uuid()}),
                dedupe_key: "conflicted-credential-refresh-test".to_owned(),
                now: at(100),
            })
            .await
            .unwrap();
        let job = queue
            .claim_next(
                "job-worker",
                at(100),
                Duration::seconds(30),
                &[JobType::CredentialRefresh],
            )
            .await
            .unwrap()
            .unwrap();

        assert!(matches!(
            handler.execute_job(&job, at(100)).await,
            JobExecution::Retry { retry_at, .. } if retry_at == at(105)
        ));
        assert!(leases
            .acquire(account_id(), "another-worker", Duration::seconds(10))
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn refresh_job_rejects_malformed_payload_without_calling_service() {
        let accounts = MemoryCredentialRepository::new(at(100));
        let leases = MemoryAccountLeaseRepository::new(at(100));
        let refresher = RecordingRefresher::default();
        let calls = Arc::clone(&refresher.calls);
        let handler = CredentialRefreshJobHandler::new(
            service(accounts, leases, refresher),
            "auth-worker",
            Duration::seconds(5),
        )
        .unwrap();
        let queue = MemoryJobRepository::new();
        queue
            .enqueue(NewJob {
                job_type: JobType::CredentialRefresh,
                source_id: None,
                priority: 1,
                run_after: at(100),
                max_attempts: 2,
                payload: serde_json::json!({"account_id": "not-a-uuid"}),
                dedupe_key: "malformed-credential-refresh-test".to_owned(),
                now: at(100),
            })
            .await
            .unwrap();
        let job = queue
            .claim_next(
                "job-worker",
                at(100),
                Duration::seconds(30),
                &[JobType::CredentialRefresh],
            )
            .await
            .unwrap()
            .unwrap();

        assert!(matches!(
            handler.execute_job(&job, at(100)).await,
            JobExecution::Failed { error } if error == "credential-refresh job payload has no valid account id"
        ));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn rejects_invalid_auth_timing_and_expiry_boundaries() {
        assert!(matches!(
            AuthServiceConfig::new(
                Duration::seconds(1),
                Duration::seconds(10),
                Duration::seconds(10),
            ),
            Err(AuthServiceConfigError::HeartbeatNotBeforeLeaseExpiry)
        ));
        assert!(matches!(
            AuthServiceConfig::new(
                Duration::seconds(-1),
                Duration::seconds(10),
                Duration::seconds(1),
            ),
            Err(AuthServiceConfigError::NegativeRefreshWindow)
        ));
        let value = credentials(DateTime::<Utc>::MAX_UTC);
        assert!(value.needs_refresh(DateTime::<Utc>::MAX_UTC, Duration::seconds(1)));
    }
    #[tokio::test]
    async fn account_status_returns_metadata_without_decrypting_or_exposing_tokens() {
        let accounts = MemoryCredentialRepository::new(at(100));
        let service = service(
            accounts,
            MemoryAccountLeaseRepository::new(at(100)),
            RecordingRefresher::default(),
        );
        service
            .provision(CredentialProvision {
                account_id: account_id(),
                display_name: "primary".to_owned(),
                credentials: credentials(at(3_600))
                    .with_web_cookie("wr_vid=vid-old; wr_skey=access-old; wr_rt=refresh-old")
                    .unwrap(),
            })
            .await
            .unwrap();

        let account = service.account(account_id()).await.unwrap();
        assert_eq!(account.display_name(), "primary");
        assert_eq!(account.credential_version(), 1);
        assert_eq!(account.access_expires_at(), Some(at(3_600)));
        assert!(matches!(
            service
                .account(WeReadAccountId::from_uuid(Uuid::from_u128(99)))
                .await,
            Err(AuthServiceError::AccountNotFound { .. })
        ));
    }

    #[tokio::test]
    async fn account_lifecycle_can_disable_reauthenticate_rename_enable_and_delete() {
        let accounts = MemoryCredentialRepository::new(at(100));
        let leases = MemoryAccountLeaseRepository::new(at(100));
        let service = service(accounts, leases, RecordingRefresher::default());
        service
            .provision(CredentialProvision {
                account_id: account_id(),
                display_name: "primary".to_owned(),
                credentials: credentials(at(3_600))
                    .with_web_cookie("wr_vid=vid-old; wr_skey=access-old; wr_rt=refresh-old")
                    .unwrap(),
            })
            .await
            .unwrap();

        let disabled = service.set_disabled(account_id(), true).await.unwrap();
        assert!(disabled.disabled());
        assert!(matches!(
            service.credentials(account_id()).await,
            Err(AuthServiceError::AccountDisabled { .. })
        ));

        let replaced = service
            .replace(
                CredentialProvision {
                    account_id: account_id(),
                    display_name: "renamed".to_owned(),
                    credentials: WeReadCredentials::new(
                        "access-new",
                        "refresh-new",
                        at(7_200),
                        at(100),
                    )
                    .unwrap()
                    .with_web_cookie("wr_vid=vid-new; wr_skey=access-new; wr_rt=refresh-new")
                    .unwrap(),
                },
                "admin:primary",
            )
            .await
            .unwrap();
        assert_eq!(replaced.display_name(), "renamed");
        assert!(replaced.disabled());

        let enabled = service.set_disabled(account_id(), false).await.unwrap();
        assert!(!enabled.disabled());
        let stored = service.credentials(account_id()).await.unwrap();
        assert_eq!(stored.access_token(), "access-new");
        assert_eq!(
            stored.web_cookie(),
            Some("wr_vid=vid-new; wr_skey=access-new; wr_rt=refresh-new")
        );

        service.delete(account_id()).await.unwrap();
        assert!(matches!(
            service.delete(account_id()).await,
            Err(AuthServiceError::AccountNotFound { .. })
        ));
    }

    #[tokio::test]
    async fn manual_replacement_rotates_credentials_and_advances_version() {
        let accounts = MemoryCredentialRepository::new(at(100));
        let leases = MemoryAccountLeaseRepository::new(at(100));
        let service = service(accounts, leases, RecordingRefresher::default());
        service
            .provision(CredentialProvision {
                account_id: account_id(),
                display_name: "primary".to_owned(),
                credentials: credentials(at(3_600))
                    .with_web_cookie("wr_vid=vid-old; wr_skey=access-old; wr_rt=refresh-old")
                    .unwrap(),
            })
            .await
            .unwrap();

        let updated = service
            .replace(
                CredentialProvision {
                    account_id: account_id(),
                    display_name: "primary".to_owned(),
                    credentials: WeReadCredentials::new(
                        "access-new",
                        "refresh-new",
                        at(7_200),
                        at(100),
                    )
                    .unwrap()
                    .with_web_cookie("wr_vid=vid-new; wr_skey=access-new; wr_rt=refresh-new")
                    .unwrap(),
                },
                "admin:primary",
            )
            .await
            .unwrap();

        assert_eq!(updated.credential_version(), 2);
        let stored = service.credentials(account_id()).await.unwrap();
        assert_eq!(stored.access_token(), "access-new");
        assert_eq!(stored.refresh_token(), "refresh-new");
        assert_eq!(
            stored.web_cookie(),
            Some("wr_vid=vid-new; wr_skey=access-new; wr_rt=refresh-new")
        );
    }

    #[tokio::test]
    async fn manual_replacement_reports_busy_when_another_owner_holds_lease() {
        let accounts = MemoryCredentialRepository::new(at(100));
        let leases = MemoryAccountLeaseRepository::new(at(100));
        let service = service(accounts, leases.clone(), RecordingRefresher::default());
        service
            .provision(CredentialProvision {
                account_id: account_id(),
                display_name: "primary".to_owned(),
                credentials: credentials(at(3_600)),
            })
            .await
            .unwrap();
        assert!(leases
            .acquire(account_id(), "other-owner", Duration::minutes(5))
            .await
            .unwrap()
            .is_some());

        assert!(matches!(
            service
                .replace(
                    CredentialProvision {
                        account_id: account_id(),
                        display_name: "primary".to_owned(),
                        credentials: credentials(at(7_200)),
                    },
                    "admin:primary",
                )
                .await,
            Err(AuthServiceError::AccountBusy { .. })
        ));
    }
}

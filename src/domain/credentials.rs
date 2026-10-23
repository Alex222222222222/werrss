//! WeRead credential domain model.
//!
//! This module describes access tokens, refresh tokens, device identity,
//! profile metadata, account labels, and credential lifecycle state.
//!
//! Responsibilities: distinguish basic configured credentials from refreshable
//! credentials, provide a stable non-secret `WeReadAccountId`, and document
//! secret-handling and distributed account-lease invariants.
//!
//! Non-responsibilities: QR polling, credential exchange, encryption key
//! management, database persistence, or exposing login state over HTTP.
//!
//! Security: secret fields must be wrapped in secrecy-aware types, excluded
//! from logs and API serialization, and encrypted before PostgreSQL storage.

//! High availability: authenticated account use is fenced by a durable account
//! lease. The lease stores only account identity and ownership metadata, never
//! access/refresh tokens. Version one may expose one account while retaining the
//! explicit identifier required for cross-replica serialization.

use std::fmt;

use chrono::{DateTime, Duration, Utc};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

/// Errors raised while constructing refreshable WeRead credentials.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CredentialError {
    /// A token must contain non-whitespace data.
    #[error("credential token must not be empty")]
    EmptyToken,
    /// Access credentials must expire after they are issued.
    #[error("access credential expiry must be in the future")]
    ExpiryNotAfterIssue,
    /// Browser cookie headers must contain valid name/value pairs.
    #[error("WeRead web cookie header is invalid")]
    InvalidWebCookie,
}

/// The secret material needed for authenticated WeRead requests.
///
/// The values are intentionally not serializable or printable. Persistence
/// must encrypt this value before it reaches PostgreSQL; callers should only
/// borrow the secrets for the duration of one authenticated request.
#[derive(Clone)]
pub struct WeReadCredentials {
    access_token: SecretString,
    refresh_token: SecretString,
    web_cookie: Option<SecretString>,
    access_expires_at: DateTime<Utc>,
}

impl fmt::Debug for WeReadCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WeReadCredentials")
            .field("access_token", &"<secret>")
            .field("refresh_token", &"<secret>")
            .field("access_expires_at", &self.access_expires_at)
            .finish()
    }
}

impl WeReadCredentials {
    /// Creates credentials and validates their non-secret lifecycle metadata.
    pub fn new(
        access_token: impl Into<String>,
        refresh_token: impl Into<String>,
        access_expires_at: DateTime<Utc>,
        issued_at: DateTime<Utc>,
    ) -> Result<Self, CredentialError> {
        let access_token = access_token.into();
        let refresh_token = refresh_token.into();
        if access_token.trim().is_empty() || refresh_token.trim().is_empty() {
            return Err(CredentialError::EmptyToken);
        }
        if access_expires_at <= issued_at {
            return Err(CredentialError::ExpiryNotAfterIssue);
        }
        Ok(Self {
            access_token: SecretString::new(access_token.into_boxed_str()),
            refresh_token: SecretString::new(refresh_token.into_boxed_str()),
            web_cookie: None,
            access_expires_at,
        })
    }

    /// Attaches the browser cookie header used by WeRead's web article APIs.
    ///
    /// The header is kept separate from the refresh-token values because the
    /// web API authenticates with cookies rather than an OAuth bearer token.
    /// It is validated before being retained and remains secret material.
    pub fn with_web_cookie(
        mut self,
        cookie_header: impl Into<String>,
    ) -> Result<Self, CredentialError> {
        let cookie_header = cookie_header.into();
        if cookie_header.trim().is_empty()
            || cookie_header.chars().any(char::is_control)
            || cookie_header.split(';').any(|part| {
                let part = part.trim();
                part.is_empty()
                    || part
                        .split_once('=')
                        .is_none_or(|(name, _value)| name.trim().is_empty())
            })
        {
            return Err(CredentialError::InvalidWebCookie);
        }
        self.web_cookie = Some(SecretString::new(cookie_header.into_boxed_str()));
        Ok(self)
    }

    /// Returns the access token only to the authenticated transport boundary.
    pub fn access_token(&self) -> &str {
        self.access_token.expose_secret()
    }

    /// Returns the refresh token only to the refresh transport boundary.
    pub fn refresh_token(&self) -> &str {
        self.refresh_token.expose_secret()
    }

    /// Returns the browser cookie header only to the authenticated transport.
    pub fn web_cookie(&self) -> Option<&str> {
        self.web_cookie.as_ref().map(ExposeSecret::expose_secret)
    }

    /// Returns the access-token expiry without exposing secret material.
    pub const fn access_expires_at(&self) -> DateTime<Utc> {
        self.access_expires_at
    }

    /// Reports whether refresh should happen before an authenticated request.
    pub fn needs_refresh(&self, now: DateTime<Utc>, refresh_before: Duration) -> bool {
        now.checked_add_signed(refresh_before)
            .is_none_or(|deadline| self.access_expires_at <= deadline)
    }

    /// Replaces tokens after a successful refresh response.
    pub fn refreshed(
        &self,
        access_token: impl Into<String>,
        refresh_token: Option<String>,
        access_expires_at: DateTime<Utc>,
        issued_at: DateTime<Utc>,
    ) -> Result<Self, CredentialError> {
        let credentials = Self::new(
            access_token,
            refresh_token.unwrap_or_else(|| self.refresh_token().to_owned()),
            access_expires_at,
            issued_at,
        )?;
        match self.web_cookie() {
            Some(cookie) => credentials.with_web_cookie(cookie.to_owned()),
            None => Ok(credentials),
        }
    }
}

/// Non-secret account state returned by authentication status operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WeReadAccount {
    account_id: WeReadAccountId,
    display_name: String,
    credential_version: i64,
    access_expires_at: Option<DateTime<Utc>>,
    disabled: bool,
    setup_required: bool,
}

impl WeReadAccount {
    /// Reconstructs account metadata returned by a trusted repository.
    pub(crate) fn from_parts(
        account_id: WeReadAccountId,
        display_name: String,
        credential_version: i64,
        access_expires_at: Option<DateTime<Utc>>,
        disabled: bool,
        setup_required: bool,
    ) -> Self {
        Self {
            account_id,
            display_name,
            credential_version,
            access_expires_at,
            disabled,
            setup_required,
        }
    }

    /// Returns the stable account identity.
    pub const fn account_id(&self) -> WeReadAccountId {
        self.account_id
    }
    /// Returns the operator-facing label.
    pub fn display_name(&self) -> &str {
        &self.display_name
    }
    /// Returns the optimistic credential revision.
    pub const fn credential_version(&self) -> i64 {
        self.credential_version
    }
    /// Returns the access-token expiry, if credentials are enrolled.
    pub const fn access_expires_at(&self) -> Option<DateTime<Utc>> {
        self.access_expires_at
    }
    /// Reports whether account credentials still require setup.
    pub const fn setup_required(&self) -> bool {
        self.setup_required
    }
    /// Reports whether this account is disabled.
    pub const fn disabled(&self) -> bool {
        self.disabled
    }
}

/// Stable identity of one configured WeRead account.
///
/// This identifier is deliberately separate from credential material and is
/// safe to use in job payloads, source relationships, logs, and lease rows.
/// The account record that owns the credentials will be added by a later
/// persistence slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WeReadAccountId(Uuid);

impl WeReadAccountId {
    /// Wraps the UUID assigned to an account.
    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    /// Returns the durable UUID.
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Display for WeReadAccountId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Fencing token for one incarnation of an account lease.
///
/// A new token is generated for every acquisition, including takeover after
/// expiry. Workers must retain this token and present it for heartbeat and
/// release so a stale replica cannot mutate a later owner’s lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AccountLeaseToken(Uuid);

impl AccountLeaseToken {
    /// Generates a token for a new lease incarnation.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Wraps a token read from PostgreSQL.
    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    /// Returns the UUID persisted with the lease.
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for AccountLeaseToken {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of a currently held account lease.
///
/// Lease snapshots contain no access or refresh token. They are only the
/// capability required to heartbeat or release a distributed account lock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountLease {
    account_id: WeReadAccountId,
    owner: String,
    token: AccountLeaseToken,
    lease_until: DateTime<Utc>,
    heartbeat_at: DateTime<Utc>,
}

impl AccountLease {
    /// Reconstructs a lease returned by a trusted repository.
    pub(crate) fn from_parts(
        account_id: WeReadAccountId,
        owner: String,
        token: AccountLeaseToken,
        lease_until: DateTime<Utc>,
        heartbeat_at: DateTime<Utc>,
    ) -> Self {
        Self {
            account_id,
            owner,
            token,
            lease_until,
            heartbeat_at,
        }
    }

    /// Returns the leased account identity.
    pub const fn account_id(&self) -> WeReadAccountId {
        self.account_id
    }

    /// Returns the owning application instance.
    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// Returns the fencing token for this lease incarnation.
    pub const fn token(&self) -> AccountLeaseToken {
        self.token
    }

    /// Returns the expiry timestamp assigned by the repository clock.
    pub const fn lease_until(&self) -> DateTime<Utc> {
        self.lease_until
    }

    /// Returns the last heartbeat or acquisition timestamp.
    pub const fn heartbeat_at(&self) -> DateTime<Utc> {
        self.heartbeat_at
    }

    /// Reports whether the lease is live at an explicitly supplied instant.
    pub fn is_live_at(&self, now: DateTime<Utc>) -> bool {
        self.lease_until > now
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn credentials() -> WeReadCredentials {
        WeReadCredentials::new(
            "access",
            "refresh",
            DateTime::<Utc>::UNIX_EPOCH + Duration::hours(1),
            DateTime::<Utc>::UNIX_EPOCH,
        )
        .unwrap()
    }

    #[test]
    fn web_cookie_is_retained_across_refresh() {
        let credentials = credentials()
            .with_web_cookie("wr_vid=vid; wr_skey=access; wr_rt=refresh")
            .unwrap();
        let refreshed = credentials
            .refreshed(
                "new-access",
                Some("new-refresh".to_owned()),
                DateTime::<Utc>::UNIX_EPOCH + Duration::hours(2),
                DateTime::<Utc>::UNIX_EPOCH,
            )
            .unwrap();

        assert_eq!(
            refreshed.web_cookie(),
            Some("wr_vid=vid; wr_skey=access; wr_rt=refresh")
        );
        assert_eq!(refreshed.access_token(), "new-access");
        assert_eq!(refreshed.refresh_token(), "new-refresh");
    }

    #[test]
    fn web_cookie_rejects_empty_pairs_and_control_characters() {
        for cookie in [
            "",
            "wr_skey=access;",
            "wr_skey=access; =value",
            "wr_skey=access\n",
        ] {
            assert!(matches!(
                credentials().with_web_cookie(cookie),
                Err(CredentialError::InvalidWebCookie)
            ));
        }
    }

    #[test]
    fn web_cookie_accepts_optional_empty_values() {
        let cookie = "wr_avatar=; wr_skey=access; wr_rt=refresh; _qimei_h38=";
        assert_eq!(
            credentials().with_web_cookie(cookie).unwrap().web_cookie(),
            Some(cookie)
        );
    }
}

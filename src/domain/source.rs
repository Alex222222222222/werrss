//! Source domain model.
//!
//! A source represents one subscribed WeChat public account. It contains the
//! normalized `book_id`, display name, optional originating article URL, enabled state,
//! sync interval, RSS item limit, stable WeRead account relationship, monotonic
//! feed revision, scheduling timestamps, and scheduling gate.
//!
//! Responsibilities: document source identity, validation bounds, lifecycle
//! state, and the relationship between a source mutation and feed-cache
//! invalidation.
//!
//! Non-responsibilities: source persistence, URL resolution, job insertion,
//! browser access, and HTTP form validation.
//!
//! High availability: source scheduling data is persisted in PostgreSQL and
//! must not rely on one process's memory. Changes should enqueue or invalidate
//! work through application services.
//!
//! The scheduling gate is one of `ready`, `authentication_required`, or
//! `risk_controlled`; `enabled=false` is the operator-controlled pause. Only
//! enabled, ready, due sources are automatically enqueued. Feed-visible changes
//! increment the source revision atomically with their persistence.

use std::{fmt, str::FromStr};

use chrono::{DateTime, Duration, Utc};
use serde::{de::Error as SerdeError, Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;
use url::Url;
use uuid::Uuid;

use super::credentials::WeReadAccountId;

/// Stable identity of one subscribed source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SourceId(Uuid);

impl SourceId {
    /// Wraps the UUID assigned to a source.
    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    /// Returns the durable UUID.
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Display for SourceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Monotonic revision of the feed-visible state for one source.
///
/// The value is persisted as a non-negative PostgreSQL `BIGINT`. A revision
/// changes only when the normalized source/article data can change RSS output;
/// idempotent retries must reuse the current value. It is intentionally kept
/// separate from timestamps because timestamps cannot fence two concurrent
/// rebuilds reliably.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FeedRevision(u64);

impl FeedRevision {
    /// Returns the initial revision for a newly created source.
    pub const fn zero() -> Self {
        Self(0)
    }

    /// Wraps a persisted or otherwise trusted revision value.
    pub const fn from_u64(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric value used by domain comparisons and persistence.
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Advances the revision, returning `None` only at the numeric limit.
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

impl fmt::Display for FeedRevision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Errors raised while constructing or advancing a source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SourceError {
    /// A source UUID must identify an actual source.
    #[error("source id must not be nil")]
    InvalidId,
    /// A normalized WeRead book identifier is required.
    #[error("source book_id must not be empty")]
    EmptyBookId,
    /// A source must have a display name for feed metadata and administration.
    #[error("source display_name must not be empty")]
    EmptyDisplayName,
    /// A URL value object cannot be constructed from an empty string.
    #[error("source article_url must not be empty")]
    EmptyArticleUrl,
    /// The source article URL must identify an allowed public WeChat page.
    #[error(
        "source article_url must be an https mp.weixin.qq.com URL without credentials, fragments, or a non-default port"
    )]
    InvalidArticleUrl,
    /// Source synchronization must have a positive interval.
    #[error("source sync_interval must be positive")]
    InvalidSyncInterval,
    /// RSS item limits must leave room for at least one item.
    #[error("source rss_item_limit must be positive")]
    InvalidRssItemLimit,
    /// Retry limits must leave room for at least one attempt.
    #[error("source max_attempts must be positive")]
    InvalidMaxAttempts,
    /// A configured account UUID must identify an account.
    #[error("source account id must not be nil")]
    InvalidAccountId,
    /// A persisted scheduling gate is outside the supported state machine.
    #[error("source scheduling gate is invalid")]
    InvalidSchedulingGate,
    /// A feed revision cannot advance beyond the persisted numeric range.
    #[error("source feed revision is exhausted")]
    FeedRevisionExhausted,
    /// A persisted feed revision is outside the supported non-negative range.
    #[error("source feed revision is invalid")]
    InvalidRevision,
}

/// A canonical, safe destination for a public WeChat article page.
///
/// This value object is the domain-side capability boundary shared by source
/// persistence and future public-page acquisition. It accepts only `https`
/// URLs whose host is exactly `mp.weixin.qq.com`; URL user information,
/// fragments, and non-default ports are rejected. An explicit HTTPS default
/// port is normalized away. Query parameters remain available because WeChat
/// article identity can be carried in the query string.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VerifiedWechatArticleUrl(String);

impl VerifiedWechatArticleUrl {
    const HOST: &'static str = "mp.weixin.qq.com";

    /// Parses and canonicalizes a public WeChat article URL.
    pub fn parse(value: &str) -> Result<Self, SourceError> {
        let value = value.trim();
        if value.is_empty() {
            return Err(SourceError::EmptyArticleUrl);
        }

        let mut url = Url::parse(value).map_err(|_| SourceError::InvalidArticleUrl)?;
        if url.scheme() != "https"
            || url.host_str() != Some(Self::HOST)
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || url.port().is_some_and(|port| port != 443)
            || crate::domain::url_policy::has_provider_signature(&url)
        {
            return Err(SourceError::InvalidArticleUrl);
        }

        if url.port() == Some(443) {
            url.set_port(None)
                .map_err(|_| SourceError::InvalidArticleUrl)?;
        }
        Ok(Self(url.to_string()))
    }

    /// Returns the canonical URL string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for VerifiedWechatArticleUrl {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for VerifiedWechatArticleUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for VerifiedWechatArticleUrl {
    type Err = SourceError;

    /// Parses a canonical public WeChat article URL.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for VerifiedWechatArticleUrl {
    type Error = SourceError;

    /// Parses an owned URL string and retains only its canonical form.
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}

impl Serialize for VerifiedWechatArticleUrl {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for VerifiedWechatArticleUrl {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(SerdeError::custom)
    }
}

/// Scheduling gate persisted with a source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulingGate {
    /// Automatic synchronization is allowed when the source is due.
    Ready,
    /// Authentication must be repaired before automatic synchronization.
    AuthenticationRequired,
    /// An operator must clear a risk-control condition before resuming work.
    RiskControlled,
}

impl SchedulingGate {
    /// Returns whether the source can be selected by the scheduler.
    pub const fn is_automatically_eligible(self) -> bool {
        matches!(self, Self::Ready)
    }

    /// Returns the stable value stored in PostgreSQL.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::AuthenticationRequired => "authentication_required",
            Self::RiskControlled => "risk_controlled",
        }
    }
}

impl FromStr for SchedulingGate {
    type Err = SourceError;

    /// Parses the stable value stored in PostgreSQL.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "ready" => Ok(Self::Ready),
            "authentication_required" => Ok(Self::AuthenticationRequired),
            "risk_controlled" => Ok(Self::RiskControlled),
            _ => Err(SourceError::InvalidSchedulingGate),
        }
    }
}

/// Input required to create a normalized source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewSource {
    /// Durable source identity.
    pub id: SourceId,
    /// Normalized WeRead book identifier.
    pub book_id: String,
    /// Human-readable source name.
    pub display_name: String,
    /// Verified originating public WeChat article URL, when one was supplied.
    pub article_url: Option<VerifiedWechatArticleUrl>,
    /// Whether automatic synchronization is enabled.
    pub enabled: bool,
    /// Interval used by successful synchronization scheduling.
    pub sync_interval: Duration,
    /// Maximum number of articles included in an RSS document.
    pub rss_item_limit: u32,
    /// Account used by authenticated WeRead list acquisition, if configured.
    pub account_id: Option<WeReadAccountId>,
    /// Initial scheduling gate.
    pub scheduling_gate: SchedulingGate,
    /// First time at which automatic synchronization may run.
    pub next_fetch_at: DateTime<Utc>,
    /// Scheduling priority copied to source-sync jobs.
    pub priority: i32,
    /// Retry failure budget copied to source-sync jobs.
    pub max_attempts: u32,
}

/// Feed and scheduling configuration that replaces the mutable fields of an
/// existing source while preserving its durable ID and scheduler state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceUpdate {
    /// Normalized WeRead book identifier.
    pub book_id: String,
    /// Human-readable source name.
    pub display_name: String,
    /// Verified originating public WeChat article URL, if configured.
    pub article_url: Option<VerifiedWechatArticleUrl>,
    /// Interval used by successful synchronization scheduling.
    pub sync_interval: Duration,
    /// Maximum number of articles included in an RSS document.
    pub rss_item_limit: u32,
    /// Account used for authenticated list acquisition, if configured.
    pub account_id: Option<WeReadAccountId>,
    /// Scheduling priority copied to source-sync jobs.
    pub priority: i32,
    /// Retry failure budget copied to source-sync jobs.
    pub max_attempts: u32,
}

/// Partial source configuration used by administrative edits.
///
/// `None` means that a field was omitted and must retain its current value.
/// The nested options for `article_url` and `account_id` additionally allow a
/// caller to distinguish omission from explicitly clearing a nullable field.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SourcePatch {
    /// Replacement WeRead book identifier, when supplied.
    pub book_id: Option<String>,
    /// Replacement human-readable source name, when supplied.
    pub display_name: Option<String>,
    /// Replacement URL, explicit `None` to clear, or omitted.
    pub article_url: Option<Option<VerifiedWechatArticleUrl>>,
    /// Replacement synchronization interval, when supplied.
    pub sync_interval: Option<Duration>,
    /// Replacement maximum RSS item count, when supplied.
    pub rss_item_limit: Option<u32>,
    /// Replacement account binding, explicit `None` to clear, or omitted.
    pub account_id: Option<Option<WeReadAccountId>>,
    /// Replacement scheduling priority, when supplied.
    pub priority: Option<i32>,
    /// Replacement retry failure budget, when supplied.
    pub max_attempts: Option<u32>,
}

impl NewSource {
    /// Returns a compact development/default source specification.
    #[cfg(test)]
    pub(crate) fn test_default() -> Self {
        Self {
            id: SourceId::from_uuid(Uuid::from_u128(1)),
            book_id: "book-1".to_owned(),
            display_name: "Example".to_owned(),
            article_url: Some(
                "https://mp.weixin.qq.com/s/example"
                    .parse()
                    .expect("test URL should be valid"),
            ),
            enabled: true,
            sync_interval: Duration::hours(1),
            rss_item_limit: 20,
            account_id: None,
            scheduling_gate: SchedulingGate::Ready,
            next_fetch_at: Utc::now(),
            priority: 10,
            max_attempts: 3,
        }
    }
}

/// Normalized source aggregate used by source and scheduler application ports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source {
    id: SourceId,
    book_id: String,
    display_name: String,
    article_url: Option<VerifiedWechatArticleUrl>,
    enabled: bool,
    sync_interval: Duration,
    rss_item_limit: u32,
    account_id: Option<WeReadAccountId>,
    scheduling_gate: SchedulingGate,
    feed_revision: FeedRevision,
    next_fetch_at: DateTime<Utc>,
    failure_cooldown_until: Option<DateTime<Utc>>,
    schedule_reserved_until: Option<DateTime<Utc>>,
    priority: i32,
    max_attempts: u32,
}

/// Trusted persisted state used to reconstruct a [`Source`] from a repository.
///
/// The fields stay crate-visible so persistence can perform row decoding
/// without exposing a public construction path that bypasses validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SourceParts {
    pub(crate) id: SourceId,
    pub(crate) book_id: String,
    pub(crate) display_name: String,
    pub(crate) article_url: Option<VerifiedWechatArticleUrl>,
    pub(crate) enabled: bool,
    pub(crate) sync_interval: Duration,
    pub(crate) rss_item_limit: u32,
    pub(crate) account_id: Option<WeReadAccountId>,
    pub(crate) scheduling_gate: SchedulingGate,
    pub(crate) feed_revision: FeedRevision,
    pub(crate) next_fetch_at: DateTime<Utc>,
    pub(crate) failure_cooldown_until: Option<DateTime<Utc>>,
    pub(crate) schedule_reserved_until: Option<DateTime<Utc>>,
    pub(crate) priority: i32,
    pub(crate) max_attempts: u32,
}

impl Source {
    /// Creates and validates a new source with revision zero and no transient
    /// scheduler reservation or failure cooldown.
    pub fn new(spec: NewSource) -> Result<Self, SourceError> {
        let source = Self {
            id: spec.id,
            book_id: spec.book_id.trim().to_owned(),
            display_name: spec.display_name.trim().to_owned(),
            article_url: spec.article_url,
            enabled: spec.enabled,
            sync_interval: spec.sync_interval,
            rss_item_limit: spec.rss_item_limit,
            account_id: spec.account_id,
            scheduling_gate: spec.scheduling_gate,
            feed_revision: FeedRevision::zero(),
            next_fetch_at: spec.next_fetch_at,
            failure_cooldown_until: None,
            schedule_reserved_until: None,
            priority: spec.priority,
            max_attempts: spec.max_attempts,
        };
        source.validate()?;
        Ok(source)
    }

    /// Reconstructs a source returned by a trusted repository.
    pub(crate) fn from_parts(parts: SourceParts) -> Result<Self, SourceError> {
        let source = Self {
            id: parts.id,
            book_id: parts.book_id,
            display_name: parts.display_name,
            article_url: parts.article_url,
            enabled: parts.enabled,
            sync_interval: parts.sync_interval,
            rss_item_limit: parts.rss_item_limit,
            account_id: parts.account_id,
            scheduling_gate: parts.scheduling_gate,
            feed_revision: parts.feed_revision,
            next_fetch_at: parts.next_fetch_at,
            failure_cooldown_until: parts.failure_cooldown_until,
            schedule_reserved_until: parts.schedule_reserved_until,
            priority: parts.priority,
            max_attempts: parts.max_attempts,
        };
        source.validate()?;
        Ok(source)
    }

    /// Applies operator-editable source fields without resetting scheduler
    /// timestamps, transient gates, or accumulated feed state.
    ///
    /// A changed book ID, display name, article URL, or RSS item limit can
    /// change the rendered feed and therefore advances the feed revision. The
    /// caller persists the returned value atomically with the update.
    pub fn apply_update(&self, update: SourceUpdate) -> Result<Self, SourceError> {
        let mut updated = self.clone();
        updated.book_id = update.book_id.trim().to_owned();
        updated.display_name = update.display_name.trim().to_owned();
        updated.article_url = update.article_url;
        updated.sync_interval = update.sync_interval;
        updated.rss_item_limit = update.rss_item_limit;
        updated.account_id = update.account_id;
        updated.priority = update.priority;
        updated.max_attempts = update.max_attempts;
        updated.validate()?;

        let feed_visible_change = updated.book_id != self.book_id
            || updated.display_name != self.display_name
            || updated.article_url != self.article_url
            || updated.rss_item_limit != self.rss_item_limit;
        if feed_visible_change {
            updated.advance_feed_revision()?;
        }
        Ok(updated)
    }

    /// Applies a partial operator update to the current source.
    ///
    /// Persistence adapters must call this after acquiring their row lock so
    /// omitted fields are merged with the latest committed source state.
    pub fn apply_patch(&self, patch: SourcePatch) -> Result<Self, SourceError> {
        self.apply_update(SourceUpdate {
            book_id: patch.book_id.unwrap_or_else(|| self.book_id().to_owned()),
            display_name: patch
                .display_name
                .unwrap_or_else(|| self.display_name().to_owned()),
            article_url: patch
                .article_url
                .unwrap_or_else(|| self.article_url().cloned()),
            sync_interval: patch.sync_interval.unwrap_or_else(|| self.sync_interval()),
            rss_item_limit: patch
                .rss_item_limit
                .unwrap_or_else(|| self.rss_item_limit()),
            account_id: patch.account_id.unwrap_or_else(|| self.account_id()),
            priority: patch.priority.unwrap_or_else(|| self.priority()),
            max_attempts: patch.max_attempts.unwrap_or_else(|| self.max_attempts()),
        })
    }

    fn validate(&self) -> Result<(), SourceError> {
        if self.id.as_uuid().is_nil() {
            return Err(SourceError::InvalidId);
        }
        if self.book_id.is_empty() {
            return Err(SourceError::EmptyBookId);
        }
        if self.display_name.is_empty() {
            return Err(SourceError::EmptyDisplayName);
        }
        if self.sync_interval <= Duration::zero() {
            return Err(SourceError::InvalidSyncInterval);
        }
        if self.sync_interval != Duration::seconds(self.sync_interval.num_seconds()) {
            return Err(SourceError::InvalidSyncInterval);
        }
        if self.rss_item_limit == 0 {
            return Err(SourceError::InvalidRssItemLimit);
        }
        if self.max_attempts == 0 {
            return Err(SourceError::InvalidMaxAttempts);
        }
        if self
            .account_id
            .is_some_and(|account_id| account_id.as_uuid().is_nil())
        {
            return Err(SourceError::InvalidAccountId);
        }
        Ok(())
    }

    /// Returns the stable source identity.
    pub const fn id(&self) -> SourceId {
        self.id
    }

    /// Returns the normalized WeRead book identifier.
    pub fn book_id(&self) -> &str {
        &self.book_id
    }

    /// Returns the display name.
    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    /// Returns the optional source article URL used for identity acquisition.
    pub fn article_url(&self) -> Option<&VerifiedWechatArticleUrl> {
        self.article_url.as_ref()
    }

    /// Returns whether automatic synchronization is enabled.
    pub const fn enabled(&self) -> bool {
        self.enabled
    }

    /// Returns the configured interval between successful source fetches.
    pub const fn sync_interval(&self) -> Duration {
        self.sync_interval
    }

    /// Returns the maximum number of articles rendered into the feed.
    pub const fn rss_item_limit(&self) -> u32 {
        self.rss_item_limit
    }

    /// Returns the stable account used for authenticated list acquisition.
    pub const fn account_id(&self) -> Option<WeReadAccountId> {
        self.account_id
    }

    /// Returns the persisted scheduling gate.
    pub const fn scheduling_gate(&self) -> SchedulingGate {
        self.scheduling_gate
    }

    /// Returns the feed-visible revision.
    pub const fn feed_revision(&self) -> FeedRevision {
        self.feed_revision
    }

    /// Returns the next automatic synchronization eligibility time.
    pub const fn next_fetch_at(&self) -> DateTime<Utc> {
        self.next_fetch_at
    }

    /// Returns the optional cooldown expiry after ordinary failures.
    pub const fn failure_cooldown_until(&self) -> Option<DateTime<Utc>> {
        self.failure_cooldown_until
    }

    /// Returns the scheduler's short reservation expiry.
    pub const fn schedule_reserved_until(&self) -> Option<DateTime<Utc>> {
        self.schedule_reserved_until
    }

    /// Returns the scheduling priority copied to source-sync jobs.
    pub const fn priority(&self) -> i32 {
        self.priority
    }

    /// Returns the retry failure budget copied to source-sync jobs.
    pub const fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    /// Advances the feed-visible revision, or reports exhaustion.
    pub fn advance_feed_revision(&mut self) -> Result<FeedRevision, SourceError> {
        let next = self
            .feed_revision
            .next()
            .ok_or(SourceError::FeedRevisionExhausted)?;
        self.feed_revision = next;
        Ok(next)
    }
}

/// Fencing token for one feed-build lease incarnation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FeedBuildLeaseToken(Uuid);

impl FeedBuildLeaseToken {
    /// Generates a token for a new feed-build lease incarnation.
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

impl Default for FeedBuildLeaseToken {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of a currently held per-source feed-build lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedBuildLease {
    source_id: SourceId,
    owner: String,
    token: FeedBuildLeaseToken,
    lease_until: DateTime<Utc>,
    heartbeat_at: DateTime<Utc>,
}

impl FeedBuildLease {
    /// Reconstructs a lease returned by a trusted repository.
    pub(crate) fn from_parts(
        source_id: SourceId,
        owner: String,
        token: FeedBuildLeaseToken,
        lease_until: DateTime<Utc>,
        heartbeat_at: DateTime<Utc>,
    ) -> Self {
        Self {
            source_id,
            owner,
            token,
            lease_until,
            heartbeat_at,
        }
    }

    /// Returns the source whose cache is being built.
    pub const fn source_id(&self) -> SourceId {
        self.source_id
    }

    /// Returns the owning application instance.
    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// Returns the fencing token for this lease incarnation.
    pub const fn token(&self) -> FeedBuildLeaseToken {
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

    fn new_source() -> NewSource {
        NewSource::test_default()
    }

    #[test]
    fn only_ready_sources_are_automatically_eligible() {
        assert!(SchedulingGate::Ready.is_automatically_eligible());
        assert!(!SchedulingGate::AuthenticationRequired.is_automatically_eligible());
        assert!(!SchedulingGate::RiskControlled.is_automatically_eligible());
    }

    #[test]
    fn scheduling_gate_values_are_stable_database_values() {
        assert_eq!(SchedulingGate::Ready.as_str(), "ready");
        assert_eq!(
            SchedulingGate::AuthenticationRequired.as_str(),
            "authentication_required"
        );
        assert_eq!(SchedulingGate::RiskControlled.as_str(), "risk_controlled");
    }

    #[test]
    fn source_creation_trims_text_and_preserves_schedule_configuration() {
        let mut spec = new_source();
        spec.book_id = " book-1 ".to_owned();
        spec.display_name = " Example ".to_owned();
        spec.article_url = Some(
            " https://mp.weixin.qq.com/s/example "
                .parse()
                .expect("valid URL"),
        );

        let source = Source::new(spec).expect("valid source should be created");
        assert_eq!(source.book_id(), "book-1");
        assert_eq!(source.display_name(), "Example");
        assert_eq!(
            source.article_url().map(VerifiedWechatArticleUrl::as_str),
            Some("https://mp.weixin.qq.com/s/example")
        );
        assert_eq!(source.priority(), 10);
        assert_eq!(source.max_attempts(), 3);
        assert_eq!(source.feed_revision(), FeedRevision::zero());
    }

    #[test]
    fn source_creation_allows_book_only_sources_without_an_article_url() {
        let mut spec = new_source();
        spec.article_url = None;

        let source = Source::new(spec).expect("book-only source should be valid");

        assert_eq!(source.article_url(), None);
    }

    #[test]
    fn source_updates_trim_values_preserve_scheduler_state_and_avoid_idempotent_revision_bumps() {
        let source = Source::new(new_source()).expect("valid source should be created");
        let updated = source
            .apply_update(SourceUpdate {
                book_id: " book-1 ".to_owned(),
                display_name: " Renamed ".to_owned(),
                article_url: None,
                sync_interval: Duration::hours(2),
                rss_item_limit: 10,
                account_id: None,
                priority: 20,
                max_attempts: 5,
            })
            .expect("valid source update should be applied");

        assert_eq!(updated.book_id(), "book-1");
        assert_eq!(updated.display_name(), "Renamed");
        assert_eq!(updated.article_url(), None);
        assert_eq!(updated.sync_interval(), Duration::hours(2));
        assert_eq!(updated.feed_revision(), FeedRevision::from_u64(1));
        assert_eq!(updated.next_fetch_at(), source.next_fetch_at());

        let repeated = updated
            .apply_update(SourceUpdate {
                book_id: "book-1".to_owned(),
                display_name: "Renamed".to_owned(),
                article_url: None,
                sync_interval: Duration::hours(2),
                rss_item_limit: 10,
                account_id: None,
                priority: 20,
                max_attempts: 5,
            })
            .expect("repeating a valid source update should succeed");
        assert_eq!(repeated.feed_revision(), updated.feed_revision());
    }

    #[test]
    fn source_patches_retain_omitted_values_and_clear_nullable_values() {
        let mut spec = new_source();
        let account_id = WeReadAccountId::from_uuid(Uuid::from_u128(9));
        spec.account_id = Some(account_id);
        let source = Source::new(spec).expect("valid source should be created");

        let renamed = source
            .apply_patch(SourcePatch {
                display_name: Some("Renamed".to_owned()),
                ..SourcePatch::default()
            })
            .expect("partial source update should be valid");
        assert_eq!(renamed.display_name(), "Renamed");
        assert_eq!(renamed.book_id(), source.book_id());
        assert_eq!(renamed.article_url(), source.article_url());
        assert_eq!(renamed.sync_interval(), source.sync_interval());
        assert_eq!(renamed.account_id(), Some(account_id));

        let cleared = renamed
            .apply_patch(SourcePatch {
                article_url: Some(None),
                account_id: Some(None),
                ..SourcePatch::default()
            })
            .expect("nullable source fields should be clearable");
        assert_eq!(cleared.article_url(), None);
        assert_eq!(cleared.account_id(), None);
        assert_eq!(cleared.display_name(), "Renamed");
    }

    #[test]
    fn source_creation_rejects_invalid_identity_and_schedule_values() {
        let mut spec = new_source();
        spec.id = SourceId::from_uuid(Uuid::nil());
        assert_eq!(Source::new(spec).unwrap_err(), SourceError::InvalidId);

        let mut spec = new_source();
        spec.sync_interval = Duration::zero();
        assert_eq!(
            Source::new(spec).unwrap_err(),
            SourceError::InvalidSyncInterval
        );

        let mut spec = new_source();
        spec.max_attempts = 0;
        assert_eq!(
            Source::new(spec).unwrap_err(),
            SourceError::InvalidMaxAttempts
        );
    }

    #[test]
    fn source_revision_advances_without_mutating_other_state() {
        let mut source = Source::new(new_source()).expect("valid source should be created");
        assert_eq!(
            source
                .advance_feed_revision()
                .expect("revision should advance"),
            FeedRevision::from_u64(1)
        );
        assert_eq!(source.feed_revision(), FeedRevision::from_u64(1));
    }

    #[test]
    fn scheduling_gate_parser_rejects_unknown_values() {
        assert_eq!(
            "authentication_required"
                .parse::<SchedulingGate>()
                .expect("known gate"),
            SchedulingGate::AuthenticationRequired
        );
        assert_eq!(
            "unknown".parse::<SchedulingGate>(),
            Err(SourceError::InvalidSchedulingGate)
        );
    }

    #[test]
    fn verified_article_url_rejects_unsafe_destinations() {
        for value in [
            "http://mp.weixin.qq.com/s/example",
            "https://evil.example/s/example",
            "https://user:password@mp.weixin.qq.com/s/example",
            "https://mp.weixin.qq.com:444/s/example",
            "https://mp.weixin.qq.com/s/example#fragment",
        ] {
            assert_eq!(
                value.parse::<VerifiedWechatArticleUrl>(),
                Err(SourceError::InvalidArticleUrl),
                "URL should be rejected: {value}"
            );
        }
    }

    #[test]
    fn verified_article_url_rejects_signed_referer_credentials() {
        assert_eq!(
            VerifiedWechatArticleUrl::parse(
                "https://mp.weixin.qq.com/s/page?sv=1&se=123&sig=secret"
            ),
            Err(SourceError::InvalidArticleUrl),
        );
    }

    #[test]
    fn verified_article_url_preserves_ordinary_query_identity() {
        let value = "https://mp.weixin.qq.com/s/page?__biz=abc&token=resource-token&signature=chapter&x=%2f&x=2";
        assert_eq!(
            VerifiedWechatArticleUrl::parse(value).unwrap().as_str(),
            value
        );
    }

    #[test]
    fn verified_article_url_trims_and_normalizes_default_port() {
        let url = " https://mp.weixin.qq.com:443/s/example?__biz=abc "
            .parse::<VerifiedWechatArticleUrl>()
            .expect("valid URL should be accepted");
        assert_eq!(url.as_str(), "https://mp.weixin.qq.com/s/example?__biz=abc");
    }

    #[test]
    fn verified_article_url_serde_uses_the_same_validation() {
        let url = "https://mp.weixin.qq.com/s/example"
            .parse::<VerifiedWechatArticleUrl>()
            .expect("valid URL should be accepted");
        assert_eq!(
            serde_json::to_string(&url).expect("URL should serialize"),
            "\"https://mp.weixin.qq.com/s/example\""
        );
        assert!(serde_json::from_str::<VerifiedWechatArticleUrl>(
            "\"https://evil.example/s/example\""
        )
        .is_err());
    }
}

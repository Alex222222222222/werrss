//! PostgreSQL persistence boundary.
//!
//! Owns connection pools, migrations, transaction conventions,
//! row-to-domain mapping, and repository construction. SQL must remain inside
//! this subsystem; application services should depend on repository traits.
//!
//! PostgreSQL is the source of truth for sources, articles, archive metadata,
//! credentials, jobs, sync runs, and feed cache. Transactions must group
//! article/archive changes, cache rebuilds, source status, and job completion
//! through [`unit_of_work`]. Browser/network work never runs inside that
//! transaction.
//!
//! The first implemented persistence slices are the PostgreSQL pool/migration
//! helpers in [`postgres`], the shared transaction owner in [`unit_of_work`],
//! the job repository contract plus SQLx and memory implementations in
//! [`repositories::job_repository`], and the PostgreSQL/memory implementation
//! of the storage-neutral account-lease port in
//! [`repositories::account_lease_repository`] plus the durable
//! feed-build lease and revision-aware feed-cache repository in
//! [`repositories::feed_cache_repository`]. The PostgreSQL scheduler repository
//! in [`repositories::scheduler_repository`] now atomically enqueues due source
//! jobs and records source reservations. Source identity/create/read and
//! transaction-scoped scheduling/revision mutations are implemented in
//! [`repositories::source_repository`], and normalized article upserts plus
//! source-scoped feed reads are implemented in
//! [`repositories::article_repository`]. Synchronization-run start/finish and
//! source history reads are implemented in
//! [`repositories::sync_run_repository`]. Hash-only public feed-token storage
//! and active-source resolution are implemented in
//! [`repositories::feed_token_repository`]. Encrypted WeRead account
//! credentials are stored by [`repositories::credential_repository`]; QR/login
//! exchange remains outside this persistence boundary.
//! The job repository exports separate `JobQueue`, `JobEnqueueTransaction`,
//! `JobOutcomeTransaction`, and `ExpiredJobRecovery` ports;
//! `UnitOfWork::job_enqueue` and `UnitOfWork::job_outcomes` are the narrow
//! transaction views used by application orchestration.

pub mod postgres;
pub mod repositories;
pub mod unit_of_work;

pub use repositories::config_transfer_repository::{
    ConfigTransferRepository, ConfigTransferRepositoryError, PostgresConfigTransferRepository,
};

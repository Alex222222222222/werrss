//! Repository contracts for PostgreSQL-backed state.
//!
//! Repository modules own SQL and map rows to domain types. They must expose
//! transaction-friendly operations so application services can atomically
//! persist idempotent article changes, cache updates, and job state.

pub mod account_lease_repository;
pub mod article_repository;
pub mod asset_repository;
pub mod credential_repository;
pub mod feed_cache_repository;
pub mod feed_token_repository;
pub mod job_repository;
pub mod opendal_blob_store;
pub mod scheduler_repository;
pub mod source_repository;
pub mod sync_run_repository;

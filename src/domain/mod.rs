//! Domain layer containing storage-independent Werrss business concepts.
//!
//! These modules define the vocabulary shared by application services,
//! acquisition adapters, repositories, and the RSS renderer. They must remain
//! independent of Axum, Thirtyfour, SQLx, HTML parsers, and deployment details.
//!
//! The domain owns invariants such as stable `review_id` article identity,
//! explicit job state transitions, bounded retry semantics, and risk-control
//! states that require operator attention. Opaque public feed tokens are
//! generated and strictly parsed here, but their digests are persisted by the
//! persistence layer. The domain does not perform I/O or mutate the RSS cache
//! itself.

pub mod article;
pub mod credentials;
pub mod feed;
pub mod feed_token;
pub mod job;
pub mod pacing;
pub mod source;
pub mod sync;
pub mod url_policy;

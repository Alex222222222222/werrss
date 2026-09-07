//! RSS publication subsystem.
//!
//! Provides deterministic rendering from normalized source, article, content,
//! and optional asset-reference values. The pure renderer is executable now:
//! external asset URLs and stable local asset routes are valid input for the
//! version-one feed. The application layer may convert stable routes to public
//! absolute URLs while building a feed; asset storage itself is outside this
//! module. It has no browser, upstream HTTP, scheduler, or PostgreSQL query
//! responsibilities.

pub mod renderer;

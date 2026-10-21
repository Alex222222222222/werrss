//! Process-wide logging initialization.
//!
//! Libraries and application components emit [`tracing`] events. The binary
//! installs this subscriber after configuration has been validated so the
//! configured level controls every runtime role consistently.

use tracing::level_filters::LevelFilter;
use tracing_subscriber::{
    filter::Targets,
    fmt::{self, format::FmtSpan},
    layer::SubscriberExt,
    util::SubscriberInitExt,
    Layer,
};

const THIRTYFOUR_TARGET: &str = "thirtyfour";
const OPENDAL_TARGET: &str = "opendal";

/// Installs the human-readable process logger.
///
/// Event severity has a deliberate operational meaning throughout the
/// application: `debug`/`trace` describe control flow and bounded diagnostic
/// values, `info` records lifecycle and successful work, `warn` records a
/// recoverable or expected degradation, and `error` records a failure that
/// terminates a role or prevents the process from serving its contract.
/// Credentials, cookies, tokens, request bodies, and raw upstream documents
/// must never be added to any of those events.
///
/// Each line contains a system timestamp, level, event target (normally the
/// Rust module path), and event details. ANSI escapes are disabled because the
/// output is intended for container logs and log aggregation.
pub fn init(level: LevelFilter) {
    let filter = target_filter(level);
    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .with_target(true)
                .with_level(true)
                .with_timer(fmt::time())
                .with_span_events(FmtSpan::CLOSE)
                .with_ansi(false)
                .compact()
                .with_filter(filter),
        )
        .init();
}

fn target_filter(level: LevelFilter) -> Targets {
    Targets::new()
        .with_default(level)
        // thirtyfour logs the complete WebDriver response body at DEBUG.
        // Those responses can contain article content, so never enable that
        // dependency target above WARN when application diagnostics are
        // requested. OpenDAL builder debug events include backend config,
        // including WebDAV usernames, so keep its diagnostics at WARN too.
        .with_target(THIRTYFOUR_TARGET, third_party_target_level(level))
        .with_target(OPENDAL_TARGET, third_party_target_level(level))
}

fn third_party_target_level(level: LevelFilter) -> LevelFilter {
    match level {
        LevelFilter::OFF => LevelFilter::OFF,
        LevelFilter::ERROR => LevelFilter::ERROR,
        LevelFilter::WARN => LevelFilter::WARN,
        LevelFilter::INFO | LevelFilter::DEBUG | LevelFilter::TRACE => LevelFilter::WARN,
    }
}

/// Installs the logger early enough to report configuration failures.
///
/// Configuration validation remains the source of truth for accepted values.
/// An invalid `LOG_LEVEL` therefore falls back to `warn` for this bootstrap
/// logger and is then reported as a structured configuration error by the
/// binary instead of silently changing the configured policy.
pub fn init_from_env() {
    let value = std::env::var("LOG_LEVEL").ok();
    let level = bootstrap_level(value.as_deref());
    init(level);
}

fn bootstrap_level(value: Option<&str>) -> LevelFilter {
    value
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(LevelFilter::WARN)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_level_defaults_to_warn_when_unset() {
        assert_eq!(bootstrap_level(None), LevelFilter::WARN);
    }

    #[test]
    fn bootstrap_level_accepts_a_valid_level() {
        assert_eq!(bootstrap_level(Some(" debug ")), LevelFilter::DEBUG);
    }

    #[test]
    fn bootstrap_level_falls_back_to_warn_for_invalid_input() {
        assert_eq!(bootstrap_level(Some("verbose")), LevelFilter::WARN);
    }

    #[test]
    fn third_party_webdriver_debug_events_are_filtered() {
        let filter = target_filter(LevelFilter::TRACE);

        assert!(filter.would_enable("thirtyfour::session::http", &tracing::Level::WARN));
        assert!(!filter.would_enable("thirtyfour::session::http", &tracing::Level::DEBUG));
    }

    #[test]
    fn third_party_webdriver_events_are_disabled_with_off_level() {
        let filter = target_filter(LevelFilter::OFF);

        assert!(!filter.would_enable("thirtyfour::session::http", &tracing::Level::WARN));
    }

    #[test]
    fn third_party_webdriver_warn_events_follow_error_level() {
        let filter = target_filter(LevelFilter::ERROR);

        assert!(filter.would_enable("thirtyfour::session::http", &tracing::Level::ERROR));
        assert!(!filter.would_enable("thirtyfour::session::http", &tracing::Level::WARN));
    }

    #[test]
    fn opendal_debug_events_that_include_backend_configuration_are_filtered() {
        let filter = target_filter(LevelFilter::TRACE);

        assert!(filter.would_enable("opendal::services::webdav", &tracing::Level::WARN));
        assert!(!filter.would_enable("opendal::services::webdav", &tracing::Level::DEBUG));
        assert!(!filter.would_enable("opendal::services::webdav::backend", &tracing::Level::DEBUG));
    }

    #[test]
    fn application_diagnostics_keep_the_configured_verbosity() {
        let filter = target_filter(LevelFilter::TRACE);

        assert!(filter.would_enable(module_path!(), &tracing::Level::TRACE));
    }
}

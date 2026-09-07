//! Integration coverage for configuration-to-role runtime composition.

use werrss::{
    application::runtime::{RuntimeComponent, RuntimePlan, RuntimePlanError},
    config::{AppConfig, AppRole, AppRoles},
    domain::job::JobType,
};

fn config(roles: &str) -> AppConfig {
    AppConfig::from_env_iter([
        (
            "DATABASE_URL".to_owned(),
            "postgres://user:pass@db/feed".to_owned(),
        ),
        (
            "CREDENTIAL_ENCRYPTION_KEY".to_owned(),
            "test-key".to_owned(),
        ),
        ("APP_ROLES".to_owned(), roles.to_owned()),
        (
            "APP_INSTANCE_ID".to_owned(),
            "integration-runtime".to_owned(),
        ),
        ("WORKER_CONCURRENCY".to_owned(), "16".to_owned()),
        ("JOB_POLL_SECONDS".to_owned(), "7".to_owned()),
    ])
    .expect("test configuration should be valid")
}

#[test]
fn role_selection_is_stable_and_does_not_construct_unselected_components() {
    let plan = RuntimePlan::from_config(&config("scheduler,api")).unwrap();

    assert_eq!(plan.roles(), AppRoles::parse("api,scheduler").unwrap());
    assert_eq!(plan.components().len(), 2);
    assert!(matches!(plan.components()[0], RuntimeComponent::Api(_)));
    assert!(matches!(
        plan.components()[1],
        RuntimeComponent::Scheduler(_)
    ));
    assert!(plan.component(AppRole::Worker).is_none());
}

#[test]
fn api_plan_uses_listener_host_and_port_from_environment() {
    let config = AppConfig::from_env_iter([
        (
            "DATABASE_URL".to_owned(),
            "postgres://user:pass@db/feed".to_owned(),
        ),
        (
            "CREDENTIAL_ENCRYPTION_KEY".to_owned(),
            "test-key".to_owned(),
        ),
        ("APP_ROLES".to_owned(), "api".to_owned()),
        ("HTTP_BIND".to_owned(), "127.0.0.1".to_owned()),
        ("HTTP_PORT".to_owned(), "18080".to_owned()),
    ])
    .unwrap();
    let plan = RuntimePlan::from_config(&config).unwrap();
    let RuntimeComponent::Api(api) = plan.component(AppRole::Api).unwrap() else {
        panic!("API role should produce an API plan")
    };

    assert_eq!(api.bind(), "127.0.0.1");
    assert_eq!(api.port(), 18_080);
}

#[test]
fn worker_plan_uses_only_executable_jobs_and_preserves_concurrency() {
    let plan = RuntimePlan::from_config(&config("worker")).unwrap();
    let RuntimeComponent::Worker(worker) = plan.component(AppRole::Worker).unwrap() else {
        panic!("worker role should produce a worker component")
    };

    assert_eq!(worker.concurrency(), 16);
    assert_eq!(
        worker.worker_config().allowed_job_types(),
        &[
            JobType::FeedRebuild,
            JobType::SourceSync,
            JobType::ArticleBackfill,
        ]
    );
    assert_eq!(
        worker.loop_config().idle_poll_interval(),
        std::time::Duration::from_secs(7)
    );
    assert_eq!(
        worker.loop_config().error_backoff(),
        std::time::Duration::from_secs(7)
    );
    assert_eq!(worker.job_service_config().owner(), "integration-runtime");
}

#[test]
fn worker_plan_propagates_the_absolute_execution_timeout() {
    let mut config = config("worker");
    config.job_execution_timeout = std::time::Duration::from_secs(90);

    let plan = RuntimePlan::from_config(&config).unwrap();
    let RuntimeComponent::Worker(worker) = plan.component(AppRole::Worker).unwrap() else {
        panic!("worker role should produce a worker component")
    };

    assert_eq!(
        worker.worker_config().execution_timeout(),
        std::time::Duration::from_secs(90)
    );
}

#[test]
fn api_and_scheduler_roles_ignore_invalid_worker_only_values() {
    let mut config = config("api,scheduler");
    config.worker_concurrency = 0;
    config.job_lease = std::time::Duration::ZERO;
    config.job_heartbeat = std::time::Duration::ZERO;

    let plan = RuntimePlan::from_config(&config).unwrap();
    assert!(plan.component(AppRole::Api).is_some());
    assert!(plan.component(AppRole::Scheduler).is_some());
    assert!(plan.component(AppRole::Worker).is_none());
}

#[test]
fn invalid_api_endpoint_is_rejected_only_when_api_is_selected() {
    let mut config = config("worker");
    config.http_bind = " ".to_owned();
    config.http_port = 0;
    assert!(RuntimePlan::from_config(&config).is_ok());

    config.roles = AppRoles::parse("api").unwrap();
    assert_eq!(
        RuntimePlan::from_config(&config),
        Err(RuntimePlanError::EmptyHttpBind)
    );
}

#[test]
fn invalid_worker_clock_is_rejected_before_a_worker_plan_is_returned() {
    let mut config = config("worker");
    config.job_heartbeat = std::time::Duration::ZERO;

    assert_eq!(
        RuntimePlan::from_config(&config),
        Err(RuntimePlanError::InvalidDuration {
            field: "worker heartbeat"
        })
    );
}

#[test]
fn invalid_worker_execution_timeout_is_rejected_before_startup() {
    let mut config = config("worker");
    config.job_execution_timeout = std::time::Duration::ZERO;

    assert_eq!(
        RuntimePlan::from_config(&config),
        Err(RuntimePlanError::InvalidDuration {
            field: "worker execution timeout"
        })
    );
}

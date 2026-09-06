//! Role-aware runtime composition.
//!
//! [`RuntimePlan`] is the executable composition boundary between environment
//! configuration and process roles. It deliberately contains no database
//! connections or spawned tasks: construction remains deterministic and
//! side-effect free, while the eventual process supervisor can use the typed
//! component plans to construct only the roles selected by `APP_ROLES`.
//!
//! Feed rebuild and source synchronization are executable worker jobs. Account
//! selection is deliberately deferred to source-sync execution so an account
//! can be enrolled through the admin panel after the process starts. A worker
//! must never claim work that its runtime cannot execute and durably complete.
//!
//! API readiness does not depend on WebDriver availability. The API plan only
//! contains HTTP settings, while the runtime supervisor composes a separate
//! browser-dependent worker readiness monitor.

use std::time::Duration as StdDuration;

use chrono::Duration as ChronoDuration;
use thiserror::Error;

use crate::{
    config::{AppConfig, AppRole, AppRoles},
    domain::job::JobType,
};

use super::{
    job_service::{JobServiceConfig, JobServiceConfigError},
    scheduler::{SchedulerConfig, SchedulerLoopConfig, SchedulerLoopConfigError},
    worker::{WorkerConfig, WorkerConfigError, WorkerLoopConfig, WorkerLoopConfigError},
};

/// The default executable worker job kinds.
///
/// Credential refresh is added separately only when a deployment injects a
/// concrete refresh transport into [`super::runtime_supervisor::RuntimeSupervisor`].
/// Keeping this default list explicit prevents runtime composition from
/// silently turning an uncomposed handler into claimed work.
pub const EXECUTABLE_WORKER_JOB_TYPES: &[JobType] = &[
    JobType::FeedRebuild,
    JobType::SourceSync,
    JobType::ArticleBackfill,
];

/// A validated HTTP component plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiRuntimePlan {
    bind: String,
    port: u16,
    admin_enabled: bool,
}

impl ApiRuntimePlan {
    /// Returns the configured bind address or hostname.
    pub fn bind(&self) -> &str {
        &self.bind
    }

    /// Returns the configured listening port.
    pub const fn port(&self) -> u16 {
        self.port
    }

    /// Returns whether administrative routes should be registered.
    pub const fn admin_enabled(&self) -> bool {
        self.admin_enabled
    }
}

/// A validated scheduler component plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerRuntimePlan {
    scheduler: SchedulerConfig,
    loop_config: SchedulerLoopConfig,
    credential_refresh_enabled: bool,
}

impl SchedulerRuntimePlan {
    /// Returns the one-pass scheduler settings.
    pub const fn scheduler_config(&self) -> SchedulerConfig {
        self.scheduler
    }

    /// Returns the shutdown-aware scheduler loop settings.
    pub const fn loop_config(&self) -> SchedulerLoopConfig {
        self.loop_config
    }

    /// Returns whether this scheduler should create credential-refresh jobs.
    pub const fn credential_refresh_enabled(&self) -> bool {
        self.credential_refresh_enabled
    }

    pub(crate) fn enable_credential_refresh(&mut self) {
        self.credential_refresh_enabled = true;
    }
}

/// A validated worker component plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerRuntimePlan {
    concurrency: u32,
    jobs: JobServiceConfig,
    dispatch: WorkerConfig,
    loop_config: WorkerLoopConfig,
    source_sync_enabled: bool,
}

impl WorkerRuntimePlan {
    /// Returns the number of worker tasks the supervisor should run.
    pub const fn concurrency(&self) -> u32 {
        self.concurrency
    }

    /// Returns the queue owner, lease, and recovery settings.
    pub const fn job_service_config(&self) -> &JobServiceConfig {
        &self.jobs
    }

    /// Returns the allowed job kinds for each worker task.
    pub const fn worker_config(&self) -> &WorkerConfig {
        &self.dispatch
    }

    /// Returns the shutdown-aware worker loop settings.
    pub const fn loop_config(&self) -> WorkerLoopConfig {
        self.loop_config
    }

    /// Returns whether authenticated source synchronization is composed.
    pub const fn source_sync_enabled(&self) -> bool {
        self.source_sync_enabled
    }

    pub(crate) fn enable_credential_refresh(&mut self) {
        if !self
            .dispatch
            .allowed_job_types()
            .contains(&JobType::CredentialRefresh)
        {
            self.dispatch
                .allowed_job_types_mut()
                .push(JobType::CredentialRefresh);
        }
    }
}

/// One role-specific component selected by [`RuntimePlan`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeComponent {
    /// HTTP/API component.
    Api(ApiRuntimePlan),
    /// Due-source scheduler component.
    Scheduler(SchedulerRuntimePlan),
    /// Feed-rebuild worker component.
    Worker(WorkerRuntimePlan),
}

/// Side-effect-free role composition derived from [`AppConfig`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimePlan {
    roles: AppRoles,
    components: Vec<RuntimeComponent>,
}

impl RuntimePlan {
    pub(crate) fn enable_credential_refresh(&mut self) {
        for component in &mut self.components {
            match component {
                RuntimeComponent::Scheduler(scheduler) => scheduler.enable_credential_refresh(),
                RuntimeComponent::Worker(worker) => worker.enable_credential_refresh(),
                RuntimeComponent::Api(_) => {}
            }
        }
    }

    /// Builds the role plan from validated application configuration.
    ///
    /// Components are emitted in stable API, scheduler, worker order. No
    /// connection, browser session, listener, or task is created here; those
    /// side effects belong to the process supervisor after this plan has been
    /// validated.
    pub fn from_config(config: &AppConfig) -> Result<Self, RuntimePlanError> {
        let mut components = Vec::new();

        if config.roles.contains(AppRole::Api) {
            if config.http_bind.trim().is_empty() {
                return Err(RuntimePlanError::EmptyHttpBind);
            }
            if config.http_port == 0 {
                return Err(RuntimePlanError::InvalidHttpPort);
            }
            components.push(RuntimeComponent::Api(ApiRuntimePlan {
                bind: config.http_bind.clone(),
                port: config.http_port,
                admin_enabled: config.admin_enabled,
            }));
        }

        if config.roles.contains(AppRole::Scheduler) {
            let poll_interval = config.job_poll_interval;
            let scheduler_loop = SchedulerLoopConfig::new(poll_interval, poll_interval)
                .map_err(RuntimePlanError::SchedulerLoop)?;
            components.push(RuntimeComponent::Scheduler(SchedulerRuntimePlan {
                scheduler: SchedulerConfig::default(),
                loop_config: scheduler_loop,
                credential_refresh_enabled: false,
            }));
        }

        if config.roles.contains(AppRole::Worker) {
            if config.worker_concurrency == 0 {
                return Err(RuntimePlanError::InvalidWorkerConcurrency);
            }
            let job_lease = chrono_duration(config.job_lease, "worker lease")?;
            let job_service = JobServiceConfig::new(config.instance_id.clone(), job_lease, 1_000)
                .map_err(RuntimePlanError::JobService)?;
            let heartbeat = positive_duration(config.job_heartbeat, "worker heartbeat")?;
            let lease = config.job_lease;
            if heartbeat >= lease {
                return Err(RuntimePlanError::WorkerConfig(
                    WorkerConfigError::HeartbeatNotShorterThanLease,
                ));
            }
            let mut allowed_job_types = EXECUTABLE_WORKER_JOB_TYPES.to_vec();
            if config.asset_archive.database_policy().is_some() {
                allowed_job_types.push(JobType::AssetRepair);
            }
            let source_sync_enabled = true;
            let dispatch = WorkerConfig::new(allowed_job_types, heartbeat)
                .map_err(RuntimePlanError::WorkerConfig)?;
            let poll_interval = config.job_poll_interval;
            let loop_config = WorkerLoopConfig::new(poll_interval, poll_interval)
                .map_err(RuntimePlanError::WorkerLoop)?;
            components.push(RuntimeComponent::Worker(WorkerRuntimePlan {
                concurrency: config.worker_concurrency,
                jobs: job_service,
                dispatch,
                loop_config,
                source_sync_enabled,
            }));
        }

        Ok(Self {
            roles: config.roles,
            components,
        })
    }

    /// Returns the selected roles.
    pub const fn roles(&self) -> AppRoles {
        self.roles
    }

    /// Returns the role components in stable construction order.
    pub fn components(&self) -> &[RuntimeComponent] {
        &self.components
    }

    /// Finds the selected component for one role.
    pub fn component(&self, role: AppRole) -> Option<&RuntimeComponent> {
        self.components.iter().find(|component| {
            matches!(
                (role, component),
                (AppRole::Api, RuntimeComponent::Api(_))
                    | (AppRole::Scheduler, RuntimeComponent::Scheduler(_))
                    | (AppRole::Worker, RuntimeComponent::Worker(_))
            )
        })
    }
}

fn chrono_duration(
    value: StdDuration,
    field: &'static str,
) -> Result<ChronoDuration, RuntimePlanError> {
    ChronoDuration::from_std(value).map_err(|_| RuntimePlanError::InvalidDuration { field })
}

fn positive_duration(
    value: StdDuration,
    field: &'static str,
) -> Result<StdDuration, RuntimePlanError> {
    if value.is_zero() {
        Err(RuntimePlanError::InvalidDuration { field })
    } else {
        Ok(value)
    }
}

/// Errors raised while deriving a role-specific runtime plan.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RuntimePlanError {
    /// An API component cannot bind an empty address.
    #[error("HTTP bind address must not be empty when the API role is enabled")]
    EmptyHttpBind,
    /// Port zero is not a stable deployment contract.
    #[error("HTTP port must be greater than zero when the API role is enabled")]
    InvalidHttpPort,
    /// A worker with no tasks cannot make progress.
    #[error("worker concurrency must be greater than zero")]
    InvalidWorkerConcurrency,
    /// A configured chrono duration could not become a Tokio duration.
    #[error("{field} must be positive and representable")]
    InvalidDuration { field: &'static str },
    /// The worker queue owner/lease policy was invalid.
    #[error(transparent)]
    JobService(#[from] JobServiceConfigError),
    /// Worker heartbeat/dispatch settings were invalid.
    #[error(transparent)]
    WorkerConfig(#[from] WorkerConfigError),
    /// Worker polling settings were invalid.
    #[error(transparent)]
    WorkerLoop(#[from] WorkerLoopConfigError),
    /// Scheduler polling settings were invalid.
    #[error(transparent)]
    SchedulerLoop(#[from] SchedulerLoopConfigError),
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::{
        archive::asset_store::{AssetCachePolicy, AssetRepairPolicy},
        config::{AppConfig, AssetArchiveConfig},
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
            ("APP_INSTANCE_ID".to_owned(), "runtime-test".to_owned()),
            ("WORKER_CONCURRENCY".to_owned(), "4".to_owned()),
        ])
        .expect("test configuration should be valid")
    }

    #[test]
    fn composes_only_the_selected_roles_in_stable_order() {
        let plan = RuntimePlan::from_config(&config("worker,api")).unwrap();

        assert_eq!(plan.components().len(), 2);
        assert!(matches!(plan.components()[0], RuntimeComponent::Api(_)));
        assert!(matches!(plan.components()[1], RuntimeComponent::Worker(_)));
        assert!(plan.component(AppRole::Scheduler).is_none());
    }

    #[test]
    fn worker_plan_claims_source_sync_jobs_before_an_account_is_enrolled() {
        let plan = RuntimePlan::from_config(&config("worker")).unwrap();
        let RuntimeComponent::Worker(worker) = plan.component(AppRole::Worker).unwrap() else {
            panic!("worker role should produce a worker component")
        };

        assert_eq!(worker.concurrency(), 4);
        assert_eq!(
            worker.worker_config().allowed_job_types(),
            &[
                JobType::FeedRebuild,
                JobType::SourceSync,
                JobType::ArticleBackfill,
            ]
        );
        assert!(worker.source_sync_enabled());
        assert_eq!(worker.job_service_config().owner(), "runtime-test");
    }

    #[test]
    fn worker_plan_keeps_source_sync_when_a_default_account_is_configured() {
        let config = AppConfig::from_env_iter([
            (
                "DATABASE_URL".to_owned(),
                "postgres://user:pass@db/feed".to_owned(),
            ),
            (
                "CREDENTIAL_ENCRYPTION_KEY".to_owned(),
                "test-key".to_owned(),
            ),
            ("APP_ROLES".to_owned(), "worker".to_owned()),
            ("APP_INSTANCE_ID".to_owned(), "runtime-test".to_owned()),
            ("WORKER_CONCURRENCY".to_owned(), "4".to_owned()),
            (
                "WEREAD_ACCOUNT_ID".to_owned(),
                "00000000-0000-0000-0000-000000000001".to_owned(),
            ),
        ])
        .unwrap();
        let plan = RuntimePlan::from_config(&config).unwrap();
        let RuntimeComponent::Worker(worker) = plan.component(AppRole::Worker).unwrap() else {
            panic!("worker role should produce a worker component")
        };

        assert!(worker.source_sync_enabled());
        assert_eq!(
            worker.worker_config().allowed_job_types(),
            &[
                JobType::FeedRebuild,
                JobType::SourceSync,
                JobType::ArticleBackfill,
            ]
        );
    }

    #[test]
    fn database_asset_mode_adds_the_database_only_repair_job() {
        let mut config = config("worker");
        config.asset_archive = AssetArchiveConfig::Database {
            policy: AssetCachePolicy::default(),
            repair_policy: AssetRepairPolicy::default(),
        };
        let plan = RuntimePlan::from_config(&config).unwrap();
        let RuntimeComponent::Worker(worker) = plan.component(AppRole::Worker).unwrap() else {
            panic!("worker role should produce a worker component")
        };

        assert!(worker
            .worker_config()
            .allowed_job_types()
            .contains(&JobType::AssetRepair));
    }

    #[test]
    fn api_only_plan_does_not_require_worker_or_scheduler_settings() {
        let mut config = config("api");
        config.worker_concurrency = 0;
        config.job_heartbeat = Duration::ZERO;
        config.job_lease = Duration::ZERO;

        let plan = RuntimePlan::from_config(&config).unwrap();
        assert!(matches!(
            plan.component(AppRole::Api),
            Some(RuntimeComponent::Api(_))
        ));
        assert!(plan.component(AppRole::Worker).is_none());
        assert!(plan.component(AppRole::Scheduler).is_none());
    }

    #[test]
    fn rejects_invalid_worker_duration_without_starting_any_worker() {
        let mut config = config("worker");
        config.job_heartbeat = Duration::ZERO;

        assert_eq!(
            RuntimePlan::from_config(&config),
            Err(RuntimePlanError::InvalidDuration {
                field: "worker heartbeat"
            })
        );
    }

    #[test]
    fn rejects_empty_bind_only_when_api_is_selected() {
        let mut config = config("scheduler");
        config.http_bind = " ".to_owned();
        assert!(RuntimePlan::from_config(&config).is_ok());

        config.roles = AppRoles::parse("api").unwrap();
        assert_eq!(
            RuntimePlan::from_config(&config),
            Err(RuntimePlanError::EmptyHttpBind)
        );
    }
}

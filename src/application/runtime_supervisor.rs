//! Side-effectful process supervision for the executable runtime roles.
//!
//! [`super::runtime::RuntimePlan`] validates role selection without opening
//! resources. This module is the next boundary: it opens the shared
//! PostgreSQL pool, builds only the selected adapters, and supervises the API,
//! scheduler, and executable feed-rebuild/source-sync worker loops.
//!
//! Authenticated source work is constructed for worker roles and resolves a
//! usable WeRead account at job time and injects the encrypted cookie enrolled
//! through the admin panel. QR login is exposed by the admin router through a
//! bounded application login-attempt manager.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration as StdDuration};

use axum::Router;
use chrono::Duration;
use sqlx::PgPool;
use thiserror::Error;
use tokio::{net::TcpListener, sync::watch, task::JoinSet};

use crate::{
    acquisition::{
        article_page::WebDriverArticlePageFetcher,
        browser_pool::BrowserPool,
        identity::BrowserArticleIdentityResolver,
        pacing::PacingController,
        webdriver::{BrowserProfile, BrowserViewport, WebDriverFactory},
        weread::{
            BrowserWeReadAdapter, WeReadCredentialProvider, WeReadCredentialProviderError,
            WeReadEndpointError,
        },
    },
    application::auth_service::{
        AuthService, AuthServiceConfig, AuthServiceDependencies, CredentialRefreshJobHandler,
        CredentialRefresher, ManualCredentialRefresher, RingCredentialCipher,
    },
    config::{AppConfig, AppRole},
    domain::job::JobType,
    persistence::{
        postgres::{connect_pool, migrate},
        repositories::{
            account_lease_repository::PostgresAccountLeaseRepository,
            article_repository::PostgresArticleRepository,
            asset_repository::PostgresAssetStore,
            credential_repository::PostgresCredentialRepository,
            feed_cache_repository::{
                PostgresFeedBuildLeaseRepository, PostgresFeedCacheRepository,
            },
            feed_token_repository::PostgresFeedTokenRepository,
            job_repository::PostgresJobRepository,
            scheduler_repository::PostgresSchedulerRepository,
            source_repository::PostgresSourceRepository,
            sync_run_repository::PostgresSyncRunRepository,
        },
        unit_of_work::UnitOfWorkFactory,
    },
    web::{
        admin::{
            admin_router_with_identity_resolver_and_server_root_url,
            admin_router_with_server_root_url,
        },
        api::feed_router_with_browser_health_and_assets,
        auth::AdminAuthenticator,
    },
};

use super::{
    article_backfill_handler::{
        ArticleBackfillJobHandler, ArticleBackfillJobHandlerConfig,
        ArticleBackfillJobHandlerConfigError, ArticleBackfillJobHandlerDependencies,
    },
    asset_archive_service::{AssetArchiveService, AssetArchiveServiceError},
    asset_repair_handler::AssetRepairJobHandler,
    browser_health::{BrowserHealth, BrowserHealthMonitor},
    feed_rebuild_handler::{
        FeedRebuildJobHandler, FeedRebuildJobHandlerConfig, FeedRebuildJobHandlerConfigError,
    },
    feed_rebuild_service::{FeedRebuildConfig, FeedRebuildDependencies, FeedRebuildService},
    feed_service::{
        FeedRebuildJobConfig, FeedService, FeedServiceConfig, PostgresFeedRebuildQueue,
    },
    feed_token_service::FeedTokenService,
    job_service::JobService,
    runtime::{RuntimeComponent, RuntimePlan, RuntimePlanError},
    scheduler::{CredentialRefreshScheduleConfig, Scheduler, SchedulerConfigError},
    source_service::SourceService,
    source_sync_acquirer::{
        BrowserSourceSyncAcquirer, BrowserSourceSyncAcquirerConfig,
        BrowserSourceSyncAcquirerConfigError, BrowserSourceSyncAcquirerDependencies,
        CredentialRepositoryAccountSelector,
    },
    source_sync_handler::{
        SourceSyncJobHandler, SourceSyncJobHandlerConfig, SourceSyncJobHandlerConfigError,
        SourceSyncJobHandlerDependencies,
    },
    sync_service::SyncService,
    worker::{JobHandler, Worker},
};

type FeedRebuildHandler = FeedRebuildJobHandler<
    PostgresSourceRepository,
    PostgresArticleRepository,
    PostgresFeedBuildLeaseRepository,
    UnitOfWorkFactory,
>;

type RuntimeFeedRebuildService = FeedRebuildService<
    PostgresSourceRepository,
    PostgresArticleRepository,
    PostgresFeedBuildLeaseRepository,
    UnitOfWorkFactory,
>;

type SourceSyncHandler = SourceSyncJobHandler<
    PostgresSourceRepository,
    PostgresArticleRepository,
    RuntimeSourceSyncAcquirer,
>;

type RuntimeSourceSyncAcquirer = BrowserSourceSyncAcquirer<
    PostgresAccountLeaseRepository,
    BrowserWeReadAdapter,
    WebDriverArticlePageFetcher,
    CredentialRepositoryAccountSelector<PostgresCredentialRepository>,
>;

type ArticleBackfillHandler = ArticleBackfillJobHandler<
    PostgresSourceRepository,
    PostgresArticleRepository,
    RuntimeSourceSyncAcquirer,
>;

type RuntimeAuthService = AuthService<
    PostgresCredentialRepository,
    PostgresAccountLeaseRepository,
    ManualCredentialRefresher,
    RingCredentialCipher,
>;

#[derive(Clone)]
struct RuntimeWeReadCredentialProvider {
    auth: RuntimeAuthService,
}

#[async_trait::async_trait]
impl WeReadCredentialProvider for RuntimeWeReadCredentialProvider {
    async fn credentials(
        &self,
        account_id: crate::domain::credentials::WeReadAccountId,
    ) -> Result<crate::domain::credentials::WeReadCredentials, WeReadCredentialProviderError> {
        self.auth.credentials(account_id).await.map_err(|error| {
            if matches!(
                error,
                crate::application::auth_service::AuthServiceError::AccountNotFound { .. }
            ) {
                WeReadCredentialProviderError::NotProvisioned
            } else {
                WeReadCredentialProviderError::Unavailable
            }
        })
    }
}

impl<S, L, R, C> OptionalJobHandler for CredentialRefreshJobHandler<S, L, R, C>
where
    S: crate::persistence::repositories::credential_repository::CredentialRepository,
    L: crate::acquisition::browser_pool::AccountLeaseStore + Clone + 'static,
    R: CredentialRefresher,
    C: crate::application::auth_service::CredentialCipher,
{
    fn execute<'a>(
        &'a self,
        lease: &'a crate::persistence::repositories::job_repository::JobLease,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Pin<Box<dyn Future<Output = crate::application::worker::JobExecution> + Send + 'a>> {
        Box::pin(self.execute_job(lease, now))
    }
}

/// Object-safe adapter for an optional runtime job handler.
pub trait OptionalJobHandler: Send + Sync {
    /// Executes a claimed job with a `Send` future suitable for role tasks.
    fn execute<'a>(
        &'a self,
        lease: &'a crate::persistence::repositories::job_repository::JobLease,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Pin<Box<dyn Future<Output = crate::application::worker::JobExecution> + Send + 'a>>;
}

pub struct RuntimeJobHandler<F, S> {
    feed_rebuild: F,
    source_sync: Option<S>,
    article_backfill: Option<Box<dyn JobHandler>>,
    asset_repair: Option<Box<dyn JobHandler>>,
    credential_refresh: Option<Box<dyn OptionalJobHandler>>,
}

impl<F, S> RuntimeJobHandler<F, S> {
    /// Creates a type-aware runtime dispatcher.
    pub fn new(feed_rebuild: F, source_sync: Option<S>) -> Self {
        Self {
            feed_rebuild,
            source_sync,
            article_backfill: None,
            asset_repair: None,
            credential_refresh: None,
        }
    }

    /// Adds the browser-backed article repair handler to this dispatcher.
    pub fn with_article_backfill<H>(mut self, handler: H) -> Self
    where
        H: JobHandler + 'static,
    {
        self.article_backfill = Some(Box::new(handler));
        self
    }

    /// Adds the transport-backed credential refresh handler to this worker.
    pub fn with_credential_refresh<H>(mut self, handler: H) -> Self
    where
        H: OptionalJobHandler + 'static,
    {
        self.credential_refresh = Some(Box::new(handler));
        self
    }

    /// Adds the database-backed missing-asset repair handler.
    pub fn with_asset_repair<H>(mut self, handler: H) -> Self
    where
        H: JobHandler + 'static,
    {
        self.asset_repair = Some(Box::new(handler));
        self
    }
}

#[async_trait::async_trait]
impl<F, S> JobHandler for RuntimeJobHandler<F, S>
where
    F: JobHandler,
    S: JobHandler,
{
    async fn execute(
        &self,
        lease: &crate::persistence::repositories::job_repository::JobLease,
        now: chrono::DateTime<chrono::Utc>,
    ) -> crate::application::worker::JobExecution {
        match lease.job.job_type() {
            JobType::FeedRebuild => self.feed_rebuild.execute(lease, now).await,
            JobType::SourceSync => match &self.source_sync {
                Some(handler) => handler.execute(lease, now).await,
                None => crate::application::worker::JobExecution::Failed {
                    error: "source-sync handler is not configured".to_owned(),
                },
            },
            JobType::ArticleBackfill => match &self.article_backfill {
                Some(handler) => handler.execute(lease, now).await,
                None => crate::application::worker::JobExecution::Failed {
                    error: "article-backfill handler is not configured".to_owned(),
                },
            },
            JobType::AssetRepair => match &self.asset_repair {
                Some(handler) => handler.execute(lease, now).await,
                None => crate::application::worker::JobExecution::Failed {
                    error: "asset-repair handler is not configured".to_owned(),
                },
            },
            JobType::CredentialRefresh => match &self.credential_refresh {
                Some(handler) => handler.execute(lease, now).await,
                None => crate::application::worker::JobExecution::Failed {
                    error: "credential-refresh handler is not configured".to_owned(),
                },
            },
        }
    }
}

type RuntimeWorker = Worker<
    PostgresJobRepository,
    UnitOfWorkFactory,
    RuntimeJobHandler<FeedRebuildHandler, SourceSyncHandler>,
>;

/// A runtime supervisor with validated role selection and an open database
/// pool. The pool is shared by every selected role and is never included in
/// debug output or error messages.
pub struct RuntimeSupervisor {
    config: AppConfig,
    plan: RuntimePlan,
    pool: PgPool,
    browser_pool: Option<BrowserPool>,
    browser_health: BrowserHealth,
    browser_monitor: Option<BrowserHealthMonitor>,
    credential_refresher: Option<Arc<dyn CredentialRefresher>>,
}

impl RuntimeSupervisor {
    /// Opens the configured PostgreSQL pool, applies pending migrations, and
    /// prepares the selected runtime roles.
    #[tracing::instrument(skip_all, level = "debug")]
    pub async fn from_config(config: AppConfig) -> Result<Self, RuntimeSupervisorError> {
        let plan = Self::validated_plan(&config)?;
        let pool = connect_pool(&config)
            .await
            .map_err(RuntimeSupervisorError::DatabaseConnection)?;
        migrate(&pool)
            .await
            .map_err(RuntimeSupervisorError::Migration)?;
        let browser_pool = browser_pool_for_plan(&plan)?;
        let browser_health = BrowserHealth::new(config.timezone);
        let browser_monitor = browser_pool.as_ref().map(|browser_pool| {
            BrowserHealthMonitor::with_browser_pool(
                browser_factory(&config),
                browser_health.clone(),
                browser_pool.clone(),
            )
        });
        tracing::info!(
            components = plan.components().len(),
            "runtime supervisor initialized"
        );
        Ok(Self {
            config,
            plan,
            pool,
            browser_pool,
            browser_health,
            browser_monitor,
            credential_refresher: None,
        })
    }

    /// Creates a supervisor from an already-open pool.
    ///
    /// This constructor is useful for tests and for deployments that run
    /// migrations as a separately authorized step.
    #[tracing::instrument(skip_all, level = "debug")]
    pub fn new(config: AppConfig, pool: PgPool) -> Result<Self, RuntimeSupervisorError> {
        let plan = Self::validated_plan(&config)?;
        let browser_pool = browser_pool_for_plan(&plan)?;
        let browser_health = BrowserHealth::new(config.timezone);
        let browser_monitor = browser_pool.as_ref().map(|browser_pool| {
            BrowserHealthMonitor::with_browser_pool(
                browser_factory(&config),
                browser_health.clone(),
                browser_pool.clone(),
            )
        });
        tracing::debug!(
            components = plan.components().len(),
            "runtime supervisor created from existing pool"
        );
        Ok(Self {
            config,
            plan,
            pool,
            browser_pool,
            browser_health,
            browser_monitor,
            credential_refresher: None,
        })
    }

    /// Installs the deployment-specific non-interactive refresh transport.
    ///
    /// The transport is intentionally injected because the upstream refresh
    /// exchange is not part of the browser adapter. Installing it also makes
    /// `credential_refresh` jobs claimable by worker plans; without it those
    /// jobs remain outside the runtime dispatch set.
    pub fn with_credential_refresher<R>(mut self, refresher: R) -> Self
    where
        R: CredentialRefresher + 'static,
    {
        self.credential_refresher = Some(Arc::new(refresher));
        self.plan.enable_credential_refresh();
        tracing::info!("credential refresh transport enabled for runtime workers");
        self
    }

    fn validated_plan(config: &AppConfig) -> Result<RuntimePlan, RuntimeSupervisorError> {
        let plan = RuntimePlan::from_config(config).map_err(RuntimeSupervisorError::Plan)?;
        // The API can rebuild a missing/expired feed cache on demand, so it
        // needs the same canonical channel URL as a feed worker.
        let feed_builder_required =
            plan.component(AppRole::Api).is_some() || plan.component(AppRole::Worker).is_some();
        if feed_builder_required && config.server_root_url.is_none() {
            return Err(RuntimeSupervisorError::ServerRootUrlNotConfigured);
        }
        Ok(plan)
    }

    /// Returns the validated, side-effect-free role plan used by this
    /// supervisor.
    pub fn plan(&self) -> &RuntimePlan {
        &self.plan
    }

    /// Builds the public feed router when the API role is selected.
    ///
    /// The returned router shares the supervisor's pool but does not bind a
    /// listener. This keeps route integration tests independent of a port and
    /// lets embedders provide their own listener policy.
    pub fn api_router(&self) -> Result<Option<Router>, RuntimeSupervisorError> {
        if !self.config.roles.contains(AppRole::Api) {
            return Ok(None);
        }
        Ok(Some(self.build_api_router()?))
    }

    /// Runs selected roles until the supplied shutdown watch becomes true.
    ///
    /// The supervisor treats an unexpected role-task exit as an error and
    /// requests graceful shutdown from every remaining task. A normal
    /// shutdown waits for all API, scheduler, and worker tasks to finish.
    pub async fn run_until_shutdown(
        self,
        shutdown: watch::Receiver<bool>,
    ) -> Result<(), RuntimeSupervisorError> {
        if *shutdown.borrow() {
            tracing::debug!("runtime supervisor received an already-triggered shutdown");
            return Ok(());
        }

        tracing::info!(
            components = self.plan.components().len(),
            "runtime supervisor starting roles"
        );

        let (role_shutdown_tx, role_shutdown_rx) = watch::channel(false);
        let mut tasks = JoinSet::new();

        if let Some(policy) = self.config.asset_archive.database_policy() {
            let store = PostgresAssetStore::new(self.pool.clone(), policy).with_repair_policy(
                self.config
                    .asset_archive
                    .database_repair_policy()
                    .expect("database asset policy has repair policy"),
            );
            let role_shutdown = role_shutdown_rx.clone();
            tracing::info!("database asset-cache maintenance started");
            tasks.spawn(async move {
                run_asset_maintenance(store, role_shutdown).await;
                Ok(())
            });
        }

        if let Some(monitor) = self.browser_monitor.clone() {
            let role_shutdown = role_shutdown_rx.clone();
            let interval = self.config.job_poll_interval;
            tracing::info!("browser health monitor started");
            tasks.spawn(async move {
                monitor.run_until_shutdown(role_shutdown, interval).await;
                Ok(())
            });
        }

        for component in self.plan.components().iter().cloned() {
            match component {
                RuntimeComponent::Api(api) => {
                    let listener = bind_listener(&api).await?;
                    let router = self.build_api_router()?;
                    tracing::info!(bind = %listener.local_addr().map_or_else(|_| "unknown".to_owned(), |address| address.to_string()), "API role started");
                    let role_shutdown = role_shutdown_rx.clone();
                    tasks.spawn(async move {
                        axum::serve(
                            listener,
                            router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
                        )
                        .with_graceful_shutdown(wait_for_shutdown(role_shutdown))
                        .await
                        .map_err(RuntimeSupervisorError::HttpServe)
                    });
                }
                RuntimeComponent::Scheduler(scheduler) => {
                    let scheduler_config = scheduler.scheduler_config();
                    let loop_config = scheduler.loop_config();
                    let credential_refresh_enabled = scheduler.credential_refresh_enabled();
                    let repository = PostgresSchedulerRepository::new(self.pool.clone());
                    let scheduler =
                        Scheduler::new(repository, scheduler_config, self.config.quiet_hours);
                    let scheduler = match credential_refresh_enabled {
                        true => {
                            let refresh_config = CredentialRefreshScheduleConfig::new(
                                Duration::minutes(5),
                                self.config.job_max_attempts,
                            )
                            .map_err(RuntimeSupervisorError::SchedulerConfig)?;
                            scheduler.with_credential_refresh(refresh_config)
                        }
                        false => scheduler,
                    };
                    let role_shutdown = role_shutdown_rx.clone();
                    tracing::info!("scheduler role started");
                    tasks.spawn(async move {
                        scheduler
                            .run_until_shutdown(role_shutdown, loop_config)
                            .await;
                        Ok(())
                    });
                }
                RuntimeComponent::Worker(worker) => {
                    for worker_index in 0..worker.concurrency() {
                        let loop_config = worker.loop_config();
                        let worker = self
                            .build_worker(&worker, worker_index)?
                            .with_browser_readiness(Arc::new(self.browser_health.clone()));
                        let role_shutdown = role_shutdown_rx.clone();
                        tracing::info!(worker_index, "worker role started");
                        tasks.spawn(async move {
                            worker.run_until_shutdown(role_shutdown, loop_config).await;
                            Ok(())
                        });
                    }
                }
            }
        }

        let mut shutdown = shutdown;
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        tracing::info!("runtime supervisor received shutdown; stopping roles");
                        let _ = role_shutdown_tx.send(true);
                        drain_tasks(tasks).await?;
                        tracing::info!("runtime supervisor stopped all roles");
                        return Ok(());
                    }
                }
                joined = tasks.join_next() => {
                    let result = match joined {
                        Some(Ok(result)) => result,
                        Some(Err(error)) => {
                            tracing::error!(error = %error, "runtime role task panicked; stopping all roles");
                            let _ = role_shutdown_tx.send(true);
                            drain_tasks(tasks).await?;
                            return Err(RuntimeSupervisorError::TaskJoin(error));
                        }
                        None => return Err(RuntimeSupervisorError::NoRuntimeTasks),
                    };
                    let error = result.err().unwrap_or(RuntimeSupervisorError::UnexpectedTaskExit);
                    tracing::error!(error = %error, "runtime role exited unexpectedly; stopping all roles");
                    let _ = role_shutdown_tx.send(true);
                    drain_tasks(tasks).await?;
                    return Err(error);
                }
            }
        }
    }

    /// Runs selected roles until the operating system requests shutdown.
    pub async fn run_until_signal(self) -> Result<(), RuntimeSupervisorError> {
        tracing::debug!("runtime supervisor waiting for shutdown signal");
        let (shutdown_tx, shutdown) = watch::channel(false);
        let runtime = tokio::spawn(self.run_until_shutdown(shutdown));
        tokio::pin!(runtime);
        let signal = tokio::signal::ctrl_c();
        tokio::pin!(signal);

        tokio::select! {
            result = &mut runtime => result.map_err(RuntimeSupervisorError::TaskJoin)?,
            signal_result = &mut signal => {
                tracing::info!("shutdown signal received");
                let signal_result = signal_result.map_err(RuntimeSupervisorError::Signal);
                let _ = shutdown_tx.send(true);
                let runtime_result = runtime.await.map_err(RuntimeSupervisorError::TaskJoin)?;
                match signal_result {
                    Ok(()) => runtime_result,
                    Err(error) => Err(error),
                }
            }
        }
    }

    fn build_api_router(&self) -> Result<Router, RuntimeSupervisorError> {
        let stale_window = chrono_duration(
            self.config.rss_stale_while_revalidate,
            "RSS_STALE_WHILE_REVALIDATE_SECONDS",
        )?;
        let miss_retry =
            chrono_duration(self.config.rss_cache_miss_wait, "RSS_CACHE_MISS_WAIT_MS")?;
        let feed_config = FeedServiceConfig::new(stale_window, miss_retry)
            .map_err(RuntimeSupervisorError::FeedServiceConfig)?;
        let queue_config = FeedRebuildJobConfig::new(0, self.config.job_max_attempts)
            .map_err(RuntimeSupervisorError::FeedRebuildJobConfig)?;
        let queue = PostgresFeedRebuildQueue::new(
            PostgresJobRepository::new(self.pool.clone()),
            queue_config,
        );
        let rebuild_service =
            self.build_feed_rebuild_service(format!("{}-feed-api", self.config.instance_id))?;
        let feed_service = FeedService::new(
            PostgresFeedCacheRepository::new(self.pool.clone()),
            queue,
            rebuild_service,
            feed_config,
        );
        let asset_store = self.config.asset_archive.database_policy().map(|policy| {
            PostgresAssetStore::new(self.pool.clone(), policy).with_repair_policy(
                self.config
                    .asset_archive
                    .database_repair_policy()
                    .expect("database asset policy has repair policy"),
            )
        });
        let mut router = feed_router_with_browser_health_and_assets(
            FeedTokenService::new(PostgresFeedTokenRepository::new(self.pool.clone())),
            feed_service,
            self.pool.clone(),
            self.config.timezone,
            self.browser_health.clone(),
            asset_store,
        );
        if self.config.admin_enabled {
            let auth = AdminAuthenticator::new(
                self.config
                    .admin_username
                    .clone()
                    .ok_or(RuntimeSupervisorError::AdminAuthConfig)?,
                self.config
                    .admin_password
                    .clone()
                    .ok_or(RuntimeSupervisorError::AdminAuthConfig)?,
                self.config
                    .session_signing_key
                    .clone()
                    .ok_or(RuntimeSupervisorError::AdminAuthConfig)?,
            )
            .map_err(RuntimeSupervisorError::AdminAuth)?;
            let weread_auth = self.build_auth_service()?;
            let sources = SourceService::new(
                PostgresSourceRepository::new(self.pool.clone()),
                UnitOfWorkFactory::new(self.pool.clone()),
            );
            let feed_tokens =
                FeedTokenService::new(PostgresFeedTokenRepository::new(self.pool.clone()));
            let sync_runs = PostgresSyncRunRepository::new(self.pool.clone());
            if let Some(browser_pool) = self.browser_pool.clone() {
                let webdriver = browser_factory(&self.config);
                let identity_resolver =
                    Arc::new(BrowserArticleIdentityResolver::new(webdriver, browser_pool));
                router = router.merge(admin_router_with_identity_resolver_and_server_root_url(
                    auth,
                    sources,
                    feed_tokens,
                    sync_runs,
                    weread_auth,
                    identity_resolver,
                    self.config.server_root_url.clone(),
                ));
            } else {
                router = router.merge(admin_router_with_server_root_url(
                    auth,
                    sources,
                    feed_tokens,
                    sync_runs,
                    weread_auth,
                    self.config.server_root_url.clone(),
                ));
            }
        }
        Ok(router)
    }

    fn build_worker(
        &self,
        plan: &super::runtime::WorkerRuntimePlan,
        worker_index: u32,
    ) -> Result<RuntimeWorker, RuntimeSupervisorError> {
        let lease_for = chrono_duration(self.config.feed_build_lease, "FEED_BUILD_LEASE_SECONDS")?;
        let cache_ttl = chrono_duration(self.config.rss_cache_ttl, "RSS_CACHE_TTL_SECONDS")?;
        let retry_after = chrono_duration(self.config.job_poll_interval, "JOB_POLL_SECONDS")?;
        let rebuild_config = self.feed_rebuild_config(lease_for, cache_ttl)?;
        let owner = format!("{}-feed-worker-{worker_index}", self.config.instance_id);
        let rebuild_service = self.build_feed_rebuild_service_with_config(rebuild_config, owner)?;
        let handler_config = FeedRebuildJobHandlerConfig::new(retry_after)
            .map_err(RuntimeSupervisorError::FeedRebuildHandlerConfig)?;
        let feed_handler = FeedRebuildJobHandler::new(rebuild_service, handler_config);
        let asset_archiver = if plan.source_sync_enabled()
            || plan
                .worker_config()
                .allowed_job_types()
                .contains(&JobType::AssetRepair)
        {
            self.build_asset_archiver()?
        } else {
            None
        };
        let asset_repair = if plan
            .worker_config()
            .allowed_job_types()
            .contains(&JobType::AssetRepair)
        {
            let policy = self
                .config
                .asset_archive
                .database_policy()
                .ok_or(RuntimeSupervisorError::AssetArchiveNotConfigured)?;
            let repair_policy = self
                .config
                .asset_archive
                .database_repair_policy()
                .ok_or(RuntimeSupervisorError::AssetArchiveNotConfigured)?;
            let archiver = asset_archiver
                .clone()
                .ok_or(RuntimeSupervisorError::AssetArchiveNotConfigured)?;
            Some(AssetRepairJobHandler::new(
                PostgresAssetStore::new(self.pool.clone(), policy)
                    .with_repair_policy(repair_policy),
                archiver,
                retry_after,
            ))
        } else {
            None
        };
        let source_sync = plan
            .source_sync_enabled()
            .then(|| self.build_source_sync_handler(worker_index, asset_archiver.clone()))
            .transpose()?;
        let article_backfill = plan
            .source_sync_enabled()
            .then(|| self.build_article_backfill_handler(worker_index, asset_archiver.clone()))
            .transpose()?;
        let handler = RuntimeJobHandler::new(feed_handler, source_sync);
        let handler = match article_backfill {
            Some(article_backfill) => handler.with_article_backfill(article_backfill),
            None => handler,
        };
        let handler = match asset_repair {
            Some(asset_repair) => handler.with_asset_repair(asset_repair),
            None => handler,
        };
        let handler = match self.credential_refresher.clone() {
            Some(refresher) => handler.with_credential_refresh(
                self.build_credential_refresh_handler(refresher, worker_index)?,
            ),
            None => handler,
        };
        Worker::new(
            JobService::new(
                PostgresJobRepository::new(self.pool.clone()),
                plan.job_service_config().clone(),
            ),
            UnitOfWorkFactory::new(self.pool.clone()),
            handler,
            plan.worker_config().clone(),
        )
        .map_err(RuntimeSupervisorError::WorkerConfig)
    }

    fn build_credential_refresh_handler(
        &self,
        refresher: Arc<dyn CredentialRefresher>,
        worker_index: u32,
    ) -> Result<
        CredentialRefreshJobHandler<
            PostgresCredentialRepository,
            PostgresAccountLeaseRepository,
            Arc<dyn CredentialRefresher>,
            RingCredentialCipher,
        >,
        RuntimeSupervisorError,
    > {
        let auth_config = AuthServiceConfig::new(
            Duration::minutes(5),
            chrono_duration(self.config.account_lease, "ACCOUNT_LEASE_SECONDS")?,
            chrono_duration(self.config.account_heartbeat, "ACCOUNT_HEARTBEAT_SECONDS")?,
        )
        .map_err(RuntimeSupervisorError::AuthServiceConfig)?;
        let cipher = RingCredentialCipher::new(&self.config.credential_encryption_key)
            .map_err(RuntimeSupervisorError::CredentialCipher)?;
        let service = AuthService::new(
            AuthServiceDependencies {
                accounts: PostgresCredentialRepository::new(self.pool.clone()),
                leases: PostgresAccountLeaseRepository::new(self.pool.clone()),
                refresher,
                cipher,
            },
            auth_config,
        );
        CredentialRefreshJobHandler::new(
            service,
            format!("{}-auth-worker-{worker_index}", self.config.instance_id),
            chrono_duration(self.config.job_poll_interval, "JOB_POLL_SECONDS")?,
        )
        .map_err(RuntimeSupervisorError::AuthService)
    }

    fn build_source_sync_handler(
        &self,
        worker_index: u32,
        asset_archiver: Option<AssetArchiveService>,
    ) -> Result<SourceSyncHandler, RuntimeSupervisorError> {
        let acquirer = self.build_source_sync_acquirer(worker_index)?;
        let handler_config = SourceSyncJobHandlerConfig::new(
            chrono_duration(self.config.job_poll_interval, "JOB_POLL_SECONDS")?,
            chrono_duration(
                self.config.source_failure_cooldown,
                "SOURCE_FAILURE_COOLDOWN_SECONDS",
            )?,
        )
        .map_err(RuntimeSupervisorError::SourceSyncHandlerConfig)?
        .with_quiet_hours(self.config.quiet_hours);
        Ok(SourceSyncJobHandler::new(
            SourceSyncJobHandlerDependencies {
                sources: PostgresSourceRepository::new(self.pool.clone()),
                articles: PostgresArticleRepository::new(self.pool.clone()),
                unit_of_work: UnitOfWorkFactory::new(self.pool.clone()),
                acquirer,
                sync_service: SyncService::new(),
                asset_archiver,
            },
            handler_config,
        ))
    }

    fn build_article_backfill_handler(
        &self,
        worker_index: u32,
        asset_archiver: Option<AssetArchiveService>,
    ) -> Result<ArticleBackfillHandler, RuntimeSupervisorError> {
        let acquirer = self.build_source_sync_acquirer(worker_index)?;
        let handler_config = ArticleBackfillJobHandlerConfig::new(chrono_duration(
            self.config.job_poll_interval,
            "JOB_POLL_SECONDS",
        )?)
        .map_err(RuntimeSupervisorError::ArticleBackfillHandlerConfig)?
        .with_quiet_hours(self.config.quiet_hours);
        Ok(ArticleBackfillJobHandler::new(
            ArticleBackfillJobHandlerDependencies {
                sources: PostgresSourceRepository::new(self.pool.clone()),
                articles: PostgresArticleRepository::new(self.pool.clone()),
                unit_of_work: UnitOfWorkFactory::new(self.pool.clone()),
                acquirer,
                sync_service: SyncService::new(),
                asset_archiver,
            },
            handler_config,
        ))
    }

    fn build_asset_archiver(&self) -> Result<Option<AssetArchiveService>, RuntimeSupervisorError> {
        let Some(policy) = self.config.asset_archive.database_policy() else {
            return Ok(None);
        };
        AssetArchiveService::new(policy, self.config.browser_user_agent.clone())
            .map(Some)
            .map_err(RuntimeSupervisorError::AssetArchive)
    }

    fn build_source_sync_acquirer(
        &self,
        worker_index: u32,
    ) -> Result<RuntimeSourceSyncAcquirer, RuntimeSupervisorError> {
        let webdriver = browser_factory(&self.config);
        let acquisition_config = BrowserSourceSyncAcquirerConfig::new(
            self.config.weread_account_id,
            format!("{}-source-worker", self.config.instance_id),
            chrono_duration(self.config.account_lease, "ACCOUNT_LEASE_SECONDS")?,
            chrono_duration(self.config.account_heartbeat, "ACCOUNT_HEARTBEAT_SECONDS")?,
        )
        .map_err(RuntimeSupervisorError::SourceSyncAcquirerConfig)?;
        let pacing = PacingController::from_entropy(self.config.pacing);
        let weread = BrowserWeReadAdapter::new(self.config.weread_article_list_url.clone())
            .map_err(RuntimeSupervisorError::WeReadEndpoint)?
            .with_pacing(pacing.clone())
            .with_timezone(self.config.timezone)
            .with_credential_provider(Arc::new(RuntimeWeReadCredentialProvider {
                auth: self.build_auth_service()?,
            }));
        let acquirer = BrowserSourceSyncAcquirer::new(
            BrowserSourceSyncAcquirerDependencies {
                browser_pool: self
                    .browser_pool
                    .clone()
                    .ok_or(RuntimeSupervisorError::SourceSyncNotConfigured)?,
                webdriver,
                account_leases: PostgresAccountLeaseRepository::new(self.pool.clone()),
                weread,
                article_pages: WebDriverArticlePageFetcher::new(self.config.timezone)
                    .with_pacing(pacing),
                account_selector: CredentialRepositoryAccountSelector::new(
                    PostgresCredentialRepository::new(self.pool.clone()),
                ),
            },
            acquisition_config,
            worker_index,
        );
        Ok(acquirer)
    }

    fn build_auth_service(&self) -> Result<RuntimeAuthService, RuntimeSupervisorError> {
        let auth_config = AuthServiceConfig::new(
            Duration::minutes(5),
            chrono_duration(self.config.account_lease, "ACCOUNT_LEASE_SECONDS")?,
            chrono_duration(self.config.account_heartbeat, "ACCOUNT_HEARTBEAT_SECONDS")?,
        )
        .map_err(RuntimeSupervisorError::AuthServiceConfig)?;
        let cipher = RingCredentialCipher::new(&self.config.credential_encryption_key)
            .map_err(RuntimeSupervisorError::CredentialCipher)?;
        Ok(AuthService::new(
            AuthServiceDependencies {
                accounts: PostgresCredentialRepository::new(self.pool.clone()),
                leases: PostgresAccountLeaseRepository::new(self.pool.clone()),
                refresher: ManualCredentialRefresher,
                cipher,
            },
            auth_config,
        ))
    }

    fn feed_rebuild_config(
        &self,
        lease_for: Duration,
        cache_ttl: Duration,
    ) -> Result<FeedRebuildConfig, RuntimeSupervisorError> {
        let feed_url = self
            .config
            .server_root_url
            .as_ref()
            .ok_or(RuntimeSupervisorError::ServerRootUrlNotConfigured)?;
        FeedRebuildConfig::new(
            lease_for,
            cache_ttl,
            feed_url.as_str(),
            "WeChat article feed",
        )
        .map_err(RuntimeSupervisorError::FeedRebuildConfig)
    }

    fn build_feed_rebuild_service(
        &self,
        owner: impl Into<String>,
    ) -> Result<RuntimeFeedRebuildService, RuntimeSupervisorError> {
        let lease_for = chrono_duration(self.config.feed_build_lease, "FEED_BUILD_LEASE_SECONDS")?;
        let cache_ttl = chrono_duration(self.config.rss_cache_ttl, "RSS_CACHE_TTL_SECONDS")?;
        let rebuild_config = self.feed_rebuild_config(lease_for, cache_ttl)?;
        self.build_feed_rebuild_service_with_config(rebuild_config, owner)
    }

    fn build_feed_rebuild_service_with_config(
        &self,
        rebuild_config: FeedRebuildConfig,
        owner: impl Into<String>,
    ) -> Result<RuntimeFeedRebuildService, RuntimeSupervisorError> {
        FeedRebuildService::new(
            FeedRebuildDependencies::new(
                PostgresSourceRepository::new(self.pool.clone()),
                PostgresArticleRepository::new(self.pool.clone()),
                PostgresFeedBuildLeaseRepository::new(self.pool.clone()),
                UnitOfWorkFactory::new(self.pool.clone()),
            ),
            rebuild_config,
            owner,
        )
        .map_err(RuntimeSupervisorError::FeedRebuild)
    }
}

async fn bind_listener(
    api: &super::runtime::ApiRuntimePlan,
) -> Result<TcpListener, RuntimeSupervisorError> {
    let address = listener_address(api.bind(), api.port());
    tracing::debug!(bind = %address, "binding API listener");
    let result = TcpListener::bind(&address).await;
    match result {
        Ok(listener) => {
            tracing::debug!(bind = %address, "API listener bound");
            Ok(listener)
        }
        Err(error) => {
            tracing::error!(bind = %address, error = %error, "unable to bind API listener");
            Err(RuntimeSupervisorError::HttpBind { address, error })
        }
    }
}

fn listener_address(bind: &str, port: u16) -> String {
    // A raw IPv6 address needs brackets before a port is appended. Preserve
    // bracketed values so operators may also provide the conventional form.
    if bind.contains(':') && !bind.starts_with('[') {
        format!("[{bind}]:{port}")
    } else {
        format!("{bind}:{port}")
    }
}

fn browser_pool_for_plan(
    plan: &RuntimePlan,
) -> Result<Option<BrowserPool>, RuntimeSupervisorError> {
    let capacity = match plan.component(AppRole::Worker) {
        Some(RuntimeComponent::Worker(worker)) => worker.concurrency() as usize,
        _ if plan.component(AppRole::Api).is_some_and(
            |component| matches!(component, RuntimeComponent::Api(api) if api.admin_enabled()),
        ) =>
        {
            1
        }
        _ => return Ok(None),
    };
    BrowserPool::new(capacity)
        .map(Some)
        .map_err(RuntimeSupervisorError::BrowserPool)
}

fn browser_factory(config: &AppConfig) -> WebDriverFactory {
    let profile = BrowserProfile {
        user_agent: config.browser_user_agent.clone(),
        viewport: BrowserViewport::new(
            config.browser_viewport_width,
            config.browser_viewport_height,
        ),
        locale: config.browser_locale.clone(),
        expected_timezone: Some(config.timezone),
        extra_args: config.browser_extra_args.clone(),
    };
    WebDriverFactory::new(config.webdriver_url.clone(), config.browser_engine).with_profile(profile)
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    let _ = shutdown.wait_for(|value| *value).await;
}

async fn drain_tasks(
    mut tasks: JoinSet<Result<(), RuntimeSupervisorError>>,
) -> Result<(), RuntimeSupervisorError> {
    while let Some(result) = tasks.join_next().await {
        result.map_err(RuntimeSupervisorError::TaskJoin)??;
    }
    Ok(())
}

/// Periodically enforces database asset age/size limits and removes orphaned
/// metadata. Maintenance is deliberately best effort: a transient database
/// failure must not terminate otherwise healthy API or worker roles.
async fn run_asset_maintenance(store: PostgresAssetStore, mut shutdown: watch::Receiver<bool>) {
    const INTERVAL: StdDuration = StdDuration::from_secs(60 * 60);

    if *shutdown.borrow() {
        return;
    }
    let mut ticker = tokio::time::interval(INTERVAL);
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    tracing::debug!("database asset-cache maintenance stopped");
                    return;
                }
            }
            _ = ticker.tick() => {
                match store.maintenance().await {
                    Ok(result) if result.changed() => {
                        tracing::info!(
                            stale_blobs = result.stale_blobs,
                            size_evicted_blobs = result.size_evicted_blobs,
                            orphan_records = result.orphan_records,
                            orphan_blobs = result.orphan_blobs,
                            "database asset-cache maintenance changed state"
                        );
                    }
                    Ok(_) => tracing::debug!("database asset-cache maintenance found no work"),
                    Err(error) => tracing::warn!(error = %error, "database asset-cache maintenance failed"),
                }
            }
        }
    }
}

fn chrono_duration(
    value: StdDuration,
    field: &'static str,
) -> Result<Duration, RuntimeSupervisorError> {
    Duration::from_std(value).map_err(|_| RuntimeSupervisorError::InvalidDuration { field })
}

/// Errors raised while constructing or supervising runtime roles.
#[derive(Debug, Error)]
pub enum RuntimeSupervisorError {
    /// Environment configuration could not be loaded.
    #[error(transparent)]
    Config(#[from] crate::config::ConfigError),
    /// Role configuration was invalid.
    #[error(transparent)]
    Plan(#[from] RuntimePlanError),
    /// Enabled administration was missing one of the validated credentials.
    #[error("administrative authentication configuration is incomplete")]
    AdminAuthConfig,
    /// Administrative authentication could not be constructed.
    #[error(transparent)]
    AdminAuth(#[from] crate::web::auth::AuthConfigError),
    /// The source-sync handler could not be configured.
    #[error("source-sync handler could not be configured")]
    SourceSyncNotConfigured,
    /// API and feed workers must not publish a placeholder channel URL.
    #[error("SERVER_ROOT_URL is required when the API or worker role is enabled")]
    ServerRootUrlNotConfigured,
    /// PostgreSQL could not be reached.
    #[error("database connection failed")]
    DatabaseConnection(#[source] sqlx::Error),
    /// A checked-in migration could not be applied.
    #[error("database migration failed")]
    Migration(#[source] sqlx::migrate::MigrateError),
    /// A configured duration could not be represented by chrono.
    #[error("{field} is outside the supported runtime duration range")]
    InvalidDuration { field: &'static str },
    /// Feed delivery timing was invalid after conversion.
    #[error(transparent)]
    FeedServiceConfig(#[from] super::feed_service::FeedServiceConfigError),
    /// Feed rebuild queue settings were invalid.
    #[error(transparent)]
    FeedRebuildJobConfig(#[from] super::feed_service::FeedRebuildJobConfigError),
    /// Feed rebuild settings were invalid after conversion.
    #[error(transparent)]
    FeedRebuildConfig(#[from] super::feed_rebuild_service::FeedRebuildConfigError),
    /// Feed rebuild handler settings were invalid after conversion.
    #[error(transparent)]
    FeedRebuildHandlerConfig(#[from] FeedRebuildJobHandlerConfigError),
    /// Feed rebuild service construction failed.
    #[error(transparent)]
    FeedRebuild(#[from] super::feed_rebuild_service::FeedRebuildError),
    /// Browser-pool capacity could not be constructed.
    #[error(transparent)]
    BrowserPool(#[from] crate::acquisition::browser_pool::BrowserPoolError),
    /// Source-sync account lease policy was invalid after configuration.
    #[error(transparent)]
    SourceSyncAcquirerConfig(#[from] BrowserSourceSyncAcquirerConfigError),
    /// The authenticated WeRead endpoint violated its destination policy.
    #[error(transparent)]
    WeReadEndpoint(#[from] WeReadEndpointError),
    /// Source-sync handler policy was invalid after configuration.
    #[error(transparent)]
    SourceSyncHandlerConfig(#[from] SourceSyncJobHandlerConfigError),
    /// Article-backfill handler policy was invalid after configuration.
    #[error(transparent)]
    ArticleBackfillHandlerConfig(#[from] ArticleBackfillJobHandlerConfigError),
    /// Scheduler credential-refresh settings were invalid after conversion.
    #[error(transparent)]
    SchedulerConfig(#[from] SchedulerConfigError),
    /// Authentication refresh policy was invalid after configuration.
    #[error(transparent)]
    AuthServiceConfig(#[from] super::auth_service::AuthServiceConfigError),
    /// Authentication credential encryption could not be initialized.
    #[error(transparent)]
    CredentialCipher(#[from] super::auth_service::CredentialCipherError),
    /// Authentication refresh handler construction failed.
    #[error(transparent)]
    AuthService(#[from] super::auth_service::AuthServiceError),
    /// The optional database asset HTTP client could not be constructed.
    #[error(transparent)]
    AssetArchive(#[from] AssetArchiveServiceError),
    /// An asset-repair worker was planned without database asset settings.
    #[error("database asset archive configuration is required for asset repair")]
    AssetArchiveNotConfigured,
    /// Worker construction failed.
    #[error(transparent)]
    WorkerConfig(#[from] super::worker::WorkerConfigError),
    /// The API listener could not bind.
    #[error("HTTP listener could not bind to {address}")]
    HttpBind {
        /// Address selected by the API plan.
        address: String,
        /// Underlying bind failure.
        #[source]
        error: std::io::Error,
    },
    /// The API server stopped because it could not continue serving.
    #[error("HTTP server failed")]
    HttpServe(#[source] std::io::Error),
    /// The operating system signal handler failed.
    #[error("shutdown signal handler failed")]
    Signal(#[source] std::io::Error),
    /// A role task panicked or was cancelled.
    #[error("runtime role task failed to join")]
    TaskJoin(#[source] tokio::task::JoinError),
    /// A role exited before shutdown was requested.
    #[error("runtime role exited unexpectedly")]
    UnexpectedTaskExit,
    /// No role task remained to supervise.
    #[error("runtime started without any role tasks")]
    NoRuntimeTasks,
}

#[cfg(test)]
mod tests {
    use sqlx::postgres::PgPoolOptions;

    use super::*;
    use crate::{
        application::auth_service::{CredentialRefreshError, RefreshedCredentials},
        config::AppConfig,
        domain::credentials::WeReadAccountId,
    };

    fn config(roles: &str) -> AppConfig {
        AppConfig::from_env_iter([
            (
                "DATABASE_URL".to_owned(),
                "postgres://user:pass@db/feed".to_owned(),
            ),
            (
                "CREDENTIAL_ENCRYPTION_KEY".to_owned(),
                "runtime-test-key".to_owned(),
            ),
            ("APP_ROLES".to_owned(), roles.to_owned()),
            (
                "APP_INSTANCE_ID".to_owned(),
                "runtime-supervisor-test".to_owned(),
            ),
            ("WORKER_CONCURRENCY".to_owned(), "2".to_owned()),
            (
                "SERVER_ROOT_URL".to_owned(),
                "https://feeds.example.test/werrss.xml".to_owned(),
            ),
        ])
        .expect("test configuration should be valid")
    }

    fn authenticated_config(roles: &str) -> AppConfig {
        AppConfig::from_env_iter([
            (
                "DATABASE_URL".to_owned(),
                "postgres://user:pass@db/feed".to_owned(),
            ),
            (
                "CREDENTIAL_ENCRYPTION_KEY".to_owned(),
                "runtime-test-key".to_owned(),
            ),
            ("APP_ROLES".to_owned(), roles.to_owned()),
            (
                "APP_INSTANCE_ID".to_owned(),
                "runtime-supervisor-test".to_owned(),
            ),
            ("WORKER_CONCURRENCY".to_owned(), "2".to_owned()),
            (
                "SERVER_ROOT_URL".to_owned(),
                "https://feeds.example.test/werrss.xml".to_owned(),
            ),
            (
                "WEREAD_ACCOUNT_ID".to_owned(),
                "00000000-0000-0000-0000-000000000001".to_owned(),
            ),
        ])
        .expect("authenticated test configuration should be valid")
    }

    fn lazy_pool() -> PgPool {
        PgPoolOptions::new()
            .connect_lazy("postgres://unused")
            .expect("lazy test pool URL should be valid")
    }

    #[derive(Clone)]
    struct UnavailableRefresher;

    #[async_trait::async_trait]
    impl CredentialRefresher for UnavailableRefresher {
        async fn refresh(
            &self,
            _account_id: WeReadAccountId,
            _refresh_token: &str,
        ) -> Result<RefreshedCredentials, CredentialRefreshError> {
            Err(CredentialRefreshError::Transient)
        }
    }

    #[tokio::test]
    async fn injected_refresh_transport_adds_credential_jobs_to_worker_dispatch() {
        let supervisor = RuntimeSupervisor::new(config("worker"), lazy_pool())
            .unwrap()
            .with_credential_refresher(UnavailableRefresher);
        let RuntimeComponent::Worker(plan) = supervisor
            .plan()
            .component(AppRole::Worker)
            .expect("worker plan should exist")
        else {
            panic!("worker role should produce a worker plan")
        };

        assert!(plan
            .worker_config()
            .allowed_job_types()
            .contains(&JobType::CredentialRefresh));
    }

    #[tokio::test]
    async fn injected_refresh_transport_enables_scheduler_refresh_passes() {
        let supervisor = RuntimeSupervisor::new(authenticated_config("all"), lazy_pool())
            .unwrap()
            .with_credential_refresher(UnavailableRefresher);
        let RuntimeComponent::Scheduler(plan) = supervisor
            .plan()
            .component(AppRole::Scheduler)
            .expect("scheduler plan should exist")
        else {
            panic!("scheduler role should produce a scheduler plan")
        };

        assert!(plan.credential_refresh_enabled());
    }

    #[tokio::test]
    async fn api_router_is_composed_without_opening_an_extra_connection() {
        let supervisor = RuntimeSupervisor::new(config("api"), lazy_pool()).unwrap();

        assert!(supervisor.api_router().unwrap().is_some());
        assert!(supervisor.plan().component(AppRole::Worker).is_none());
    }

    #[test]
    fn listener_address_brackets_raw_ipv6_binds() {
        assert_eq!(listener_address("::1", 18_080), "[::1]:18080");
    }

    #[test]
    fn listener_address_preserves_bracketed_ipv6_and_hostnames() {
        assert_eq!(listener_address("[::1]", 18_081), "[::1]:18081");
        assert_eq!(listener_address("localhost", 18_082), "localhost:18082");
    }

    #[tokio::test]
    async fn non_worker_roles_do_not_require_browser_capacity() {
        let mut config = config("api");
        config.worker_concurrency = 0;

        let supervisor = RuntimeSupervisor::new(config, lazy_pool())
            .expect("API-only runtime should not construct a worker browser pool");

        assert!(supervisor.browser_pool.is_none());
    }

    #[tokio::test]
    async fn worker_dependencies_are_constructible_before_the_first_job_exists() {
        let supervisor = RuntimeSupervisor::new(config("worker"), lazy_pool()).unwrap();
        let RuntimeComponent::Worker(plan) = supervisor
            .plan()
            .component(AppRole::Worker)
            .expect("worker plan should exist")
        else {
            panic!("worker role should produce a worker plan")
        };

        supervisor
            .build_worker(plan, 0)
            .expect("feed rebuild worker should be constructible");
    }

    #[tokio::test]
    async fn scheduler_starts_without_an_enrolled_weread_account() {
        let supervisor = RuntimeSupervisor::new(config("scheduler"), lazy_pool())
            .expect("scheduler startup must not depend on account enrollment");
        assert!(matches!(
            supervisor.plan().component(AppRole::Scheduler),
            Some(RuntimeComponent::Scheduler(_))
        ));
    }

    #[tokio::test]
    async fn all_roles_start_without_an_enrolled_weread_account() {
        let supervisor = RuntimeSupervisor::new(config("all"), lazy_pool())
            .expect("all roles must wait for account enrollment at job time");
        assert!(supervisor.plan().component(AppRole::Scheduler).is_some());
        assert!(supervisor.plan().component(AppRole::Worker).is_some());
    }

    #[tokio::test]
    async fn scheduler_and_source_sync_worker_are_composed_when_configured() {
        let supervisor = RuntimeSupervisor::new(authenticated_config("all"), lazy_pool())
            .expect("authenticated roles should be accepted");
        assert!(matches!(
            supervisor.plan().component(AppRole::Scheduler),
            Some(RuntimeComponent::Scheduler(_))
        ));
        let RuntimeComponent::Worker(plan) = supervisor
            .plan()
            .component(AppRole::Worker)
            .expect("worker plan should exist")
        else {
            panic!("worker role should produce a worker plan")
        };
        assert!(plan.source_sync_enabled());
        supervisor
            .build_worker(plan, 0)
            .expect("authenticated source-sync worker should be constructible");
    }

    #[tokio::test]
    async fn panel_enrolled_accounts_allow_source_sync() {
        let supervisor = RuntimeSupervisor::new(authenticated_config("all"), lazy_pool())
            .expect("an enrolled-account runtime should use panel credentials");
        let RuntimeComponent::Worker(plan) = supervisor
            .plan()
            .component(AppRole::Worker)
            .expect("worker role should exist")
        else {
            panic!("worker role should produce a worker plan")
        };

        supervisor
            .build_worker(plan, 0)
            .expect("panel-credential source-sync worker should be constructible");
    }

    #[tokio::test]
    async fn worker_requires_and_uses_the_configured_feed_url() {
        let mut missing_url_config = config("worker");
        missing_url_config.server_root_url = None;
        assert!(matches!(
            RuntimeSupervisor::new(missing_url_config, lazy_pool()),
            Err(RuntimeSupervisorError::ServerRootUrlNotConfigured)
        ));

        let supervisor = RuntimeSupervisor::new(config("worker"), lazy_pool()).unwrap();
        let rebuild_config = supervisor
            .feed_rebuild_config(Duration::minutes(10), Duration::minutes(30))
            .expect("configured feed URL should produce rebuild settings");
        assert_eq!(
            rebuild_config.feed_url(),
            "https://feeds.example.test/werrss.xml"
        );
    }

    #[tokio::test]
    async fn api_requires_a_configured_feed_url_for_on_demand_rebuilds() {
        let mut config = config("api");
        config.server_root_url = None;

        assert!(matches!(
            RuntimeSupervisor::new(config, lazy_pool()),
            Err(RuntimeSupervisorError::ServerRootUrlNotConfigured)
        ));
    }

    #[tokio::test]
    async fn enabled_administration_requires_credentials_when_router_is_built() {
        let mut config = config("api");
        config.admin_enabled = true;

        let supervisor = RuntimeSupervisor::new(config, lazy_pool()).unwrap();
        assert!(matches!(
            supervisor.api_router(),
            Err(RuntimeSupervisorError::AdminAuthConfig)
        ));
    }

    #[tokio::test]
    async fn shutdown_already_requested_does_not_bind_or_touch_the_database() {
        let supervisor = RuntimeSupervisor::new(config("api"), lazy_pool()).unwrap();
        let (_shutdown_tx, shutdown) = watch::channel(true);

        supervisor
            .run_until_shutdown(shutdown)
            .await
            .expect("pre-requested shutdown should be clean");
    }
}

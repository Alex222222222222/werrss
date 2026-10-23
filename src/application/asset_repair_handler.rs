//! Worker handler for repairing one evicted or otherwise missing asset.
//!
//! A public cache miss only admits a small durable job. The actual HTTP fetch
//! happens here, outside the request path, and restores bytes into the same
//! asset record so existing feed URLs remain valid. The operation is
//! idempotent: a worker crash after the restore but before job completion sees
//! an already-available record on its next claim.

use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;
use uuid::Uuid;

use crate::{
    application::{
        asset_archive_service::{
            AssetArchiveService, RepairAssetFetchFailureKind, RepairAssetFetchOutcome,
        },
        worker::{JobExecution, JobHandler},
    },
    domain::{job::JobType, source::VerifiedWechatArticleUrl},
    persistence::repositories::{
        asset_repository::{AssetRepairRestoreResult, PostgresAssetStore},
        job_repository::JobLease,
    },
};

/// Secret-free parameters persisted in an asset-repair job.
#[derive(Debug, Clone, Deserialize)]
struct AssetRepairPayload {
    asset_id: Uuid,
}

/// Executes database-backed asset repair jobs.
#[derive(Clone)]
pub struct AssetRepairJobHandler {
    assets: PostgresAssetStore,
    archiver: AssetArchiveService,
    retry_after: Duration,
}

impl std::fmt::Debug for AssetRepairJobHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AssetRepairJobHandler")
            .field("assets", &self.assets)
            .field("archiver", &self.archiver)
            .field("retry_after", &self.retry_after)
            .finish()
    }
}

impl AssetRepairJobHandler {
    /// Creates a handler using the same bounded public asset fetcher as source
    /// synchronization.
    pub fn new(
        assets: PostgresAssetStore,
        archiver: AssetArchiveService,
        retry_after: Duration,
    ) -> Self {
        Self {
            assets,
            archiver,
            retry_after,
        }
    }

    async fn confirmed_expiry(
        &self,
        lease: &JobLease,
        asset_id: Uuid,
        now: DateTime<Utc>,
    ) -> JobExecution {
        match self.assets.finish_confirmed_expiry(lease, asset_id).await {
            Ok(_) => JobExecution::Committed,
            Err(_) => JobExecution::Retry {
                retry_at: now + self.retry_after,
                error: "asset expiry handoff could not be recorded".to_owned(),
            },
        }
    }

    async fn failure(
        &self,
        lease: &JobLease,
        asset_id: Uuid,
        now: DateTime<Utc>,
        error: &'static str,
    ) -> JobExecution {
        match self
            .assets
            .record_repair_failure(lease, asset_id, error)
            .await
        {
            Ok(true) => JobExecution::Failed {
                error: error.to_owned(),
            },
            Ok(false) => JobExecution::Retry {
                retry_at: now + self.retry_after,
                error: error.to_owned(),
            },
            Err(_) => JobExecution::Retry {
                retry_at: now + self.retry_after,
                error: "asset repair state could not be updated".to_owned(),
            },
        }
    }
}

#[async_trait::async_trait]
impl JobHandler for AssetRepairJobHandler {
    async fn execute(&self, lease: &JobLease, now: DateTime<Utc>) -> JobExecution {
        if lease.job.job_type() != JobType::AssetRepair {
            return JobExecution::Failed {
                error: "asset repair handler received an unsupported job type".to_owned(),
            };
        }
        let payload =
            match serde_json::from_value::<AssetRepairPayload>(lease.job.payload().clone()) {
                Ok(payload) if !payload.asset_id.is_nil() => payload,
                _ => {
                    return JobExecution::Failed {
                        error: "asset repair job payload is invalid".to_owned(),
                    };
                }
            };

        let target = match self.assets.repair_target(payload.asset_id).await {
            Ok(Some(target)) => target,
            Ok(None) => {
                // The article may have been deleted, or another worker may
                // have restored the bytes. Both outcomes make this job done.
                if self
                    .assets
                    .complete_repair(lease, payload.asset_id)
                    .await
                    .is_err()
                {
                    return JobExecution::Retry {
                        retry_at: now + self.retry_after,
                        error: "asset repair completion could not be recorded".to_owned(),
                    };
                }
                return JobExecution::Succeeded;
            }
            Err(_) => {
                return self
                    .failure(
                        lease,
                        payload.asset_id,
                        now,
                        "asset repair target could not be loaded",
                    )
                    .await;
            }
        };
        if target.exhausted {
            return JobExecution::Failed {
                error: "asset repair attempts are exhausted".to_owned(),
            };
        }
        if let Some(resume_at) = target.next_allowed_at.filter(|next| *next > now) {
            return JobExecution::Deferred { resume_at };
        }
        let referer = match VerifiedWechatArticleUrl::parse(target.referer_url.as_str()) {
            Ok(referer) => referer,
            Err(_) => {
                return self
                    .failure(
                        lease,
                        payload.asset_id,
                        now,
                        "asset repair referer is invalid",
                    )
                    .await;
            }
        };
        let input = match self
            .archiver
            .fetch_asset_for_repair(
                &referer,
                &target.source_url,
                target.origin.as_deref(),
                target.user_agent.as_deref(),
            )
            .await
        {
            RepairAssetFetchOutcome::Fetched(input) => *input,
            RepairAssetFetchOutcome::Failed(failure)
                if failure.kind == RepairAssetFetchFailureKind::ConfirmedSignedUrlExpiry =>
            {
                return self.confirmed_expiry(lease, payload.asset_id, now).await;
            }
            RepairAssetFetchOutcome::Failed(_) => {
                return self
                    .failure(lease, payload.asset_id, now, "asset repair fetch failed")
                    .await;
            }
        };

        match self
            .assets
            .restore_missing_asset(lease, payload.asset_id, &input)
            .await
        {
            Ok(AssetRepairRestoreResult::Restored | AssetRepairRestoreResult::AlreadyAvailable) => {
                if self
                    .assets
                    .complete_repair(lease, payload.asset_id)
                    .await
                    .is_err()
                {
                    return JobExecution::Retry {
                        retry_at: now + self.retry_after,
                        error: "asset repair completion could not be recorded".to_owned(),
                    };
                }
                JobExecution::Succeeded
            }
            Ok(AssetRepairRestoreResult::NotFound) => {
                if self
                    .assets
                    .complete_repair(lease, payload.asset_id)
                    .await
                    .is_err()
                {
                    return JobExecution::Retry {
                        retry_at: now + self.retry_after,
                        error: "asset repair completion could not be recorded".to_owned(),
                    };
                }
                JobExecution::Succeeded
            }
            Err(_) => {
                self.failure(
                    lease,
                    payload.asset_id,
                    now,
                    "asset repair bytes could not be stored",
                )
                .await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AssetRepairJobHandler, AssetRepairPayload};
    use crate::{
        application::{asset_archive_service::AssetArchiveService, worker::JobHandler},
        archive::asset_store::{AssetCachePolicy, AssetInput},
        domain::{
            article::{ArticleObservationVersion, NewArticle},
            job::JobType,
            source::{NewSource, SchedulingGate, SourceId, VerifiedWechatArticleUrl},
        },
        persistence::{
            repositories::{
                article_repository::ArticleTransactionRepository,
                asset_repository::{
                    AssetRepairEnqueueResult, AssetTransactionRepository, PostgresAssetStore,
                },
                job_repository::{JobQueue, PostgresJobRepository},
                source_repository::SourceTransactionRepository,
            },
            unit_of_work::UnitOfWorkFactory,
        },
    };
    use chrono::{Duration, Utc};
    use serde_json::json;
    use sqlx::PgPool;
    use std::time::Duration as StdDuration;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use url::Url;
    use uuid::Uuid;

    #[test]
    fn accepts_only_a_non_nil_asset_id_payload() {
        let id = Uuid::new_v4();
        let payload: AssetRepairPayload =
            serde_json::from_value(json!({ "asset_id": id })).expect("payload should decode");
        assert_eq!(payload.asset_id, id);
    }

    #[test]
    fn rejects_payload_without_an_asset_id() {
        assert!(serde_json::from_value::<AssetRepairPayload>(json!({})).is_err());
    }

    #[sqlx::test(migrator = "crate::persistence::postgres::MIGRATOR")]
    async fn confirmed_provider_expiry_refunds_repair_and_admits_refresh(pool: PgPool) {
        let source_id = SourceId::from_uuid(Uuid::new_v4());
        let referer =
            VerifiedWechatArticleUrl::parse("https://mp.weixin.qq.com/s/repair-expiry").unwrap();
        let policy = AssetCachePolicy::new(
            0,
            StdDuration::from_secs(30),
            1024,
            10,
            1024 * 1024,
            StdDuration::from_secs(10),
            StdDuration::from_secs(2),
            2,
        )
        .unwrap();
        let input = AssetInput::new(
            "http://assets.example.test/expired.png".parse().unwrap(),
            "http://assets.example.test/expired.png".parse().unwrap(),
            "image/png".to_owned(),
            b"\x89PNG\r\n\x1a\nexpired".to_vec(),
            0,
            Url::parse(referer.as_str()).unwrap(),
            Some("https://mp.weixin.qq.com".to_owned()),
            Some("repair-test".to_owned()),
        );
        let factory = UnitOfWorkFactory::new(pool.clone());
        let mut uow = factory
            .begin_with_assets(std::slice::from_ref(&input))
            .await
            .unwrap();
        uow.source()
            .insert(NewSource {
                id: source_id,
                book_id: format!("repair-expiry-{source_id}"),
                display_name: "Repair expiry source".to_owned(),
                article_url: Some(referer.clone()),
                enabled: true,
                sync_interval: Duration::hours(1),
                rss_item_limit: 20,
                account_id: None,
                scheduling_gate: SchedulingGate::Ready,
                next_fetch_at: Utc::now(),
                priority: 0,
                max_attempts: 3,
            })
            .await
            .unwrap();
        uow.articles()
            .upsert(NewArticle {
                source_id,
                review_id: "repair-expiry".to_owned(),
                title: "Repair expiry".to_owned(),
                author: None,
                summary: None,
                cover_url: None,
                original_url: Some(referer),
                published_at: Utc::now(),
                content_html: "<p>body</p>".to_owned(),
                content_hash: Some("repair-expiry".to_owned()),
                observation_version: ArticleObservationVersion::from_u64(1),
                fetched_at: Utc::now(),
            })
            .await
            .unwrap();
        let stored = uow
            .assets(policy)
            .store_for_article(source_id, "repair-expiry", &[input])
            .await
            .unwrap()
            .remove(0);
        uow.commit().await.unwrap();
        sqlx::query("UPDATE asset_blobs SET data = NULL WHERE id = $1")
            .bind(stored.blob_id())
            .execute(&pool)
            .await
            .unwrap();

        let store = PostgresAssetStore::new(pool.clone(), policy);
        let repair_id = match store.enqueue_repair(stored.id()).await.unwrap() {
            AssetRepairEnqueueResult::Enqueued { job_id } => job_id,
            other => panic!("repair admission failed: {other:?}"),
        };
        let lease = PostgresJobRepository::new(pool.clone())
            .claim_next(
                "repair-expiry-test",
                Utc::now(),
                Duration::minutes(5),
                &[JobType::AssetRepair],
            )
            .await
            .unwrap()
            .expect("repair job should be claimable");
        assert_eq!(lease.job.id(), repair_id);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0; 1024];
            loop {
                let read = stream.read(&mut buffer).await.unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                if request.windows(4).any(|part| part == b"\r\n\r\n") {
                    break;
                }
            }
            let body = br#"{"code":"ExpiredToken"}"#;
            let response = format!("HTTP/1.1 403 Forbidden\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        });
        let client = reqwest::Client::builder()
            .no_proxy()
            .resolve("assets.example.test", address)
            .build()
            .unwrap();
        let archiver = AssetArchiveService::with_client_for_test(policy, None, client)
            .with_expiry_rule_for_test(
                "assets.example.test",
                reqwest::StatusCode::FORBIDDEN,
                "ExpiredToken",
            );
        let handler = AssetRepairJobHandler::new(store, archiver, Duration::seconds(1));

        assert_eq!(
            handler.execute(&lease, Utc::now()).await,
            crate::application::worker::JobExecution::Committed
        );
        server.await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM jobs WHERE id = $1")
                .bind(repair_id)
                .fetch_one(&pool)
                .await
                .unwrap(),
            "succeeded"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM jobs WHERE job_type = 'asset_refresh' AND status = 'queued'"
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            1
        );
        assert!(sqlx::query_scalar::<_, bool>(
            "SELECT expiry_refunded FROM asset_repair_jobs WHERE job_id = $1"
        )
        .bind(repair_id)
        .fetch_one(&pool)
        .await
        .unwrap());
    }
}

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
        asset_archive_service::AssetArchiveService,
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
        let Some(input) = self
            .archiver
            .fetch_assets_with_context(
                &referer,
                std::slice::from_ref(&target.source_url),
                target.origin.as_deref(),
                target.user_agent.as_deref(),
            )
            .await
            .into_iter()
            .next()
        else {
            return self
                .failure(
                    lease,
                    payload.asset_id,
                    now,
                    "asset repair fetch returned no image",
                )
                .await;
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
    use super::AssetRepairPayload;
    use serde_json::json;
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
}

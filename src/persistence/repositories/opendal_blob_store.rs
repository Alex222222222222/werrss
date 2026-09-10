//! OpenDAL-backed adapters for immutable asset blobs.

use std::{any::Any, future::IntoFuture, path::PathBuf, time::Duration};

use async_trait::async_trait;
use fs2::FileExt;
use opendal::{services, ErrorKind, Operator};
use serde::{Deserialize, Serialize};

use crate::archive::asset_store::{
    AssetBlob, AssetBlobKey, AssetBlobMetadata, AssetBlobStore, AssetBlobStoreError,
};

const METADATA_SUFFIX: &str = ".metadata.json";
const WRITE_LOCK_FILE: &str = ".werrss.lock";
const REMOTE_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct OpenDalBlobStoreCore {
    operator: Operator,
    root: Option<PathBuf>,
    operation_timeout: Option<Duration>,
}

/// Stores raw asset bytes in a local directory through OpenDAL's filesystem
/// service. Metadata is kept in a sidecar so `stat` does not load the body.
#[derive(Clone)]
pub struct OpenDalFsBlobStore {
    core: OpenDalBlobStoreCore,
}

/// Stores raw asset bytes in an S3-compatible service through OpenDAL.
#[derive(Clone)]
pub struct OpenDalS3BlobStore {
    core: OpenDalBlobStoreCore,
}

/// Stores raw asset bytes in Google Cloud Storage through OpenDAL.
#[derive(Clone)]
pub struct OpenDalGcsBlobStore {
    core: OpenDalBlobStoreCore,
}

/// Stores raw asset bytes in Azure Blob Storage through OpenDAL.
#[derive(Clone)]
pub struct OpenDalAzureBlobStore {
    core: OpenDalBlobStoreCore,
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredMetadata {
    checksum: String,
    media_type: String,
    byte_size: u64,
}

impl OpenDalFsBlobStore {
    /// Creates a filesystem-backed store rooted at `root`.
    pub fn new(root: impl AsRef<str>) -> Result<Self, AssetBlobStoreError> {
        let root = PathBuf::from(root.as_ref());
        let root_string = root.to_string_lossy();
        let builder = services::Fs::default().root(&root_string);
        let operator = Operator::new(builder).map_err(backend_error)?;
        Ok(Self {
            core: OpenDalBlobStoreCore {
                operator,
                root: Some(root),
                operation_timeout: None,
            },
        })
    }
}

impl OpenDalS3BlobStore {
    /// Creates an S3-compatible backend with explicit credentials.
    pub fn new(
        bucket: &str,
        endpoint: Option<&str>,
        region: &str,
        access_key_id: &str,
        secret_access_key: &str,
    ) -> Result<Self, AssetBlobStoreError> {
        Self::new_with_operation_timeout(
            bucket,
            endpoint,
            region,
            access_key_id,
            secret_access_key,
            REMOTE_OPERATION_TIMEOUT,
        )
    }

    fn new_with_operation_timeout(
        bucket: &str,
        endpoint: Option<&str>,
        region: &str,
        access_key_id: &str,
        secret_access_key: &str,
        operation_timeout: Duration,
    ) -> Result<Self, AssetBlobStoreError> {
        let builder = services::S3::default()
            .bucket(bucket)
            .region(region)
            .access_key_id(access_key_id)
            .secret_access_key(secret_access_key)
            .disable_config_load();
        let builder = match endpoint {
            Some(endpoint) => builder.endpoint(endpoint),
            None => builder,
        };
        let operator = Operator::new(builder).map_err(backend_error)?;
        Ok(Self {
            core: OpenDalBlobStoreCore {
                operator,
                root: None,
                operation_timeout: Some(operation_timeout),
            },
        })
    }
}

impl OpenDalGcsBlobStore {
    /// Creates a Google Cloud Storage backend with an explicit credential.
    ///
    /// `credential` is the base64-encoded service-account or external-account
    /// JSON accepted by OpenDAL's GCS service.
    pub fn new(
        bucket: &str,
        endpoint: Option<&str>,
        credential: &str,
    ) -> Result<Self, AssetBlobStoreError> {
        Self::new_with_options(bucket, endpoint, Some(credential), false)
    }

    /// Creates a Google Cloud Storage backend with explicit authentication options.
    ///
    /// `credential` is the base64-encoded service-account or external-account
    /// JSON accepted by OpenDAL's GCS service. It may be omitted when
    /// `skip_signature` is enabled for an unauthenticated emulator.
    pub fn new_with_options(
        bucket: &str,
        endpoint: Option<&str>,
        credential: Option<&str>,
        skip_signature: bool,
    ) -> Result<Self, AssetBlobStoreError> {
        let mut builder = services::Gcs::default()
            .bucket(bucket)
            .disable_config_load()
            .disable_vm_metadata();
        if let Some(credential) = credential {
            builder = builder.credential(credential);
        }
        if skip_signature {
            builder = builder.skip_signature();
        }
        if let Some(endpoint) = endpoint {
            builder = builder.endpoint(endpoint);
        }
        let operator = Operator::new(builder).map_err(backend_error)?;
        Ok(Self {
            core: OpenDalBlobStoreCore {
                operator,
                root: None,
                operation_timeout: Some(REMOTE_OPERATION_TIMEOUT),
            },
        })
    }
}

impl OpenDalAzureBlobStore {
    /// Creates an Azure Blob backend with explicit connection settings.
    pub fn new(
        container: &str,
        endpoint: &str,
        account_name: &str,
        account_key: &str,
    ) -> Result<Self, AssetBlobStoreError> {
        Self::new_with_options(
            container,
            endpoint,
            Some(account_name),
            Some(account_key),
            false,
        )
    }

    /// Creates an Azure Blob backend with explicit authentication options.
    ///
    /// `account_name` and `account_key` may be omitted when `skip_signature`
    /// is enabled for an unauthenticated emulator.
    pub fn new_with_options(
        container: &str,
        endpoint: &str,
        account_name: Option<&str>,
        account_key: Option<&str>,
        skip_signature: bool,
    ) -> Result<Self, AssetBlobStoreError> {
        let mut builder = services::Azblob::default()
            .container(container)
            .endpoint(endpoint);
        if let Some(account_name) = account_name {
            builder = builder.account_name(account_name);
        }
        if let Some(account_key) = account_key {
            builder = builder.account_key(account_key);
        }
        if skip_signature {
            builder = builder.skip_signature();
        }
        let operator = Operator::new(builder).map_err(backend_error)?;
        Ok(Self {
            core: OpenDalBlobStoreCore {
                operator,
                root: None,
                operation_timeout: Some(REMOTE_OPERATION_TIMEOUT),
            },
        })
    }
}

impl OpenDalBlobStoreCore {
    async fn run_backend_operation<T, F>(&self, operation: F) -> Result<T, opendal::Error>
    where
        F: IntoFuture<Output = Result<T, opendal::Error>>,
    {
        let operation = operation.into_future();
        match self.operation_timeout {
            Some(timeout) => tokio::time::timeout(timeout, operation)
                .await
                .map_err(|_| {
                    opendal::Error::new(
                        ErrorKind::Unexpected,
                        format!("asset blob backend operation timed out after {timeout:?}"),
                    )
                })?,
            None => operation.await,
        }
    }

    fn metadata_key(key: &AssetBlobKey) -> String {
        format!("{}{}", key.as_str(), METADATA_SUFFIX)
    }

    async fn acquire_write_lock(&self) -> Result<std::fs::File, AssetBlobStoreError> {
        // Keep one advisory lock per configured root instead of one inode per
        // blob. The lock file is intentionally bounded and harmless to retain.
        let Some(root) = self.root.clone() else {
            return Err(AssetBlobStoreError::Backend(
                "S3 backends do not use filesystem locks".to_owned(),
            ));
        };
        tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&root).map_err(backend_error)?;
            let lock_file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(root.join(WRITE_LOCK_FILE))
                .map_err(backend_error)?;
            lock_file.lock_exclusive().map_err(backend_error)?;
            Ok(lock_file)
        })
        .await
        .map_err(backend_error)?
    }

    async fn delete_body(&self, key: &AssetBlobKey) -> Result<(), AssetBlobStoreError> {
        match self
            .run_backend_operation(self.operator.delete(key.as_str()))
            .await
        {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(backend_error(error)),
        }
    }

    async fn write_metadata(
        &self,
        key: &AssetBlobKey,
        blob: &AssetBlob,
    ) -> Result<(), AssetBlobStoreError> {
        let metadata = StoredMetadata {
            checksum: blob.checksum().to_owned(),
            media_type: blob.media_type().to_owned(),
            byte_size: blob.byte_size(),
        };
        let encoded = serde_json::to_vec(&metadata).map_err(backend_error)?;
        match self
            .run_backend_operation(
                self.operator
                    .write_with(&Self::metadata_key(key), encoded)
                    .if_not_exists(true),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if is_existing_error(&error) => match self.read(key).await? {
                Some(existing) if existing == *blob => Ok(()),
                Some(_) => Err(AssetBlobStoreError::Conflict),
                None => Err(backend_error(error)),
            },
            Err(error) => Err(backend_error(error)),
        }
    }

    async fn recover_body_only_partial_write(
        &self,
        key: &AssetBlobKey,
        blob: &AssetBlob,
        original_error: impl std::fmt::Display,
    ) -> Result<(), AssetBlobStoreError> {
        let bytes = match self
            .run_backend_operation(self.operator.read(key.as_str()))
            .await
        {
            Ok(bytes) => bytes.to_vec(),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Err(backend_error(original_error));
            }
            Err(error) if self.root.is_none() => {
                return self
                    .reconcile_external_write(key, blob, backend_error(error))
                    .await;
            }
            Err(error) => return Err(backend_error(error)),
        };
        if bytes != blob.bytes() {
            if self.root.is_some() {
                if let Err(cleanup_error) = self.delete_body(key).await {
                    return Err(AssetBlobStoreError::Backend(format!(
                        "partial body conflicts with requested blob: {original_error}; cleanup failed: {cleanup_error}"
                    )));
                }
            }
            return Err(AssetBlobStoreError::Conflict);
        }
        match self.write_metadata(key, blob).await {
            Ok(()) => Ok(()),
            Err(metadata_error) if self.root.is_none() => {
                self.reconcile_external_write(key, blob, metadata_error)
                    .await
            }
            Err(metadata_error) => match self.delete(key).await {
                Ok(()) => Err(metadata_error),
                Err(cleanup_error) => Err(AssetBlobStoreError::Backend(format!(
                    "partial blob metadata recovery failed: {metadata_error}; cleanup failed: {cleanup_error}"
                ))),
            },
        }
    }

    async fn read_metadata(
        &self,
        key: &AssetBlobKey,
    ) -> Result<Option<StoredMetadata>, AssetBlobStoreError> {
        let metadata_key = Self::metadata_key(key);
        let bytes = match self
            .run_backend_operation(self.operator.read(&metadata_key))
            .await
        {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(backend_error(error)),
        };
        serde_json::from_slice(&bytes.to_vec())
            .map(Some)
            .map_err(backend_error)
    }
}

impl OpenDalBlobStoreCore {
    async fn read(&self, key: &AssetBlobKey) -> Result<Option<AssetBlob>, AssetBlobStoreError> {
        let Some(metadata) = self.read_metadata(key).await? else {
            return Ok(None);
        };
        let bytes = match self
            .run_backend_operation(self.operator.read(key.as_str()))
            .await
        {
            Ok(bytes) => bytes.to_vec(),
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(backend_error(error)),
        };
        AssetBlob::new(bytes, metadata.media_type, metadata.checksum).map(Some)
    }

    async fn stat(
        &self,
        key: &AssetBlobKey,
    ) -> Result<Option<AssetBlobMetadata>, AssetBlobStoreError> {
        let Some(metadata) = self.read_metadata(key).await? else {
            return Ok(None);
        };
        match self
            .run_backend_operation(self.operator.stat(key.as_str()))
            .await
        {
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(backend_error(error)),
        }
        Ok(Some(AssetBlobMetadata::new(
            metadata.checksum,
            metadata.media_type,
            metadata.byte_size,
            true,
        )?))
    }

    async fn put(&self, key: &AssetBlobKey, blob: &AssetBlob) -> Result<(), AssetBlobStoreError> {
        let _write_guard = match self.root.is_some() {
            true => Some(self.acquire_write_lock().await?),
            false => None,
        };
        if let Some(existing) = self.read(key).await? {
            return if existing == *blob {
                Ok(())
            } else {
                Err(AssetBlobStoreError::Conflict)
            };
        }
        match self
            .run_backend_operation(
                self.operator
                    .write_with(key.as_str(), blob.bytes().to_vec())
                    .if_not_exists(true),
            )
            .await
        {
            Ok(_) => {}
            Err(error) if is_existing_error(&error) => {
                return match self.read(key).await? {
                    Some(existing) if existing == *blob => Ok(()),
                    Some(_) => Err(AssetBlobStoreError::Conflict),
                    None => self.recover_body_only_partial_write(key, blob, error).await,
                };
            }
            Err(error) if self.root.is_none() => {
                return self
                    .reconcile_external_write(key, blob, backend_error(error))
                    .await;
            }
            Err(error) => return Err(backend_error(error)),
        }
        match self.write_metadata(key, blob).await {
            Ok(()) => Ok(()),
            Err(error) if self.root.is_none() => {
                self.reconcile_external_write(key, blob, error).await
            }
            Err(error) => {
                // The body was created before its metadata. Remove it before
                // returning so the caller never loses track of a partial put.
                let cleanup = self.delete(key).await;
                match cleanup {
                    Ok(()) => Err(error),
                    Err(cleanup_error) => Err(AssetBlobStoreError::Backend(format!(
                        "metadata write failed: {error}; cleanup failed: {cleanup_error}"
                    ))),
                }
            }
        }
    }

    async fn reconcile_external_write(
        &self,
        key: &AssetBlobKey,
        blob: &AssetBlob,
        original_error: AssetBlobStoreError,
    ) -> Result<(), AssetBlobStoreError> {
        let original_error = original_error.to_string();
        match self
            .run_backend_operation(self.operator.stat(key.as_str()))
            .await
        {
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Err(AssetBlobStoreError::Backend(original_error));
            }
            Err(error) => {
                return Err(AssetBlobStoreError::Backend(format!(
                    "external blob write could not be reconciled: {original_error}; body check failed: {error}"
                )));
            }
        }

        match self.read(key).await {
            Ok(Some(existing)) => {
                return if existing == *blob {
                    Ok(())
                } else {
                    Err(AssetBlobStoreError::Conflict)
                };
            }
            Ok(None) => {}
            Err(error) => {
                return Err(AssetBlobStoreError::Backend(format!(
                    "external blob write could not be reconciled: {original_error}; read failed: {error}"
                )));
            }
        }

        if let Err(retry_error) = self.write_metadata(key, blob).await {
            return Err(AssetBlobStoreError::Backend(format!(
                "external blob write could not be reconciled: {original_error}; metadata retry failed: {retry_error}"
            )));
        }
        match self.read(key).await {
            Ok(Some(existing)) if existing == *blob => Ok(()),
            Ok(Some(_)) => Err(AssetBlobStoreError::Conflict),
            Ok(None) => Err(AssetBlobStoreError::Backend(format!(
                "external blob write could not be reconciled: {original_error}; metadata remained unavailable"
            ))),
            Err(error) => Err(AssetBlobStoreError::Backend(format!(
                "external blob write could not be reconciled: {original_error}; final read failed: {error}"
            ))),
        }
    }

    async fn delete(&self, key: &AssetBlobKey) -> Result<(), AssetBlobStoreError> {
        for object in [key.as_str().to_owned(), Self::metadata_key(key)] {
            match self
                .run_backend_operation(self.operator.delete(&object))
                .await
            {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => return Err(backend_error(error)),
            }
        }
        Ok(())
    }
}

macro_rules! impl_blob_store_for_open_dal {
    ($type:ty) => {
        #[async_trait]
        impl AssetBlobStore for $type {
            fn as_any(&self) -> &dyn Any {
                self
            }

            async fn read(
                &self,
                key: &AssetBlobKey,
            ) -> Result<Option<AssetBlob>, AssetBlobStoreError> {
                self.core.read(key).await
            }

            async fn stat(
                &self,
                key: &AssetBlobKey,
            ) -> Result<Option<AssetBlobMetadata>, AssetBlobStoreError> {
                self.core.stat(key).await
            }

            async fn put(
                &self,
                key: &AssetBlobKey,
                blob: &AssetBlob,
            ) -> Result<(), AssetBlobStoreError> {
                self.core.put(key, blob).await
            }

            async fn delete(&self, key: &AssetBlobKey) -> Result<(), AssetBlobStoreError> {
                self.core.delete(key).await
            }
        }
    };
}

impl_blob_store_for_open_dal!(OpenDalFsBlobStore);
impl_blob_store_for_open_dal!(OpenDalS3BlobStore);
impl_blob_store_for_open_dal!(OpenDalGcsBlobStore);
impl_blob_store_for_open_dal!(OpenDalAzureBlobStore);

fn backend_error(error: impl std::fmt::Display) -> AssetBlobStoreError {
    AssetBlobStoreError::Backend(error.to_string())
}

fn is_existing_error(error: &opendal::Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::AlreadyExists | ErrorKind::ConditionNotMatch
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::{
        collections::HashMap,
        env,
        ffi::OsString,
        sync::{Arc, Mutex},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::{Mutex as TokioMutex, Notify},
    };
    use uuid::Uuid;

    fn blob() -> AssetBlob {
        let bytes = b"local asset".to_vec();
        let checksum = format!("{:x}", Sha256::digest(&bytes));
        AssetBlob::new(bytes, "image/png".to_owned(), checksum).unwrap()
    }

    fn other_blob() -> AssetBlob {
        let bytes = b"another local asset".to_vec();
        let checksum = format!("{:x}", Sha256::digest(&bytes));
        AssetBlob::new(bytes, "image/jpeg".to_owned(), checksum).unwrap()
    }

    static AWS_ENDPOINT_ENV_LOCK: TokioMutex<()> = TokioMutex::const_new(());
    const AWS_ENDPOINT_ENV_NAMES: [&str; 3] =
        ["AWS_ENDPOINT_URL", "AWS_ENDPOINT", "AWS_S3_ENDPOINT"];

    struct AwsEndpointEnvironmentGuard {
        previous: Vec<(&'static str, Option<OsString>)>,
    }

    impl Drop for AwsEndpointEnvironmentGuard {
        fn drop(&mut self) {
            for (name, value) in &self.previous {
                match value {
                    Some(value) => env::set_var(name, value),
                    None => env::remove_var(name),
                }
            }
        }
    }

    fn force_ambient_aws_endpoint(endpoint: &str) -> AwsEndpointEnvironmentGuard {
        let previous = AWS_ENDPOINT_ENV_NAMES
            .iter()
            .map(|name| (*name, env::var_os(name)))
            .collect();
        for name in AWS_ENDPOINT_ENV_NAMES {
            env::set_var(name, endpoint);
        }
        AwsEndpointEnvironmentGuard { previous }
    }

    #[tokio::test]
    async fn s3_backend_operations_have_a_bounded_timeout() {
        let root = std::env::temp_dir().join(format!("werrss-opendal-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let filesystem_store = OpenDalFsBlobStore::new(root.to_str().unwrap()).unwrap();
        let core = OpenDalBlobStoreCore {
            operator: filesystem_store.core.operator.clone(),
            root: None,
            operation_timeout: Some(std::time::Duration::from_millis(10)),
        };

        let result = core
            .run_backend_operation(std::future::pending::<Result<(), opendal::Error>>())
            .await;

        let error = result.expect_err("a stalled backend operation should time out");
        assert_eq!(error.kind(), ErrorKind::Unexpected);
        assert!(error.to_string().contains("timed out"));
        assert_eq!(
            OpenDalS3BlobStore::new(
                "werrss-assets",
                Some("http://127.0.0.1:9000"),
                "us-east-1",
                "access",
                "secret",
            )
            .unwrap()
            .core
            .operation_timeout,
            Some(REMOTE_OPERATION_TIMEOUT)
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn gcs_backend_operations_have_a_bounded_timeout() {
        let store = OpenDalGcsBlobStore::new(
            "werrss-assets",
            Some("http://127.0.0.1:9000"),
            "base64-encoded-service-account",
        )
        .unwrap();
        let core = OpenDalBlobStoreCore {
            operator: store.core.operator.clone(),
            root: None,
            operation_timeout: Some(std::time::Duration::from_millis(10)),
        };
        let result = core
            .run_backend_operation(std::future::pending::<Result<(), opendal::Error>>())
            .await;

        let error = result.expect_err("a stalled backend operation should time out");
        assert_eq!(error.kind(), ErrorKind::Unexpected);
        assert!(error.to_string().contains("timed out"));
        assert_eq!(store.core.operation_timeout, Some(REMOTE_OPERATION_TIMEOUT));
    }

    #[tokio::test]
    async fn azure_backend_operations_have_a_bounded_timeout() {
        let store = OpenDalAzureBlobStore::new(
            "werrss-assets",
            "http://127.0.0.1:10000/devstoreaccount1",
            "devstoreaccount1",
            "ZHVtbXktYWNjb3VudC1rZXk=",
        )
        .unwrap();
        let core = OpenDalBlobStoreCore {
            operator: store.core.operator.clone(),
            root: None,
            operation_timeout: Some(std::time::Duration::from_millis(10)),
        };
        let result = core
            .run_backend_operation(std::future::pending::<Result<(), opendal::Error>>())
            .await;

        let error = result.expect_err("a stalled backend operation should time out");
        assert_eq!(error.kind(), ErrorKind::Unexpected);
        assert!(error.to_string().contains("timed out"));
        assert_eq!(store.core.operation_timeout, Some(REMOTE_OPERATION_TIMEOUT));
    }

    #[tokio::test]
    async fn s3_store_ignores_ambient_endpoint_overrides() {
        let _environment_lock = AWS_ENDPOINT_ENV_LOCK.lock().await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let request_seen = Arc::new(Notify::new());
        let server_request_seen = request_seen.clone();
        let server = tokio::spawn(async move {
            if listener.accept().await.is_ok() {
                server_request_seen.notify_one();
            }
        });
        let _environment = force_ambient_aws_endpoint(&endpoint);
        let store = OpenDalS3BlobStore::new_with_operation_timeout(
            "werrss-assets",
            None,
            "us-east-1",
            "access",
            "secret",
            std::time::Duration::from_millis(20),
        )
        .unwrap();
        let key = AssetBlobKey::for_blob_id(Uuid::new_v4());

        let result = tokio::time::timeout(std::time::Duration::from_secs(1), store.core.read(&key))
            .await
            .expect("the configured S3 operation timeout should bound the read");
        assert!(result.is_err());
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(50),
            request_seen.notified(),
        )
        .await
        .is_err());

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn filesystem_store_round_trips_and_is_idempotent() {
        let root = std::env::temp_dir().join(format!("werrss-opendal-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let store = OpenDalFsBlobStore::new(root.to_str().unwrap()).unwrap();
        let key = AssetBlobKey::for_blob_id(Uuid::new_v4());
        let value = blob();

        store.put(&key, &value).await.unwrap();
        store.put(&key, &value).await.unwrap();
        assert_eq!(
            store.stat(&key).await.unwrap().unwrap().byte_size(),
            value.byte_size()
        );
        assert_eq!(store.read(&key).await.unwrap().unwrap(), value);
        store.delete(&key).await.unwrap();
        assert!(store.read(&key).await.unwrap().is_none());
        assert!(store.stat(&key).await.unwrap().is_none());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn filesystem_store_rejects_reusing_a_key_for_different_data() {
        let root = std::env::temp_dir().join(format!("werrss-opendal-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let store = OpenDalFsBlobStore::new(root.to_str().unwrap()).unwrap();
        let key = AssetBlobKey::for_blob_id(Uuid::new_v4());
        store.put(&key, &blob()).await.unwrap();
        let bytes = b"different".to_vec();
        let checksum = format!("{:x}", Sha256::digest(&bytes));
        let other = AssetBlob::new(bytes, "image/png".to_owned(), checksum).unwrap();
        assert_eq!(
            store.put(&key, &other).await,
            Err(AssetBlobStoreError::Conflict)
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn filesystem_store_serializes_same_key_puts() {
        let root = std::env::temp_dir().join(format!("werrss-opendal-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let store = OpenDalFsBlobStore::new(root.to_str().unwrap()).unwrap();
        let key = AssetBlobKey::for_blob_id(Uuid::new_v4());
        let value = blob();

        let mut tasks = Vec::new();
        for _ in 0..16 {
            let store = store.clone();
            let key = key.clone();
            let value = value.clone();
            tasks.push(tokio::spawn(async move { store.put(&key, &value).await }));
        }

        for task in tasks {
            task.await.unwrap().unwrap();
        }
        assert_eq!(store.read(&key).await.unwrap().unwrap(), value);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn filesystem_store_preserves_conflicts_across_store_instances() {
        let root = std::env::temp_dir().join(format!("werrss-opendal-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let root = root.to_str().unwrap().to_owned();
        let store_a = OpenDalFsBlobStore::new(&root).unwrap();
        let store_b = OpenDalFsBlobStore::new(&root).unwrap();
        let key = AssetBlobKey::for_blob_id(Uuid::new_v4());
        let first = blob();
        let second = other_blob();

        let task_a = tokio::spawn({
            let store = store_a.clone();
            let key = key.clone();
            let value = first.clone();
            async move { store.put(&key, &value).await }
        });
        let task_b = tokio::spawn({
            let store = store_b.clone();
            let key = key.clone();
            let value = second.clone();
            async move { store.put(&key, &value).await }
        });
        let result_a = task_a.await.unwrap();
        let result_b = task_b.await.unwrap();

        assert!(matches!(
            (result_a, result_b),
            (Ok(()), Err(AssetBlobStoreError::Conflict))
                | (Err(AssetBlobStoreError::Conflict), Ok(()))
        ));
        let stored = store_a.read(&key).await.unwrap().unwrap();
        assert!(stored == first || stored == second);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn filesystem_store_uses_one_bounded_lock_file_per_root() {
        let root = std::env::temp_dir().join(format!("werrss-opendal-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let store = OpenDalFsBlobStore::new(root.to_str().unwrap()).unwrap();

        store
            .put(&AssetBlobKey::for_blob_id(Uuid::new_v4()), &blob())
            .await
            .unwrap();
        store
            .put(&AssetBlobKey::for_blob_id(Uuid::new_v4()), &other_blob())
            .await
            .unwrap();

        assert!(root.join(WRITE_LOCK_FILE).is_file());
        assert!(!root.join(".werrss-locks").exists());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn filesystem_store_recovers_body_only_partial_writes() {
        let root = std::env::temp_dir().join(format!("werrss-opendal-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let store = OpenDalFsBlobStore::new(root.to_str().unwrap()).unwrap();
        let key = AssetBlobKey::for_blob_id(Uuid::new_v4());
        let value = blob();

        store
            .core
            .operator
            .write(key.as_str(), value.bytes().to_vec())
            .await
            .unwrap();
        store.put(&key, &value).await.unwrap();

        assert_eq!(store.read(&key).await.unwrap().unwrap(), value);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn filesystem_store_removes_conflicting_body_only_partial_writes() {
        let root = std::env::temp_dir().join(format!("werrss-opendal-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let store = OpenDalFsBlobStore::new(root.to_str().unwrap()).unwrap();
        let key = AssetBlobKey::for_blob_id(Uuid::new_v4());
        let value = blob();
        let conflicting = b"conflicting body".to_vec();

        store
            .core
            .operator
            .write(key.as_str(), conflicting)
            .await
            .unwrap();
        assert_eq!(
            store.put(&key, &value).await,
            Err(AssetBlobStoreError::Conflict)
        );
        assert!(store.core.operator.read(key.as_str()).await.is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[derive(Default)]
    struct MockS3State {
        body_present: bool,
        metadata_present: bool,
        metadata_puts: usize,
        requests: Vec<String>,
    }

    async fn read_mock_http_request(stream: &mut TcpStream) -> Option<(String, String)> {
        let (method, target, _) = read_mock_http_request_with_body(stream).await?;
        Some((method, target))
    }

    async fn read_mock_http_request_with_body(
        stream: &mut TcpStream,
    ) -> Option<(String, String, Vec<u8>)> {
        let mut request = Vec::new();
        let header_end;
        loop {
            let mut chunk = [0_u8; 1024];
            let read = stream.read(&mut chunk).await.ok()?;
            if read == 0 {
                return None;
            }
            request.extend_from_slice(&chunk[..read]);
            if let Some(position) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                header_end = position + 4;
                break;
            }
        }

        let headers = String::from_utf8(request[..header_end].to_vec()).ok()?;
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.strip_prefix("Content-Length:")
                    .or_else(|| line.strip_prefix("content-length:"))
            })
            .and_then(|value| value.trim().parse::<usize>().ok())
            .unwrap_or_default();
        while request.len() < header_end + content_length {
            let mut chunk = [0_u8; 1024];
            let read = stream.read(&mut chunk).await.ok()?;
            if read == 0 {
                return None;
            }
            request.extend_from_slice(&chunk[..read]);
        }

        let mut request_line = headers.lines().next()?.split_whitespace();
        Some((
            request_line.next()?.to_owned(),
            request_line.next()?.to_owned(),
            request[header_end..header_end + content_length].to_vec(),
        ))
    }

    async fn write_mock_http_response(
        stream: &mut TcpStream,
        status: &str,
        body: &[u8],
    ) -> std::io::Result<()> {
        write_mock_http_response_with_headers(stream, status, &[], body).await
    }

    async fn write_mock_http_response_with_headers(
        stream: &mut TcpStream,
        status: &str,
        extra_headers: &[(&str, &str)],
        body: &[u8],
    ) -> std::io::Result<()> {
        let extra_headers = extra_headers
            .iter()
            .map(|(name, value)| format!("{name}: {value}\r\n"))
            .collect::<String>();
        let headers = format!(
            "HTTP/1.1 {status}\r\n{extra_headers}Content-Length: {}\r\nConnection: close\r\n\r\n",
            body.len(),
        );
        stream.write_all(headers.as_bytes()).await?;
        stream.write_all(body).await
    }

    async fn serve_mock_s3_connection(
        mut stream: TcpStream,
        state: Arc<Mutex<MockS3State>>,
        metadata_accepted: Arc<Notify>,
        body: Vec<u8>,
        metadata: Vec<u8>,
    ) {
        let Some((method, target)) = read_mock_http_request(&mut stream).await else {
            return;
        };
        let target = target.split('?').next().unwrap_or_default();
        let is_metadata = target.ends_with(METADATA_SUFFIX);
        let request = format!("{method} {target}");

        let (status, response_body, delay_metadata_response) = match method.as_str() {
            "GET" | "HEAD" => {
                let present = {
                    let mut state = state.lock().unwrap();
                    state.requests.push(request);
                    if is_metadata {
                        state.metadata_present
                    } else {
                        state.body_present
                    }
                };
                if present {
                    if is_metadata {
                        ("200 OK", metadata.clone(), false)
                    } else {
                        ("200 OK", body.clone(), false)
                    }
                } else {
                    ("404 Not Found", Vec::new(), false)
                }
            }
            "PUT" => {
                let first_metadata_put = {
                    let mut state = state.lock().unwrap();
                    state.requests.push(request);
                    if is_metadata {
                        state.metadata_present = true;
                        state.metadata_puts += 1;
                        state.metadata_puts == 1
                    } else {
                        state.body_present = true;
                        false
                    }
                };
                if first_metadata_put {
                    metadata_accepted.notify_one();
                }
                ("200 OK", Vec::new(), first_metadata_put)
            }
            "DELETE" => {
                let mut state = state.lock().unwrap();
                state.requests.push(request);
                if is_metadata {
                    state.metadata_present = false;
                } else {
                    state.body_present = false;
                }
                ("204 No Content", Vec::new(), false)
            }
            _ => {
                state.lock().unwrap().requests.push(request);
                ("404 Not Found", Vec::new(), false)
            }
        };

        if delay_metadata_response {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            return;
        }
        let _ = write_mock_http_response(&mut stream, status, &response_body).await;
    }

    #[tokio::test]
    async fn s3_metadata_timeout_does_not_delete_an_object_accepted_by_a_retry() {
        let value = blob();
        let metadata = serde_json::to_vec(&StoredMetadata {
            checksum: value.checksum().to_owned(),
            media_type: value.media_type().to_owned(),
            byte_size: value.byte_size(),
        })
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(MockS3State::default()));
        let metadata_accepted = Arc::new(Notify::new());
        let server_state = state.clone();
        let server_metadata_accepted = metadata_accepted.clone();
        let server_body = value.bytes().to_vec();
        let server_metadata = metadata.clone();
        let server = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(serve_mock_s3_connection(
                    stream,
                    server_state.clone(),
                    server_metadata_accepted.clone(),
                    server_body.clone(),
                    server_metadata.clone(),
                ));
            }
        });

        let first = OpenDalS3BlobStore::new_with_operation_timeout(
            "werrss-assets",
            Some(&endpoint),
            "us-east-1",
            "access",
            "secret",
            std::time::Duration::from_millis(40),
        )
        .unwrap();
        let second = first.clone();
        let key = AssetBlobKey::for_blob_id(Uuid::new_v4());
        let first_task = tokio::spawn({
            let key = key.clone();
            let value = value.clone();
            async move { first.put(&key, &value).await }
        });

        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            metadata_accepted.notified(),
        )
        .await
        .expect("the first metadata request should be accepted");

        tokio::time::timeout(std::time::Duration::from_secs(1), second.put(&key, &value))
            .await
            .expect("the retry should not wait for the ambiguous first request")
            .expect("the retry should reuse the committed object");

        let first_result = tokio::time::timeout(std::time::Duration::from_secs(1), first_task)
            .await
            .expect("the first request should be bounded")
            .expect("the first task should not panic");
        assert_eq!(first_result, Ok(()));

        {
            let state = state.lock().unwrap();
            assert!(state.body_present);
            assert!(state.metadata_present);
            assert!(state
                .requests
                .iter()
                .all(|request| !request.starts_with("DELETE ")));
        }

        server.abort();
        let _ = server.await;
    }

    #[derive(Default)]
    struct MockGcsState {
        objects: HashMap<String, Vec<u8>>,
        sessions: HashMap<String, String>,
        targets: Vec<String>,
    }

    fn gcs_object_name(target: &str) -> Option<String> {
        let url = url::Url::parse(&format!("http://gcs.test{target}")).ok()?;
        if let Some((_, name)) = url.query_pairs().find(|(key, _)| key == "name") {
            return Some(name.into_owned());
        }
        let encoded_name = url.path().split("/o/").nth(1)?;
        percent_encoding::percent_decode_str(encoded_name)
            .decode_utf8()
            .ok()
            .map(|name| name.into_owned())
    }

    async fn serve_mock_gcs_connection(
        mut stream: TcpStream,
        state: Arc<Mutex<MockGcsState>>,
        endpoint: String,
    ) {
        let Some((method, target, body)) = read_mock_http_request_with_body(&mut stream).await
        else {
            return;
        };
        let object_name = gcs_object_name(&target);
        let (status, response_headers, response_body) = {
            let mut state = state.lock().unwrap();
            state.targets.push(target.clone());
            match method.as_str() {
                "GET" => match object_name.and_then(|name| state.objects.get(&name).cloned()) {
                    Some(body) => ("200 OK", Vec::new(), body),
                    None => ("404 Not Found", Vec::new(), Vec::new()),
                },
                "POST" => {
                    if target.contains("uploadType=resumable") {
                        let Some(object_name) = object_name else {
                            return;
                        };
                        let session_path = format!("/session/{}", state.sessions.len() + 1);
                        state.sessions.insert(session_path.clone(), object_name);
                        let location = format!("{endpoint}{session_path}");
                        ("200 OK", vec![("Location", location)], Vec::new())
                    } else {
                        let Some(object_name) = object_name else {
                            return;
                        };
                        let size = body.len();
                        state.objects.insert(object_name, body);
                        (
                            "200 OK",
                            Vec::new(),
                            format!(r#"{{"size":"{size}"}}"#).into_bytes(),
                        )
                    }
                }
                "PUT" if target.starts_with("/session/") => {
                    let Some(object_name) = state.sessions.get(&target).cloned() else {
                        return;
                    };
                    let size = body.len();
                    state.objects.insert(object_name, body);
                    (
                        "200 OK",
                        Vec::new(),
                        format!(r#"{{"size":"{size}"}}"#).into_bytes(),
                    )
                }
                "DELETE" => {
                    if let Some(object_name) = object_name {
                        state.objects.remove(&object_name);
                    }
                    ("204 No Content", Vec::new(), Vec::new())
                }
                _ => ("404 Not Found", Vec::new(), Vec::new()),
            }
        };
        let response_headers = response_headers
            .iter()
            .map(|(name, value)| (*name, value.as_str()))
            .collect::<Vec<_>>();
        let _ = write_mock_http_response_with_headers(
            &mut stream,
            status,
            &response_headers,
            &response_body,
        )
        .await;
    }

    #[derive(Default)]
    struct MockAzureState {
        objects: HashMap<String, Vec<u8>>,
        targets: Vec<String>,
    }

    fn azure_object_name(target: &str) -> Option<String> {
        let path = target.split('?').next().unwrap_or_default();
        let encoded_name = path.split("/werrss-assets/").nth(1)?;
        percent_encoding::percent_decode_str(encoded_name)
            .decode_utf8()
            .ok()
            .map(|name| name.into_owned())
    }

    async fn serve_mock_azure_connection(mut stream: TcpStream, state: Arc<Mutex<MockAzureState>>) {
        let Some((method, target, body)) = read_mock_http_request_with_body(&mut stream).await
        else {
            return;
        };
        let object_name = azure_object_name(&target);
        let (status, response_body) = {
            let mut state = state.lock().unwrap();
            state.targets.push(target);
            match method.as_str() {
                "GET" | "HEAD" => {
                    match object_name.and_then(|name| state.objects.get(&name).cloned()) {
                        Some(body) if method == "GET" => ("200 OK", body),
                        Some(_) => ("200 OK", Vec::new()),
                        None => ("404 Not Found", Vec::new()),
                    }
                }
                "PUT" => {
                    let Some(object_name) = object_name else {
                        return;
                    };
                    state.objects.insert(object_name, body);
                    ("201 Created", Vec::new())
                }
                "DELETE" => {
                    if let Some(object_name) = object_name {
                        state.objects.remove(&object_name);
                    }
                    ("202 Accepted", Vec::new())
                }
                _ => ("404 Not Found", Vec::new()),
            }
        };
        let _ = write_mock_http_response(&mut stream, status, &response_body).await;
    }

    #[tokio::test]
    async fn gcs_unsigned_store_round_trips_nested_asset_keys_without_double_encoding() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(MockGcsState::default()));
        let server_state = state.clone();
        let server_endpoint = endpoint.clone();
        let server = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(serve_mock_gcs_connection(
                    stream,
                    server_state.clone(),
                    server_endpoint.clone(),
                ));
            }
        });

        let store =
            OpenDalGcsBlobStore::new_with_options("werrss-assets", Some(&endpoint), None, true)
                .unwrap();
        let key = AssetBlobKey::for_blob_id(Uuid::new_v4());
        let value = blob();

        store.put(&key, &value).await.unwrap();
        assert_eq!(store.read(&key).await.unwrap(), Some(value));

        {
            let state = state.lock().unwrap();
            assert!(state.targets.iter().any(|target| target.contains("%2F")));
            assert!(state.targets.iter().all(|target| !target.contains("%252F")));
            assert!(state.objects.contains_key(key.as_str()));
            assert!(state
                .objects
                .contains_key(&format!("{}{METADATA_SUFFIX}", key.as_str())));
        }

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn azure_unsigned_store_round_trips_asset_blobs() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(MockAzureState::default()));
        let server_state = state.clone();
        let server = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(serve_mock_azure_connection(stream, server_state.clone()));
            }
        });

        let store =
            OpenDalAzureBlobStore::new_with_options("werrss-assets", &endpoint, None, None, true)
                .unwrap();
        let key = AssetBlobKey::for_blob_id(Uuid::new_v4());
        let value = blob();

        store.put(&key, &value).await.unwrap();
        assert_eq!(store.read(&key).await.unwrap(), Some(value.clone()));
        assert_eq!(
            store.stat(&key).await.unwrap().unwrap().byte_size(),
            value.byte_size()
        );
        store.delete(&key).await.unwrap();
        assert!(store.read(&key).await.unwrap().is_none());

        {
            let state = state.lock().unwrap();
            assert!(state.targets.iter().all(|target| !target.contains("%252F")));
            assert!(!state.objects.contains_key(key.as_str()));
            assert!(!state
                .objects
                .contains_key(&format!("{}{METADATA_SUFFIX}", key.as_str())));
        }

        server.abort();
        let _ = server.await;
    }

    #[test]
    fn s3_store_accepts_an_explicit_compatible_endpoint() {
        let store = OpenDalS3BlobStore::new(
            "werrss-assets",
            Some("http://127.0.0.1:9000"),
            "us-east-1",
            "access",
            "secret",
        );
        assert!(store.is_ok());
    }

    #[test]
    fn gcs_store_accepts_an_explicit_endpoint_and_credential() {
        let store = OpenDalGcsBlobStore::new(
            "werrss-assets",
            Some("http://127.0.0.1:9000"),
            "base64-encoded-service-account",
        );
        assert!(store.is_ok());
    }

    #[test]
    fn gcs_store_accepts_unsigned_emulator_options_without_credentials() {
        let store = OpenDalGcsBlobStore::new_with_options(
            "werrss-assets",
            Some("http://127.0.0.1:9000"),
            None,
            true,
        );
        assert!(store.is_ok());
    }

    #[test]
    fn azure_store_accepts_an_explicit_endpoint_and_account_key() {
        let store = OpenDalAzureBlobStore::new(
            "werrss-assets",
            "http://127.0.0.1:10000/devstoreaccount1",
            "devstoreaccount1",
            "ZHVtbXktYWNjb3VudC1rZXk=",
        );
        assert!(store.is_ok());
    }

    #[test]
    fn azure_store_accepts_unsigned_emulator_options_without_credentials() {
        let store = OpenDalAzureBlobStore::new_with_options(
            "werrss-assets",
            "http://127.0.0.1:10000/devstoreaccount1",
            None,
            None,
            true,
        );
        assert!(store.is_ok());
    }
}

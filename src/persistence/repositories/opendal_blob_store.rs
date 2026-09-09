//! OpenDAL filesystem adapter for immutable asset blobs.

use std::{any::Any, path::PathBuf};

use async_trait::async_trait;
use fs2::FileExt;
use opendal::{services, ErrorKind, Operator};
use serde::{Deserialize, Serialize};

use crate::archive::asset_store::{
    AssetBlob, AssetBlobKey, AssetBlobMetadata, AssetBlobStore, AssetBlobStoreError,
};

const METADATA_SUFFIX: &str = ".metadata.json";
const WRITE_LOCK_FILE: &str = ".werrss.lock";

/// Stores raw asset bytes in a local directory through OpenDAL's filesystem
/// service. Metadata is kept in a sidecar so `stat` does not load the body.
#[derive(Clone)]
pub struct OpenDalFsBlobStore {
    operator: Operator,
    root: PathBuf,
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
        let operator = Operator::new(builder).map_err(backend_error)?.finish();
        Ok(Self { operator, root })
    }

    fn metadata_key(key: &AssetBlobKey) -> String {
        format!("{}{}", key.as_str(), METADATA_SUFFIX)
    }

    async fn acquire_write_lock(&self) -> Result<std::fs::File, AssetBlobStoreError> {
        // Keep one advisory lock per configured root instead of one inode per
        // blob. The lock file is intentionally bounded and harmless to retain.
        let root = self.root.clone();
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
        match self.operator.delete(key.as_str()).await {
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
            .operator
            .write_with(&Self::metadata_key(key), encoded)
            .if_not_exists(true)
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
        let bytes = match self.operator.read(key.as_str()).await {
            Ok(bytes) => bytes.to_vec(),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Err(backend_error(original_error));
            }
            Err(error) => return Err(backend_error(error)),
        };
        if bytes != blob.bytes() {
            return match self.delete_body(key).await {
                Ok(()) => Err(AssetBlobStoreError::Conflict),
                Err(cleanup_error) => Err(AssetBlobStoreError::Backend(format!(
                    "partial body conflicts with requested blob: {original_error}; cleanup failed: {cleanup_error}"
                ))),
            };
        }
        match self.write_metadata(key, blob).await {
            Ok(()) => Ok(()),
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
        let bytes = match self.operator.read(&metadata_key).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(backend_error(error)),
        };
        serde_json::from_slice(&bytes.to_vec())
            .map(Some)
            .map_err(backend_error)
    }
}

#[async_trait]
impl AssetBlobStore for OpenDalFsBlobStore {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read(&self, key: &AssetBlobKey) -> Result<Option<AssetBlob>, AssetBlobStoreError> {
        let Some(metadata) = self.read_metadata(key).await? else {
            return Ok(None);
        };
        let bytes = match self.operator.read(key.as_str()).await {
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
        match self.operator.stat(key.as_str()).await {
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
        let _write_guard = self.acquire_write_lock().await?;
        if let Some(existing) = self.read(key).await? {
            return if existing == *blob {
                Ok(())
            } else {
                Err(AssetBlobStoreError::Conflict)
            };
        }
        match self
            .operator
            .write_with(key.as_str(), blob.bytes().to_vec())
            .if_not_exists(true)
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
            Err(error) => return Err(backend_error(error)),
        }
        match self.write_metadata(key, blob).await {
            Ok(()) => Ok(()),
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

    async fn delete(&self, key: &AssetBlobKey) -> Result<(), AssetBlobStoreError> {
        for object in [key.as_str().to_owned(), Self::metadata_key(key)] {
            match self.operator.delete(&object).await {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => return Err(backend_error(error)),
            }
        }
        Ok(())
    }
}

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
            .operator
            .write(key.as_str(), conflicting)
            .await
            .unwrap();
        assert_eq!(
            store.put(&key, &value).await,
            Err(AssetBlobStoreError::Conflict)
        );
        assert!(store.operator.read(key.as_str()).await.is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
}

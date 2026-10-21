use std::env;

use sha2::{Digest, Sha256};
use sqlx::{postgres::PgPoolOptions, PgPool};
use uuid::Uuid;
use werrss::{
    archive::asset_store::{AssetBlob, AssetBlobKey, AssetBlobStore, AssetBlobStoreError},
    persistence::repositories::opendal_blob_store::{
        OpenDalFtpBlobStore, OpenDalSftpBlobStore, OpenDalWebdavBlobStore,
    },
};

fn required(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} must be set for this ignored test"))
}

fn optional(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn blob(bytes: &[u8], media_type: &str) -> AssetBlob {
    AssetBlob::new(
        bytes.to_vec(),
        media_type.to_owned(),
        format!("{:x}", Sha256::digest(bytes)),
    )
    .unwrap()
}

async fn contract(store: &dyn AssetBlobStore) -> Result<(), String> {
    let key = AssetBlobKey::for_blob_id(Uuid::new_v4());
    let original = blob(b"protocol integration asset", "image/png");
    let conflicting = blob(b"different protocol integration asset", "image/jpeg");
    let result: Result<(), String> = async {
        let missing_read = store.read(&key).await.map_err(|error| error.to_string())?;
        if missing_read.is_some() {
            return Err("new blob unexpectedly existed".to_owned());
        }
        let missing_stat = store.stat(&key).await.map_err(|error| error.to_string())?;
        if missing_stat.is_some() {
            return Err("new blob unexpectedly had metadata".to_owned());
        }
        store
            .put(&key, &original)
            .await
            .map_err(|error| error.to_string())?;
        let stored = store.read(&key).await.map_err(|error| error.to_string())?;
        if stored.as_ref() != Some(&original) {
            return Err("read did not return the written blob".to_owned());
        }
        let metadata = store.stat(&key).await.map_err(|error| error.to_string())?;
        let metadata = metadata.ok_or_else(|| "written blob had no metadata".to_owned())?;
        if metadata.checksum() != original.checksum()
            || metadata.byte_size() != original.byte_size()
            || !metadata.is_present()
        {
            return Err("stat did not match the written blob".to_owned());
        }
        store
            .put(&key, &original)
            .await
            .map_err(|error| error.to_string())?;
        let conflict = store.put(&key, &conflicting).await;
        if conflict != Err(AssetBlobStoreError::Conflict) {
            return Err("conflicting write was not rejected".to_owned());
        }
        let after_conflict = store.read(&key).await.map_err(|error| error.to_string())?;
        if after_conflict.as_ref() != Some(&original) {
            return Err("conflicting write changed the original bytes".to_owned());
        }
        Ok(())
    }
    .await;
    let cleanup = store.delete(&key).await;
    result?;
    cleanup.map_err(|error| error.to_string())?;
    if store
        .read(&key)
        .await
        .map_err(|error| error.to_string())?
        .is_some()
        || store
            .stat(&key)
            .await
            .map_err(|error| error.to_string())?
            .is_some()
    {
        return Err("delete left the blob or metadata behind".to_owned());
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires an operator-configured FTP test server and PostgreSQL database"]
async fn ftp_direct_write_does_not_wait_for_the_application_pool_connection() {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&required("DATABASE_URL"))
        .await
        .unwrap();
    let store = OpenDalFtpBlobStore::new(
        pool.clone(),
        &required("WERRSS_TEST_FTP_ENDPOINT"),
        optional("WERRSS_TEST_FTP_ROOT").as_deref(),
        optional("WERRSS_TEST_FTP_USER").as_deref(),
        optional("WERRSS_TEST_FTP_PASSWORD").as_deref(),
    )
    .unwrap();
    let _held_connection = pool.acquire().await.unwrap();
    let key = AssetBlobKey::for_blob_id(Uuid::new_v4());
    let value = blob(b"independent FTP coordination connection", "image/png");

    let put_result =
        tokio::time::timeout(std::time::Duration::from_secs(35), store.put(&key, &value)).await;
    drop(_held_connection);
    let cleanup_result = store.delete(&key).await;

    put_result
        .expect("coordination pool should be independent of the held application connection")
        .unwrap();
    cleanup_result.unwrap();
}

#[tokio::test]
#[ignore = "requires an operator-configured FTP test server and PostgreSQL database"]
async fn ftp_blob_store_obeys_the_immutable_blob_contract() {
    let pool = PgPool::connect(&required("DATABASE_URL")).await.unwrap();
    let store = OpenDalFtpBlobStore::new(
        pool,
        &required("WERRSS_TEST_FTP_ENDPOINT"),
        optional("WERRSS_TEST_FTP_ROOT").as_deref(),
        optional("WERRSS_TEST_FTP_USER").as_deref(),
        optional("WERRSS_TEST_FTP_PASSWORD").as_deref(),
    )
    .unwrap();
    contract(&store).await.unwrap();
}

#[tokio::test]
#[ignore = "requires an operator-configured SFTP test server and trusted host key"]
async fn sftp_blob_store_obeys_the_immutable_blob_contract() {
    let store = OpenDalSftpBlobStore::new(
        &required("WERRSS_TEST_SFTP_ENDPOINT"),
        optional("WERRSS_TEST_SFTP_ROOT").as_deref(),
        &required("WERRSS_TEST_SFTP_USER"),
        &required("WERRSS_TEST_SFTP_KEY"),
    )
    .unwrap();
    contract(&store).await.unwrap();
}

#[tokio::test]
#[ignore = "requires an operator-configured WebDAV test server and PostgreSQL database"]
async fn webdav_blob_store_obeys_the_immutable_blob_contract() {
    let pool = PgPool::connect(&required("DATABASE_URL")).await.unwrap();
    let store = OpenDalWebdavBlobStore::new(
        pool,
        &required("WERRSS_TEST_WEBDAV_ENDPOINT"),
        optional("WERRSS_TEST_WEBDAV_ROOT").as_deref(),
        optional("WERRSS_TEST_WEBDAV_USER").as_deref(),
        optional("WERRSS_TEST_WEBDAV_PASSWORD").as_deref(),
    )
    .unwrap();
    contract(&store).await.unwrap();
}

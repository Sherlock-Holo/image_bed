use std::fmt::{self, Debug, Formatter};
use std::io;

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use futures_util::{AsyncReadExt, StreamExt};
use futures_util::io::AsyncRead;
use hyper::StatusCode;
use rusoto_core::{ByteStream, HttpClient, Region, RusotoError};
use rusoto_core::credential::StaticProvider;
use rusoto_s3::{
    CreateBucketRequest, Delete, DeleteBucketRequest, DeleteObjectRequest, DeleteObjectsRequest,
    GetObjectRequest, HeadBucketRequest, HeadObjectRequest, ListObjectsRequest, ObjectIdentifier,
    PutObjectRequest, S3, S3Client, S3Error,
};
use thiserror::Error;

use crate::store::StoreBackend;

#[derive(Debug, Error)]
pub enum Error {
    #[error("bucket {0} not found")]
    BucketNotFound(String),

    #[error("resource {0} not found")]
    ResourceNotFound(String),

    #[error("bucket {0} is exist")]
    BucketExist(String),

    #[error("resource {0} is exist")]
    ResourceExist(String),

    #[error("io error {0}")]
    IoError(#[from] io::Error),

    #[error("cos error: {0:?}")]
    CosError(Box<dyn Debug>),

    #[error("bucket {0} is not empty")]
    BucketNotEmpty(String),
}

impl<E: 'static + std::error::Error> From<RusotoError<E>> for Error {
    fn from(err: RusotoError<E>) -> Self {
        Error::CosError(Box::new(err))
    }
}

impl From<S3Error> for Error {
    fn from(err: S3Error) -> Self {
        Error::CosError(Box::new(format!("{:?}", err)))
    }
}

#[derive(Clone)]
pub struct CosBackend {
    client: S3Client,
    app_id: String,
}

impl Debug for CosBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CosBackend").finish()
    }
}

#[async_trait]
impl StoreBackend for CosBackend {
    type Error = Error;

    async fn put<R: AsyncRead + Send>(
        &self,
        bucket: &str,
        resource_id: &str,
        resource: R,
    ) -> Result<(), Self::Error> {
        let real_bucket = self.get_real_bucket_name(bucket);

        if !self.is_bucket_exist(&real_bucket).await? {
            self.client
                .create_bucket(CreateBucketRequest {
                    acl: None,
                    bucket: real_bucket.clone(),
                    create_bucket_configuration: None,
                    grant_full_control: None,
                    grant_read: None,
                    grant_read_acp: None,
                    grant_write: None,
                    grant_write_acp: None,
                    object_lock_enabled_for_bucket: None,
                })
                .await?;
        }

        if self.is_resource_exist(&real_bucket, resource_id).await? {
            return Err(Error::ResourceExist(resource_id.to_owned()));
        }

        let mut buf = Vec::with_capacity(4096);

        futures_util::pin_mut!(resource);

        resource.read_to_end(&mut buf).await?;

        self.client
            .put_object(PutObjectRequest {
                acl: None,
                body: Some(ByteStream::from(buf)),
                bucket: real_bucket,
                cache_control: None,
                content_disposition: None,
                content_encoding: None,
                content_language: None,
                content_length: None,
                content_md5: None,
                content_type: None,
                expires: None,
                grant_full_control: None,
                grant_read: None,
                grant_read_acp: None,
                grant_write_acp: None,
                key: resource_id.to_owned(),
                metadata: None,
                object_lock_legal_hold_status: None,
                object_lock_mode: None,
                object_lock_retain_until_date: None,
                request_payer: None,
                sse_customer_algorithm: None,
                sse_customer_key: None,
                sse_customer_key_md5: None,
                ssekms_encryption_context: None,
                ssekms_key_id: None,
                server_side_encryption: None,
                storage_class: None,
                tagging: None,
                website_redirect_location: None,
            })
            .await?;

        Ok(())
    }

    async fn get<S, E>(
        &self,
        bucket: &str,
        resource_id: &str,
        start: S,
        end: E,
    ) -> Result<Bytes, Self::Error>
        where
            S: Into<Option<u64>> + Send,
            E: Into<Option<u64>> + Send,
    {
        let real_bucket = self.get_real_bucket_name(bucket);

        let start = start.into();
        let end = end.into();

        if !self.is_bucket_exist(&real_bucket).await? {
            return Err(Error::BucketNotFound(bucket.to_owned()));
        }

        if !self.is_resource_exist(&real_bucket, resource_id).await? {
            return Err(Error::ResourceNotFound(resource_id.to_owned()));
        }

        let range = match (start, end) {
            (None, None) => None,
            (Some(start), None) => Some(format!("bytes={}-", start)),
            (Some(start), Some(end)) => Some(format!("bytes={}-{}", start, end)),
            (None, Some(end)) => Some(format!("bytes=-{}", end)),
        };

        let object_output = self
            .client
            .get_object(GetObjectRequest {
                bucket: real_bucket,
                if_match: None,
                if_modified_since: None,
                if_none_match: None,
                if_unmodified_since: None,
                key: resource_id.to_owned(),
                part_number: None,
                range,
                request_payer: None,
                response_cache_control: None,
                response_content_disposition: None,
                response_content_encoding: None,
                response_content_language: None,
                response_content_type: None,
                response_expires: None,
                sse_customer_algorithm: None,
                sse_customer_key: None,
                sse_customer_key_md5: None,
                version_id: None,
            })
            .await?;

        match object_output.body {
            None => Ok(Bytes::new()),
            Some(mut body) => {
                let mut buf = BytesMut::new();

                while let Some(result) = body.next().await {
                    let data = result?;

                    buf.put(data);
                }

                Ok(buf.freeze())
            }
        }
    }

    async fn delete(&self, bucket: &str, resource_id: &str) -> Result<(), Self::Error> {
        let bucket = self.get_real_bucket_name(bucket);

        if !self.is_bucket_exist(&bucket).await? {
            return Ok(());
        }

        if let Err(err) = self
            .client
            .delete_object(DeleteObjectRequest {
                bucket,
                bypass_governance_retention: None,
                key: resource_id.to_owned(),
                mfa: None,
                request_payer: None,
                version_id: None,
            })
            .await
        {
            match &err {
                RusotoError::Service(_) => Ok(()),
                RusotoError::Unknown(raw_resp) => {
                    if raw_resp.status == StatusCode::NOT_FOUND {
                        Ok(())
                    } else {
                        Err(err.into())
                    }
                }

                _ => Err(err.into()),
            }
        } else {
            Ok(())
        }
    }

    async fn delete_bucket(&self, bucket: &str, need_empty: bool) -> Result<(), Self::Error> {
        let real_bucket = self.get_real_bucket_name(bucket);

        if !self.is_bucket_exist(&real_bucket).await? {
            return Ok(());
        }

        loop {
            let list_objects_output = match self
                .client
                .list_objects(ListObjectsRequest {
                    bucket: real_bucket.to_owned(),
                    delimiter: None,
                    encoding_type: None,
                    marker: None,
                    max_keys: None,
                    prefix: None,
                    request_payer: None,
                })
                .await
            {
                Err(err) => {
                    return if is_service_err_or_not_found(&err) {
                        Ok(())
                    } else {
                        Err(err.into())
                    };
                }

                Ok(resp) => resp,
            };

            match list_objects_output.contents {
                None => break,
                Some(contents) => {
                    if contents.is_empty() {
                        break;
                    }

                    if need_empty {
                        return Err(Error::BucketNotEmpty(bucket.to_owned()));
                    }

                    match self
                        .client
                        .delete_objects(DeleteObjectsRequest {
                            bucket: real_bucket.clone(),
                            bypass_governance_retention: None,
                            delete: Delete {
                                objects: contents
                                    .into_iter()
                                    .filter_map(|content| {
                                        content.key.map(|key| ObjectIdentifier {
                                            key,
                                            version_id: None,
                                        })
                                    })
                                    .collect(),
                                quiet: None,
                            },
                            mfa: None,
                            request_payer: None,
                        })
                        .await
                    {
                        Err(err) => {
                            if let RusotoError::Unknown(resp) = &err {
                                if resp.status == StatusCode::NOT_FOUND {
                                    continue;
                                }

                                return Err(err.into());
                            }
                        }

                        Ok(resp) => {
                            if let Some(mut errors) = resp.errors {
                                if !errors.is_empty() {
                                    return Err(errors.remove(0).into());
                                }
                            }
                        }
                    }
                }
            }
        }

        self.delete_bucket(&real_bucket).await
    }
}

impl CosBackend {
    pub fn new(access_key: &str, secret_key: &str, region: &str, app_id: &str) -> Self {
        let http_client = HttpClient::new().expect("create http client failed");

        let region = Region::Custom {
            name: region.to_owned(),
            endpoint: format!("https://cos.{}.myqcloud.com", region),
        };

        let credential =
            StaticProvider::new(access_key.to_owned(), secret_key.to_owned(), None, None);

        Self {
            client: S3Client::new_with(http_client, credential, region),
            app_id: app_id.to_owned(),
        }
    }

    async fn is_bucket_exist(&self, bucket: &str) -> Result<bool, Error> {
        if let Err(err) = self
            .client
            .head_bucket(HeadBucketRequest {
                bucket: bucket.to_owned(),
            })
            .await
        {
            match &err {
                RusotoError::Service(_) => Ok(false),
                RusotoError::Unknown(raw_resp) => {
                    if raw_resp.status == StatusCode::NOT_FOUND {
                        return Ok(false);
                    }

                    Err(err.into())
                }

                _ => Err(err.into()),
            }
        } else {
            Ok(true)
        }
    }

    async fn is_resource_exist(&self, bucket: &str, resource_id: &str) -> Result<bool, Error> {
        if let Err(err) = self
            .client
            .head_object(HeadObjectRequest {
                bucket: bucket.to_owned(),
                if_match: None,
                if_modified_since: None,
                if_none_match: None,
                if_unmodified_since: None,
                key: resource_id.to_owned(),
                part_number: None,
                range: None,
                request_payer: None,
                sse_customer_algorithm: None,
                sse_customer_key: None,
                sse_customer_key_md5: None,
                version_id: None,
            })
            .await
        {
            if is_service_err_or_not_found(&err) {
                Ok(false)
            } else {
                Err(err.into())
            }
        } else {
            Ok(true)
        }
    }

    async fn delete_bucket(&self, bucket: &str) -> Result<(), Error> {
        if let Err(err) = self
            .client
            .delete_bucket(DeleteBucketRequest {
                bucket: bucket.to_owned(),
            })
            .await
        {
            if is_service_err_or_not_found(&err) {
                Ok(())
            } else {
                Err(err.into())
            }
        } else {
            Ok(())
        }
    }

    fn get_real_bucket_name(&self, bucket: &str) -> String {
        format!("{}-{}", bucket, self.app_id)
    }
}

fn is_service_err_or_not_found<E>(err: &RusotoError<E>) -> bool {
    match &err {
        RusotoError::Service(_) => true,
        RusotoError::Unknown(raw_resp) => raw_resp.status == StatusCode::NOT_FOUND,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use super::*;

    #[tokio::test]
    async fn test_get_exist_resource() {
        let access_key = env::var("COS_ACCESS_KEY").expect("need set COS_ACCESS_KEY env");
        let secret_key = env::var("COS_SECRET_KEY").expect("need set COS_SECRET_KEY env");
        let region = env::var("COS_REGION").expect("need set COS_REGION env");
        let app_id = env::var("COS_APP_ID").expect("need set COS_APP_ID env");

        let cos_backend = CosBackend::new(&access_key, &secret_key, &region, &app_id);

        let data = cos_backend
            .get("test-bucket", "test-resource-id", None, None)
            .await
            .unwrap();

        println!("data is {}", String::from_utf8_lossy(&data));
    }

    #[tokio::test]
    async fn test_get_not_exist_resource() {
        let access_key = env::var("COS_ACCESS_KEY").expect("need set COS_ACCESS_KEY env");
        let secret_key = env::var("COS_SECRET_KEY").expect("need set COS_SECRET_KEY env");
        let region = env::var("COS_REGION").expect("need set COS_REGION env");
        let app_id = env::var("COS_APP_ID").expect("need set COS_APP_ID env");

        let cos_backend = CosBackend::new(&access_key, &secret_key, &region, &app_id);

        if let Err(err) = cos_backend
            .get("test-bucket", "not-exist", None, None)
            .await
        {
            if let Error::ResourceNotFound(res_id) = &err {
                if res_id == "not-exist" {
                    return;
                } else {
                    panic!("{:?}", err)
                }
            }
        }

        panic!("resource should not exist")
    }

    #[tokio::test]
    async fn test_get_not_exist_bucket() {
        let access_key = env::var("COS_ACCESS_KEY").expect("need set COS_ACCESS_KEY env");
        let secret_key = env::var("COS_SECRET_KEY").expect("need set COS_SECRET_KEY env");
        let region = env::var("COS_REGION").expect("need set COS_REGION env");
        let app_id = env::var("COS_APP_ID").expect("need set COS_APP_ID env");

        let cos_backend = CosBackend::new(&access_key, &secret_key, &region, &app_id);

        if let Err(err) = cos_backend.get("not-exist", "not-exist", None, None).await {
            if let Error::BucketNotFound(res_id) = &err {
                if res_id == "not-exist" {
                    return;
                } else {
                    panic!("{:?}", err)
                }
            }
        }

        panic!("bucket should not exist")
    }

    #[tokio::test]
    async fn test_put_exist_resource() {
        let access_key = env::var("COS_ACCESS_KEY").expect("need set COS_ACCESS_KEY env");
        let secret_key = env::var("COS_SECRET_KEY").expect("need set COS_SECRET_KEY env");
        let region = env::var("COS_REGION").expect("need set COS_REGION env");
        let app_id = env::var("COS_APP_ID").expect("need set COS_APP_ID env");

        let cos_backend = CosBackend::new(&access_key, &secret_key, &region, &app_id);

        if let Err(err) = cos_backend
            .put("test-bucket", "test-resource-id", &[] as &[u8])
            .await
        {
            if let Error::ResourceExist(res_id) = &err {
                if res_id == "test-resource-id" {
                    return;
                } else {
                    panic!("{:?}", err)
                }
            }
        }

        panic!("resource should exist")
    }

    #[tokio::test]
    async fn test_put_resource() {
        let access_key = env::var("COS_ACCESS_KEY").expect("need set COS_ACCESS_KEY env");
        let secret_key = env::var("COS_SECRET_KEY").expect("need set COS_SECRET_KEY env");
        let region = env::var("COS_REGION").expect("need set COS_REGION env");
        let app_id = env::var("COS_APP_ID").expect("need set COS_APP_ID env");

        let cos_backend = CosBackend::new(&access_key, &secret_key, &region, &app_id);

        let random = rand::random::<u64>();

        cos_backend
            .put(
                "test-bucket",
                &format!("test-resource-id-{}", random),
                &[0u8, 1, 2, 3] as &[u8],
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_delete_resource() {
        let access_key = env::var("COS_ACCESS_KEY").expect("need set COS_ACCESS_KEY env");
        let secret_key = env::var("COS_SECRET_KEY").expect("need set COS_SECRET_KEY env");
        let region = env::var("COS_REGION").expect("need set COS_REGION env");
        let app_id = env::var("COS_APP_ID").expect("need set COS_APP_ID env");

        let cos_backend = CosBackend::new(&access_key, &secret_key, &region, &app_id);

        let random = rand::random::<u64>();

        let res_id = format!("test-resource-id-{}", random);

        cos_backend
            .put("test-bucket", &res_id, &[0u8, 1, 2, 3] as &[u8])
            .await
            .unwrap();

        cos_backend.delete("test-bucket", &res_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_delete_not_exist_resource() {
        let access_key = env::var("COS_ACCESS_KEY").expect("need set COS_ACCESS_KEY env");
        let secret_key = env::var("COS_SECRET_KEY").expect("need set COS_SECRET_KEY env");
        let region = env::var("COS_REGION").expect("need set COS_REGION env");
        let app_id = env::var("COS_APP_ID").expect("need set COS_APP_ID env");

        let cos_backend = CosBackend::new(&access_key, &secret_key, &region, &app_id);

        cos_backend
            .delete("test-bucket", "not-exist")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_delete_not_empty_bucket() {
        let access_key = env::var("COS_ACCESS_KEY").expect("need set COS_ACCESS_KEY env");
        let secret_key = env::var("COS_SECRET_KEY").expect("need set COS_SECRET_KEY env");
        let region = env::var("COS_REGION").expect("need set COS_REGION env");
        let app_id = env::var("COS_APP_ID").expect("need set COS_APP_ID env");

        let cos_backend = CosBackend::new(&access_key, &secret_key, &region, &app_id);

        let random = rand::random::<u64>();

        let bucket = format!("test-bucket-{}", random);

        cos_backend
            .put(&bucket, "test-resource", &[0u8, 1, 2, 3] as &[u8])
            .await
            .unwrap();

        StoreBackend::delete_bucket(&cos_backend, &bucket, false)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_delete_empty_bucket() {
        let access_key = env::var("COS_ACCESS_KEY").expect("need set COS_ACCESS_KEY env");
        let secret_key = env::var("COS_SECRET_KEY").expect("need set COS_SECRET_KEY env");
        let region = env::var("COS_REGION").expect("need set COS_REGION env");
        let app_id = env::var("COS_APP_ID").expect("need set COS_APP_ID env");

        let cos_backend = CosBackend::new(&access_key, &secret_key, &region, &app_id);

        let random = rand::random::<u64>();

        let bucket = format!("test-bucket-{}", random);

        cos_backend
            .put(&bucket, "test-resource", &[0u8, 1, 2, 3] as &[u8])
            .await
            .unwrap();
        cos_backend.delete(&bucket, "test-resource").await.unwrap();

        StoreBackend::delete_bucket(&cos_backend, &bucket, true)
            .await
            .unwrap();
    }
}

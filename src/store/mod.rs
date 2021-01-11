use std::fmt::{Debug, Display};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::io::AsyncRead;

pub mod cos;

#[async_trait]
pub trait StoreBackend {
    type Error: Debug + Display;

    async fn put<R: AsyncRead + Send>(
        &self,
        bucket: &str,
        resource_id: &str,
        resource: R,
    ) -> Result<(), Self::Error>;

    async fn get<S, E>(
        &self,
        bucket: &str,
        resource_id: &str,
        start: S,
        end: E,
    ) -> Result<Bytes, Self::Error>
    where
        S: Into<Option<u64>> + Send,
        E: Into<Option<u64>> + Send;

    async fn delete(&self, bucket: &str, resource_id: &str) -> Result<(), Self::Error>;

    async fn delete_bucket(&self, bucket: &str, need_empty: bool) -> Result<(), Self::Error>;
}

#[async_trait]
impl<T: StoreBackend + Send + Sync> StoreBackend for &T {
    type Error = T::Error;

    async fn put<R: AsyncRead + Send>(
        &self,
        bucket: &str,
        resource_id: &str,
        resource: R,
    ) -> Result<(), Self::Error> {
        (*self).put(bucket, resource_id, resource).await
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
        (*self).get(bucket, resource_id, start, end).await
    }

    async fn delete(&self, bucket: &str, resource_id: &str) -> Result<(), Self::Error> {
        (*self).delete(bucket, resource_id).await
    }

    async fn delete_bucket(&self, bucket: &str, need_empty: bool) -> Result<(), Self::Error> {
        (*self).delete_bucket(bucket, need_empty).await
    }
}

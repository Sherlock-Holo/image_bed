use std::fmt::Debug;
use std::time::Duration;
use std::time::SystemTime;

use anyhow::Result;
use slog::error;
use sqlx::{Error, PgPool};

use crate::log::{self, LogContext};

#[derive(Debug, sqlx::FromRow, Clone)]
pub struct Resource {
    id: String,
    bucket: String,
    create_time: i64,
    resource_size: i64,
}

impl Resource {
    pub fn get_id(&self) -> &str {
        &self.id
    }

    pub fn get_bucket(&self) -> &str {
        &self.bucket
    }

    pub fn get_create_time(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(self.create_time as _)
    }

    pub fn get_resource_size(&self) -> u64 {
        self.resource_size as _
    }
}

#[derive(Debug, Clone)]
pub struct Database {
    db_pool: PgPool,
}

impl Database {
    pub async fn new(db_pool: &PgPool) -> Result<Self> {
        sqlx::query("select from resources limit 1")
            .execute(db_pool)
            .await?;

        Ok(Self {
            db_pool: db_pool.clone(),
        })
    }

    pub async fn insert_resource(
        &self,
        bucket: &str,
        resource_id: &str,
        resource_hash: &str,
        resource_size: u64,
        _log_cx: &LogContext,
    ) -> Result<Resource> {
        let now = SystemTime::now();
        let unix_timestamp = now.duration_since(SystemTime::UNIX_EPOCH)?.as_secs();

        sqlx::query(
            "insert into resources (id, bucket, create_time, hash, resource_size) values ($1, $2, $3, $4, $5)",
        )
            .bind(resource_id)
            .bind(bucket)
            .bind(unix_timestamp as i64)
            .bind(resource_hash)
            .bind(resource_size as i64)
            .execute(&self.db_pool)
            .await?;

        Ok(Resource {
            id: resource_id.to_owned(),
            bucket: bucket.to_owned(),
            create_time: unix_timestamp as _,
            resource_size: resource_size as _,
        })
    }

    pub async fn get_resource_by_hash(
        &self,
        resource_hash: &str,
        log_cx: &LogContext,
    ) -> Result<Option<Resource>> {
        match sqlx::query_as::<_, Resource>("select * from resources where hash=$1 limit 1")
            .bind(resource_hash)
            .fetch_one(&self.db_pool)
            .await
        {
            Err(err) => {
                if let Error::RowNotFound = &err {
                    Ok(None)
                } else {
                    error!(log::get_logger(), "get resource by hash {} failed: {:?}", resource_hash, err; log_cx);

                    Err(err.into())
                }
            }

            Ok(resource) => Ok(Some(resource)),
        }
    }

    pub async fn get_resource_by_id(
        &self,
        resource_id: &str,
        log_cx: &LogContext,
    ) -> Result<Option<Resource>> {
        match sqlx::query_as::<_, Resource>("select * from resources where id=$1")
            .bind(resource_id)
            .fetch_one(&self.db_pool)
            .await
        {
            Err(err) => {
                if let Error::RowNotFound = err {
                    Ok(None)
                } else {
                    error!(log::get_logger(), "get resource by id {} failed: {:?}", resource_id, err; log_cx);

                    Err(err.into())
                }
            }

            Ok(res) => Ok(Some(res)),
        }
    }

    pub async fn update_resource_create_time(
        &self,
        resource_id: &str,
        log_cx: &LogContext,
    ) -> Result<Option<()>> {
        if let Err(err) = sqlx::query("update set resources (create_time) values ($1) where id=$2")
            .bind(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?)
            .bind(resource_id)
            .execute(&self.db_pool)
            .await
        {
            if let Error::RowNotFound = err {
                Ok(None)
            } else {
                error!(
                    log::get_logger(),
                    "update resource {} create time failed: {:?}",
                    resource_id, err;
                    log_cx
                );

                Err(err.into())
            }
        } else {
            Ok(Some(()))
        }
    }

    pub async fn delete_out_of_date_resources(
        &self,
        delete_before: &SystemTime,
        log_cx: &LogContext,
    ) -> Result<Vec<Resource>> {
        let unix_timestamp = delete_before
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_secs();

        let mut offset = 0;

        let mut delete_resources = vec![];

        loop {
            match sqlx::query_as::<_, Resource>(
                "select * from resources where create_time<=$1 offset $2 limit 1000",
            )
                .bind(unix_timestamp as i64)
                .bind(offset)
                .fetch_all(&self.db_pool)
                .await
            {
                Err(err) => {
                    if let Error::RowNotFound = &err {
                        break;
                    } else {
                        error!(
                            log::get_logger(),
                            "query resources before {:?} failed: {:?}",
                            delete_before, err;
                            log_cx
                        );

                        return Err(err.into());
                    }
                }

                Ok(resources) => {
                    if resources.is_empty() {
                        break;
                    }

                    offset += resources.len() as i32;

                    delete_resources.extend(resources);
                }
            }
        }

        if let Err(err) = sqlx::query("delete from resources where id in $1")
            .bind(
                delete_resources
                    .iter()
                    .map(|res| res.id.as_str())
                    .collect::<Vec<_>>(),
            )
            .execute(&self.db_pool)
            .await
        {
            if let Error::RowNotFound = err {
                Ok(delete_resources)
            } else {
                error!(
                    log::get_logger(),
                    "delete resource info {:?} before {:?} failed: {:?}",
                    delete_resources, delete_before, err;
                    log_cx
                );

                Err(err.into())
            }
        } else {
            Ok(delete_resources)
        }
    }
}

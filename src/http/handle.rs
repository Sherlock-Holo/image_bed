use std::convert::Infallible;
use std::error::Error;
use std::future;
use std::future::Ready;
use std::sync::Arc;
use std::task::{Context, Poll};

use chrono::Local;
use hyper::{body, Method};
use hyper::{Body, Request, Response, StatusCode, Uri};
use hyper::service::Service;
use log::{info, warn};
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;

use crate::db::Database;
use crate::http::ServiceResult;
use crate::http::size_limit::SizeLimitService;
use crate::id::generate::Generator;
use crate::store::StoreBackend;

type BoxError = Box<dyn Error + Send + Sync>;

const UPLOAD_PATH: &str = "/upload";
const GET_PATH: &str = "/get";

#[derive(Debug)]
pub struct Handler<S: StoreBackend> {
    store_backend: Arc<S>,
    id_generator: Generator,
    db: Database,
    domain: Arc<String>,
    max_body_size: u64,
}

impl<S: StoreBackend> Handler<S> {
    pub async fn new(
        domain: &str,
        db_uri: &str,
        store_backend: S,
        max_body_size: impl Into<u64>,
    ) -> anyhow::Result<Self> {
        const ID_TYPE: &str = "image_bed";

        let db_pool = PgPoolOptions::new()
            .max_connections(20)
            .connect(db_uri)
            .await?;

        info!("db pool is connected");

        let id_generator = Generator::new(&db_pool, ID_TYPE).await?;

        info!("id generator is init");

        let db = Database::new(&db_pool).await?;

        info!("db is init");

        Ok(Self {
            store_backend: Arc::new(store_backend),
            id_generator,
            db,
            domain: Arc::new(domain.to_owned()),
            max_body_size: max_body_size.into(),
        })
    }
}

impl<T, S> Service<T> for Handler<S>
    where
        S: StoreBackend + Send + Sync,
{
    type Response = SizeLimitService<Handle<S>>;
    type Error = Infallible;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: T) -> Self::Future {
        let max_body_size = self.max_body_size;
        let handle = Handle::from(self);

        future::ready(Ok(SizeLimitService::new(max_body_size, handle)))
    }
}

#[derive(Debug)]
pub struct Handle<S: StoreBackend> {
    store_backend: Arc<S>,
    id_generator: Generator,
    db: Database,
    domain: Arc<String>,
}

impl<S: StoreBackend> Clone for Handle<S> {
    fn clone(&self) -> Self {
        Self {
            store_backend: self.store_backend.clone(),
            id_generator: self.id_generator.clone(),
            db: self.db.clone(),
            domain: self.domain.clone(),
        }
    }
}

impl<'a, S: StoreBackend> From<&'a mut Handler<S>> for Handle<S> {
    fn from(h: &'a mut Handler<S>) -> Self {
        Handle {
            store_backend: h.store_backend.clone(),
            id_generator: h.id_generator.clone(),
            db: h.db.clone(),
            domain: h.domain.clone(),
        }
    }
}

impl<S> Service<Request<Body>> for Handle<S>
    where
        S: StoreBackend + 'static + Send + Sync,
        S::Error: Send + Sync,
{
    type Response = Response<Body>;
    type Error = BoxError;
    type Future = ServiceResult<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let path = req.uri().path();

        if path.starts_with(UPLOAD_PATH) {
            let handle = self.clone();

            Box::pin(async move { handle.handle_upload(req).await })
        } else if path.starts_with(GET_PATH) {
            let handle = self.clone();

            Box::pin(async move { handle.handle_get(req).await })
        } else {
            warn!("illegal request {:?}", req);

            let mut resp = Response::new(Body::empty());
            *resp.status_mut() = StatusCode::BAD_REQUEST;

            Box::pin(async move { Ok(resp) })
        }
    }
}

impl<S> Handle<S>
    where
        S: StoreBackend + Send + Sync + 'static,
        S::Error: Send + Sync,
{
    async fn handle_upload(&self, req: Request<Body>) -> Result<Response<Body>, BoxError> {
        if req.method() != Method::POST {
            warn!("reject illegal method {} for upload", req.method());

            return Ok(self.return_bad_request(req).await?);
        }

        let data = body::to_bytes(req.into_body()).await?;

        let mut hasher = Sha256::new();
        hasher.update(&data);

        let hash_result = hex::encode(hasher.finalize());

        let resource = if let Some(resource) = self.db.get_resource_by_hash(&hash_result).await? {
            resource
        } else {
            let resource_id = self.id_generator.get_id().await?;

            let bucket = Local::today().format("%Y-%m").to_string();

            let resource = self
                .db
                .insert_resource(&bucket, &resource_id, &hash_result)
                .await?;

            self.store_backend
                .put(&bucket, &resource_id, data.as_ref())
                .await?;

            resource
        };

        let resource_uri = Uri::builder()
            .scheme("https")
            .authority(self.domain.as_str())
            .path_and_query(format!("{}/{}", GET_PATH, resource.get_id()))
            .build()?
            .to_string();

        let mut resp = Response::new(Body::from(resource_uri));
        let headers = resp.headers_mut();
        headers.append("content-type", "text/plain".parse()?);
        headers.append("content-type", "charset=utf-8".parse()?);

        Ok(resp)
    }

    async fn handle_get(&self, req: Request<Body>) -> Result<Response<Body>, BoxError> {
        if req.method() != Method::GET {
            warn!("reject illegal method {} for get", req.method());

            return Ok(self.return_bad_request(req).await?);
        }

        let path = req.uri().path().replace(GET_PATH, "");
        let resource_id = path.strip_prefix('/').unwrap_or(&path);

        let resource = match self.db.get_resource_by_id(resource_id).await? {
            None => {
                return Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())?);
            }

            Some(resource) => resource,
        };

        let (start, end) = match req.headers().get("range") {
            None => (None, None),
            Some(range) => range.to_str().map_or((None, None), |range| {
                if !range.starts_with("bytes=") {
                    (None, None)
                } else {
                    let bytes = range.replace("bytes=", "");
                    let start_end = bytes.split('-').collect::<Vec<_>>();

                    if start_end.len() != 2 {
                        (None, None)
                    } else {
                        let start = start_end[0].parse::<u64>().ok();
                        let end = start_end[1].parse::<u64>().ok();

                        (start, end)
                    }
                }
            }),
        };

        let status_code = if start.is_some() || end.is_some() {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        };

        let data = self
            .store_backend
            .get(resource.get_bucket(), resource.get_id(), start, end)
            .await?;

        let mut resp_builder = Response::builder();
        resp_builder = resp_builder.header("content-type", "text/plain");
        resp_builder = resp_builder.header("content-type", "charset=utf-8");
        resp_builder = resp_builder.status(status_code);

        if status_code == StatusCode::PARTIAL_CONTENT {
            let start = start.unwrap_or(0);
            // content-range is [start, end], not [start, end)
            let end = end.unwrap_or_else(|| (data.len() as u64) - start - 1);

            resp_builder =
                resp_builder.header("content-range", format!("bytes: {}-{}/*", start, end));
        }

        Ok(resp_builder.body(Body::from(data))?)
    }

    async fn return_bad_request(&self, _req: Request<Body>) -> anyhow::Result<Response<Body>> {
        Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::empty())
            .map_err(|err| err.into())
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::str::FromStr;

    use sqlx::postgres::PgPoolOptions;

    use crate::store::cos::CosBackend;

    use super::*;

    #[tokio::test]
    async fn put_resource() {
        let access_key = env::var("COS_ACCESS_KEY").expect("need set COS_ACCESS_KEY env");
        let secret_key = env::var("COS_SECRET_KEY").expect("need set COS_SECRET_KEY env");
        let region = env::var("COS_REGION").expect("need set COS_REGION env");
        let app_id = env::var("COS_APP_ID").expect("need set COS_APP_ID env");
        let pg_uri = env::var("PG_URI").expect("must set environment PG_URI");
        let id_type = env::var("ID_TYPE").expect("must set environment ID_TYPE");

        let pg_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&pg_uri)
            .await
            .unwrap();

        let id_generator = Generator::new(&pg_pool, &id_type).await.unwrap();
        let store_backend = CosBackend::new(&access_key, &secret_key, &region, &app_id);
        let db = Database::new(&pg_pool).await.unwrap();

        let mut handler = Handler {
            store_backend: Arc::new(store_backend),
            id_generator,
            db,
            domain: Arc::new("test.com".to_string()),
            max_body_size: 10 * 1024 * 1024,
        };

        let data = b"test";

        let mut req = Request::new(Body::from(&data[..]));
        *req.method_mut() = Method::POST;
        *req.uri_mut() = Uri::from_static("https://test.com/upload");

        let mut handle = handler.call(()).await.unwrap();

        let mut resp = handle.call(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);

        eprintln!(
            "result {}",
            String::from_utf8_lossy(&body::to_bytes(resp.body_mut()).await.unwrap())
        );
    }

    #[tokio::test]
    async fn get_resource() {
        let access_key = env::var("COS_ACCESS_KEY").expect("need set COS_ACCESS_KEY env");
        let secret_key = env::var("COS_SECRET_KEY").expect("need set COS_SECRET_KEY env");
        let region = env::var("COS_REGION").expect("need set COS_REGION env");
        let app_id = env::var("COS_APP_ID").expect("need set COS_APP_ID env");
        let pg_uri = env::var("PG_URI").expect("must set environment PG_URI");
        let id_type = env::var("ID_TYPE").expect("must set environment ID_TYPE");

        let pg_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&pg_uri)
            .await
            .unwrap();

        let id_generator = Generator::new(&pg_pool, &id_type).await.unwrap();
        let store_backend = CosBackend::new(&access_key, &secret_key, &region, &app_id);
        let db = Database::new(&pg_pool).await.unwrap();

        let mut handler = Handler {
            store_backend: Arc::new(store_backend),
            id_generator,
            db,
            domain: Arc::new("test.com".to_string()),
            max_body_size: 10 * 1024 * 1024,
        };

        let data = b"test";

        let mut post_req = Request::new(Body::from(&data[..]));
        *post_req.method_mut() = Method::POST;
        *post_req.uri_mut() = Uri::from_static("https://test.com/upload");

        let mut handle = handler.call(()).await.unwrap();

        let mut post_resp = handle.call(post_req).await.unwrap();

        assert_eq!(post_resp.status(), StatusCode::OK);

        let resp_data = body::to_bytes(post_resp.body_mut()).await.unwrap();
        let get_uri = String::from_utf8_lossy(&resp_data);
        eprintln!("get uri {}", get_uri);

        let get_req = Request::builder()
            .uri(Uri::from_str(&get_uri).unwrap())
            .body(Body::empty())
            .unwrap();

        let get_resp = handle.call(get_req).await.unwrap();

        eprintln!("get resp {:?}", get_resp);

        assert_eq!(body::to_bytes(get_resp).await.unwrap().as_ref(), b"test");
    }
}
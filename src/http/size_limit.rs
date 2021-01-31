use std::error::Error;
use std::task::Context;
use std::task::Poll;

use bytes::{BufMut, BytesMut};
use futures_util::StreamExt;
use hyper::{Body, Request, Response, StatusCode};
use hyper::service::Service;
use slog::warn;

use crate::http::ServiceResult;
use crate::log::{self, LogContext};

#[derive(Debug)]
pub struct SizeLimitService<S> {
    max_size: u64,
    service: S,
}

impl<S> SizeLimitService<S> {
    pub fn new(max_size: u64, service: S) -> Self {
        Self { max_size, service }
    }
}

impl<S> Service<Request<Body>> for SizeLimitService<S>
    where
        S: Service<Request<Body>, Response=Response<Body>> + Send + Clone + 'static,
        S::Future: Send,
        S::Error: Into<Box<dyn Error + Send + Sync>>,
{
    type Response = Response<Body>;
    type Error = Box<dyn Error + Send + Sync>;
    type Future = ServiceResult<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut req: Request<Body>) -> Self::Future {
        let mut inner_service = self.service.clone();
        let max_size = self.max_size;

        let log_cx = LogContext::builder()
            .request_id(get_request_id(&req))
            .build();

        Box::pin(async move {
            let mut buf = BytesMut::with_capacity(max_size as _);

            while let Some(result) = req.body_mut().next().await {
                let data = result?;

                buf.put(data);

                if buf.len() > max_size as _ {
                    let err_resp = Response::builder()
                        .status(StatusCode::PAYLOAD_TOO_LARGE)
                        .body(Body::empty())?;

                    warn!(log::get_logger(), "request body is too large"; log_cx);

                    return Ok(err_resp);
                }
            }

            *req.body_mut() = Body::from(buf.freeze());

            inner_service.call(req).await.map_err(|err| err.into())
        })
    }
}

impl<S: Clone> Clone for SizeLimitService<S> {
    fn clone(&self) -> Self {
        SizeLimitService {
            max_size: self.max_size,
            service: self.service.clone(),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        self.max_size = source.max_size;
        self.service = source.service.clone()
    }
}

fn get_request_id(req: &Request<Body>) -> &str {
    req.headers()
        .get("X-image-bed-request-id")
        .map(|value| value.to_str().unwrap_or(""))
        .unwrap_or("")
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::future;
    use std::future::Ready;

    use super::*;

    #[derive(Clone)]
    struct MockService;

    impl Service<Request<Body>> for MockService {
        type Response = Response<Body>;
        type Error = Infallible;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<Body>) -> Self::Future {
            future::ready(Ok(Response::new(Body::empty())))
        }
    }

    #[tokio::test]
    async fn test_normal() {
        let mut service = SizeLimitService::new(100, MockService);

        let resp = service
            .call(Request::new(Body::from(&b"test"[..])))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_out_size() {
        let mut service = SizeLimitService::new(1, MockService);

        let resp = service
            .call(Request::new(Body::from(&b"test"[..])))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }
}

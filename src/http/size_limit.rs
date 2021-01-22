use std::error::Error;
use std::task::Context;
use std::task::Poll;

use bytes::{BufMut, BytesMut};
use futures_util::StreamExt;
use hyper::{Body, Request, Response, StatusCode};
use hyper::service::Service;

use crate::http::ServiceResult;

#[derive(Debug)]
pub struct SizeLimitService<S> {
    max_size: u64,
    service: Option<S>,
}

impl<S> SizeLimitService<S> {
    pub fn new(max_size: u64, service: S) -> Self {
        Self {
            max_size,
            service: Some(service),
        }
    }
}

impl<S> Service<Request<Body>> for SizeLimitService<S>
    where
        S: Service<Request<Body>, Response=Response<Body>> + Send + 'static,
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
        let mut inner_service = self.service.take().unwrap();
        let max_size = self.max_size;

        Box::pin(async move {
            let mut buf = BytesMut::with_capacity(max_size as _);

            while let Some(result) = req.body_mut().next().await {
                let data = result?;

                buf.put(data);

                if buf.len() > max_size as _ {
                    let err_resp = Response::builder()
                        .status(StatusCode::PAYLOAD_TOO_LARGE)
                        .body(Body::empty())?;

                    return Ok(err_resp);
                }
            }

            *req.body_mut() = Body::from(buf.freeze());

            inner_service.call(req).await.map_err(|err| err.into())
        })
    }
}

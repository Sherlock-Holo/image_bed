use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use hyper::{Body, Request, Response};
use hyper::http::HeaderValue;
use hyper::service::Service;
use rand::{RngCore, SeedableRng};
use rand::rngs::StdRng;

#[derive(Debug)]
pub struct RequestIdFuture<F: Future> {
    request_id: String,
    fut: F,
}

impl<F, E> Future for RequestIdFuture<F>
    where
        F: Future<Output=Result<Response<Body>, E>> + Unpin,
{
    type Output = Result<Response<Body>, E>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut resp = futures_util::ready!(Pin::new(&mut self.fut).poll(cx))?;

        resp.headers_mut().insert(
            "X-image-bed-request-id",
            HeaderValue::from_str(&self.request_id)
                .unwrap_or_else(|_| panic!("request id {} is invalid head value", self.request_id)),
        );

        Poll::Ready(Ok(resp))
    }
}

#[derive(Debug)]
pub struct RequestIdService<S> {
    service: S,
}

impl<S> RequestIdService<S> {
    pub fn new(service: S) -> Self {
        Self { service }
    }
}

impl<S> From<S> for RequestIdService<S> {
    fn from(service: S) -> Self {
        Self { service }
    }
}

impl<S> Service<Request<Body>> for RequestIdService<S>
    where
        S: Service<Request<Body>, Response=Response<Body>>,
        S::Future: Unpin,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = RequestIdFuture<S::Future>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut req: Request<Body>) -> Self::Future {
        let mut request_id_buf = vec![0; 8];

        StdRng::from_entropy().fill_bytes(&mut request_id_buf);

        let request_id = "s".to_owned() + &hex::encode(request_id_buf);

        req.headers_mut().insert(
            "X-image-bed-request-id",
            HeaderValue::from_str(&request_id)
                .unwrap_or_else(|_| panic!("request id {} is invalid head value", request_id)),
        );

        let fut = self.service.call(req);

        RequestIdFuture { request_id, fut }
    }
}

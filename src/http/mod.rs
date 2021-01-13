use std::future::Future;
use std::pin::Pin;

pub mod handle;
mod size_limit;

type ServiceResult<T, E> = Pin<Box<dyn Future<Output=Result<T, E>> + 'static + Send>>;

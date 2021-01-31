use std::io;
use std::sync::Mutex;

use once_cell::sync::OnceCell;
use slog::{Drain, FnValue, KV, Logger, Record, Result, Serializer};

#[derive(Debug, Clone)]
pub struct LogContext {
    request_id: String,
}

impl LogContext {
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub fn builder() -> LogContextBuilder {
        LogContextBuilder::new()
    }

    fn new(request_id: String) -> Self {
        Self { request_id }
    }
}

impl KV for LogContext {
    fn serialize(&self, _record: &Record, serializer: &mut dyn Serializer) -> Result {
        serializer.emit_str("requestId", &self.request_id)
    }
}

#[derive(Debug, Default, Clone)]
pub struct LogContextBuilder {
    request_id: Option<String>,
}

impl LogContextBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn request_id(mut self, request_id: &str) -> Self {
        self.request_id.replace(request_id.to_owned());

        self
    }

    pub fn build(mut self) -> LogContext {
        LogContext::new(self.request_id.take().unwrap_or_else(|| "".to_owned()))
    }
}

pub fn get_logger() -> &'static Logger {
    static LOGGER: OnceCell<Logger> = OnceCell::new();

    LOGGER.get_or_init(|| {
        let json = slog_json::Json::new(io::stderr())
            .add_default_keys()
            .add_key_value(slog::o!(
                "file"=> FnValue(move |rinfo: &Record| format!("{}:{}", rinfo.file(), rinfo.line()))
            ))
            .build();

        Logger::root(Mutex::new(json).map(slog::Fuse), slog::o!())
    })
}

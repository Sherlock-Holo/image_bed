use std::fs::File;
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::str::FromStr;

use hyper::Server;
use log::LevelFilter;
use simple_logger::SimpleLogger;

use crate::argument::Argument;
use crate::config::Config;
use crate::http::handle::HandlerBuilder;
use crate::store::cos::CosBackend;

mod argument;
mod config;
mod db;
mod http;
mod id;
mod store;

fn init_log() {
    SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .init()
        .unwrap()
}

pub async fn run() -> anyhow::Result<()> {
    init_log();

    let argument = Argument::new();

    let config: Config = if argument.config == Path::new("-") {
        let stdin = std::io::stdin();

        serde_yaml::from_reader(stdin.lock())?
    } else {
        let file = File::open(argument.config)?;
        serde_yaml::from_reader(file)?
    };

    let mut handler_builder = HandlerBuilder::new();

    handler_builder
        .set_database_name(&config.database_name)
        .set_domain(&config.domain)
        .set_host(&config.host)
        .set_user(&config.user)
        .set_password(&config.password);

    config.port.map(|port| handler_builder.set_port(port));
    config
        .max_body_size
        .map(|size| handler_builder.set_max_body_size(size));

    let backend = CosBackend::new(
        &config.access_key,
        &config.secret_key,
        &config.region,
        &config.app_id,
    );

    handler_builder.set_store_backend(backend);

    let handler = handler_builder.build().await?;

    let ip_addr = IpAddr::from_str(&config.listen_addr)?;

    Ok(
        Server::bind(&SocketAddr::from((ip_addr, config.listen_port)))
            .tcp_nodelay(true)
            .serve(handler)
            .await?,
    )
}

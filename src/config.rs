use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub domain: String,
    pub database_name: String,
    pub host: String,
    pub user: String,
    pub password: String,
    pub port: Option<u16>,
    pub max_body_size: Option<u64>,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    pub app_id: String,
    pub listen_addr: String,
    pub listen_port: u16,
}

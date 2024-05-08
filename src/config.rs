use std::env::var;
use std::path::PathBuf;

use once_cell::sync::Lazy;
use tracing::debug;

pub(crate) static CONFIG: Lazy<Config> = Lazy::new(Config::from_env);

static CACHE_TTL_DEFAULT: u64 = 3600;
static CACHE_DB_DEFAULT: &str = "events-db";

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub cache_ttl: u64,
    pub cache_db: PathBuf,
}

impl Config {
    fn from_env() -> Self {
        // This will panic if format conversion fails, but that's ok as this is called early during startup.
        let config = Self {
            cache_ttl: var("CACHE_TTL")
                .map(|v| v.parse::<u64>().unwrap())
                .unwrap_or(CACHE_TTL_DEFAULT),
            cache_db: var("CACHE_DB").unwrap_or(CACHE_DB_DEFAULT.into()).into(),
        };
        debug!("Config: {:?}", config);
        config
    }
}

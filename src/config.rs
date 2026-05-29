use reqwest::Client;
use std::env;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct HttpClient {
    pub client: Arc<Client>,
}

impl HttpClient {
    pub fn new() -> Self {
        let client = Client::builder()
            .pool_idle_timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(10)
            .http2_adaptive_window(true)
            .http2_keep_alive_interval(Duration::from_secs(30))
            .tcp_nodelay(true)
            .tcp_keepalive(Duration::from_secs(60))
            .gzip(true)
            .brotli(true)
            .build()
            .expect("Failed to build HTTP client");

        Self {
            client: Arc::new(client),
        }
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
pub struct Config {
    pub database_url: String,
    pub port: u16,
    pub log_level: String,
    pub hetzner_extract_url: Option<String>,
    pub hetzner_extract_secret: Option<String>,
    pub pipeline_api_secret: Option<String>,
    pub bff_cron_secret: Option<String>,
    pub http_client: HttpClient,
}

impl Config {
    pub fn load_config() -> anyhow::Result<Self> {
        dotenvy::dotenv().ok();

        let database_url = env::var("DATABASE_URL")
            .map_err(|_| anyhow::anyhow!("DATABASE_URL environment variable is required"))?;

        let port = env::var("PORT")
            .unwrap_or_else(|_| "3000".to_string())
            .parse::<u16>()
            .unwrap_or(3000);

        let log_level = env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string());

        let hetzner_extract_url = env::var("HETZNER_EXTRACT_URL").ok();
        let hetzner_extract_secret = env::var("HETZNER_EXTRACT_SECRET").ok();
        let pipeline_api_secret = env::var("PIPELINE_API_SECRET").ok();
        let bff_cron_secret = env::var("BFF_CRON_SECRET").ok();

        Ok(Self {
            database_url,
            port,
            log_level,
            hetzner_extract_url,
            hetzner_extract_secret,
            pipeline_api_secret,
            bff_cron_secret,
            http_client: HttpClient::new(),
        })
    }

    pub fn for_tests() -> Self {
        Self {
            database_url: String::new(),
            port: 3000,
            log_level: "debug".to_string(),
            hetzner_extract_url: None,
            hetzner_extract_secret: None,
            pipeline_api_secret: None,
            bff_cron_secret: None,
            http_client: HttpClient::default(),
        }
    }
}

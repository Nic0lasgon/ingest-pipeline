use std::env;

#[allow(dead_code)]
pub struct Config {
    pub database_url: String,
    pub port: u16,
    pub log_level: String,
    pub hetzner_extract_url: Option<String>,
    pub hetzner_extract_secret: Option<String>,
    pub pipeline_api_secret: Option<String>,
    pub bff_cron_secret: Option<String>,
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
        })
    }
}

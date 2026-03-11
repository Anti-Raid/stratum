use serde::{Deserialize, Serialize};
use std::fs::File;
use std::sync::LazyLock;

type Error = Box<dyn std::error::Error + Send + Sync>;

/// Global config object
pub static CONFIG: LazyLock<Config> =
    LazyLock::new(|| Config::load().expect("Failed to load config"));


#[derive(Serialize, Deserialize)]
pub struct Config {
    pub grpc_access_key: String,
    pub grpc_address: String,

    #[serde(skip)]
    /// Setup by load() for statistics
    pub start_time: chrono::DateTime<chrono::Utc>,
}

impl Config {
    pub fn load() -> Result<Self, Error> {
        // Open config.yaml from parent directory
        let file = File::open("config.yaml");

        match file {
            Ok(file) => {
                // Parse config.yaml
                let mut cfg: Config = serde_yaml::from_reader(file)?;

                cfg.start_time = chrono::Utc::now();

                // Return config
                Ok(cfg)
            }
            Err(e) => {
                // Print error
                println!("config.yaml could not be loaded: {}", e);

                // Exit
                std::process::exit(1);
            }
        }
    }
}

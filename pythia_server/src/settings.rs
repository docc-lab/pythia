use std::collections::HashMap;
use std::path::PathBuf;

use config::{Config, File, FileFormat};

#[derive(Debug)]
pub struct Settings {
    pub server_address: String,
    pub manifest_root: PathBuf,
    pub redis_url: String,
}

impl Settings {
    pub fn read() -> Settings {
        let mut settings = Config::default();
        settings
            .merge(File::new("/etc/pythia/server.toml", FileFormat::Toml))
            .unwrap();
        let results = settings.try_into::<HashMap<String, String>>().unwrap();
        Settings {
            server_address: results.get("server_address").unwrap().to_string(),
            redis_url: results.get("redis_url").unwrap().to_string(),
            manifest_root: PathBuf::from(results.get("manifest_root").unwrap()),
        }
    }
}

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use config::{Config, File, FileFormat};

use crate::search::SearchStrategyType;

#[derive(Debug)]
pub struct Settings {
    pub manifest_file: PathBuf,
    pub pythia_clients: Vec<String>,
    pub redis_url: String,
    pub application: ApplicationType,
    pub xtrace_url: String,
    pub decision_epoch: Duration,
    pub search_strategy: SearchStrategyType,
}

#[derive(Debug)]
pub enum ApplicationType {
    HDFS,
    OpenStack,
    Uber,
}

impl Settings {
    pub fn read() -> Settings {
        let mut settings = Config::default();
        settings
            .merge(File::new("/etc/pythia/controller.toml", FileFormat::Toml))
            .unwrap();
        let results = settings.try_into::<HashMap<String, String>>().unwrap();
        let manifest_file = PathBuf::from(results.get("manifest_file").unwrap());
        Settings {
            manifest_file: manifest_file,
            redis_url: results.get("redis_url").unwrap().to_string(),
            pythia_clients: results
                .get("pythia_clients")
                .unwrap()
                .split(",")
                .map(|x| x.to_string())
                .collect(),
            application: match results.get("application").unwrap().as_str() {
                "OpenStack" => ApplicationType::OpenStack,
                "HDFS" => ApplicationType::HDFS,
                "Uber" => ApplicationType::Uber,
                _ => panic!("Unknown application type"),
            },
            xtrace_url: results.get("xtrace_url").unwrap().to_string(),
            decision_epoch: Duration::from_secs(120),
            search_strategy: match results.get("search_strategy").unwrap().as_str() {
                "Flat" => SearchStrategyType::Flat,
                _ => panic!("Unknown search strategy"),
            },
        }
    }
}

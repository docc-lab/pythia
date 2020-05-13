use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use config::{Config, File, FileFormat};

use crate::search::SearchStrategyType;

const SETTINGS_PATH: &str = "/etc/pythia/controller.toml";
const DECISION_EPOCH: Duration = Duration::from_secs(120);
const PYTHIA_JIFFY: Duration = Duration::from_secs(20);
const GC_EPOCH: Duration = Duration::from_secs(120);
const TRACEPOINTS_PER_EPOCH: usize = 3;

#[derive(Debug)]
pub struct Settings {
    pub application: ApplicationType,
    pub manifest_file: PathBuf,
    pub pythia_clients: Vec<String>,
    pub redis_url: String,
    pub xtrace_url: String,
    pub uber_trace_dir: PathBuf,

    pub search_strategy: SearchStrategyType,
    pub jiffy: Duration,
    pub decision_epoch: Duration,
    pub gc_epoch: Duration,
    pub tracepoints_per_epoch: usize,
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
            .merge(File::new(SETTINGS_PATH, FileFormat::Toml))
            .unwrap();
        let results = settings.try_into::<HashMap<String, String>>().unwrap();
        let manifest_file = PathBuf::from(results.get("manifest_file").unwrap());
        Settings {
            manifest_file: manifest_file,
            redis_url: results.get("redis_url").unwrap().to_string(),
            uber_trace_dir: PathBuf::from(results.get("uber_trace_dir").unwrap()),
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
            decision_epoch: DECISION_EPOCH,
            search_strategy: match results.get("search_strategy").unwrap().as_str() {
                "Flat" => SearchStrategyType::Flat,
                _ => panic!("Unknown search strategy"),
            },
            tracepoints_per_epoch: TRACEPOINTS_PER_EPOCH,
            jiffy: PYTHIA_JIFFY,
            gc_epoch: GC_EPOCH,
        }
    }
}

//! This file contains all the hard-coded settings and parsing code for the toml file.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use config::{Config, File, FileFormat};

use crate::search::SearchStrategyType;

const SETTINGS_PATH: &str = "/etc/pythia/controller.toml";
const DECISION_EPOCH: Duration = Duration::from_secs(120);
const PYTHIA_JIFFY: Duration = Duration::from_secs(20);
const GC_EPOCH: Duration = Duration::from_secs(120);
const GC_KEEP_DURATION: Duration = Duration::from_secs(360);
const TRACEPOINTS_PER_EPOCH: usize = 3;
const DISABLE_RATIO: f32 = 0.1;
const TRACE_SIZE_LIMIT: u32 = 100000000;
const N_WORKERS: usize = 4;
const FREE_KEYS: bool = false;

#[derive(Debug)]
pub struct Settings {
    pub application: ApplicationType,
    pub manifest_file: PathBuf,
    pub pythia_clients: Vec<String>,
    pub redis_url: String,
    pub xtrace_url: String,
    pub uber_trace_dir: PathBuf,
    pub hdfs_control_file: PathBuf,

    pub search_strategy: SearchStrategyType,
    pub jiffy: Duration,
    pub decision_epoch: Duration,
    pub gc_epoch: Duration,
    pub gc_keep_duration: Duration,
    pub tracepoints_per_epoch: usize,
    pub disable_ratio: f32,
    pub trace_size_limit: u32,
    pub n_workers: usize,
    pub free_keys: bool,
}

#[derive(Debug, Eq, PartialEq)]
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
        let hdfs_control_file = PathBuf::from(results.get("hdfs_control_file").unwrap());
        let pythia_clients = results.get("pythia_clients").unwrap();
        let pythia_clients = if pythia_clients.len() == 0 {
            Vec::new()
        } else {
            pythia_clients.split(",").map(|x| x.to_string()).collect()
        };
        Settings {
            manifest_file,
            hdfs_control_file,
            pythia_clients,
            redis_url: results.get("redis_url").unwrap().to_string(),
            uber_trace_dir: PathBuf::from(results.get("uber_trace_dir").unwrap()),
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
                "Hierarchical" => SearchStrategyType::Hierarchical,
                "Historic" => SearchStrategyType::Historic,
                _ => panic!("Unknown search strategy"),
            },
            tracepoints_per_epoch: TRACEPOINTS_PER_EPOCH,
            jiffy: PYTHIA_JIFFY,
            gc_epoch: GC_EPOCH,
            gc_keep_duration: GC_KEEP_DURATION,
            disable_ratio: DISABLE_RATIO,
            trace_size_limit: TRACE_SIZE_LIMIT,
            n_workers: N_WORKERS,
            free_keys: FREE_KEYS,
        }
    }
}

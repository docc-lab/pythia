/*
BSD 2-Clause License

Copyright (c) 2022, Diagnosis and Control of Clouds Laboratory
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

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
    pub DEATHSTAR_trace_dir: PathBuf,
    pub hdfs_control_file: PathBuf,
    pub deathstar_control_file: PathBuf,

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
    DEATHSTAR
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
        let deathstar_control_file = PathBuf::from(results.get("hdfs_control_file").unwrap());
        let pythia_clients = results.get("pythia_clients").unwrap();
        let pythia_clients = if pythia_clients.len() == 0 {
            Vec::new()
        } else {
            pythia_clients.split(",").map(|x| x.to_string()).collect()
        };
        Settings {
            manifest_file,
            hdfs_control_file,
            deathstar_control_file,
            pythia_clients,
            redis_url: results.get("redis_url").unwrap().to_string(),
            uber_trace_dir: PathBuf::from(results.get("uber_trace_dir").unwrap()),
            DEATHSTAR_trace_dir: PathBuf::from(results.get("DEATHSTAR_trace_dir").unwrap()),
            application: match results.get("application").unwrap().as_str() {
                "OpenStack" => ApplicationType::OpenStack,
                "HDFS" => ApplicationType::HDFS,
                "Uber" => ApplicationType::Uber,
                "DEATHSTAR" => ApplicationType::DEATHSTAR,
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

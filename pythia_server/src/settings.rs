/*
This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.

Copyright (c) 2022, Diagnosis and Control of Clouds Laboratory
All rights reserved.
*/

use std::collections::HashMap;
use std::path::PathBuf;

use config::{Config, File, FileFormat};

#[derive(Debug)]
pub struct Settings {
    pub server_address: String,
    pub manifest_root: PathBuf,
    pub redis_url: String,
    pub network_interface: String,
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
            network_interface: results.get("network_interface").unwrap().to_string(),
        }
    }
}

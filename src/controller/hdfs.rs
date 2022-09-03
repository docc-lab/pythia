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

use std::collections::HashSet;
use std::fs::File;
use std::io::prelude::*;
use std::mem::drop;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use itertools::Itertools;

use pythia_common::RequestType;

use crate::controller::Controller;
use crate::manifest::Manifest;
use crate::settings::Settings;
use crate::trace::TracepointID;

pub struct HDFSController {
    controller_file: PathBuf,
    all_tracepoints: HashSet<TracepointID>,
    disabled_tracepoints: Arc<Mutex<HashSet<TracepointID>>>,
    // This should only be valid after disable_all is called
    enabled_tracepoints: Arc<Mutex<HashSet<(TracepointID, Option<RequestType>)>>>,
}

impl Controller for HDFSController {
    fn enable(&self, points: &Vec<(TracepointID, Option<RequestType>)>) {
        eprintln!("Enabling {:?}", points);
        let mut disabled_tracepoints = self.disabled_tracepoints.lock().unwrap();
        for p in points {
            disabled_tracepoints.remove(&p.0);
        }
        drop(disabled_tracepoints);
        self.flush();
    }

    fn disable(&self, points: &Vec<(TracepointID, Option<RequestType>)>) {
        eprintln!("Disabling {:?}", points);
        let mut disabled_tracepoints = self.disabled_tracepoints.lock().unwrap();
        for p in points {
            disabled_tracepoints.insert(p.0);
        }
        drop(disabled_tracepoints);
        self.flush();
    }

    fn is_enabled(&self, point: &(TracepointID, Option<RequestType>)) -> bool {
        let disabled_tracepoints = self.disabled_tracepoints.lock().unwrap();
        // A tracepoint is enabled either globally or for a request type
        disabled_tracepoints.get(&point.0).is_none()
    }

    /// Also removes request-type-specific controllers
    fn disable_all(&self) {
        let mut disabled_tracepoints = self.disabled_tracepoints.lock().unwrap();
        disabled_tracepoints.extend(self.all_tracepoints.iter());
        drop(disabled_tracepoints);
        self.flush();
    }

    /// Also removes request-type-specific controllers
    fn enable_all(&self) {
        let mut disabled_tracepoints = self.disabled_tracepoints.lock().unwrap();
        disabled_tracepoints.clear();
        drop(disabled_tracepoints);
        self.flush();
    }
    fn enabled_tracepoints(&self) -> Vec<(TracepointID, Option<RequestType>)> {
        self.enabled_tracepoints
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect()
    }
}

impl HDFSController {
    pub fn from_settings(settings: &Settings) -> Self {
        let manifest = Manifest::from_file(&settings.manifest_file.as_path()).unwrap();
        HDFSController {
            controller_file: settings.hdfs_control_file.clone(),
            all_tracepoints: manifest.all_tracepoints(),
            disabled_tracepoints: Arc::new(Mutex::new(HashSet::new())),
            enabled_tracepoints: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    fn flush(&self) {
        // Synchronizes pythia file with internal settings
        let disabled_tracepoints = self.disabled_tracepoints.lock().unwrap();
        let mut tracepoints = Vec::new();
        for tp in disabled_tracepoints.iter() {
            tracepoints.push(tp.to_string());
        }
        tracepoints.sort();
        let mut writer = File::create(self.controller_file.as_path()).unwrap();
        writeln!(writer, "{}", tracepoints.iter().join("\n")).ok();
    }
}

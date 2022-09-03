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
use std::sync::{Arc, Mutex};

use pythia_common::RequestType;

use crate::controller::Controller;
use crate::rpclib::set_all_client_tracepoints;
use crate::rpclib::set_client_tracepoints;
use crate::settings::Settings;
use crate::trace::TracepointID;

pub struct OSProfilerController {
    client_list: Vec<String>,

    /// This should only be valid after disable_all is called
    enabled_tracepoints: Arc<Mutex<HashSet<(TracepointID, Option<RequestType>)>>>,
}

impl Controller for OSProfilerController {
    fn enable(&self, points: &Vec<(TracepointID, Option<RequestType>)>) {
        eprintln!("Enabling {:?}", points);
        let mut enabled_tracepoints = self.enabled_tracepoints.lock().unwrap();
        for p in points {
            if p.1 == Some(RequestType::Unknown) {
                enabled_tracepoints.insert((p.0, None));
            } else {
                enabled_tracepoints.insert(p.clone());
            }
        }
        self.write_to_tracepoints(points, b"1");
    }

    fn disable(&self, points: &Vec<(TracepointID, Option<RequestType>)>) {
        eprintln!("Disabling {:?}", points);
        let mut enabled_tracepoints = self.enabled_tracepoints.lock().unwrap();
        for p in points {
            if p.1 == Some(RequestType::Unknown) {
                enabled_tracepoints.remove(&(p.0, None));
            } else {
                enabled_tracepoints.remove(p);
            }
        }
        self.write_to_tracepoints(points, b"0");
    }

    fn is_enabled(&self, point: &(TracepointID, Option<RequestType>)) -> bool {
        let enabled_tracepoints = self.enabled_tracepoints.lock().unwrap();
        // A tracepoint is enabled either globally or for a request type
        !enabled_tracepoints.get(point).is_none()
            || !enabled_tracepoints.get(&(point.0, None)).is_none()
    }

    /// Also removes request-type-specific controllers
    fn disable_all(&self) {
        self.set_all_tracepoints(b"0");
    }

    /// Also removes request-type-specific controllers
    fn enable_all(&self) {
        self.set_all_tracepoints(b"1");
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

impl OSProfilerController {
    pub fn from_settings(settings: &Settings) -> OSProfilerController {
        OSProfilerController {
            client_list: settings.pythia_clients.clone(),
            enabled_tracepoints: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    fn write_to_tracepoints(
        &self,
        points: &Vec<(TracepointID, Option<RequestType>)>,
        to_write: &[u8; 1],
    ) {
        for client in self.client_list.iter() {
            set_client_tracepoints(
                client,
                points
                    .iter()
                    .map(|(x, y)| ((*x).clone(), y.clone(), to_write.clone()))
                    .collect(),
            );
        }
    }

    fn set_all_tracepoints(&self, to_write: &[u8; 1]) {
        for client in self.client_list.iter() {
            set_all_client_tracepoints(client, *to_write);
        }
    }


}

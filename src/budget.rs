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

//! Budget decides how many trace points to enable/disable per cycle.
//!
//! # Usage
//! At each cycle, run `read_stats` and `update_new_paths` with the newest critical paths. The
//! other methods are reader methods which will provide various stats if necessary.

use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
use std::time::Duration;
use std::time::Instant;

use pythia_common::NodeStats;
use pythia_common::RequestType;

use crate::critical::CriticalPath;
use crate::critical::Path;
use crate::rpclib::read_client_stats;
use crate::settings::Settings;
use crate::trace::TracepointID;

/// Methods to collect stats from application nodes, and decide whether we are over the limit in
/// terms of instrumentation budget. Also contains garbage collection.
pub struct BudgetManager {
    clients: Vec<String>,
    last_stats: HashMap<String, NodeStats>,
    /// The time each tracepoint was last observed in a trace.
    last_seen: HashMap<(TracepointID, Option<RequestType>), Instant>,
    gc_keep_duration: Duration,
    trace_size_limit: u32,
}

impl BudgetManager {
    pub fn from_settings(settings: &Settings) -> Self {
        BudgetManager {
            clients: settings.pythia_clients.clone(),
            last_stats: HashMap::new(),
            last_seen: HashMap::new(),
            gc_keep_duration: settings.gc_keep_duration,
            trace_size_limit: settings.trace_size_limit,
        }
    }

    pub fn read_stats(&mut self) {
        for client in &self.clients {
            self.last_stats
                .insert(client.clone(), read_client_stats(client));
        }
    }

    pub fn write_stats(&self, file: &mut File) {
        for (client, stats) in &self.last_stats {
            writeln!(file, "{}: {:?}", client, stats).ok();
        }
    }

    pub fn print_stats(&self) {
        for (client, stats) in &self.last_stats {
            eprintln!("{}: {:?}", client, stats);
        }
    }

    /// Did we over run our budget?
    pub fn overrun(&self) -> bool {
        let mut total_traces = 0;
        for stats in self.last_stats.values() {
            total_traces += stats.trace_size;
        }
        total_traces > self.trace_size_limit
    }

    /// Update the garbage collector
    pub fn update_new_paths(&mut self, paths: &Vec<CriticalPath>) {
        let now = Instant::now();
        for path in paths {
            let mut nidx = path.start_node;
            while nidx != path.end_node {
                self.last_seen
                    .insert((path.at(nidx), Some(path.request_type)), now);
                nidx = path.next_node(nidx).unwrap();
            }
        }
    }

    /// Tracepoints that were not seen for some time. These should be disabled during garbage
    /// collection.
    pub fn old_tracepoints(&self) -> Vec<(TracepointID, Option<RequestType>)> {
        let mut result = Vec::new();
        for (&tp, seen) in &self.last_seen {
            if seen.elapsed() > self.gc_keep_duration {
                result.push(tp);
            }
        }
        result
    }
}

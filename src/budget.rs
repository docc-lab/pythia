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

pub struct BudgetManager {
    clients: Vec<String>,
    last_stats: HashMap<String, NodeStats>,
    last_seen: HashMap<(TracepointID, Option<RequestType>), Instant>,
    gc_keep_duration: Duration,
}

impl BudgetManager {
    pub fn from_settings(settings: &Settings) -> Self {
        BudgetManager {
            clients: settings.pythia_clients.clone(),
            last_stats: HashMap::new(),
            last_seen: HashMap::new(),
            gc_keep_duration: settings.gc_keep_duration,
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
            write!(file, "{}: {:?}", client, stats).ok();
        }
    }

    pub fn print_stats(&self) {
        for (client, stats) in &self.last_stats {
            eprintln!("{}: {:?}", client, stats);
        }
    }

    pub fn overrun(&self) -> bool {
        for stats in self.last_stats.values() {
            if stats.load_avg_1_min > 6.0 {
                return true;
            }
            if stats.written_trace_bytes_per_sec > 100 * 1024 {
                return true;
            }
        }
        false
    }

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

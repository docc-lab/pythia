use std::collections::HashMap;

use pythia_common::NodeStats;

use crate::rpclib::read_client_stats;
use crate::settings::Settings;

pub struct BudgetManager {
    clients: Vec<String>,
    last_stats: HashMap<String, NodeStats>,
}

impl BudgetManager {
    pub fn from_settings(settings: &Settings) -> Self {
        BudgetManager {
            clients: settings.pythia_clients.clone(),
            last_stats: HashMap::new(),
        }
    }

    pub fn read_stats(&mut self) {
        for client in &self.clients {
            self.last_stats
                .insert(client.clone(), read_client_stats(client));
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
}

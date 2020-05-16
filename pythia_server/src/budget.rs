use std::error::Error;
use std::time::Instant;

use procfs::{
    net::{dev_status, DeviceStatus},
    LoadAverage,
};

use pythia_common::NodeStats;

use crate::settings::Settings;

pub struct NodeStatReader {
    interface: String,
    last_stats: Option<NetworkStats>,
    last_measurement: Option<Instant>,
}

struct NetworkStats {
    receive_bytes: u64,
    transmit_bytes: u64,
    receive_drop: u64,
    transmit_drop: u64,
}

impl NetworkStats {
    fn read(stat: &DeviceStatus) -> Self {
        NetworkStats {
            receive_bytes: stat.recv_bytes,
            transmit_bytes: stat.sent_bytes,
            receive_drop: stat.recv_drop,
            transmit_drop: stat.sent_drop,
        }
    }
}

impl NodeStatReader {
    pub fn from_settings(settings: &Settings) -> Self {
        let mut result = NodeStatReader {
            interface: settings.network_interface.clone(),
            last_stats: None,
            last_measurement: None,
        };
        result.read_node_stats().ok();
        result
    }

    pub fn read_node_stats(&mut self) -> Result<NodeStats, Box<dyn Error>> {
        let loadavg = LoadAverage::new()?;
        let netstat = dev_status()?;
        let measure_time = Instant::now();
        let current_stats = NetworkStats::read(netstat.get(&self.interface).unwrap());
        if self.last_measurement.is_none() {
            // First run
            self.last_measurement = Some(measure_time);
            self.last_stats = Some(current_stats);
            return Ok(NodeStats {
                receive_bytes_per_sec: 0,
                transmit_bytes_per_sec: 0,
                receive_drop_per_sec: 0,
                transmit_drop_per_sec: 0,
                load_avg_1_min: 0.0,
                load_avg_5_min: 0.0,
                tasks_runnable: 0,
            });
        }
        let elapsed = self.last_measurement.unwrap().elapsed().as_secs();

        let result = NodeStats {
            // Network stats
            receive_bytes_per_sec: (current_stats.receive_bytes
                - self.last_stats.as_ref().unwrap().receive_bytes)
                / elapsed,
            transmit_bytes_per_sec: (current_stats.transmit_bytes
                - self.last_stats.as_ref().unwrap().transmit_bytes)
                / elapsed,
            receive_drop_per_sec: (current_stats.receive_drop
                - self.last_stats.as_ref().unwrap().receive_drop)
                / elapsed,
            transmit_drop_per_sec: (current_stats.transmit_drop
                - self.last_stats.as_ref().unwrap().transmit_drop)
                / elapsed,

            // Load stats
            load_avg_1_min: loadavg.one,
            load_avg_5_min: loadavg.five,
            tasks_runnable: loadavg.cur,
        };
        self.last_stats = Some(current_stats);
        self.last_measurement = Some(measure_time);
        Ok(result)
    }
}

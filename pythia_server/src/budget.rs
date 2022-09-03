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

use std::error::Error;
use std::time::Instant;

use procfs::{
    net::{dev_status, DeviceStatus},
    process::Process,
    LoadAverage,
};

use pythia_common::NodeStats;

use crate::osprofiler::OSProfilerReader;
use crate::settings::Settings;

/// Contains values last read, time of last reading and some settings.
pub struct NodeStatReader {
    /// Name of network interface
    interface: String,
    last_stats: Option<NetworkStats>,
    last_cputime: Option<u64>,
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
    pub fn from_settings(settings: &Settings, reader: &mut OSProfilerReader) -> Self {
        let mut result = NodeStatReader {
            interface: settings.network_interface.clone(),
            last_stats: None,
            last_cputime: None,
            last_measurement: None,
        };
        result.read_node_stats(reader).ok();
        eprintln!("Measuring node stats -- Mert 11");
        result
    }

    pub fn read_node_stats(
        &mut self,
        reader: &mut OSProfilerReader,
    ) -> Result<NodeStats, Box<dyn Error>> {
        let loadavg = LoadAverage::new()?;
        let netstat = dev_status()?;
        let (current_trace_bytes, trace_size) = reader.get_stats();
        let stat = Process::myself()?.stat()?;
        let measure_time = Instant::now();
        let current_stats = NetworkStats::read(netstat.get(&self.interface).unwrap());
        let cputime = stat.utime + stat.stime + (stat.cutime + stat.cstime) as u64;
        let tps = procfs::ticks_per_second()? as u64;
        if self.last_measurement.is_none()
            || self.last_stats.is_none()
            || self.last_cputime.is_none()
        {
            eprintln!("Measuring node stats - if 1");
            // First run
            self.last_measurement = Some(measure_time);
            self.last_stats = Some(current_stats);
            self.last_cputime = Some(cputime);
            return Ok(NodeStats {
                receive_bytes_per_sec: 0,
                transmit_bytes_per_sec: 0,
                receive_drop_per_sec: 0,
                transmit_drop_per_sec: 0,
                load_avg_1_min: 0.0,
                load_avg_5_min: 0.0,
                tasks_runnable: 0,
                trace_input_kbps: 0.0,
                agent_cpu_time: 0.0,
                trace_size: 0,
            });
        }
        let elapsed = self.last_measurement.unwrap().elapsed().as_secs();
        eprintln!("Measuring node stats -- MERT 2");
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

            // Trace stats
            trace_input_kbps: current_trace_bytes,
            agent_cpu_time: ((cputime - self.last_cputime.unwrap()) / tps) as f64 / elapsed as f64,
            trace_size: trace_size,
        };
        eprintln!("MERT hata oldumu bakalim");
        self.last_stats = Some(current_stats);
        self.last_measurement = Some(measure_time);
        self.last_cputime = Some(cputime);
        eprintln!("MERTiko {:?}", result);
        Ok(result)
    }
}

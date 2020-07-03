use serde::{Deserialize, Serialize};

/// The usage statistics sent from Pythia agents to the controller.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeStats {
    /// Network stats
    pub receive_bytes_per_sec: u64,
    pub transmit_bytes_per_sec: u64,
    pub receive_drop_per_sec: u64,
    pub transmit_drop_per_sec: u64,

    /// Load stats
    pub load_avg_1_min: f32,
    pub load_avg_5_min: f32,
    pub tasks_runnable: u32,

    /// Tracing stats
    pub trace_input_kbps: f32,
    pub agent_cpu_time: f64,
    pub trace_size: u32,
}

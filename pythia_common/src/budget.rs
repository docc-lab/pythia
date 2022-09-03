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

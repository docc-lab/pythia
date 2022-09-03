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

//! The Pythia agents that run on every compute node.
//!
//! # Running the agent
//! If using systemd, copy the files to `/etc/` and then `sudo systemctl start pythia`.
//! Otherwise, just run the binary. Remember the port/address in the configuration so that
//! the main Pythia would know where the agents are running.
//!
//! # Things related with the code
//! The RPC commands it provides are inside `PythiaAPI`. There is some
//! internal state of the server, which means for many requests it can
//! only process them one at a time. This is an implementation limitation
//! (state is encapsulated in `Arc<Mutex<>>`, even though some state never
//! changes after init) and someone who knows more Rust can probably solve it.

pub mod budget;
pub mod controller;
pub mod osprofiler;
pub mod settings;

use std::sync::{Arc, Mutex};

use jsonrpc_core::{IoHandler, Result, Value};
use jsonrpc_derive::rpc;
use jsonrpc_http_server::ServerBuilder;
use serde_json;

use pythia_common::RequestType;

use crate::budget::NodeStatReader;
use crate::controller::OSProfilerController;
use crate::osprofiler::OSProfilerReader;
use crate::settings::Settings;

#[rpc(server)]
pub trait PythiaAPI {
    /// Returns all events from local redis that matches the `trace_id`
    #[rpc(name = "get_events")]
    fn get_events(&self, trace_id: String) -> Result<Value>;

    /// Apply tracepoint configuration locally.
    ///
    /// The configuration is tuples of tracepoint ID, `Option<RequestType>` (`None`
    /// applies it to all request types), and the setting (can be 0 (disabled) or 1
    /// (enabled)).
    #[rpc(name = "set_tracepoints")]
    fn set_tracepoints(&self, settings: Vec<(String, Option<RequestType>, [u8; 1])>) -> Result<()>;

    /// Change setting for all local tracepoints. `to_write` decides whether to disable (0) or
    /// enable (1) all tracepoints.
    #[rpc(name = "set_all_tracepoints")]
    fn set_all_tracepoints(&self, to_write: [u8; 1]) -> Result<()>;

    /// Read the local statistics. For counters, it divides the increase in counter with
    /// time elapsed since last read.
    #[rpc(name = "read_node_stats")]
    fn read_node_stats(&self) -> Result<Value>;

    /// Delete these keys from redis. Used to free up memory, but deleted any records of traces.
    #[rpc(name = "free_keys")]
    fn free_keys(&self, keys: Vec<String>) -> Result<()>;
}

struct PythiaAPIImpl {
    reader: Arc<Mutex<OSProfilerReader>>,
    controller: Arc<Mutex<OSProfilerController>>,
    stats: Arc<Mutex<NodeStatReader>>,
}

impl PythiaAPI for PythiaAPIImpl {
    fn get_events(&self, trace_id: String) -> Result<Value> {
        eprintln!("Got request for {}", trace_id);
        Ok(serde_json::to_value(self.reader.lock().unwrap().get_matches(&trace_id)).unwrap())
    }

    fn set_tracepoints(&self, settings: Vec<(String, Option<RequestType>, [u8; 1])>) -> Result<()> {
        eprintln!("Setting {} tracepoints", settings.len());
        self.controller.lock().unwrap().apply_settings(settings);
        Ok(())
    }

    fn set_all_tracepoints(&self, to_write: [u8; 1]) -> Result<()> {
        eprintln!("Setting all tracepoints to {:?}", to_write);
        self.controller.lock().unwrap().write_client_dir(&to_write);
        Ok(())
    }

    fn read_node_stats(&self) -> Result<Value> {
        eprintln!("Measuring node stats -- MERT");
        Ok(serde_json::to_value(
            self.stats
                .lock()
                .unwrap()
                .read_node_stats(&mut self.reader.lock().unwrap())
                .unwrap(),
        )
        .unwrap())
    }

    fn free_keys(&self, keys: Vec<String>) -> Result<()> {
        eprintln!("Freeing keys {:?}", keys);
        self.reader.lock().unwrap().free_keys(keys);
        Ok(())
    }
}

/// Starts the server in port specified at the config file and waits for requests.
///
/// Needs root access.
pub fn run_pythia_server() {
    eprintln!("Did you remember to run as root?");
    let settings = Settings::read();
    let reader = Arc::new(Mutex::new(OSProfilerReader::from_settings(&settings)));
    let controller = Arc::new(Mutex::new(OSProfilerController::from_settings(&settings)));
    let stats = Arc::new(Mutex::new(NodeStatReader::from_settings(
        &settings,
        &mut reader.lock().unwrap(),
    )));
    let mut io = IoHandler::new();
    io.extend_with(
        PythiaAPIImpl {
            reader,
            controller,
            stats,
        }
        .to_delegate(),
    );

    let address = settings.server_address;
    println!("Starting the server at {}", address);

    let _server = ServerBuilder::new(io)
        .start_http(&address.parse().unwrap())
        .expect("Unable to start RPC server");

    _server.wait();
}

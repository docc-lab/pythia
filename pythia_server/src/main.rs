use std::sync::{Arc, Mutex};

use jsonrpc_core::{IoHandler, Result, Value};
use jsonrpc_derive::rpc;
use jsonrpc_http_server::ServerBuilder;
use serde_json;

use pythia_common::RequestType;

use pythia_server::budget::NodeStatReader;
use pythia_server::controller::OSProfilerController;
use pythia_server::osprofiler::OSProfilerReader;
use pythia_server::settings::Settings;

#[rpc(server)]
pub trait PythiaAPI {
    #[rpc(name = "get_events")]
    fn get_events(&self, trace_id: String) -> Result<Value>;
    #[rpc(name = "set_tracepoints")]
    fn set_tracepoints(&self, settings: Vec<(String, Option<RequestType>, [u8; 1])>) -> Result<()>;
    #[rpc(name = "set_all_tracepoints")]
    fn set_all_tracepoints(&self, to_write: [u8; 1]) -> Result<()>;
    #[rpc(name = "read_node_stats")]
    fn read_node_stats(&self) -> Result<Value>;
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
        eprintln!("Measuring node stats");
        Ok(serde_json::to_value(self.stats.lock().unwrap().read_node_stats().unwrap()).unwrap())
    }
}

fn main() {
    eprintln!("Did you remember to run as root?");
    let settings = Settings::read();
    let reader = Arc::new(Mutex::new(OSProfilerReader::from_settings(&settings)));
    let controller = Arc::new(Mutex::new(OSProfilerController::from_settings(&settings)));
    let stats = Arc::new(Mutex::new(NodeStatReader::from_settings(&settings)));
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

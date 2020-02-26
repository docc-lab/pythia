extern crate pythia;

use std::sync::{Arc, Mutex};

use jsonrpc_core::*;
use jsonrpc_http_server::*;
use serde_json;

use pythia::get_settings;
use pythia::osprofiler::OSProfilerReader;

fn main() {
    let settings = get_settings();
    let reader = Arc::new(Mutex::new(OSProfilerReader::from_settings(&settings)));
    let mut io = IoHandler::new();
    io.add_method("get_trace", move |a: Params| {
        let res: Vec<String> = a.parse().unwrap();
        let mut result = serde_json::Map::new();
        for i in res {
            result.insert(
                i.to_string(),
                serde_json::to_value(reader.lock().unwrap().get_trace_from_base_id(&i)).unwrap(),
            );
        }
        Ok(Value::Object(result))
    });

    let address: &String = settings.get("server_address").unwrap();
    println!("Starting the server at {}", address);

    let _server = ServerBuilder::new(io)
        .start_http(&address.parse().unwrap())
        .expect("Unable to start RPC server");

    _server.wait();
}

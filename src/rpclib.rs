use std::sync::{Arc, Mutex};

use jsonrpc_core::{Result, Value, IoHandler};
use jsonrpc_derive::rpc;
use jsonrpc_http_server::ServerBuilder;
use serde_json;

use crate::get_settings;
use crate::osprofiler::OSProfilerReader;

#[rpc]
pub trait PythiaClient {
    #[rpc(name = "get_trace")]
    fn get_trace(&self, ids: Vec<String>) -> Result<Value>;
}

pub struct PythiaClientImpl {
    reader: Arc<Mutex<OSProfilerReader>>,
}

impl PythiaClient for PythiaClientImpl {
    fn get_trace(&self, ids: Vec<String>) -> Result<Value> {
        let mut result = serde_json::Map::new();
        for i in ids {
            result.insert(
                i.to_string(),
                serde_json::to_value(self.reader.lock().unwrap().get_trace_from_base_id(&i))
                    .unwrap(),
            );
        }
        Ok(Value::Object(result))
    }
}

pub fn start_rpc_server() {
    let settings = get_settings();
    let reader = Arc::new(Mutex::new(OSProfilerReader::from_settings(&settings)));
    let mut io = IoHandler::new();
    io.extend_with(PythiaClientImpl{
        reader: reader}.to_delegate());

    let address: &String = settings.get("server_address").unwrap();
    println!("Starting the server at {}", address);

    let _server = ServerBuilder::new(io)
        .start_http(&address.parse().unwrap())
        .expect("Unable to start RPC server");

    _server.wait();
}

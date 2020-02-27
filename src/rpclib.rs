use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::time::Duration;

use jsonrpc_core::{Result, Value, IoHandler};
use jsonrpc_core_client::{RpcChannel, RpcError, TypedClient};
use jsonrpc_client_transports::transports::http;
use jsonrpc_derive::rpc;
use jsonrpc_http_server::{hyper, ServerBuilder};
use serde_json;
use futures::future::Future;

use crate::get_settings;
use crate::osprofiler::{OSProfilerReader, OSProfilerDAG};

#[rpc]
pub trait PythiaAPI {
    #[rpc(name = "get_trace")]
    fn get_trace(&self, ids: Vec<String>) -> Result<Value>;
}

struct PythiaAPIImpl {
    reader: Arc<Mutex<OSProfilerReader>>,
}

impl PythiaAPI for PythiaAPIImpl {
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

#[derive(Clone)]
struct PythiaClient(TypedClient);

impl From<RpcChannel> for PythiaClient {
    fn from(channel: RpcChannel) -> Self {
        PythiaClient(channel.into())
    }
}

impl PythiaClient {
    fn get_trace(&self, ids: Vec<String>) -> impl Future<Item = Value, Error = RpcError> {
        self.0.call_method("get_trace", "String", (ids,))
    }
}

pub fn start_rpc_server() {
    let settings = get_settings();
    let reader = Arc::new(Mutex::new(OSProfilerReader::from_settings(&settings)));
    let mut io = IoHandler::new();
    io.extend_with(PythiaAPIImpl{reader: reader}.to_delegate());

    let address: &String = settings.get("server_address").unwrap();
    println!("Starting the server at {}", address);

    let _server = ServerBuilder::new(io)
        .start_http(&address.parse().unwrap())
        .expect("Unable to start RPC server");

    _server.wait();
}

pub fn get_traces_from_client(traces: Vec<String>) -> HashMap<String, OSProfilerDAG> {
    let (tx, rx) = std::sync::mpsc::channel();
    let run = http::connect("cp-1:3030")
            .and_then(|client: PythiaClient| {
                    client.get_trace(traces).and_then(move |result| {
                            drop(client);
                            let _ = tx.send(result);
                            Ok(())
                    })
            })
            .map_err(|e| eprintln!("RPC Client error: {:?}", e));

    hyper::rt::run(run);

    match rx.recv_timeout(Duration::from_secs(3)).unwrap() {
        Value::Object(o) => o.into_iter().map(|(k, v)| {
            match v {
            Value::String(s) => (k, s.to_string()),
            _ => panic!("Got something weird within request")
            }}).map(|(k, v)| {
                (k, serde_json::from_str(&v).unwrap())}
        ).collect(),
        _ => panic!("Got something weird from request")
    }
}

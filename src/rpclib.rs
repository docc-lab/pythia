use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures::future::Future;
use futures::stream::Stream;
use futures::Async;
use hyper::rt;
use jsonrpc_client_transports::transports::http;
use jsonrpc_core::{IoHandler, Result, Value};
use jsonrpc_core_client::{RpcChannel, RpcError, TypedClient};
use jsonrpc_derive::rpc;
use jsonrpc_http_server::ServerBuilder;
use serde_json;

use crate::get_settings;
use crate::osprofiler::{OSProfilerReader, OSProfilerSpan};

#[rpc]
pub trait PythiaAPI {
    #[rpc(name = "get_events")]
    fn get_events(&self, ids: Vec<String>) -> Result<Value>;
}

struct PythiaAPIImpl {
    reader: Arc<Mutex<OSProfilerReader>>,
}

impl PythiaAPI for PythiaAPIImpl {
    fn get_events(&self, ids: Vec<String>) -> Result<Value> {
        eprintln!("Got request for {:?}", ids);
        let mut result = serde_json::Map::new();
        for i in ids {
            result.insert(
                i.to_string(),
                serde_json::to_value(self.reader.lock().unwrap().get_matches(&i)).unwrap(),
            );
        }
        Ok(Value::Object(result))
    }
}

pub fn start_rpc_server() {
    let settings = get_settings();
    let reader = Arc::new(Mutex::new(OSProfilerReader::from_settings(&settings)));
    let mut io = IoHandler::new();
    io.extend_with(PythiaAPIImpl { reader: reader }.to_delegate());

    let address: &String = settings.get("server_address").unwrap();
    println!("Starting the server at {}", address);

    let _server = ServerBuilder::new(io)
        .start_http(&address.parse().unwrap())
        .expect("Unable to start RPC server");

    _server.wait();
}

#[derive(Clone)]
struct PythiaClient(TypedClient);

impl From<RpcChannel> for PythiaClient {
    fn from(channel: RpcChannel) -> Self {
        PythiaClient(channel.into())
    }
}

impl PythiaClient {
    fn get_events(&self, ids: Vec<String>) -> impl Future<Item = Value, Error = RpcError> {
        self.0.call_method("get_events", "String", (ids,))
    }
}

pub fn get_events_from_client(
    client_uri: &str,
    traces: Vec<String>,
) -> HashMap<String, Vec<OSProfilerSpan>> {
    let (tx, mut rx) = futures::sync::mpsc::unbounded();

    let run = http::connect(client_uri)
        .and_then(|client: PythiaClient| {
            client.get_events(traces).and_then(move |result| {
                drop(client);
                let _ = tx.unbounded_send(result);
                Ok(())
            })
        })
        .map_err(|e| eprintln!("RPC Client error: {:?}", e));

    rt::run(run);
    let mut final_result = HashMap::new();

    loop {
        match rx.poll() {
            Ok(Async::Ready(Some(v))) => {
                let traces = match v {
                    Value::Object(o) => o,
                    _ => panic!("Got something weird from request"),
                };
                let str_traces = traces.into_iter().filter_map(|(k, v)| match v {
                    Value::Array(a) => {
                        if a.len() == 0 {
                            None
                        } else {
                            Some((k, a.iter().map(|x| x.to_string()).collect()))
                        }
                    }
                    _ => panic!("Got something weird within request: {:?}", v),
                });
                final_result.extend(str_traces.map(|(k, v): (String, Vec<String>)| {
                    (
                        k,
                        v.iter()
                            .map(|x| serde_json::from_str(&x).unwrap())
                            .collect::<Vec<OSProfilerSpan>>(),
                    )
                }));
            }
            Ok(Async::NotReady) => {}
            Ok(Async::Ready(None)) => {
                break;
            }
            Err(e) => panic!("Got error from poll: {:?}", e),
        }
    }
    final_result
}

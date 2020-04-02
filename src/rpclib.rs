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
use uuid::Uuid;

use crate::controller::OSProfilerController;
use crate::osprofiler::{OSProfilerReader, OSProfilerSpan};
use crate::settings::Settings;
use crate::trace::RequestType;
use crate::trace::TRACEPOINT_ID_MAP;

#[rpc]
pub trait PythiaAPI {
    #[rpc(name = "get_events")]
    fn get_events(&self, trace_id: String) -> Result<Value>;
    #[rpc(name = "set_tracepoints")]
    fn set_tracepoints(&self, settings: Vec<(String, Option<RequestType>, [u8; 1])>) -> Result<()>;
    #[rpc(name = "set_all_tracepoints")]
    fn set_all_tracepoints(&self, to_write: [u8; 1]) -> Result<()>;
}

struct PythiaAPIImpl {
    reader: Arc<Mutex<OSProfilerReader>>,
    controller: Arc<Mutex<OSProfilerController>>,
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
}

pub fn start_rpc_server() {
    let settings = Settings::read();
    let reader = Arc::new(Mutex::new(OSProfilerReader::from_settings(&settings)));
    let controller = Arc::new(Mutex::new(OSProfilerController::from_settings(&settings)));
    let mut io = IoHandler::new();
    io.extend_with(
        PythiaAPIImpl {
            reader: reader,
            controller: controller,
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

#[derive(Clone)]
struct PythiaClient(TypedClient);

impl From<RpcChannel> for PythiaClient {
    fn from(channel: RpcChannel) -> Self {
        PythiaClient(channel.into())
    }
}

impl PythiaClient {
    fn get_events(&self, trace_id: String) -> impl Future<Item = Value, Error = RpcError> {
        self.0.call_method("get_events", "String", (trace_id,))
    }

    fn set_all_tracepoints(&self, to_write: [u8; 1]) -> impl Future<Item = (), Error = RpcError> {
        self.0.call_method("set_all_tracepoints", "", (to_write,))
    }

    fn set_tracepoints(
        &self,
        settings: Vec<(usize, Option<RequestType>, [u8; 1])>,
    ) -> impl Future<Item = (), Error = RpcError> {
        let new_settings: Vec<(String, Option<RequestType>, [u8; 1])> = settings
            .iter()
            .map(|(x, y, z)| {
                (
                    TRACEPOINT_ID_MAP.lock().unwrap().get_by_right(&x).unwrap().clone(),
                    y.clone(),
                    z.clone(),
                )
            })
            .collect();
        self.0.call_method("set_tracepoints", "", (new_settings,))
    }
}

pub fn get_events_from_client(client_uri: &str, trace_id: Uuid) -> Vec<OSProfilerSpan> {
    let (tx, mut rx) = futures::sync::mpsc::unbounded();

    let run = http::connect(client_uri)
        .and_then(move |client: PythiaClient| {
            client
                .get_events(trace_id.to_hyphenated().to_string())
                .and_then(move |result| {
                    drop(client);
                    let _ = tx.unbounded_send(result);
                    Ok(())
                })
        })
        .map_err(|e| eprintln!("RPC Client error: {:?}", e));

    rt::run(run);
    let mut final_result = Vec::new();

    loop {
        match rx.poll() {
            Ok(Async::Ready(Some(v))) => {
                let traces = match v {
                    Value::Array(o) => o,
                    _ => panic!("Got something weird from request {:?}", v),
                };
                final_result.extend(
                    traces
                        .iter()
                        .map(|x| x.to_string())
                        .map(|x: String| serde_json::from_str(&x).unwrap())
                        .collect::<Vec<OSProfilerSpan>>(),
                );
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

pub fn set_all_client_tracepoints(client_uri: &str, to_write: [u8; 1]) {
    let (tx, mut rx) = futures::sync::mpsc::unbounded();

    let run = http::connect(client_uri)
        .and_then(move |client: PythiaClient| {
            client
                .set_all_tracepoints(to_write.clone())
                .and_then(move |x| {
                    drop(client);
                    let _ = tx.unbounded_send(x);
                    Ok(())
                })
        })
        .map_err(|e| eprintln!("RPC Client error: {:?}", e));

    rt::run(run);
    loop {
        match rx.poll() {
            Ok(Async::Ready(Some(()))) => {
                return;
            }
            Ok(Async::NotReady) => {}
            Ok(Async::Ready(None)) => {
                break;
            }
            Err(e) => panic!("Got error from poll: {:?}", e),
        }
    }
}

pub fn set_client_tracepoints(
    client_uri: &str,
    settings: Vec<(usize, Option<RequestType>, [u8; 1])>,
) {
    let (tx, mut rx) = futures::sync::mpsc::unbounded();

    let run = http::connect(client_uri)
        .and_then(move |client: PythiaClient| {
            client.set_tracepoints(settings).and_then(move |x| {
                drop(client);
                tx.unbounded_send(x).unwrap();
                Ok(())
            })
        })
        .map_err(|e| eprintln!("RPC Client error: {:?}", e));

    rt::run(run);
    loop {
        match rx.poll() {
            Ok(Async::Ready(Some(()))) => {
                return;
            }
            Ok(Async::NotReady) => {}
            Ok(Async::Ready(None)) => {
                break;
            }
            Err(e) => panic!("Got error from poll: {:?}", e),
        }
    }
}

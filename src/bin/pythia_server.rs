extern crate pythia;

use std::sync::{Arc, Mutex};

use jsonrpc_core::*;
use jsonrpc_http_server::*;

use pythia::get_settings;

fn main() {
    let settings = get_settings();
    let mut io = IoHandler::new();
    let count = Arc::new(Mutex::new(0));
    io.add_method("get_trace", move |a: Params| {
        let res: Vec<i64> = a.parse().unwrap();
        let mut sum = 0;
        for i in res {
            sum += i;
        }
        let mut count = count.lock().unwrap();
        *count += sum;
        Ok(Value::String(count.to_string()))
    });

    let address: &String = settings.get("server_address").unwrap();
    println!("Starting the server at {}", address);

    let _server = ServerBuilder::new(io)
        .start_http(&address.parse().unwrap())
        .expect("Unable to start RPC server");

    _server.wait();
}

extern crate pythia;

use jsonrpc_core::*;
use jsonrpc_http_server::*;

use pythia::get_settings;

fn main() {
    let settings = get_settings();
    let mut io = IoHandler::new();
    io.add_method("get_trace", |a: Params| {
        let res: Vec<String> = a.parse().unwrap();
        Ok(Value::String(res[0].clone()))
    });

    let address: &String = settings.get("server_address").unwrap();
    println!("Starting the server at {}", address);

    let _server = ServerBuilder::new(io)
        .start_http(&address.parse().unwrap())
        .expect("Unable to start RPC server");

    _server.wait();
}

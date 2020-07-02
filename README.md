# Pythia rust project

Installing rust:
```
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## Documentation
After pulling the code, use `cargo doc --open`

Using Pythia server:
```
curl --data-binary '{"jsonrpc":"2.0","id":"curltext","method":"$METHOD_NAME","params":["$PARAM1","$PARAM2",...]}' -H 'content-type:application/json' http://127.0.0.1:3030/
```

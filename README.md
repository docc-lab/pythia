# Pythia rust project

Installing rust:
```
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## Getting Documentation
After pulling the code, use `cargo doc --open`. Documentation there includes how
to install, documentation on the codebase, etc.

## Notes
Using Pythia server manually:
```
curl --data-binary '{"jsonrpc":"2.0","id":"curltext","method":"$METHOD_NAME","params":["$PARAM1","$PARAM2",...]}' -H 'content-type:application/json' http://127.0.0.1:3030/
```

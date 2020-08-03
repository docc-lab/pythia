# Pythia rust project

This repo contains the parts of Pythia written in Rust.

# Pythia repositories
* [This repo](https://github.com/docc-lab/reconstruction): Pythia agent and
  controller.
* [openstack-build-ubuntu](https://github.com/docc-lab/openstack-build-ubuntu):
  Cloudlab profile for setting up openstack. Has to be public.
* [tracing-pythia](https://github.com/docc-lab/tracing-pythia): Random stuff,
  there's a table of contents in the repo.
* [ORE](https://github.com/docc-lab/ORE): scripts to get openstack running on
  MOC.
* [pythia_client](https://github.com/docc-lab/pythia_client): failed experiment
  that tried to automatically instrument every python statement/function.
## Forks of openstack projects
* [osprofiler](https://github.com/docc-lab/osprofiler): Many changes, we have to
  run this version for the rust code to work.
* [nova](https://github.com/docc-lab/nova): Includes more instrumentation and
  instrumentation fixes.
* [python-novaclient](https://github.com/docc-lab/python-novaclient):
  instrumentation for request types.
* [python-openstackclient](https://github.com/docc-lab/python-openstackclient):
  instrumentation for request types.
* [oslo.log](https://github.com/docc-lab/oslo.log): support to add log
  statements into traces.
* [osc_lib](https://github.com/docc-lab/osc_lib): instrumentation on the client
  side.
* [oslo.messaging](https://github.com/docc-lab/oslo.messaging): split
  asynchronous requests to capture concurrency correctly.
* [neutron](https://github.com/docc-lab/neutron): more instrumentation.
* [python-blazarclient](https://github.com/docc-lab/python-blazarclient):
  support for request types.
## Other repos
* [nova-old](https://github.com/docc-lab/nova-old): seems like an old fork of
  nova. I forgot what this was for, currently unused.
* [dagreconstruction](https://github.com/docc-lab/dagreconstruction): seems like
  it's a fork of reconstruction that was used at one point.

# Installation instructions

Installing rust:
```
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## Getting Documentation
After pulling the code, use `cargo doc --open --document-private-items`.
Documentation there includes how to install, documentation on the codebase,
etc.

The `pythia_server` folder contains an independent rust project, whose documentation
should be built separately, in the same way.

## Notes
Using Pythia server manually:
```
curl --data-binary '{"jsonrpc":"2.0","id":"curltext","method":"$METHOD_NAME","params":["$PARAM1","$PARAM2",...]}' -H 'content-type:application/json' http://127.0.0.1:3030/
```

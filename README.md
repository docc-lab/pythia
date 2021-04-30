
**Key-Value Pair Updates**
Code does not compile yet. Need a way to parse key-value pairs values which have the type Value (an enum in Trace::Value) into f64 to do pearson correlation
Correlation is calculated in key_value_analysis function in Grouping.rs
In trace.rs extract key-value pairs, append tracepointID to key to make them unique. Stored in TraceNode structure (in trace.rs file)

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

# Notes/How To for Various Things

## What is pythia server/controller/agent/client/etc.?
Renaming things without extensive testing is difficult at this moment. The
`pythia_server` folder in this repository contains pythia agents that run on
each application node and collect traces. The main `src` folder inside this repo
contains the pythia controller. There should be one pythia controller running
that makes all of the instrumentation decisions.

## Using Pythia server manually:
```
curl --data-binary '{"jsonrpc":"2.0","id":"curltext","method":"$METHOD_NAME","params":["$PARAM1","$PARAM2",...]}' -H 'content-type:application/json' http://127.0.0.1:3030/
```

## How to create cloudlab image
Follow the cloudlab guide [probably
here](https://docs.cloudlab.us/cloudlab-manual.html#%28part._disk-images%29).
You have to start a single-node project and edit the image using that project,
and take a snapshot after that. To avoid doing this frequently, I added deploy
ssh keys to the cloudlab image for each repo, and the pythia install scripts
pull the most recent versions of each repo before installing anything.

## How to fork a new openstack project
You should make any changes to openstack persistent by forking the project and
adding it to the relevant install scripts.
0. Start an openstack project, make sure your changes are not breaking anything
   for a running openstack instance.
1. Fork the project on github.
2. Make any required changes and then push to github. Remember which branch you
   push to.
3. (similar to how to create cloudlab image point above)
    * Create a single-node project using our disk image.
    * Clone the repo into `/local/`. The branch should match the branch you want
      to be installed (the branch you made changes to).
    * Create a deploy key, put the deploy key inside `/local/.ssh/` (as far as I remember).
    * Add the deploy key to github.
    * Take a snapshot.
    * Modify the pythia install scripts inside
      [openstack-build-ubuntu](https://github.com/docc-lab/openstack-build-ubuntu)
      to pull and install your new repo.

## How to change sampling rate
Inside the `osprofiler` repository, change `sampling_rate` in the file
`osprofiler/opts.py`. Set to 0.3 for 30% sampling. After this, re-install
osprofiler and restart openstack (see the osprofiler repo for instructions on
how to do this).

An alternative method is to manually sample your traces in your workload scripts
(e.g., run 3 out of 10 requests with profiling enabled for 30% sampling).

## How to introduce problems to openstack
Change the variable `PROBLEM_TRACEPOINT` inside `osprofiler/profiler.py` in the
`osprofiler` repository. Any tracepoint that matches that name would randomly
sleep, imitating a performance problem. To change sleep amount, check where the
variable `PROBLEM_TRACEPOINT` is used in that file. After changing the file,
re-install osprofiler and restart openstack according to the instructions in the
"how to change sampling rate" bullet.

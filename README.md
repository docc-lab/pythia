# Pythia rust project

## About

This is the opensourced code base of **[DOCC Lab at Tufts University](https://docclab.cs.tufts.edu/)**'s full paper **[VAIF](https://dl.acm.org/doi/abs/10.1145/3544497.3544504?casa_token=rjolR5C9q9wAAAAA:7iVtHXCABI3JMd7OZ4OOqrs4U1EptpCHWNyqrLUDfUoVfYYHe65WLGeEBd2csmYe_p3eWqoIR2VJ3w) on SoCC21** (pervious short paper **[Pythia](https://dl.acm.org/doi/abs/10.1145/3357223.3362704) on SoCC19**)

VAIF is the final name in paper, but we used old name, Pythia, in code base.

This repo is under BSD 2-clause license.

## Code base

- This repo contains the parts of Pythia written in Rust, including ***pythia controller*** and ***pythia server*** in the `pythia_server` foleder.
- Currently, Pythia can work in OpenStack.
- We also opensource Pytyhia instrumentation agent for OpenStack enviornment. Please refer to [the repos list at the botoom](#pythia-repositories).



## Installation instructions

One may compile & install Pythia code base using Rust tools, but it's not recommended. Instead, we recommend to use the following two methods.

- Create a CloudLab experiment with Pythia & Openstack enviornment installed automatically

  - Accessable using [this CloudLab profile](https://www.cloudlab.us/p/Tracing-Pythia/pythia-openstack-opensource)

- Pull a Docker container that contains Pythia

  - WIP
  - No user guide for docker user since Pythia requires OpenStack enviornment

## Usage

### In CloudLab, using pythia with Openstack

Please follow [this user guide](https://docs.google.com/document/d/1h0qHo1VSJWcStmhBOY_UqypxgSanhWeEuOTkSJosF8c/edit?usp=sharing) for

- How to create an experiment with shared profile
- How to setup initial search space for Pythia
- How to execute Pythia
- How to injection different problems to OpenStack
- How to grab result of Pythia's analysis
- How to do troubleshooting

### Getting Documentation of Pythia code base

***(If you plan to contribute, and needs documentation)***

After pulling the code, use `cargo doc --open --document-private-items`.
Documentation there includes how to install, documentation on the codebase,
etc.

The `pythia_server` folder contains an independent rust project, whose documentation
should be built separately, in the same way.

## Contribution

Contribution is welcomed in general.

- Pythia is opensource under BSD 2-Clause License
- WIP

## Pythia repositories

- [This repo](https://github.com/docc-lab/reconstruction): Pythia agent and controller.
- [openstack-build-ubuntu](https://github.com/docc-lab/openstack-build-ubuntu) :
  Cloudlab profile for setting up openstack.
- [ORE](https://github.com/docc-lab/ORE): scripts to get openstack running on MOC. (Not active and not maintained)

## Forks of openstack projects

- [osprofiler](https://github.com/docc-lab/osprofiler): Many changes, we have to
  run this version for the rust code to work.
- [nova](https://github.com/docc-lab/nova): Includes more instrumentation and
  instrumentation fixes.
- [python-novaclient](https://github.com/docc-lab/python-novaclient):
  instrumentation for request types.
- [python-openstackclient](https://github.com/docc-lab/python-openstackclient):
  instrumentation for request types.
- [oslo.log](https://github.com/docc-lab/oslo.log): support to add log
  statements into traces.
- [osc_lib](https://github.com/docc-lab/osc_lib): instrumentation on the client
  side.
- [oslo.messaging](https://github.com/docc-lab/oslo.messaging): split
  asynchronous requests to capture concurrency correctly.
- [neutron](https://github.com/docc-lab/neutron): more instrumentation.
- [python-blazarclient](https://github.com/docc-lab/python-blazarclient): support for request types.

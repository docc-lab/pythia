# Pythia rust project

- [Pythia rust project](#pythia-rust-project)
  - [About \& License](#about--license)
  - [Code base](#code-base)
  - [Installation instructions](#installation-instructions)
  - [Usage](#usage)
    - [In CloudLab, using pythia with Openstack](#in-cloudlab-using-pythia-with-openstack)
    - [In docker image](#in-docker-image)
    - [Getting Documentation of Pythia code base](#getting-documentation-of-pythia-code-base)
  - [Pythia repositories](#pythia-repositories)
  - [Forks of openstack projects](#forks-of-openstack-projects)


## About & License

This is the opensourced code base of **[DOCC Lab at Tufts University](https://docclab.cs.tufts.edu/)** and **[Peac Lab at Boston University](https://www.bu.edu/peaclab/)**'s full paper **[VAIF](https://dl.acm.org/doi/abs/10.1145/3544497.3544504?casa_token=rjolR5C9q9wAAAAA:7iVtHXCABI3JMd7OZ4OOqrs4U1EptpCHWNyqrLUDfUoVfYYHe65WLGeEBd2csmYe_p3eWqoIR2VJ3w) on SoCC21** (pervious short paper **[Pythia](https://dl.acm.org/doi/abs/10.1145/3357223.3362704) on SoCC19**)

VAIF is the final name in paper, but we used old name, Pythia, in code base.

This repo is under BSD 2-clause license.

## Code base

- This repo contains the parts of Pythia written in Rust, including ***pythia controller*** and ***pythia server*** in the `pythia_server` folder.
- Currently, Pythia can work in OpenStack, HDFS (extra setup needed, not included here) enviornment etc.
- We also opensource Pytyhia instrumentation agent for OpenStack enviornment. Please refer to [the repos list at the botoom](#pythia-repositories).



## Installation instructions

One may compile & install Pythia code base using Rust tools, but it's not recommended. Instead, we recommend to use the following two methods.

- Create an CloudLab experiment with Pythia & experiment enviornment(OpenStack) installed automatically

  - Accessable using [this CloudLab profile](https://www.cloudlab.us/p/Tracing-Pythia/pythia-openstack-opensource)

- Pull a Docker container that contains Pythia

  - WiP

## Usage

### In CloudLab, using pythia with Openstack

Please follow [this user guide](https://github.com/docc-lab/pythia/blob/master/user-guide.md) for

  - How to create an experiment with shared profile
  - How to setup initial search space for Pythia
  - How to execute Pythia
  - How to injection problem to OpenStack
  - How to use Pythia to analyze injected problems
  - How to do troubleshooting

### In docker image
No user guide for docker image for now since it's already compiled and it requires OpenStack enviornment to run experiment.
- But we might add some in the future.

### Getting Documentation of Pythia code base

***(If you plan to contribute, and needs documentation)***

After pulling the code, use `cargo doc --open --document-private-items`.
Documentation there includes how to install, documentation on the codebase,
etc.

The `pythia_server` folder contains an independent rust project, whose documentation
should be built separately, in the same way.

## Pythia repositories

- [This repo](https://github.com/docc-lab/reconstruction): Pythia agent and controller.
- [openstack-build-ubuntu](https://github.com/docc-lab/openstack-build-ubuntu): Cloudlab profile for setting up openstack.

## Forks of openstack projects

- [osprofiler](https://github.com/docc-lab/osprofiler): Many changes, we have to run this version for the rust code to work.
- [nova](https://github.com/docc-lab/nova): Includes more instrumentation and instrumentation fixes.
- [python-novaclient](https://github.com/docc-lab/python-novaclient): instrumentation for request types.
- [python-openstackclient](https://github.com/docc-lab/python-openstackclient): instrumentation for request types.
- [oslo.log](https://github.com/docc-lab/oslo.log): support to add log statements into traces.
- [osc_lib](https://github.com/docc-lab/osc_lib): instrumentation on the client side.
- [oslo.messaging](https://github.com/docc-lab/oslo.messaging): split asynchronous requests to capture concurrency correctly.
- [neutron](https://github.com/docc-lab/neutron): more instrumentation.
- [python-blazarclient](https://github.com/docc-lab/python-blazarclient): support for request types.

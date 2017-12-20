---
title: Introduction
nav-index: 0
---

Well-run datacenter application architectures are heavily instrumented to provide
detailed traces of messages and remote invocations. Reconstructing user sessions,
call graphs, transaction trees, and other structural information from these
messages, a process known as sessionization, is the foundation for a variety
of diagnostic, profiling, and monitoring tasks essential to the operation of the
datacenter.

Part of Strymon is a library called **"reconstruction"** which is able to
process these logging streams to reconstruct structural information about
user sessions in real time.

This work was published at EuroSys '17. Please refer to the
[**paper**](https://people.inf.ethz.ch/zchothia/papers/online-reconstruction-eurosys17.pdf)
for more a in-depth description of the implemented mechanisms, challenges and an evaluation
on top of real-world execution traces.

The library provides the Timely Dataflow operators to perform this task. It does
however not provide any functionality to parse or ingest log files. It is up to
the user to add this functionality by implementing the appropriate interfaces
found in the [API documentation](https://strymon-system.github.io/reconstruction/).

For more information about the semantics of these interfaces, please refer to
[ Concepts and Terminology](concepts).

### Running the example

The source code contains simple Timely Dataflow program demonstrating the usage of
the library. You can download and execute the example with the following commands:

```terminal
$ git clone https://github.com/strymon-system/reconstruction.git
$ cd reconstruction/
$ cargo run
```

#### Additional information:

 - Zaheer Chothia, John Liagouris, Desislava Dimitrova, Timothy Roscoe. [Online Reconstruction of Structural Information from Datacenter Logs](https://people.inf.ethz.ch/zchothia/papers/online-reconstruction-eurosys17.pdf). **EuroSys**, 2017.
 - [Summary article](https://blog.acolyer.org/2017/05/31/online-reconstruction-of-structural-information-from-datacenter-logs/) on Adrian Colyer's blog *the morning paper*.
 - [Project Page](http://strymon.systems.ethz.ch/real_time_analytics.html) on the Strymon Research website

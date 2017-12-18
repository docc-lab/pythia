---
title: Introduction
nav-index: 0
---

The basis for understanding the dynamics of a datacenter is logging,
and so many datacenters instrument their applications (and middleware) with
functionality to emit log records when messages are sent and/or received by
each service or application process.

Part of Strymon is a library called **"reconstruction"** which is able to
process log streams at gigabits per second and reconstructs user sessions
in real time with modest compute resources.

#### Additional information:

 - Zaheer Chothia, John Liagouris, Desislava Dimitrova, Timothy Roscoe. [Online Reconstruction of Structural Information from Datacenter Logs](https://people.inf.ethz.ch/zchothia/papers/online-reconstruction-eurosys17.pdf). **EuroSys**, 2017.
 - [Summary article](https://blog.acolyer.org/2017/05/31/online-reconstruction-of-structural-information-from-datacenter-logs/) on Adrian Colyer's blog *the morning paper*.

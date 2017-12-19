---
title: Concepts and Terminology
nav-index: 1
---

This document specifies the assumptions and requirements for reconstructing
structural user-session information from log traces.

The current [`reconstruction`](https://strymon-system.github.io/reconstruction/)
library does not assume any specific data format. Instead, it implements its
operators in terms of Rust traits, which allows users to plug in their own
message format.

### Model

This section introduces the terminology used by the `reconstruction` code. While
there are some similarities to the [Dapper](https://research.google.com/pubs/pub36356.html)
model, there are also some notable differences which are discussed below.

 
![Hierarchical application tracing.](https://strymon-system.github.io/assets/docs/reconstruction-transactions.svg){: .center-image }

This figure illustrates the concepts which forms the basis of session reconstruction.
In the example, there are two clients (`A` and `B`) and three sessions, two from
user `B`. 

#### Session
A session (also called a *trace*) refers to a group of related hierarchical
application activities (*spans*). A session can for example correspond to set
of nested RPC calls caused by a single external user request.

#### Message
Many datacenters instrument their applications (and middleware) with
functionality to emit log records when requests are sent and/or received by
each service or application process. These log records, which we call *messages*
are the data from which user sessions are reconstructed. Messages fed into the sessionization
operator need to provide access to these two properties (not shown in the figure above):

  1. A unique session identifier (the *trace id* in the Dapper model)
  2. The local system timestamp of the log record

If messages are additionally tagged with a *span*, the group of messages sharing
the same session identifier can be used to reconstruct structural information
about the *trace tree*. Messages are roughly equivalent to *annotations* in Dapper.

#### Span
A span (sometimes also called *transaction*) refers to a application-defined
unit of work, which has a start and and end time. A message belongs to exactly
one span and might be used to define the span boundaries. Spans can have
parent and child relationships. Note that in our model, there can be multiple
root spans per session.

#### Span ID
The *span ID* (sometimes called the *transaction ID*) identifies the span and
its position within a session. In contrast to Dapper, our span identifiers
additionally encode the position of the span in the trace tree, and thus implicitly
encode parent/child relationship of the span:

A span ID of `2-10` implies there is a root transaction 2 and nine other siblings.
For some applications, this information might be used to detect missing log records.

#### Trace Tree
The *trace tree* refers to the reconstructed hierarchical view of the logged
application activities. This tree representation can then be used for further
analysis. One example provided in the source code is the extraction of abstract
*tree shapes* which can be used to classify the different behaviors of requests.

#### Time Granularity
 The sessionization operator is built on top of Timely's fine-grained progress
tracking mechanism to reason about the timestamps of log records which are
still in flight. For this reason, the granularity with which timestamps are
tracked can have a huge impact on performance. The sessionization operator
allows this granularity to be configured through the `epoch_time` parameter.

#### Session Inactivity
Being an online system designed for incomplete instrumentation, the `reconstruction`
library needs to be able to close sessions even in the presence of missing messages.
For this reason, users have to specify the *inactivity time* (the
`session_time` parameter), which defines the period for which a session has to
be inactive for it to be considered closed.

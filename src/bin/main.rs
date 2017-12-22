// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

/// Driver program for an small example that reconstructs sessions and computes basic statistics.

extern crate abomonation;
#[macro_use] extern crate abomonation_derive;
extern crate timely;
extern crate reconstruction;

use std::collections::HashSet;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Input, Inspect, Map, Unary};

use reconstruction::{canonical_shape, service_calls};
use reconstruction::{SessionizableMessage, SpanId, SpanPosition, Service};
use reconstruction::operators::Sessionize;
use reconstruction::operators::stats::SumPerEpoch;

/// For this example, we assign integer timestamps to events and the time axis is specified in
/// terms of **milliseconds**.   For simplicity, we ignore time zones, leap seconds and other
/// quirks of real clocks and instead simply use a linear time axis that begins at zero and
/// increases monotonically.
///
/// The following parameters specify the behaviour of the streaming computation and define:
/// * _epochs_: the finest granularity of time that can be measured and tracked.
/// * _expiry delay_: the maximum interval that can elapse before a session is considered inactive.
const EPOCH_GRANULARITY: u64 = 1000;
const EXPIRY_DELAY: u64 = 5000;

/// Representative of an event captured by instrumentation.
///
/// Real datacenter logs contains significantly more detailed annotations (e.g. IP addresses of
/// communicating endpoints, payload sizes and formats, IP addresses of communicating endpoints).
/// The idea is to give a small set of attributes that still demonstrate what the reconstruction
/// pipeline is capable of.
#[derive(Debug, Clone, Abomonation)]
struct Message {
    session_id: String,
    time: u64,
    addr: SpanId,
    service: String,
}

impl SessionizableMessage for Message {
    fn time(&self) -> u64 {
        self.time
    }

    fn session(&self) -> &str {
        &self.session_id
    }
}

impl SpanPosition for Message {
    fn get_span_id(&self) -> &SpanId {
        &self.addr
    }
}

impl Service for Message {
    type Service = String;

    fn get_service(&self) -> &Self::Service {
        &self.service
    }
}

impl Message {
    fn new(session_id: &str, time: u64, addr: SpanId, service: &str) -> Message {
        Message {
            session_id: session_id.to_string(),
            time: time,
            addr: addr,
            service: service.to_string(),
        }
    }
}

fn main() {
    timely::execute_from_args(std::env::args(), move |computation| {
        let log_data = vec![
            Message::new("A", 1000, SpanId(vec![0]), "FrontendX"),
            Message::new("A", 2100, SpanId(vec![0, 1]), "BackendX1"),
            Message::new("B", 2500, SpanId(vec![0]), "FrontendX"),
            Message::new("A", 6100, SpanId(vec![0, 2]), "BackendX2"),
            Message::new("A", 6890, SpanId(vec![0, 1, 1]), "DatabaseX"),
            Message::new("B", 12100, SpanId(vec![1]), "FrontendY"),
            Message::new("B", 12200, SpanId(vec![1, 0]), "BackendY"),
            Message::new("B", 13500, SpanId(vec![2]), "FrontendZ"),
        ];

        let mut input = computation.dataflow(move |scope| {
            let (input, stream) = scope.new_input::<Message>();
            stream.unary_notify(Pipeline, "ShowEpochComplete", Vec::new(), |input, output, notificator| {
                   input.for_each(|time, data| {
                       output.session(&time).give_content(data);
                       notificator.notify_at(time);
                   });
                   notificator.for_each(|time, _, _| {
                       println!("{:04?} | Flushing messages with timestamp {0}", time.time().inner);
                   });
               });

            let sessions = stream.sessionize(EPOCH_GRANULARITY, EXPIRY_DELAY);

            // 1. Show grouped messages emitted from the session window.
            sessions.inspect_batch(
                move |t, ds| {
                    for d in ds {
                        println!("{:04?} | Reconstructed sessions: {:?}", t.inner, d)
                    }
                },
            );

            // 2. Count the number of spans (transactions) in the session tree.
            sessions.map(|session| {
                session.messages.iter()
                    .map(|message| &message.addr)
                    .collect::<HashSet<_>>()
                    .len()
            })
            .sum_per_epoch()
            .inspect(|&(t, c)| println!("{:04?} | Number of transactions: {}", t.inner, c));

            // 3. Count the number of root spans (i.e. at the top-most level)
            sessions.map(|session| {
                session.messages.iter()
                    .map(|message| &message.addr)
                    .filter(|addr| addr.0.len() == 1)
                    .collect::<HashSet<_>>()
                    .len()
            })
            .sum_per_epoch()
            .inspect(|&(t, c)| println!("{:04?} | Number of root transactions: {}", t.inner, c));

            // 4. Emit session durations (interval between earliest and last message in a tree)
            // TODO: make this generate per-epoch statistics (e.g. histogram of duration bins)
            sessions.flat_map(|session| {
                if session.messages.len() > 1 {
                    let first_msg = session.messages.iter().min_by_key(|msg| msg.time);
                    let last_msg = session.messages.iter().max_by_key(|msg| msg.time);
                    match (first_msg, last_msg) {
                        (Some(first), Some(last)) => Some((session.session, last.time - first.time)),
                        _ => None,
                    }
                } else {
                    None  // Lone messages occur at a single instant in time and do not represent a
                          // valid span on the time axis.
                }
            })
            .inspect_batch(
                move |t, items| {
                    for &(ref session_id, duration) in items {
                        println!("{:04?} | Duration of session {:?}: {}", t.inner, session_id, duration);
                    }
                },
            );

            // 5. Measure height of each trace tree -- i.e. maximum number of levels traversed from
            //    root during a depth-first traversal.
            sessions.map(|session| {
                (session.session, session.messages.iter().map(|m| m.addr.0.len()).max())
            })
            .inspect_batch(
                move |t, items| {
                    for &(ref session_id, depth) in items {
                        println!("{:04?} | Maximum nested transaction depth in session {:?}: {}", t.inner, session_id, depth.unwrap_or(0));
                    }
                },
            );

            // 6. Top-k shapes: emits degree of each node (span) encountered during a breadth-first scan.
            sessions.map(|session| {
                let paths: Vec<&SpanId> = session.messages.iter().map(|m| m.get_span_id()).collect();
                (session.session, canonical_shape(&paths))
            })
            .inspect_batch(
                move |t, items| {
                    for &(ref session_id, ref shape) in items {
                        println!("{:04?} | Transaction tree shape of session {:?}: {:?}", t.inner, session_id, shape);
                    }
                },
            );

            // 7: Extract transitive communicating service dependencies for each session
            sessions.map(|mut session| {
                (session.session, service_calls(&mut session.messages))
            })
            .inspect_batch(
                move |t, items| {
                    for &(ref session_id, ref pairs) in items {
                        println!("{:04?} | Service dependencies of session {:?}: {:?}", t.inner, session_id, pairs);
                    }
                },
            );

            input
        });

        let mut last_epoch = 0;
        for msg in log_data {
            let epoch = msg.time / EPOCH_GRANULARITY;
            if epoch != last_epoch {
                assert!(epoch > last_epoch);
                input.advance_to(epoch);
                last_epoch = epoch;
            }
            input.send(msg);
        }
        input.close();

        // Now that we have fed tuples into the stream, the streaming computation will proceed and
        // operators will be run in turn until all the inputs have been fully consumed.  In this
        // example the _entire_ computation runs implicitly once we return from this lambda,
        // however, in a larger program the execution would be controlled more finely by attaching
        // a special probe to the end of the operator chain and then invoking
        // ```computation.step_while(|| probe.less_than(input.time()));```
        // This allows feeding logs in smaller chunks and more precise control over how far the
        // computation can proceed, when operators are scheduled, rate limiting and more.
    }).unwrap();
}

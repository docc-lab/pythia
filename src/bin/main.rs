/// Driver program for an small example that reconstructs sessions and computes basic statistics.

#[macro_use] extern crate abomonation;
extern crate timely;
extern crate sessionize;

use std::collections::HashSet;

use abomonation::Abomonation;
use timely::dataflow::Scope;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Input, Inspect, Map, Unary};

use sessionize::SessionizableMessage;
use sessionize::operators::Sessionize;
use sessionize::operators::stats::SumPerEpoch;

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
#[derive(Debug, Clone)]
struct Message {
    session_id: String,
    time: u64,
    addr: Vec<i32>,
}

unsafe_abomonate!(Message: session_id, time, addr);

impl SessionizableMessage for Message {
    fn time(&self) -> u64 {
        self.time
    }

    fn session(&self) -> &str {
        &self.session_id
    }
}

impl Message {
    fn new(session_id: String, time: u64, addr: Vec<i32>) -> Message {
        Message {
            session_id: session_id,
            time: time,
            addr: addr,
        }
    }
}

fn main() {
    timely::execute_from_args(std::env::args(), move |computation| {
        let index = computation.index();
        let log_data = vec![
            Message::new("A".into(), 1000, vec![0]),
            Message::new("A".into(), 2100, vec![0, 1]),
            Message::new("B".into(), 2500, vec![0]),
            Message::new("A".into(), 6100, vec![0, 2]),
            Message::new("A".into(), 6890, vec![0, 1, 1]),
            Message::new("B".into(), 12100, vec![1]),
            Message::new("B".into(), 13500, vec![2]),
        ];

        let mut input = computation.dataflow(move |scope| {
            let (input, stream) = scope.new_input::<Message>();
            stream.unary_notify(Pipeline, "ShowEpochComplete", Vec::new(), |input, output, notificator| {
                   input.for_each(|time, data| {
                       output.session(&time).give_content(data);
                       notificator.notify_at(time);
                   });
                   notificator.for_each(|time, _, _| {
                       println!("done with time: {:?}", time.time());
                   });
               });

            let sessions = stream.sessionize(EPOCH_GRANULARITY, EXPIRY_DELAY);

            // 1. Show grouped messages emitted from the session window.
            sessions.inspect_batch(
                move |t, ds| {
                    for d in ds {
                        println!("{}: Final output {:?}: {:?}", index, t, d)
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
            .inspect(|&(t, c)| println!("trx,{},{}", t.inner, c));

            // 3. Count the number of root spans (i.e. at the top-most level)
            sessions.map(|session| {
                session.messages.iter()
                    .map(|message| &message.addr)
                    .filter(|addr| addr.len() == 1)
                    .collect::<HashSet<_>>()
                    .len()
            })
            .sum_per_epoch()
            .inspect(|&(t, c)| println!("root_trx,{},{}", t.inner, c));

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
                        println!("duration,{},{},{}", t.inner, session_id, duration)
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
                println!("began with time: {}", epoch);
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

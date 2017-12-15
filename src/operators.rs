use std::collections::{HashSet, HashMap};
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::Sync;

use abomonation::Abomonation;
use timely::ExchangeData;
use timely::dataflow::{Stream, Scope};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Map, Unary};
use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;

use super::SessionizableMessage;

/// Hash a value with the default `Hasher` (internal algorithm unspecified).
///
/// The specified value will be hashed with this hasher and then the resulting
/// hash will be returned.
fn hash_code<T: Hash>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

#[derive(Debug, Clone)]
pub struct MessagesForSession<Message: SessionizableMessage> {
    pub session: String,
    pub messages: Vec<Message>,
}

impl<Message: SessionizableMessage> Abomonation for MessagesForSession<Message> {
    unsafe fn entomb<W: ::std::io::Write>(&self, writer: &mut W) -> ::std::io::Result<()> {
        self.session.entomb(writer)?;
        self.messages.entomb(writer)?;
        Ok(())
    }

    unsafe fn exhume<'a, 'b>(&'a mut self, mut bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        let temp = bytes;
        bytes = if let Some(bytes) = self.session.exhume(temp) {
            bytes
        } else {
            return None;
        };
        let temp = bytes;
        bytes = if let Some(bytes) = self.messages.exhume(temp) {
            bytes
        } else {
            return None;
        };
        Some(bytes)
    }
}

pub trait Sessionize<S: Scope, Message: SessionizableMessage> {
    // epoch_time is the time of an epoch
    // session_time is the time out to wait after which a session is declared as closed
    fn sessionize(
        &self,
        epoch_time: u64,
        session_time: u64,
    ) -> Stream<S, MessagesForSession<Message>>;
}

pub trait CountNumFragmentsPerSessionPerEpoch<S: Scope> {
    fn count_num_fragments_per_session(&self) -> Stream<S, Vec<(u64, u64)>>;
}

pub trait Histogram<S: Scope, K: ::timely::Data, V: ExchangeData + Hash + Eq> {
    // Returns a stream of (timestamp, value-counts)
    fn histogram<F: Fn(&K) -> V + 'static>(
        &self,
        discretizer: F,
    ) -> Stream<S, (u64, Vec<(V, u64)>)>;
}

pub trait TopK<S: Scope, K: ::timely::Data, V: ExchangeData + Hash + Eq> {
    // Returns a stream of (timestamp, value-counts)
    fn topk<F: Fn(&K) -> V + 'static>(
        &self,
        discretizer: F,
        topk: u64,
    ) -> Stream<S, (u64, Vec<(V, u64)>)>;
}

pub trait FinalHistogram<S: Scope, V: ExchangeData + Hash + Eq> {
    fn final_histogram(&self) -> Stream<S, (u64, Vec<(V, u64)>)>;
}

// Assume everything is colocated here
impl<
    S: Scope<Timestamp = Product<RootTimestamp, u64>>,
    V: ExchangeData + Hash + Eq,
> FinalHistogram<S, V> for Stream<S, Vec<(V, u64)>> {
    fn final_histogram(&self) -> Stream<S, (u64, Vec<(V, u64)>)> {
        let exchange = Exchange::new(|_| 0);
        let mut value_counter: HashMap<S::Timestamp, HashMap<V, u64>> = HashMap::new();
        self.unary_notify(exchange, "histogram", vec![], move |input,
              output,
              notificator| {
            input.for_each(|epoch, values| {
                let histogram_for_epoch = value_counter
                    .entry(epoch.time().clone())
                    .or_insert_with(|| {
                        notificator.notify_at(epoch);
                        HashMap::new()
                    });
                for value_counts in values.drain(..) {
                    for value in value_counts {
                        let value_count = histogram_for_epoch.entry(value.0).or_insert(0);
                        *value_count += value.1;
                    }
                }
            });

            notificator.for_each(|ts, _count, _notify| {
                let values: Vec<(V, u64)> =
                    value_counter.remove(&ts).unwrap().into_iter().collect();
                output.session(&ts).give((ts.inner, values));
            });
        })
    }
}

// Workers compute partial counts per epoch (stage1), which are then aggregated
// by worker 0 to produce the global counts (stage2) for each epoch
impl<
    S: Scope<Timestamp = Product<RootTimestamp, u64>>,
    K: ::timely::Data + fmt::Debug,
    V: ExchangeData + Hash + Eq + Sync + fmt::Debug,
> TopK<S, K, V> for Stream<S, K> {
    fn topk<F: Fn(&K) -> V + 'static>(
        &self,
        discretizer: F,
        topk: u64,
    ) -> Stream<S, (u64, Vec<(V, u64)>)> {

        // suffle entries
        let exchange = Exchange::new(move |x| hash_code(x));

        let large_timestamp = RootTimestamp::new(u64::max_value() / 2);

        //self.inspect(|x| println!("TopK input: {:?}",x));
        let mut value_counter: HashMap<u64, HashMap<V, u64>> = HashMap::new();
        // count number of occurences for each entry per epoch
        let stage1 = self.map(move |k: K| discretizer(&k)).unary_notify(
            exchange,
            "histogram1",
            vec![large_timestamp],
            move |input, output, notificator| {
                // map: epoch -> tree signature -> number of occurences in epoch
                input.for_each(|epoch, values| {
                    notificator.notify_at(epoch.clone());
                    let epoch_hashmap = value_counter.entry(epoch.time().inner).or_insert_with(
                        HashMap::new,
                    );
                    for value in values.drain(..) {
                        let value_count = epoch_hashmap.entry(value).or_insert(0);
                        *value_count += 1;
                    }
                });
                //if value_counter.len() > 0 {
                //    println!("Partial HashMap size: {}",value_counter.len());
                //}
                // spit counts for each epoch upon notification
                notificator.for_each(|ts, _count, _notify| {
                    let ov = value_counter.remove(&ts.time().inner); //.expect("Hist1 Notify");
                    //println!("Epoch: {}, Values: {}",ts.time().inner,ov.is_some());
                    if ov.is_some() {
                        let v = ov.unwrap();
                        let values: Vec<(V, u64)> =
                            v.iter().map(|(k, v)| (k.clone(), *v)).collect();
                        //if value_counter.len() > 0 {
                        //  println!("Partial HashMap size: {}",value_counter.len());
                        //}
                        output.session(&ts).give(values);
                    }
                });
            },
        );

        let mut aggregated_per_epoch = HashMap::new();
        // send partial counts to worker 0 and aggregate them per epoch
        let stage2 = stage1.unary_notify(Exchange::new(move |_| 0), "histogram2", vec![large_timestamp], move |input, output, notificator| {
            input.for_each(|epoch, values| {
                notificator.notify_at(epoch.clone());
                let epoch_data = aggregated_per_epoch.entry(epoch.time().inner).or_insert_with(HashMap::new);
                for value in values.drain(..) {
                    for &(ref v, count) in &value {
                        let x = epoch_data.entry(v.clone()).or_insert(0);
                        *x += count;
                    }
                }
            });
            //println!("HashMap size: {}",aggregated_per_epoch.len());
            // spit top-k results for each epoch upon notification
            notificator.for_each(|ts, _count, _notify| {
                let oepoch_data = aggregated_per_epoch.remove(&ts.time().inner);//.expect("Hist2 Notify");
                if oepoch_data.is_some() {
                    let  epoch_data = oepoch_data.unwrap();
                    let mut values : Vec<(V, u64)> = epoch_data.iter().map(|(k, v)| (k.clone(), *v)).collect();
                    values.sort_by(|a, b| b.1.cmp(&a.1));
                    values.truncate(topk as usize);
                    //println!("Values len: {}",values.len());
                    output.session(&ts).give((ts.inner, values));
                }
            });
        });
        //stage2.inspect(|x| println!("TopK output: {:?}",x));
        stage2
    }
}

impl<
    S: Scope<Timestamp = Product<RootTimestamp, u64>>,
    K: ::timely::Data,
    V: ExchangeData + Hash + Eq + Sync,
> Histogram<S, K, V> for Stream<S, K> {
    fn histogram<F: Fn(&K) -> V + 'static>(
        &self,
        discretizer: F,
    ) -> Stream<S, (u64, Vec<(V, u64)>)> {
        let exchange = Exchange::new(move |_| 0);
        let mut value_counter: HashMap<V, u64> = HashMap::new();
        let mut num_values_seen: u64 = 0;
        let large_timestamp = RootTimestamp::new(u64::max_value() / 2);
        self.map(move |k: K| discretizer(&k)).unary_notify(
            exchange,
            "histogram",
            vec![large_timestamp],
            move |input, output, notificator| {
                input.for_each(|epoch, values| {
                    notificator.notify_at(epoch);
                    num_values_seen += 1;
                    for value in values.drain(..) {
                        let value_count = value_counter.entry(value).or_insert(0);
                        *value_count += 1;
                    }
                });

                notificator.for_each(|ts, _count, _notify| {
                    let values: Vec<(V, u64)> =
                        value_counter.iter().map(|(k, v)| (k.clone(), *v)).collect();
                    output.session(&ts).give((ts.inner, values));
                });
            },
        )
    }
}

struct SessionState<Message: SessionizableMessage> {
    messages: Vec<Message>,
    epoch_to_flush_at: u64,
}

impl<Message: SessionizableMessage> SessionState<Message> {
    fn new() -> SessionState<Message> {
        SessionState {
            messages: Vec::new(),
            epoch_to_flush_at: 0,
        }
    }

    fn in_same_session(&self, session_time: u64, msg_time: u64) -> bool {
        let last_msg = self.messages.last();
        if last_msg.is_some() {
            let last_time = last_msg.unwrap().time();
            assert!(last_time <= msg_time);
            msg_time - last_time <= session_time
        } else {
            false
        }
    }
}

impl<
    S: Scope<Timestamp = Product<RootTimestamp, u64>>,
    Message: SessionizableMessage,
> CountNumFragmentsPerSessionPerEpoch<S> for Stream<S, MessagesForSession<Message>> {
    fn count_num_fragments_per_session(&self) -> Stream<S, Vec<(u64, u64)>> {
        let exchange = Exchange::new(|messages: &MessagesForSession<Message>| {
            hash_code(&messages.session)
        });
        let mut fragment_counter: HashMap<String, u64> = HashMap::new();
        let mut num_messages_seen = 0;
        self.unary_notify(exchange, "count_num_fragments_per_session", vec![], move |input, output, notificator| {
            input.for_each(|epoch, data| {
                notificator.notify_at(epoch);
                for messages_for_session in data.drain(..) {
                    num_messages_seen += 1;
                    let count = fragment_counter.entry(messages_for_session.session.clone()).or_insert(0);
                    *count += 1;
                }
            });

            notificator.for_each(|ts, _count, _notify| {
               let mut count_occurences : HashMap<u64, u64> = HashMap::new();
               for count in fragment_counter.values() {
                   let count_to_increment = count_occurences.entry(*count).or_insert(0);
                   *count_to_increment += 1;
               }
               let count_occurences : Vec<(u64, u64)> = count_occurences.into_iter().collect();
               output.session(&ts).give(count_occurences);
            });
        })
    }
}

impl<
    S: Scope<Timestamp = Product<RootTimestamp, u64>>,
    Message: SessionizableMessage + ::std::fmt::Debug,
> Sessionize<S, Message> for Stream<S, Message> {
    fn sessionize(
        &self,
        epoch_time: u64,
        session_time: u64,
    ) -> Stream<S, MessagesForSession<Message>> {
        let num_epochs_for_session = (session_time + epoch_time - 1) / epoch_time;
        assert!(num_epochs_for_session > 0);
        let exchange = Exchange::new(|msg: &Message| hash_code(&msg.session()));

        // A buffer holding messages that is looked at in time order
        // epoch -> [vector of messages]
        let mut messages_for_time: HashMap<u64, Vec<Message>> = HashMap::new();

        // In flight sessions: session -> SessionState
        let mut active_sessions: HashMap<String, SessionState<Message>> = HashMap::new();

        // Sessions to check -> These sessions might have expired by this time
        let mut sessions_to_check: HashMap<u64, HashSet<String>> = HashMap::new();

        // Used to ensure monotonicity
        let mut last_time_seen: u64 = 0;

        let mut last_notification_epoch: u64 = 0;

        self.unary_notify(exchange, "Sessionize", vec![], move |input,
              output,
              notificator| {
            input.for_each(|epoch, data| for msg in data.drain(..) {
                assert!(
                    msg.time() / epoch_time == epoch.inner,
                    "Unexpected message and epoch time mismatch"
                );
                let message_buffer = messages_for_time.entry(epoch.inner).or_insert_with(
                    Vec::new,
                );
                message_buffer.push(msg.clone());
                notificator.notify_at(epoch.clone());
            });

            notificator.for_each(|ts, _count, notify| {
                assert!(ts.inner >= last_notification_epoch);
                last_notification_epoch = ts.inner;
                let mut out = output.session(&ts);
                let messages_for_time =
                    &mut messages_for_time.remove(&ts.inner).unwrap_or_else(Vec::new);
                messages_for_time.sort_by(|m1, m2| m1.time().cmp(&m2.time()));
                for msg in messages_for_time.drain(..) {
                    let msg_time = msg.time();
                    assert!(last_time_seen <= msg_time);
                    last_time_seen = msg_time;
                    let mut session_state = active_sessions
                        .entry(msg.session().to_owned())
                        .or_insert_with(SessionState::new);
                    if !session_state.in_same_session(session_time, msg_time) &&
                        !session_state.messages.is_empty()
                    {
                        out.give(MessagesForSession::<Message> {
                            session: msg.session().to_owned(),
                            messages: session_state.messages.clone(),
                        });
                        session_state.messages.clear();
                    }
                    session_state.messages.push(msg.clone());
                    session_state.epoch_to_flush_at = ts.inner + num_epochs_for_session;
                    sessions_to_check
                        .entry(session_state.epoch_to_flush_at)
                        .or_insert_with(|| {
                            notify.notify_at(ts.delayed(
                                &RootTimestamp::new(session_state.epoch_to_flush_at),
                            ));
                            HashSet::new()
                        })
                        .insert(msg.session().to_owned());
                }

                let mut sessions_to_remove: Vec<String> = Vec::new();

                for session in sessions_to_check.remove(&ts.inner).unwrap_or_default() {
                    if let Some(session_state) = active_sessions.get(&session) {
                        if session_state.epoch_to_flush_at <= ts.inner {
                            let session_copy = session.clone();
                            sessions_to_remove.push(session);
                            assert!(
                                !session_state.messages.is_empty(),
                                "Messages for session cannot be empty here"
                            );
                            out.give(MessagesForSession::<Message> {
                                session: session_copy,
                                messages: session_state.messages.clone(),
                            });
                        }
                    }
                }

                for session in sessions_to_remove {
                    active_sessions.remove(&session);
                }
            });
        })

    }
}

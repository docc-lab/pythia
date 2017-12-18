use std::collections::{HashSet, HashMap};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::marker::Sync;
use std::fmt;

use abomonation::Abomonation;
use timely::ExchangeData;
use timely::dataflow::{Stream, Scope};
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::{Map, Unary};
use timely::dataflow::operators::aggregation::Aggregate;
use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;

use SessionizableMessage;

use super::hash_code;
use super::MessagesForSession;

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

/// Emits total value (cumulative sum) of a numerical stream once per epoch.
///
/// Equivalent to an invocation of the following, only that is uses operators built into Timely.
/// ```
/// stream.accumulate_by_epoch(0, |sum, data| {
///     for &x in data.iter() {
///         *sum += x;
///     }
/// })
/// ```
pub trait SumPerEpoch<S: Scope> {
    fn sum_per_epoch(&self) -> Stream<S, (S::Timestamp, usize)>;
}

impl<S: Scope> SumPerEpoch<S> for Stream<S, usize> {
    fn sum_per_epoch(&self) -> Stream<S, (S::Timestamp, usize)> {
        self.unary_stream(Pipeline, "TagWithTime", move |input, output| {
                input.for_each(|time, data| {
                    let t = &*time.time();
                    output.session(&time).give_iterator(data.drain(..).map(|x| ((*t).clone(), x)));
                });
            })
            .map(|x| ((), x))
            .aggregate::<_,(S::Timestamp, usize),_,_,_>(
                |_key, (t, x), agg| { agg.0 = t; agg.1 += x; },
                |_key, agg| agg,
                |_key| 0)
    }
}

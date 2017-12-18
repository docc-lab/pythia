//! A collection of various basic statistical Timely operators.
//!
//! These operators are intended as building blocks for custom operators running
//! on top the stream of reconstructed user-sessions.

use std::collections::HashMap;
use std::hash::Hash;
use std::marker::Sync;

use timely::{Data, ExchangeData};
use timely::dataflow::{Stream, Scope};
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::{Map, Unary};
use timely::dataflow::operators::aggregation::Aggregate;
use timely::progress::nested::product::Product;
use timely::progress::timestamp::{Timestamp, RootTimestamp};

use SessionizableMessage;

use super::hash_code;
use super::MessagesForSession;

/// Computes a histogram of the values observed per epoch.
///
/// This operator takes a custom `discretizer` function, i.e. a function which extracts
/// a nominal variable (bin) `V` from a given input `K`. The frequency within each bin is
/// reported per epoch.
///
/// # Examples
///
/// ```
/// extern crate timely;
/// extern crate reconstruction;
///
/// use timely::dataflow::operators::{ToStream, Map, Capture};
/// use timely::dataflow::operators::capture::Extract;
/// use timely::progress::timestamp::RootTimestamp;
///
/// use reconstruction::operators::stats::Histogram;
///
/// fn main() {
///     let captured = timely::example(|scope| {
///         (0..10).to_stream(scope)
///            .histogram(|n| n % 2) // bins for `odd` and `even`
///            .map_in_place(|x| x.1.sort()) // the created bins are not sorted
///            .capture()
///     });
///
///     let extracted = captured.extract();
///     // half of the elements should be `even` (0), the other half should be `odd` (1)
///     assert_eq!(extracted, vec![(RootTimestamp::new(0), vec![(0, vec![(0, 5), (1, 5)])])]);
/// }
/// ```
pub trait Histogram<S: Scope, K, V> {
    type Epoch: Timestamp;

    /// Returns a stream of `(timestamp, (bin, frequency))`
    fn histogram<F>(&self, discretizer: F) -> Stream<S, (Self::Epoch, Vec<(V, u64)>)>
    where
        F: Fn(&K) -> V + 'static;
}

impl<S, K, V, T> Histogram<S, K, V> for Stream<S, K>
where
    S: Scope<Timestamp = Product<RootTimestamp, T>>,
    K: Data,
    V: ExchangeData + Hash + Eq + Sync,
    T: Timestamp,
{
    type Epoch = T;

    fn histogram<F>(&self, discretizer: F) -> Stream<S, (Self::Epoch, Vec<(V, u64)>)>
    where
        F: Fn(&K) -> V + 'static,
    {
        let exchange = Exchange::new(move |_| 0);
        let mut value_counter: HashMap<T, HashMap<V, u64>> = HashMap::new();

        self.map(move |k: K| discretizer(&k)).unary_notify(
            exchange,
            "histogram",
            vec![],
            move |input, output, notificator| {
                // map: epoch -> category -> number of occurrences in epoch
                input.for_each(|epoch, values| {
                    notificator.notify_at(epoch.clone());
                    let epoch_hashmap = value_counter
                        .entry(epoch.time().inner.clone())
                        .or_insert_with(HashMap::new);
                    for value in values.drain(..) {
                        let value_count = epoch_hashmap.entry(value).or_insert(0);
                        *value_count += 1;
                    }
                });

                // spit counts for each epoch upon notification
                notificator.for_each(|ts, _count, _notify| {
                    let ov = value_counter.remove(&ts.time().inner);
                    if let Some(v) = ov {
                        let values: Vec<(V, u64)> = v.into_iter().collect();
                        output.session(&ts).give((ts.inner.clone(), values));
                    }
                });
            },
        )
    }
}

/// Calculates the top K most frequent bins per epoch.
///
/// This operator takes a custom `discretizer` function, i.e. a function which extracts
/// a nominal variable (bin) `V` from a given input `K`.
/// It reports the `top_k` bins of with the highest frequency per epoch.
/// # Examples
///
/// ```
/// extern crate timely;
/// extern crate reconstruction;
///
/// use timely::dataflow::operators::{ToStream, Capture};
/// use timely::dataflow::operators::capture::Extract;
/// use timely::progress::timestamp::RootTimestamp;
///
/// use reconstruction::operators::stats::TopK;
///
/// fn main() {
///     let captured = timely::example(|scope| {
///         vec![('a', 5), ('b', 1), ('c', 2), ('d', 1)]
///            .to_stream(scope)
///            .topk(|&(_node, w)| w, 1)
///            .capture()
///     });
///
///     let extracted = captured.extract();
///     // the most common weight is `1` with a frequency of 2
///     assert_eq!(extracted, vec![(RootTimestamp::new(0), vec![(0, vec![(1, 2)])])]);
/// }
/// ```
pub trait TopK<S: Scope, K, V> {
    type Epoch: Timestamp;

    /// Returns a stream of `(timestamp, (bin, frequency))`
    fn topk<F>(&self, discretizer: F, top_k: u64) -> Stream<S, (Self::Epoch, Vec<(V, u64)>)>
    where
        F: Fn(&K) -> V + 'static;
}

impl<S, K, V, T> TopK<S, K, V> for Stream<S, K>
where
    S: Scope<Timestamp = Product<RootTimestamp, T>>,
    K: Data,
    V: ExchangeData + Hash + Eq + Sync,
    T: Timestamp,
{
    type Epoch = T;

    fn topk<F>(&self, discretizer: F, topk: u64) -> Stream<S, (Self::Epoch, Vec<(V, u64)>)>
    where
        F: Fn(&K) -> V + 'static,
    {
        // Workers compute partial counts per epoch (stage1), which are then aggregated
        // by worker 0 to produce the global counts (stage2) for each epoch

        // ship entries with the same key to the same worker
        let exchange = Exchange::new(move |x| hash_code(x));

        let mut value_counter: HashMap<T, HashMap<V, u64>> = HashMap::new();

        // count number of occurrences for each entry per epoch
        let stage1 = self.map(move |k: K| discretizer(&k)).unary_notify(
            exchange,
            "histogram1",
            vec![],
            move |input, output, notificator| {
                // map: epoch -> signature -> number of occurrences in epoch
                input.for_each(|epoch, values| {
                    notificator.notify_at(epoch.clone());
                    let epoch_hashmap = value_counter
                        .entry(epoch.time().inner.clone())
                        .or_insert_with(HashMap::new);
                    for value in values.drain(..) {
                        let value_count = epoch_hashmap.entry(value).or_insert(0);
                        *value_count += 1;
                    }
                });

                // spit counts for each epoch upon notification
                notificator.for_each(|ts, _count, _notify| {
                    let ov = value_counter.remove(&ts.time().inner);
                    if let Some(v) = ov {
                        let values: Vec<(V, u64)> = v.into_iter().collect();
                        output.session(&ts).give(values);
                    }
                });
            },
        );

        let mut aggregated_per_epoch: HashMap<T, HashMap<V, u64>> = HashMap::new();
        // send partial counts to worker 0 and aggregate them per epoch
        let stage2 = stage1.unary_notify(
            Exchange::new(move |_| 0),
            "histogram2",
            vec![],
            move |input, output, notificator| {
                input.for_each(|epoch, values| {
                    notificator.notify_at(epoch.clone());
                    let epoch_data = aggregated_per_epoch
                        .entry(epoch.time().inner.clone())
                        .or_insert_with(HashMap::new);
                    for value in values.drain(..) {
                        for &(ref v, count) in &value {
                            let x = epoch_data.entry(v.clone()).or_insert(0);
                            *x += count;
                        }
                    }
                });
                // spit top-k results for each epoch upon notification
                notificator.for_each(|ts, _count, _notify| {
                    let oepoch_data = aggregated_per_epoch.remove(&ts.time().inner);
                    if let Some(epoch_data) = oepoch_data {
                        let mut values: Vec<(V, u64)> = epoch_data.into_iter().collect();
                        values.sort_by(|a, b| b.1.cmp(&a.1));
                        values.truncate(topk as usize);
                        output.session(&ts).give((ts.inner.clone(), values));
                    }
                });
            },
        );

        stage2
    }
}

/// Counts the number of fragmented sessions observed after each epoch.
///
/// This is intended for debugging and validation purposes. It emits a stream
/// of `(number of fragments per session, frequency)` tuples. If a session consists
/// of more than a single fragment, this means that the session prematurely expired
/// and thus the the session have been split over multiple epochs. Thus if the frequency
/// of tuples where `number of fragments per session > 1` is high, this indicates
/// that the expiry delay is too short for all messages to arrive in time.
/// In such cases, it is recommended to increase the `session_time` window given to
/// the [`Sessionize`](../trait.Sessionize.html) operator.
pub trait CountNumFragmentsPerSessionPerEpoch<S: Scope> {
    /// Reports the frequency of the number of fragmented sessions observed after each epoch.
    fn count_num_fragments_per_session(&self) -> Stream<S, Vec<(u64, u64)>>;
}

impl<S: Scope, M: SessionizableMessage> CountNumFragmentsPerSessionPerEpoch<S>
    for Stream<S, MessagesForSession<M>> {
    fn count_num_fragments_per_session(&self) -> Stream<S, Vec<(u64, u64)>> {
        let exchange = Exchange::new(|messages: &MessagesForSession<M>| {
            hash_code(&messages.session)
        });
        let mut fragment_counter: HashMap<String, u64> = HashMap::new();
        self.unary_notify(
            exchange,
            "count_num_fragments_per_session",
            vec![],
            move |input, output, notificator| {
                input.for_each(|epoch, data| {
                    notificator.notify_at(epoch);
                    for messages_for_session in data.drain(..) {
                        let count = fragment_counter
                            .entry(messages_for_session.session.clone())
                            .or_insert(0);
                        *count += 1;
                    }
                });

                notificator.for_each(|ts, _count, _notify| {
                    let mut count_occurences: HashMap<u64, u64> = HashMap::new();
                    for count in fragment_counter.values() {
                        let count_to_increment = count_occurences.entry(*count).or_insert(0);
                        *count_to_increment += 1;
                    }
                    let count_occurences: Vec<(u64, u64)> = count_occurences.into_iter().collect();
                    output.session(&ts).give(count_occurences);
                });
            },
        )
    }
}

/// Emits total value (cumulative sum) of a numerical stream once per epoch.
///
/// Equivalent to an invocation of the following, only that is uses operators built into Timely.
/// ```rust
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
                output.session(&time).give_iterator(data.drain(..).map(
                    |x| ((*t).clone(), x),
                ));
            });
        }).map(|x| ((), x))
            .aggregate::<_, (S::Timestamp, usize), _, _, _>(
                |_key, (t, x), agg| {
                    agg.0 = t;
                    agg.1 += x;
                },
                |_key, agg| agg,
                |_key| 0,
            )
    }
}

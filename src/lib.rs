// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

extern crate abomonation;
#[macro_use] extern crate abomonation_derive;
extern crate timely;

pub mod operators;
pub mod tree_repr;

use std::cmp::Ordering;
use std::collections::HashMap;

use timely::ExchangeData;

extern crate redis;
use redis::Commands;

extern crate serde_json;
extern crate serde;
extern crate uuid;
extern crate chrono;
extern crate petgraph;

use petgraph::Graph;
use uuid::Uuid;

pub mod spans;
use spans::OSProfilerSpan;
use spans::OSProfilerEnum;

pub fn redis_main() {
    let event_list = get_matches(&"7399ea9b-8556-44c5-b3a6-b32f0949ee20".to_string()).unwrap();
    let trace = create_dag(event_list);
    println!("{:?}", trace);
}

#[derive(Debug)]
struct DAGNode {
    span: OSProfilerSpan
}

impl DAGNode {
    fn from_osp_span(event: &OSProfilerSpan) -> DAGNode {
        DAGNode {span: event.clone() }
    }
}

#[derive(Debug)]
struct DAGEdge {
    duration: chrono::Duration
}

fn create_dag(mut event_list: Vec<OSProfilerSpan>) -> Graph<DAGNode, DAGEdge> {
    event_list.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
    let base_id = event_list[0].base_id;
    let start_time = event_list[0].timestamp;
    let mut dag = Graph::<DAGNode, DAGEdge>::new();
    // Latest event with the same id, end if event already finished, start if it didn't
    let mut id_map = HashMap::new();
    let mut active_spans = HashMap::new();
    // The latest completed children span for each parent id
    let mut children_per_parent = HashMap::<Uuid, Option<Uuid>>::new();
    for event in event_list.iter() {
        assert!(event.base_id == base_id);
        assert!(start_time <= event.timestamp);
        let mynode = dag.add_node(DAGNode::from_osp_span(event));
        id_map.insert(event.trace_id, mynode);
        match event.variant {
            OSProfilerEnum::FunctionEntry(_) | OSProfilerEnum::RequestEntry(_) => {
                active_spans.insert(event.trace_id, mynode);
                children_per_parent.insert(event.trace_id, None);
                if event.parent_id != event.base_id {
                    match children_per_parent.get(&event.parent_id) {
                        Some(result) => {
                            match result {
                                Some(sibling_id) => {
                                    let sibling_node = id_map.get(sibling_id).unwrap();
                                    dag.add_edge(*sibling_node, mynode, DAGEdge {
                                        duration: event.timestamp - dag[*sibling_node].span.timestamp});
                                },
                                None => {
                                    let parent_node = id_map.get(&event.parent_id).unwrap();
                                    dag.add_edge(*parent_node, mynode, DAGEdge {
                                        duration: event.timestamp - dag[*parent_node].span.timestamp});
                                }
                            }
                        },
                        None => {
                            // Parent has finished execution before child starts - shouldn't happen
                            let parent_node = &dag[*id_map.get(&event.parent_id).unwrap()];
                            assert!(event.timestamp > parent_node.span.timestamp);
                            panic!("Parent of node {:?} not found: {:?}", event, parent_node);
                        }
                    }
                }
            },
            OSProfilerEnum::FunctionExit(_) | OSProfilerEnum::RequestExit(_) => {
                let start_span = active_spans.remove(&event.trace_id).unwrap();
                match children_per_parent.remove(&event.trace_id).unwrap() {
                    Some(child_id) => {
                        let child_node = id_map.get(&child_id).unwrap();
                        dag.add_edge(*child_node, mynode, DAGEdge {
                            duration: event.timestamp - dag[*child_node].span.timestamp});
                    },
                    None => {
                        dag.add_edge(start_span, mynode, DAGEdge {
                            duration: event.timestamp - dag[start_span].span.timestamp});
                    }
                }
            },
        }
    }
    dag
}

fn parse_field(field: &String) -> serde_json::Result<OSProfilerSpan> {
    serde_json::from_str(field)
}

fn get_matches(span_id: &String) -> redis::RedisResult<Vec<OSProfilerSpan>> {
    let client = redis::Client::open("redis://localhost:6379")?;
    let mut con = client.get_connection()?;
    let matches: Vec<String> = con.scan_match("osprofiler:".to_string() + span_id + "*")
        .unwrap().collect();
    let mut result = Vec::new();
    for key in matches {
        let dict_string: String = con.get(key)?;
        match parse_field(&dict_string) {
            Ok(span) => result.push(span),
            Err(e) => panic!("Problem while parsing {}: {}", dict_string, e),
        }
    }
    Ok(result)
}

pub type Timestamp = u64;
pub type TraceId = u32;
pub type Degree = u32;

/// A sessionizable message.
///
/// Sessionizion requires two properties for each recorded message:
///
///    - a session identifier
///    - the log record timestamp
pub trait SessionizableMessage: ExchangeData {
//    type Timestamp: ExchangeData;

    fn time(&self) -> Timestamp;
    fn session(&self) -> &str;
}

pub trait SpanPosition {
    fn get_span_id(&self) -> &SpanId;
}

/// An accessor trait for retrieving the service of a message.
pub trait Service {
    type Service: ExchangeData;

    /// Returns the service which sent or received this message.
    fn get_service(&self) -> &Self::Service;
}

#[derive(Debug, Clone, Abomonation)]
pub struct MessagesForSession<M: SessionizableMessage> {
    pub session: String,
    pub messages: Vec<M>,
}

// Method to convert a Vec<Vec<u32>> indicating paths through a tree to a canonical
// representation of the tree
//
// the result is a sequence of degrees of a BFS traversal of the graph.
pub fn canonical_shape<S: AsRef<Vec<TraceId>>>(paths: &[S]) -> Vec<Degree> {
    let mut position = vec![0; paths.len()];
    let mut degrees = vec![0];
    let mut offsets = vec![1]; // where do children start?

    if let Some(max_depth) = paths.iter().map(|p| p.as_ref().len()).max() {
        for depth in 0 .. max_depth {
            // advance each position based on its offset
            // ensure that the max degree of the associated node is at least as high as it should be.
            for index in 0..paths.len() {
                if paths[index].as_ref().len() > depth {
                    if depth > 0 {
                        position[index] = (offsets[position[index]] + paths[index].as_ref()[depth-1]) as usize;
                    }

                    degrees[position[index]] = ::std::cmp::max(degrees[position[index]], paths[index].as_ref()[depth] + 1);
                }
            }

            // add zeros and transform degrees to offsets.
            let mut last = 0;
            for &x in &degrees { last += x as usize; }

            while degrees.len() <= last {
                degrees.push(0);
                offsets.push(0);
            }

            for i in 1..degrees.len() {
                offsets[i] = offsets[i-1] + degrees[i-1];
            }

        }
    }

    degrees
}

/// Describes where a span is located within the trace tree.
///
/// A hierarchical identifier is returned (e.g. `1-2-0`) which reflects the level of nesting for a
/// given span and specifies a path from the root span to the given span node.  Concretely, the
/// span ID is a vector of non-negative integers where each index corresponds to the positions
/// encountered while traversing down the tree.  In this example, the spans are nested in three
/// levels and the index `1` indicates that there were two _root_ spans with three nodes below it
/// (hence `2` as the next index) and the particular span being referred to is the first of these
/// children.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Abomonation)]
pub struct SpanId(pub Vec<TraceId>);

impl SpanId {
    /// Returns `true` if `self` is a parent of the `child` in the tree hierarchy
    pub fn is_parent_of(&self, child: &SpanId) -> bool {
        self < child
    }
}

impl AsRef<Vec<TraceId>> for SpanId {
    fn as_ref(&self) -> &Vec<TraceId> {
        &self.0
    }
}

impl PartialOrd for SpanId {
    fn partial_cmp(&self, other: &SpanId) -> Option<Ordering> {
        let mut it1 = self.0.iter();
        let mut it2 = other.0.iter();
        loop {
            match (it1.next(), it2.next()) {
                (Some(p1), Some(p2)) => {
                    if p1 != p2 { return None } else { continue }
                }
                (None, Some(_)) => return Some(Ordering::Less),
                (Some(_), None) => return Some(Ordering::Greater),
                (None, None) => return Some(Ordering::Equal),
            }
        }
    }
}

/// Extracts `(source, destination)` service pairs from a trace tree.
///
/// This function returns a list of (transitively) communicating services pair.
/// Any service of a message in the parent span is considered the `source`
/// service, eventually resulting in invocations at any of the `destination`
/// services. For example, for three nested servic calls `A -> B -> C`, the
/// resulting list is `[(A, B), (A, C), (B, C)]`.
///
/// Takes a list of messages which implement `SpanPosition`, meaning the
/// messages must form a hierarchical trace tree encoded in their `SpanId`.
///
/// ### Note:
///
/// This function can mutate the ordering of the `messages` vector. The current
/// implementation sorts the messages in lexicographical order of the span ids.
pub fn service_calls<M>(messages: &mut Vec<M>) -> Vec<(M::Service, M::Service)>
    where M: SpanPosition + Service
{
    let mut pairs = Vec::new();

    // we sort the messages by lexicographical order of the span ids,
    // this ensures that children always immediately follow their parents
    messages.sort_by(|a, b| {
        let a = a.get_span_id();
        let b = b.get_span_id();
        a.as_ref().cmp(b.as_ref())
    });

    for send in 0..messages.len() {
        let send_msg = &messages[send];
        let send_span = send_msg.get_span_id();
        for recv in send+1..messages.len() {
            let recv_msg = &messages[recv];
            let recv_span = recv_msg.get_span_id();

            if !send_span.is_parent_of(recv_span) {
                // due to the lexicographical order of messages, once we see
                // the first non-child, we know we will never see another again
                break;
            } else {
                let send_service = send_msg.get_service().clone();
                let recv_service = recv_msg.get_service().clone();
                pairs.push((send_service, recv_service));
            }
        }
    }
    pairs
}

#[cfg(test)]
mod tests {
    use super::{canonical_shape, service_calls, SpanId, Service, SpanPosition};
    use std::cmp::Ordering;

    #[test]
    fn test_tree_shape() {
        assert_eq!(canonical_shape(&vec![SpanId(vec![0])]),
                   vec![1,0]);
        assert_eq!(canonical_shape(&vec![SpanId(vec![1])]),
                   vec![2,0,0]);
        assert_eq!(canonical_shape(&vec![SpanId(vec![0, 1])]),
                   vec![1,2,0,0]);
        assert_eq!(canonical_shape(&vec![SpanId(vec![2, 1, 3]), SpanId(vec![3])]),
                   vec![4,0,0,2,0,0,4,0,0,0,0]);
    }

    #[test]
    fn test_span_id_ordering() {
        let id = SpanId(vec![1, 0, 1]);
        assert_eq!(SpanId(vec![0]).partial_cmp(&id), None);
        assert_eq!(SpanId(vec![1]).partial_cmp(&id), Some(Ordering::Less));
        assert_eq!(SpanId(vec![1, 0]).partial_cmp(&id), Some(Ordering::Less));
        assert_eq!(SpanId(vec![1, 0, 0]).partial_cmp(&id), None);
        assert_eq!(SpanId(vec![1, 0, 1]).partial_cmp(&id), Some(Ordering::Equal));
        assert_eq!(SpanId(vec![1, 0, 2]).partial_cmp(&id), None);
        assert_eq!(SpanId(vec![1, 0, 1, 0]).partial_cmp(&id), Some(Ordering::Greater));
        assert_eq!(SpanId(vec![1, 0, 1, 0, 0]).partial_cmp(&id), Some(Ordering::Greater));
        assert_eq!(SpanId(vec![1, 0, 1, 0, 1]).partial_cmp(&id), Some(Ordering::Greater));
    }

    #[test]
    fn test_span_id_parent() {
        assert!(SpanId(vec![1]).is_parent_of(&SpanId(vec![1, 0])));
        assert!(SpanId(vec![1]).is_parent_of(&SpanId(vec![1, 0, 1])));
        assert!(SpanId(vec![1, 0]).is_parent_of(&SpanId(vec![1, 0, 2])));
        assert!(!SpanId(vec![1, 0]).is_parent_of(&SpanId(vec![1, 0])));
        assert!(!SpanId(vec![1, 0]).is_parent_of(&SpanId(vec![1, 1])));
        assert!(!SpanId(vec![1, 0]).is_parent_of(&SpanId(vec![1])));
    }

    #[test]
    fn test_services_calls() {
        struct Msg(SpanId, char);

        impl SpanPosition for Msg {
            fn get_span_id(&self) -> &SpanId { &self.0 }
        }

        impl Service for Msg {
            type Service = char;
            fn get_service(&self) -> &char { &self.1 }
        }

        assert_eq!(service_calls(&mut Vec::<Msg>::new()), vec![]);

        let mut messages = vec![
            Msg(SpanId(vec![0, 1, 0]), 'C'),
        ];

        assert_eq!(service_calls(&mut messages), vec![]);

        let mut messages = vec![
            Msg(SpanId(vec![0, 1, 0]), 'C'),
            Msg(SpanId(vec![0]), 'A'),
            Msg(SpanId(vec![0, 1]), 'B'),
        ];

        assert_eq!(service_calls(&mut messages), vec![('A', 'B'), ('A', 'C'), ('B', 'C')]);

        let mut messages = vec![
            Msg(SpanId(vec![1]), 'A'),
            Msg(SpanId(vec![0, 1, 0]), 'C'),
            Msg(SpanId(vec![0, 1]), 'B'),
        ];

        assert_eq!(service_calls(&mut messages), vec![('B', 'C')]);

        let mut messages = vec![
            Msg(SpanId(vec![0]), 'A'),
            Msg(SpanId(vec![0, 1]), 'C'),
            Msg(SpanId(vec![0, 0]), 'B'),
        ];

        assert_eq!(service_calls(&mut messages), vec![('A', 'B'), ('A', 'C')]);
    }
}

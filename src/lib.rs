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
use std::fmt;
use std::fmt::Display;

use timely::ExchangeData;

extern crate redis;
use redis::Commands;

extern crate serde_json;
extern crate serde;
extern crate uuid;
extern crate chrono;
extern crate petgraph;

use petgraph::{Graph, dot::Dot, graph::NodeIndex};
use uuid::Uuid;

pub mod spans;
use spans::OSProfilerSpan;
use spans::OSProfilerEnum;

pub fn redis_main() {
    let event_list = get_matches("ffd1560e-7928-437c-87e9-a712c85ed2ac").unwrap();
    let trace = create_dag(event_list);
    println!("{}", Dot::new(&trace));
    return;
    for p in std::fs::read_dir("/opt/stack/offline_profiling").unwrap() {
        let path = p.unwrap().path();
        // println!("Working on {:?}", path);
        if path.extension().unwrap() == "dot" {
            println!("Working on {:?}", path);
            let event_list = get_matches(path.file_name().unwrap().to_str().unwrap().split('.').next().unwrap()).unwrap();
            let trace = create_dag(event_list);
            println!("{}", Dot::new(&trace));
        }
    }
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

impl Display for DAGNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.span.variant {
            OSProfilerEnum::FunctionEntry(a) => {
                write!(f, "{} start: {}", self.span.trace_id, a.tracepoint_id)
            },
            OSProfilerEnum::RequestEntry(a) => {
                write!(f, "{} start: {}", self.span.trace_id, a.tracepoint_id)
            },
            OSProfilerEnum::Annotation(a) => {
                write!(f, "{} start: {}", self.span.trace_id, a.tracepoint_id)
            },
            _ => {
                write!(f, "{} end", self.span.trace_id)
            }
        }
    }
}

#[derive(Debug)]
struct DAGEdge {
    duration: chrono::Duration,
    variant: EdgeType
}

#[derive(Debug)]
enum EdgeType {
    ChildOf,
    FollowsFrom
}

impl Display for DAGEdge {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.variant {
            EdgeType::ChildOf => write!(f, "C{}", self.duration),
            EdgeType::FollowsFrom => write!(f, "F{}", self.duration)
        }
    }
}

type OSProfilerDAG = Graph<DAGNode, DAGEdge>;

fn create_dag(mut event_list: Vec<OSProfilerSpan>) -> OSProfilerDAG {
    let mut dag = Graph::<DAGNode, DAGEdge>::new();
    dag.add_events(&mut event_list);
    dag
}

trait MyTrait {
    fn add_events(&mut self, event_list: &mut Vec<OSProfilerSpan>);
    fn add_asynch(&mut self, trace_id: &Uuid, parent: NodeIndex);
}

impl MyTrait for OSProfilerDAG {
    fn add_events(&mut self, event_list: &mut Vec<OSProfilerSpan>) {
        event_list.sort_by(|a, b| {
            if a.timestamp == b.timestamp {
                match a.variant {
                    OSProfilerEnum::FunctionEntry(_) | OSProfilerEnum::RequestEntry(_) => Ordering::Less,
                    _ => Ordering::Greater
                }
            } else {
                a.timestamp.cmp(&b.timestamp)
            }
        });
        let base_id = event_list[0].base_id;
        let start_time = event_list[0].timestamp;
        // Latest event with the same id, end if event already finished, start if it didn't
        let mut id_map = HashMap::new();
        let mut active_spans = HashMap::new();
        // The latest completed children span for each parent id
        let mut children_per_parent = HashMap::<Uuid, Option<Uuid>>::new();
        children_per_parent.insert(event_list[0].base_id, None);
        // Map of asynchronous traces that start from this DAG -> parent node in DAG
        let mut asynch_traces = HashMap::new();
        for event in event_list.iter() {
            assert!(event.base_id == base_id);
            assert!(start_time <= event.timestamp);
            let mynode = self.add_node(DAGNode::from_osp_span(event));
            id_map.insert(event.trace_id, mynode);
            match &event.variant {
                OSProfilerEnum::FunctionEntry(_) | OSProfilerEnum::RequestEntry(_) => {
                    active_spans.insert(event.trace_id, mynode);
                    children_per_parent.insert(event.trace_id, None);
                    if event.parent_id == event.base_id {
                        match children_per_parent.get(&event.parent_id).unwrap() {
                            Some(sibling_id) => {
                                let sibling_node = id_map.get(sibling_id).unwrap();
                                self.add_edge(*sibling_node, mynode, DAGEdge {
                                    duration: event.timestamp - self[*sibling_node].span.timestamp,
                                    variant: EdgeType::ChildOf
                                });
                            },
                            None => {}
                        }
                    } else {
                        match children_per_parent.get(&event.parent_id) {
                            Some(result) => {
                                match result {
                                    Some(sibling_id) => {
                                        let sibling_node = id_map.get(sibling_id).unwrap();
                                        self.add_edge(*sibling_node, mynode, DAGEdge {
                                            duration: event.timestamp - self[*sibling_node].span.timestamp,
                                            variant: EdgeType::ChildOf
                                        });
                                    },
                                    None => {
                                        let parent_node = id_map.get(&event.parent_id).unwrap();
                                        self.add_edge(*parent_node, mynode, DAGEdge {
                                            duration: event.timestamp - self[*parent_node].span.timestamp,
                                            variant: EdgeType::ChildOf
                                        });
                                    }
                                }
                            },
                            None => {
                                // Parent has finished execution before child starts - shouldn't happen
                                let parent_node = &self[*id_map.get(&event.parent_id).unwrap()];
                                assert!(event.timestamp > parent_node.span.timestamp);
                                panic!("Parent of node {:?} not found: {:?}", event, parent_node);
                            }
                        }
                    }
                },
                OSProfilerEnum::Annotation(myspan) => {
                    match children_per_parent.get(&event.parent_id).unwrap() {
                        Some(sibling_id) => {
                            let sibling_node = id_map.get(sibling_id).unwrap();
                            self.add_edge(*sibling_node, mynode, DAGEdge {
                                duration: event.timestamp - self[*sibling_node].span.timestamp,
                                variant: EdgeType::ChildOf
                            });
                        },
                        None => {
                            let parent_node = id_map.get(&event.parent_id).unwrap();
                            self.add_edge(*parent_node, mynode, DAGEdge {
                                duration: event.timestamp - self[*parent_node].span.timestamp,
                                variant: EdgeType::ChildOf
                            });
                        }
                    }
                    asynch_traces.insert(myspan.info.child_id, mynode);
                },
                OSProfilerEnum::FunctionExit(_) | OSProfilerEnum::RequestExit(_) => {
                    let start_span = match active_spans.remove(&event.trace_id) {
                        Some(start_span) => start_span,
                        None => {
                            panic!("Start span not found: {:?}", event);
                        }
                    };
                    match children_per_parent.remove(&event.trace_id).unwrap() {
                        Some(child_id) => {
                            let child_node = id_map.get(&child_id).unwrap();
                            self.add_edge(*child_node, mynode, DAGEdge {
                                duration: event.timestamp - self[*child_node].span.timestamp,
                                variant: EdgeType::ChildOf
                            });
                        },
                        None => {
                            self.add_edge(start_span, mynode, DAGEdge {
                                duration: event.timestamp - self[start_span].span.timestamp,
                                variant: EdgeType::ChildOf
                            });
                        }
                    }
                },
            }
            children_per_parent.insert(event.parent_id, Some(event.trace_id));
        }
        for (trace_id, parent) in asynch_traces.iter() {
            self.add_asynch(trace_id, *parent);
        }
    }

    fn add_asynch(&mut self, trace_id: &Uuid, parent: NodeIndex) {
        let mut event_list = get_matches(&trace_id.to_hyphenated().to_string()).unwrap();
        if event_list.len() == 0 {
            return;
        }
        self.add_events(&mut event_list);
        let first_event = event_list.iter().fold(None, |min, x| match min {
            None => Some(x),
            Some(y) => Some(if x.timestamp < y.timestamp {x} else {y}),
        }).unwrap();
        let first_node = self.node_indices().find(|idx| {
            self[*idx].span.trace_id == first_event.trace_id
        }).unwrap();
        self.add_edge(parent, first_node, DAGEdge {
            duration: first_event.timestamp - self[parent].span.timestamp,
            variant: EdgeType::FollowsFrom
        });
    }
}

fn parse_field(field: &String) -> Result<OSProfilerSpan, String> {
    let result: OSProfilerSpan = serde_json::from_str(field).unwrap();
    if result.name == "asynch_request" {
        return match result.variant {
            OSProfilerEnum::Annotation(_) => Ok(result),
            _ => {
                println!("{:?}", result);
                Err("".to_string())
            }
        };
    }
    Ok(result)
}

fn get_matches(span_id: &str) -> redis::RedisResult<Vec<OSProfilerSpan>> {
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

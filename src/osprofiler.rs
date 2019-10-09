/// Stuff related to working with osprofiler
///

use std::collections::HashSet;
use std::collections::HashMap;
use std::cmp::Ordering;
use std::fmt;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use uuid::Uuid;
use chrono::NaiveDateTime;
use serde::de;
use redis::Commands;
use petgraph::{stable_graph::StableGraph, graph::NodeIndex};
use petgraph::Direction;

use trace::{DAGNode, DAGEdge, EdgeType};
use trace::Event;
use trace::EventEnum;

pub struct OSProfilerReader {
    redis_url: String,
    trace_cache: PathBuf,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OSProfilerDAG {
    pub g: StableGraph<DAGNode, DAGEdge>,
    pub base_id: Uuid,
    pub start_node: NodeIndex,
    pub end_node: NodeIndex
}

impl OSProfilerReader {
    pub fn from_settings(settings: &HashMap<String,String>) -> OSProfilerReader {
        let mut pythia_cache = PathBuf::from(settings.get("pythia_cache").unwrap());
        pythia_cache.push("traces");
        OSProfilerReader {
            redis_url: settings.get("redis_url").unwrap().to_string(),
            trace_cache: pythia_cache
        }

    }

    pub fn get_trace_from_base_id(&self, id: &str) -> OSProfilerDAG {
        match Uuid::parse_str(id) {
            Ok(uuid) => {
                match self.fetch_from_cache(&uuid) {
                    Some(result) => {
                        result
                    },
                    None => {
                        let event_list = self.get_matches(&uuid).unwrap();
                        let dag = OSProfilerDAG::from_event_list(
                            Uuid::parse_str(id).unwrap(), event_list, &self);
                        self.store_to_cache(&dag);
                        dag
                    }
                }
            },
            Err(_) => {
                panic!("Malformed UUID received as base ID: {}", id);
            }
        }
    }

    fn get_matches(&self, span_id: &Uuid) -> redis::RedisResult<Vec<OSProfilerSpan>> {
        let client = redis::Client::open(&self.redis_url[..])?;
        let mut con = client.get_connection()?;
        let matches: Vec<String> = con.scan_match(
            "osprofiler:".to_string() + &span_id.to_hyphenated().to_string() + "*")
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

    fn fetch_from_cache(&self, id: &Uuid) -> Option<OSProfilerDAG> {
        let mut cache_file = self.trace_cache.clone();
        cache_file.push(&id.to_hyphenated().to_string());
        cache_file.set_extension("json");
        match std::fs::File::open(cache_file) {
            Ok(file) => {
                let result: OSProfilerDAG = serde_json::from_reader(file).unwrap();
                Some(result)
            },
            Err(_) => None
        }
    }

    fn store_to_cache(&self, dag: &OSProfilerDAG) {
        std::fs::create_dir_all(self.trace_cache.as_path()).expect("Failed to create trace cache");
        let mut cache_file = self.trace_cache.clone();
        cache_file.push(&dag.base_id.to_hyphenated().to_string());
        cache_file.set_extension("json");
        let writer = std::fs::File::create(cache_file).unwrap();
        serde_json::to_writer(writer, dag).expect("Failed to write trace to cache");
    }
}

impl OSProfilerDAG {
    pub fn new() -> OSProfilerDAG {
        let dag = StableGraph::<DAGNode, DAGEdge>::new();
        OSProfilerDAG {
            g: dag, base_id: Uuid::nil(), start_node: NodeIndex::end(), end_node: NodeIndex::end()
        }
    }

    fn from_event_list(id: Uuid, mut event_list: Vec<OSProfilerSpan>, reader: &OSProfilerReader) -> OSProfilerDAG {
        let mut mydag = OSProfilerDAG::new();
        mydag.base_id = id;
        mydag.add_events(&mut event_list, reader);
        mydag
    }

    pub fn can_reach_from_node(&self, trace_id: Uuid, nidx: NodeIndex) -> bool {
        let mut cur_nidx = nidx;
        loop {
            if self.g[cur_nidx].span.trace_id == trace_id {
                return true;
            }
            let next_nids = self.g.neighbors_directed(cur_nidx, Direction::Outgoing).collect::<Vec<_>>();
            if next_nids.len() == 0 {
                return false;
            } else if next_nids.len() == 1 {
                cur_nidx = next_nids[0];
            } else {
                for next_nidx in next_nids {
                    if self.can_reach_from_node(trace_id, next_nidx) {
                        return true;
                    }
                }
                return false;
            }
        }
    }

    fn _get_start_end_nodes(&self) -> (NodeIndex, NodeIndex) {
        let mut smallest_time = NaiveDateTime::parse_from_str("3000/01/01 01:01", "%Y/%m/%d %H:%M").unwrap();
        let mut largest_time = NaiveDateTime::parse_from_str("1000/01/01 01:01", "%Y/%m/%d %H:%M").unwrap();
        let mut start = NodeIndex::end();
        let mut end = NodeIndex::end();
        for i in self.g.node_indices() {
            if self.g[i].span.timestamp > largest_time {
                end = i;
                largest_time = self.g[i].span.timestamp;
            }
            if self.g[i].span.timestamp < smallest_time {
                start = i;
                smallest_time = self.g[i].span.timestamp;
            }
        }
        (start, end)
    }

    pub fn possible_end_nodes(&self) -> Vec<NodeIndex> {
        let mut result = Vec::new();
        for i in self.g.node_indices() {
            if self.g.neighbors_directed(i, Direction::Outgoing).count() == 0 {
                result.push(i);
            }
        }
        result
    }

    fn add_events(&mut self, event_list: &mut Vec<OSProfilerSpan>, reader: &OSProfilerReader) -> Option<NodeIndex> {
        // Returns last added node
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
        let mut tracepoint_id_map: HashMap::<Uuid, String> = HashMap::new();
        // Latest event with the same id, end if event already finished, start if it didn't
        let mut id_map = HashMap::new();
        let mut active_spans = HashMap::new();
        // The latest completed children span for each parent id
        let mut children_per_parent = HashMap::<Uuid, Option<Uuid>>::new();
        children_per_parent.insert(event_list[0].base_id, None);
        // Map of asynchronous traces that start from this DAG -> parent node in DAG
        let mut asynch_traces = HashMap::new();
        let mut waiters = HashMap::<Uuid, NodeIndex>::new();
        let mut wait_spans = HashSet::<Uuid>::new();
        let mut add_next_to_waiters = false;
        let mut wait_for = Vec::<Uuid>::new();
        let mut nidx = None;
        for (idx, event) in event_list.iter().enumerate() {
            assert!(event.base_id == base_id);
            assert!(start_time <= event.timestamp);
            let mut mynode = DAGNode::from_osp_span(event);
            mynode.span.tracepoint_id = event.get_tracepoint_id(&mut tracepoint_id_map);
            // Don't add asynch_wait into the DAGs
            nidx = match &event.variant {
                OSProfilerEnum::WaitAnnotation(variant) => {
                    wait_for.push(variant.info.wait_for);
                    None
                },
                _ => {
                    if wait_spans.contains(&mynode.span.trace_id) {
                        None
                    } else {
                        let nidx = self.g.add_node(mynode);
                        id_map.insert(event.trace_id, nidx);
                        if self.start_node == NodeIndex::end() {
                            self.start_node = nidx;
                        }
                        Some(nidx)
                    }
                }
            };
            if add_next_to_waiters && !nidx.is_none() {
                for waiter in wait_for.iter() {
                    waiters.insert(*waiter, nidx.unwrap());
                }
                wait_for = vec!();
                add_next_to_waiters = false;
            }
            match &event.variant {
                OSProfilerEnum::WaitAnnotation(_) => {
                    wait_spans.insert(event.trace_id);
                },
                OSProfilerEnum::FunctionEntry(_) | OSProfilerEnum::RequestEntry(_) => {
                    active_spans.insert(event.trace_id, nidx.unwrap());
                    children_per_parent.insert(event.trace_id, None);
                    if event.parent_id == event.base_id {
                        match children_per_parent.get(&event.parent_id).unwrap() {
                            Some(sibling_id) => {
                                let sibling_node = id_map.get(sibling_id).unwrap();
                                self.g.add_edge(*sibling_node, nidx.unwrap(), DAGEdge {
                                    duration: (event.timestamp - self.g[*sibling_node].span.timestamp
                                               ).to_std().unwrap(),
                                    variant: EdgeType::ChildOf
                                });
                            },
                            None => {
                                if idx != 0 { panic!("I don't know when this happens"); }
                            }
                        }
                    } else {
                        match children_per_parent.get(&event.parent_id) {
                            Some(result) => {
                                match result {
                                    Some(sibling_id) => {
                                        let sibling_node = id_map.get(sibling_id).unwrap();
                                        self.g.add_edge(*sibling_node, nidx.unwrap(), DAGEdge {
                                            duration: (event.timestamp - self.g[*sibling_node].span.timestamp
                                                       ).to_std().unwrap(),
                                            variant: EdgeType::ChildOf
                                        });
                                    },
                                    None => {
                                        let parent_node = id_map.get(&event.parent_id).unwrap();
                                        self.g.add_edge(*parent_node, nidx.unwrap(), DAGEdge {
                                            duration: (event.timestamp - self.g[*parent_node].span.timestamp
                                                       ).to_std().unwrap(),
                                            variant: EdgeType::ChildOf
                                        });
                                    }
                                }
                            },
                            None => {
                                // Parent has finished execution before child starts - shouldn't happen
                                let parent_node = &self.g[*id_map.get(&event.parent_id).unwrap()];
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
                            self.g.add_edge(*sibling_node, nidx.unwrap(), DAGEdge {
                                duration: (event.timestamp - self.g[*sibling_node].span.timestamp
                                          ).to_std().unwrap(),
                                variant: EdgeType::ChildOf
                            });
                        },
                        None => {
                            let parent_node = id_map.get(&event.parent_id).unwrap();
                            self.g.add_edge(*parent_node, nidx.unwrap(), DAGEdge {
                                duration: (event.timestamp - self.g[*parent_node].span.timestamp
                                          ).to_std().unwrap(),
                                variant: EdgeType::ChildOf
                            });
                        }
                    }
                    asynch_traces.insert(myspan.info.child_id, nidx.unwrap());
                },
                OSProfilerEnum::FunctionExit(_) | OSProfilerEnum::RequestExit(_) => {
                    if nidx.is_none() {
                        add_next_to_waiters = true;
                    } else {
                        let start_span = active_spans.remove(&event.trace_id).unwrap();
                        match children_per_parent.remove(&event.trace_id).unwrap() {
                            Some(child_id) => {
                                let child_node = id_map.get(&child_id).unwrap();
                                self.g.add_edge(*child_node, nidx.unwrap(), DAGEdge {
                                    duration: (event.timestamp - self.g[*child_node].span.timestamp
                                              ).to_std().unwrap(),
                                    variant: EdgeType::ChildOf
                                });
                            },
                            None => {
                                self.g.add_edge(start_span, nidx.unwrap(), DAGEdge {
                                    duration: (event.timestamp - self.g[start_span].span.timestamp
                                              ).to_std().unwrap(),
                                    variant: EdgeType::ChildOf
                                });
                            }
                        }
                    }
                },
            }
            if !nidx.is_none() {
                children_per_parent.insert(event.parent_id, Some(event.trace_id));
            }
        }
        self.end_node = match nidx {
            Some(nid) => nid,
            None => self.start_node
        };
        for (trace_id, parent) in asynch_traces.iter() {
            let last_node = self.add_asynch(trace_id, *parent, reader);
            match &last_node {
                Some(node) => {
                    if self.g[*node].span.timestamp > self.g[self.end_node].span.timestamp {
                        self.end_node = *node;
                    }
                    match &waiters.get(trace_id) {
                        Some(parent) => {
                            self.g.add_edge(*node, **parent, DAGEdge {
                                duration: (self.g[**parent].span.timestamp
                                           - self.g[*node].span.timestamp).to_std().unwrap(),
                                variant: EdgeType::FollowsFrom
                            });
                        },
                        None => {}
                    }
                },
                None => {}
            };
        }
        nidx
    }

    fn add_asynch(&mut self, trace_id: &Uuid, parent: NodeIndex, reader: &OSProfilerReader) -> Option<NodeIndex> {
        let mut event_list = reader.get_matches(trace_id).unwrap();
        if event_list.len() == 0 {
            return None;
        }
        let last_node = self.add_events(&mut event_list, reader);
        let first_event = event_list.iter().fold(None, |min, x| match min {
            None => Some(x),
            Some(y) => Some(if x.timestamp < y.timestamp {x} else {y}),
        }).unwrap();
        let first_node = self.g.node_indices().find(|idx| {
            self.g[*idx].span.trace_id == first_event.trace_id
        }).unwrap();
        self.g.add_edge(parent, first_node, DAGEdge {
            duration: (first_event.timestamp - self.g[parent].span.timestamp
                      ).to_std().unwrap(),
            variant: EdgeType::FollowsFrom
        });
        last_node
    }
}

fn parse_field(field: &String) -> Result<OSProfilerSpan, String> {
    let result: OSProfilerSpan = serde_json::from_str(field).unwrap();
    if result.name == "asynch_request" || result.name == "asynch_wait" {
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

impl DAGNode {
    fn from_osp_span(event: &OSProfilerSpan) -> DAGNode {
        DAGNode { span: Event {
            trace_id: event.trace_id,
            parent_id: event.parent_id,
            tracepoint_id: event.tracepoint_id.clone(),
            timestamp: event.timestamp,
            variant: match event.variant {
                OSProfilerEnum::FunctionEntry(_) | OSProfilerEnum::RequestEntry(_)
                    | OSProfilerEnum::WaitAnnotation(_) =>
                    EventEnum::Entry,
                OSProfilerEnum::FunctionExit(_) | OSProfilerEnum::RequestExit(_) =>
                    EventEnum::Exit,
                OSProfilerEnum::Annotation(_) => EventEnum::Annotation,
            }
        }}
    }
}

impl OSProfilerSpan {
    pub fn get_tracepoint_id(&self, map: &mut HashMap<Uuid, String>) -> String {
        // The map needs to be initialized and passed to it from outside :(
        match &self.variant {
            OSProfilerEnum::FunctionEntry(s) => {
                map.insert(self.trace_id, s.tracepoint_id.clone());
                s.tracepoint_id.clone()
            },
            OSProfilerEnum::RequestEntry(s) => {
                map.insert(self.trace_id, s.tracepoint_id.clone());
                s.tracepoint_id.clone()
            },
            OSProfilerEnum::WaitAnnotation(s) => {
                map.insert(self.trace_id, s.tracepoint_id.clone());
                s.tracepoint_id.clone()
            },
            OSProfilerEnum::Annotation(s) => {
                s.tracepoint_id.clone()
            },
            OSProfilerEnum::RequestExit(_) | OSProfilerEnum::FunctionExit(_) => {
                map.remove(&self.trace_id).unwrap()
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct OSProfilerSpan {
    pub trace_id: Uuid,
    pub parent_id: Uuid,
    project: String,
    pub name: String,
    pub base_id: Uuid,
    service: String,
    #[serde(skip_deserializing)]
    pub tracepoint_id: String,
    #[serde(deserialize_with = "from_osp_timestamp")]
    pub timestamp: NaiveDateTime,
    #[serde(flatten)]
    pub variant: OSProfilerEnum
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(untagged)]
pub enum OSProfilerEnum {
    WaitAnnotation(WaitAnnotationSpan),
    FunctionEntry(FunctionEntrySpan),
    FunctionExit(FunctionExitSpan),
    RequestEntry(RequestEntrySpan),
    Annotation(AnnotationSpan),
    RequestExit(RequestExitSpan),
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct WaitAnnotationSpan {
    pub info: WaitAnnotationInfo,
    pub tracepoint_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct WaitAnnotationInfo {
    function: FunctionEntryFunction,
    thread_id: u64,
    host: String,
    tracepoint_id: String,
    pid: u64,
    pub wait_for: Uuid
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct AnnotationSpan {
    pub info: AnnotationInfo,
    pub tracepoint_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct AnnotationInfo {
    thread_id: u64,
    host: String,
    pub tracepoint_id: String,
    pub child_id: Uuid,
    pid: u64
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct RequestEntrySpan {
    info: RequestEntryInfo,
    pub tracepoint_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
struct RequestEntryInfo {
    request: RequestEntryRequest,
    thread_id: u64,
    host: String,
    pub tracepoint_id: String,
    pid: u64
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
struct RequestEntryRequest {
    path: String,
    scheme: String,
    method: String,
    query: String
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct RequestExitSpan {
    info: RequestExitInfo,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
struct RequestExitInfo { host: String }

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct FunctionExitSpan {
    info: FunctionExitInfo,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
struct FunctionExitInfo {
    function: FunctionExitFunction,
    host: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
struct FunctionExitFunction { result: String }

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct FunctionEntrySpan {
    info: FunctionEntryInfo,
    pub tracepoint_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
struct FunctionEntryInfo {
    function: FunctionEntryFunction,
    thread_id: u64,
    host: String,
    tracepoint_id: String,
    pid: u64
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
struct FunctionEntryFunction { name: String, args: String, kwargs: String }

struct NaiveDateTimeVisitor;

impl<'de> de::Visitor<'de> for NaiveDateTimeVisitor {
    type Value = NaiveDateTime;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "a string represents chrono::NaiveDateTime")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S.%f") {
            Ok(t) => Ok(t),
            Err(_) => Err(de::Error::invalid_value(de::Unexpected::Str(s), &self)),
        }
    }
}

fn from_osp_timestamp<'de, D>(d: D) -> Result<NaiveDateTime, D::Error>
where
    D: de::Deserializer<'de>,
{
    d.deserialize_str(NaiveDateTimeVisitor)
}

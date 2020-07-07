//! General trace implementation
//!

use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::sync::Mutex;
use std::time::Duration;

use bimap::BiMap;
use chrono::NaiveDateTime;
use petgraph::dot::Dot;
use petgraph::graph::NodeIndex;
use petgraph::stable_graph::StableGraph;
use petgraph::Direction;
use serde::de;
use serde::ser;
use serde::{Deserialize, Serialize};
use std::path::Path;
use uuid::Uuid;

use pythia_common::RequestType;

use std::collections::HashMap;

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub enum Value {
    Int(u64),
    Str(String),
}

/// A general-purpose trace which does not contain application-specific things
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Trace {
    pub g: StableGraph<Event, DAGEdge>,
    pub base_id: Uuid,
    pub start_node: NodeIndex,
    pub end_node: NodeIndex,
    pub request_type: RequestType,
    pub duration: Duration,
    /// used by osprofiler to find keys to delete from redis
    pub keys: Vec<String>,
}

impl Trace {
    pub fn new(base_id: &Uuid) -> Self {
        Trace {
            g: StableGraph::new(),
            base_id: base_id.clone(),
            start_node: NodeIndex::end(),
            end_node: NodeIndex::end(),
            request_type: RequestType::Unknown,
            duration: Duration::new(0, 0),
            keys: Vec::new(),
        }
    }

    pub fn to_file(&self, file: &Path) {
        let writer = std::fs::File::create(file).unwrap();
        serde_json::to_writer(writer, self).ok();
    }

    /// Does a forward-scan of nodes for the node with the given trace_id
    pub fn can_reach_from_node(&self, trace_id: Uuid, nidx: NodeIndex) -> bool {
        let mut cur_nidx = nidx;
        loop {
            if self.g[cur_nidx].trace_id == trace_id {
                return true;
            }
            let next_nids = self
                .g
                .neighbors_directed(cur_nidx, Direction::Outgoing)
                .collect::<Vec<_>>();
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

    /// Return nodes with outdegree == 0
    pub fn possible_end_nodes(&self) -> Vec<NodeIndex> {
        let mut result = Vec::new();
        for i in self.g.node_indices() {
            if self.g.neighbors_directed(i, Direction::Outgoing).count() == 0 {
                result.push(i);
            }
        }
        result
    }

    fn _get_start_end_nodes(&self) -> (NodeIndex, NodeIndex) {
        let mut smallest_time =
            NaiveDateTime::parse_from_str("3000/01/01 01:01", "%Y/%m/%d %H:%M").unwrap();
        let mut largest_time =
            NaiveDateTime::parse_from_str("1000/01/01 01:01", "%Y/%m/%d %H:%M").unwrap();
        let mut start = NodeIndex::end();
        let mut end = NodeIndex::end();
        for i in self.g.node_indices() {
            if self.g[i].timestamp > largest_time {
                end = i;
                largest_time = self.g[i].timestamp;
            }
            if self.g[i].timestamp < smallest_time {
                start = i;
                smallest_time = self.g[i].timestamp;
            }
        }
        (start, end)
    }

    /// Remove branches that do not end in the ending node
    pub fn prune(&mut self) {
        let mut removed_count = 0;
        loop {
            let mut iter = self.g.externals(Direction::Outgoing);
            let mut end_node = match iter.next() {
                Some(nidx) => nidx,
                None => {
                    break;
                }
            };
            if end_node == self.end_node {
                end_node = match iter.next() {
                    None => {
                        break;
                    }
                    Some(n) => n,
                };
            }
            let mut cur_nodes = vec![end_node];
            loop {
                let cur_node = match cur_nodes.pop() {
                    None => {
                        break;
                    }
                    Some(i) => i,
                };
                let out_neighbors = self
                    .g
                    .neighbors_directed(cur_node, Direction::Outgoing)
                    .collect::<Vec<_>>();
                if out_neighbors.len() >= 1 {
                    continue;
                }
                let neighbors = self
                    .g
                    .neighbors_directed(cur_node, Direction::Incoming)
                    .collect::<Vec<_>>();
                self.g.remove_node(cur_node);
                removed_count += 1;
                for n in neighbors {
                    cur_nodes.push(n);
                }
            }
        }
        eprintln!("Removed {} nodes when pruning", removed_count);
    }
}

impl fmt::Display for Trace {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // println!("Value is: {}", Value::Int(val));
        write!(f, "{}", Dot::new(&self.g))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Event {
    /// A trace id is shared between two ends of a span, otherwise it should be unique to events
    pub trace_id: Uuid,
    /// A tracepoint id represents a place in code
    pub tracepoint_id: TracepointID,
    pub timestamp: NaiveDateTime,
    /// Synthetic nodes are added to preserve the hierarchy, they are not actual events that
    /// happened
    pub is_synthetic: bool,
    pub variant: EventType,
    pub key_value_pair: HashMap<String, Value>,
}

#[derive(Serialize, Deserialize, Hash, Debug, Clone, Copy, Eq, PartialEq)]
pub enum EventType {
    Entry,
    Exit,
    /// Annotations are free-standing events that are not part of a span
    Annotation,
}

impl Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.variant {
            EventType::Entry => write!(f, "{} start: {}", self.trace_id, self.tracepoint_id),
            EventType::Annotation => write!(f, "{}: {}", self.trace_id, self.tracepoint_id),
            EventType::Exit => write!(f, "{} end", self.trace_id),
        }
    }
}

impl PartialEq<Event> for Event {
    fn eq(&self, other: &Event) -> bool {
        self.tracepoint_id == other.tracepoint_id && self.variant == other.variant
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct DAGEdge {
    pub duration: Duration,
    pub variant: EdgeType,
}

/// These edge types are taken from OpenTracing, but they are not used much in the codebase
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum EdgeType {
    ChildOf,
    FollowsFrom,
}

impl Display for DAGEdge {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.variant {
            EdgeType::ChildOf => write!(f, "{}: C", self.duration.as_nanos()),
            EdgeType::FollowsFrom => write!(f, "{}: F", self.duration.as_nanos()),
        }
    }
}

/// A trace node is an abstract node, so it doesn't have a timestamp or trace id, it just has a
/// tracepoint id and variant.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, Eq, PartialEq)]
pub struct TraceNode {
    pub tracepoint_id: TracepointID,
    pub variant: EventType,
}

impl TraceNode {
    pub fn from_event(event: &Event) -> Self {
        TraceNode {
            tracepoint_id: event.tracepoint_id,
            variant: event.variant,
        }
    }
}

impl Display for TraceNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.variant {
            EventType::Entry => write!(f, "{}: start", self.tracepoint_id),
            EventType::Exit => write!(f, "{}: end", self.tracepoint_id),
            EventType::Annotation => write!(f, "{}", self.tracepoint_id),
        }
    }
}

impl PartialEq<TraceNode> for Event {
    fn eq(&self, other: &TraceNode) -> bool {
        self.tracepoint_id == other.tracepoint_id && self.variant == other.variant
    }
}

lazy_static! {
    static ref TRACEPOINT_ID_MAP: Mutex<BiMap<String, usize>> = Mutex::new(BiMap::new());
}

/// We do some tricks to keep tracepoint ids as `usize`s so it uses less memory than strings.
#[derive(Hash, Clone, Copy, Eq, PartialEq)]
pub struct TracepointID {
    id: usize,
}

impl TracepointID {
    pub fn to_string(&self) -> String {
        TRACEPOINT_ID_MAP
            .lock()
            .unwrap()
            .get_by_right(&self.id)
            .unwrap()
            .clone()
    }

    pub fn from_str(s: &str) -> Self {
        let mut map = TRACEPOINT_ID_MAP.lock().unwrap();
        match map.get_by_left(&s.to_string()) {
            Some(&id) => Self { id: id },
            None => {
                let id = map.len();
                map.insert(s.to_string(), id);
                Self { id: id }
            }
        }
    }

    pub fn bytes(&self) -> [u8; 8] {
        self.id.to_ne_bytes()
    }
}

impl Display for TracepointID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl Debug for TracepointID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("TracepointID")
            .field("id", &self.id)
            .field("full_name", &self.to_string())
            .finish()
    }
}

impl Serialize for TracepointID {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        s.serialize_str(&self.to_string())
    }
}

struct TracepointIDVisitor;

impl<'de> de::Visitor<'de> for TracepointIDVisitor {
    type Value = TracepointID;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "a string representing a tracepoint id")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(TracepointID::from_str(s))
    }
}

impl<'de> Deserialize<'de> for TracepointID {
    fn deserialize<D>(d: D) -> Result<TracepointID, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        d.deserialize_str(TracepointIDVisitor)
    }
}

/// General trace implementation
///
use std::fmt;
use std::fmt::Display;
use std::time::Duration;

use chrono::NaiveDateTime;
use petgraph::graph::NodeIndex;
use petgraph::stable_graph::StableGraph;
use petgraph::Direction;
use serde::{Deserialize, Serialize};
use std::path::Path;
use uuid::Uuid;

use crate::grouping::GroupNode;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Trace {
    pub g: StableGraph<Event, DAGEdge>,
    pub base_id: Uuid,
    pub start_node: NodeIndex,
    pub end_node: NodeIndex,
    pub request_type: RequestType,
    pub duration: Duration,
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
        }
    }

    pub fn to_file(&self, file: &Path) {
        let writer = std::fs::File::create(file).unwrap();
        serde_json::to_writer(writer, self).ok();
    }

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

    pub fn prune(&mut self) {
        let mut removed_count = 0;
        loop {
            let mut iter = self.g.externals(Direction::Outgoing);
            let mut end_node = iter.next().unwrap();
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
                if neighbors.len() == 0 {
                    panic!("Pruning ran to a start node from {}", self.g[cur_node]);
                } else {
                    for n in neighbors {
                        cur_nodes.push(n);
                    }
                }
            }
        }
        eprintln!("Removed {} nodes when pruning", removed_count);
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Eq, PartialEq, Hash, Clone)]
pub enum RequestType {
    ServerCreate,
    ServerDelete,
    ServerList,
    Unknown,
}

impl RequestType {
    pub fn from_str(typ: &str) -> Result<RequestType, &str> {
        match typ {
            "ServerCreate" => Ok(RequestType::ServerCreate),
            "ServerDelete" => Ok(RequestType::ServerDelete),
            "ServerList" => Ok(RequestType::ServerList),
            _ => Err("Unknown request type"),
        }
    }
}

impl fmt::Display for RequestType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct Event {
    pub trace_id: Uuid,
    pub tracepoint_id: String,
    pub timestamp: NaiveDateTime,
    pub is_synthetic: bool,
    pub variant: EventType,
}

#[derive(Serialize, Deserialize, Hash, Debug, Clone, Copy, Eq, PartialEq)]
pub enum EventType {
    Entry,
    Exit,
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

impl PartialEq<GroupNode> for Event {
    fn eq(&self, other: &GroupNode) -> bool {
        self.tracepoint_id == other.tracepoint_id && self.variant == other.variant
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct DAGEdge {
    pub duration: Duration,
    pub variant: EdgeType,
}

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

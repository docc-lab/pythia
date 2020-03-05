use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Display;
use std::time::Duration;

use crypto::digest::Digest;
use crypto::sha2::Sha256;
use petgraph::graph::NodeIndex;
use petgraph::stable_graph::StableGraph;
use petgraph::visit::EdgeFiltered;
use petgraph::visit::IntoNeighborsDirected;
use petgraph::Direction;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::critical::HashablePath;
use crate::osprofiler::OSProfilerDAG;
use crate::trace::DAGNode;

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
struct HierarchicalEdge {
    duration: Duration,
    variant: EdgeType,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Eq, PartialEq)]
enum EdgeType {
    Hierarchical,
    HappensBefore,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct HierarchicalCriticalPath {
    g: StableGraph<DAGNode, HierarchicalEdge>,
    base_id: Uuid,
    start_node: NodeIndex,
    end_node: NodeIndex,
    duration: Duration,
    hash: RefCell<Option<String>>,
}

impl HierarchicalCriticalPath {
    pub fn all_possible_paths(trace: &OSProfilerDAG) -> Vec<Self> {
        Vec::new()
    }

    pub fn next_node(&self, nidx: NodeIndex) -> Option<NodeIndex> {
        let visitor =
            EdgeFiltered::from_fn(&self.g, |e| e.weight().variant == EdgeType::HappensBefore);
        let mut matches = visitor.neighbors_directed(nidx, Direction::Outgoing);
        let result = matches.next();
        assert!(matches.next().is_none());
        result
    }
}

#[derive(Default, Serialize, Deserialize)]
pub struct SearchSpace {
    paths: HashMap<String, HierarchicalCriticalPath>, // key is the hash of the critical path
    entry_points: HashSet<String>,
}

impl SearchSpace {
    pub fn add_trace(&mut self, trace: &OSProfilerDAG) {
        for path in &HierarchicalCriticalPath::all_possible_paths(trace) {
            self.entry_points
                .insert(path.g[path.start_node].span.tracepoint_id.clone());
            self.entry_points
                .insert(path.g[path.end_node].span.tracepoint_id.clone());
            match self.paths.get(&path.hash()) {
                Some(_) => {}
                None => {
                    self.paths.insert(path.hash().clone(), path.clone());
                }
            }
        }
    }

    pub fn get_entry_points(&self) -> Vec<&String> {
        self.entry_points.iter().collect()
    }
}

impl Display for SearchSpace {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "This is a representation",)?;
        Ok(())
    }
}

impl HashablePath for HierarchicalCriticalPath {
    fn has_hash(&self) -> bool {
        !self.hash.borrow().is_none()
    }

    fn get_hash(&self) -> String {
        self.hash.borrow().as_ref().unwrap().clone()
    }

    fn calculate_hash(&self) {
        let mut hasher = Sha256::new();
        let mut cur_node = self.start_node;
        loop {
            hasher.input_str(&self.g[cur_node].span.tracepoint_id);
            cur_node = match self.next_node(cur_node) {
                Some(node) => node,
                None => break,
            };
        }
        *self.hash.borrow_mut() = Some(hasher.result_str());
    }
}

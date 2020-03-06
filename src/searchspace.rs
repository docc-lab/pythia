use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Display;
use std::time::Duration;

use crypto::digest::Digest;
use crypto::sha2::Sha256;
use petgraph::dot::Dot;
use petgraph::graph::NodeIndex;
use petgraph::stable_graph::StableGraph;
use petgraph::visit::EdgeFiltered;
use petgraph::visit::IntoNeighborsDirected;
use petgraph::Direction;
use serde::{Deserialize, Serialize};

use crate::critical::CriticalPath;
use crate::critical::HashablePath;
use crate::osprofiler::OSProfilerDAG;
use crate::trace::DAGEdge;
use crate::trace::Event;
use crate::trace::EventType;

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
struct HierarchicalEdge {
    duration: Duration,
    variant: EdgeType,
}

impl Display for HierarchicalEdge {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.variant {
            EdgeType::Hierarchical => write!(f, "Hierarchical",),
            EdgeType::HappensBefore => write!(f, "{}", self.duration.as_nanos()),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Eq, PartialEq)]
enum EdgeType {
    Hierarchical,
    HappensBefore,
}

impl HierarchicalEdge {
    fn from_dag_edge(e: &DAGEdge) -> Self {
        HierarchicalEdge {
            duration: e.duration,
            variant: EdgeType::HappensBefore,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct HierarchicalCriticalPath {
    g: StableGraph<Event, HierarchicalEdge>,
    start_node: NodeIndex,
    end_node: NodeIndex,
    duration: Duration,
    hash: RefCell<Option<String>>,
}

impl HierarchicalCriticalPath {
    pub fn all_possible_paths(trace: &OSProfilerDAG) -> Vec<Self> {
        CriticalPath::all_possible_paths(trace)
            .iter()
            .map(|x| HierarchicalCriticalPath::from_path(x))
            .collect()
    }

    pub fn from_path(path: &CriticalPath) -> Self {
        let mut g = StableGraph::new();
        // Add all nodes and happens before edges to the graph
        let mut prev_path_node = path.start_node;
        let mut prev_node = g.add_node(path.g.g[prev_path_node].clone());
        let start_node = prev_node;
        loop {
            let cur_path_node = match path.next_node(prev_path_node) {
                Some(node) => node,
                None => break,
            };
            let new_node = g.add_node(path.g.g[cur_path_node].clone());
            g.add_edge(
                prev_node,
                new_node,
                HierarchicalEdge::from_dag_edge(
                    &path.g.g[path.g.g.find_edge(prev_path_node, cur_path_node).unwrap()],
                ),
            );
            prev_node = new_node;
            prev_path_node = cur_path_node;
        }
        let mut result = HierarchicalCriticalPath {
            g: g,
            start_node: start_node,
            end_node: prev_node,
            duration: path.duration,
            hash: RefCell::new(None),
        };
        result.add_hierarchical_edges();
        result
    }

    fn add_hierarchical_edges(&mut self) {
        let mut prev_node = self.start_node;
        let mut context = Vec::new();
        assert!(self.g[prev_node].variant == EventType::Entry);
        context.push(prev_node);
        loop {
            let next_node = match self.next_node(prev_node) {
                Some(n) => n,
                None => break,
            };
            match context.last() {
                Some(&nidx) => {
                    self.g.add_edge(
                        nidx,
                        next_node,
                        HierarchicalEdge {
                            duration: Duration::new(0, 0),
                            variant: EdgeType::Hierarchical,
                        },
                    );
                }
                None => {
                    eprintln!("This node has no context: {}", self.g[next_node]);
                }
            }
            match self.g[next_node].variant {
                EventType::Entry => {
                    context.push(next_node);
                }
                EventType::Exit => {
                    let last = context.pop().unwrap();
                    assert!(self.g[last].variant == EventType::Entry);
                }
                EventType::Annotation => {}
            }
            prev_node = next_node;
        }
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
                .insert(path.g[path.start_node].tracepoint_id.clone());
            self.entry_points
                .insert(path.g[path.end_node].tracepoint_id.clone());
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
        for (hash, path) in self.paths.iter() {
            write!(f, "{}:\n{}", hash, Dot::new(&path.g))?;
        }
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
            hasher.input_str(&self.g[cur_node].tracepoint_id);
            cur_node = match self.next_node(cur_node) {
                Some(node) => node,
                None => break,
            };
        }
        *self.hash.borrow_mut() = Some(hasher.result_str());
    }
}
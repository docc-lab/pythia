use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Display;
use std::time::Instant;

use petgraph::dot::Dot;
use petgraph::graph::NodeIndex;
use petgraph::stable_graph::StableGraph;
use petgraph::visit::EdgeFiltered;
use petgraph::visit::IntoNeighborsDirected;
use petgraph::visit::IntoNodeReferences;
use petgraph::Direction;
use regex::Regex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use pythia_common::RequestType;

use crate::critical::CriticalPath;
use crate::critical::Path;
use crate::grouping::Group;
use crate::trace::DAGEdge;
use crate::trace::EventType;
use crate::trace::Trace;
use crate::trace::TraceNode;
use crate::trace::TracepointID;

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct HierarchicalEdge {
    variant: EdgeType,
}

impl Display for HierarchicalEdge {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.variant {
            EdgeType::Hierarchical => write!(f, "Hierarchical",),
            EdgeType::HappensBefore => write!(f, ""),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Eq, PartialEq)]
pub enum EdgeType {
    Hierarchical,
    HappensBefore,
}

impl HierarchicalEdge {
    fn from_dag_edge(_: &DAGEdge) -> Self {
        HierarchicalEdge {
            variant: EdgeType::HappensBefore,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HierarchicalCriticalPath {
    pub g: StableGraph<TraceNode, HierarchicalEdge>,
    pub base_id: Uuid,
    pub start_node: NodeIndex,
    pub end_node: NodeIndex,
    pub request_type: RequestType,
    hash: String,
}

impl HierarchicalCriticalPath {
    pub fn all_possible_paths<'a>(trace: &'a Trace) -> impl Iterator<Item = Self> + 'a {
        CriticalPath::all_possible_paths(trace).map(|x| HierarchicalCriticalPath::from_path(&x))
    }

    pub fn from_path(path: &CriticalPath) -> Self {
        let mut g = StableGraph::new();
        // Add all nodes and happens before edges to the graph
        let mut prev_path_node = path.start_node;
        let mut prev_node = g.add_node(TraceNode::from_event(&path.g.g[prev_path_node]));
        let start_node = prev_node;
        loop {
            let cur_path_node = match path.next_node(prev_path_node) {
                Some(node) => node,
                None => break,
            };
            let new_node = g.add_node(TraceNode::from_event(&path.g.g[cur_path_node]));
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
            g,
            start_node,
            end_node: prev_node,
            hash: "".to_string(),
            request_type: path.request_type,
            base_id: path.g.base_id.clone(),
        };
        result.add_hierarchical_edges();
        result.calculate_hash();
        result
    }

    pub fn child_nodes(&self, nidx: NodeIndex) -> Vec<NodeIndex> {
        EdgeFiltered::from_fn(&self.g, |e| e.weight().variant == EdgeType::Hierarchical)
            .neighbors_directed(nidx, Direction::Outgoing)
            .collect()
    }

    fn add_hierarchical_edges(&mut self) {
        let mut context = Vec::new();
        let mut prev_node = self.start_node;
        loop {
            match self.g[prev_node].variant {
                EventType::Entry => {
                    break;
                }
                EventType::Annotation => {
                    prev_node = match self.next_node(prev_node) {
                        Some(n) => n,
                        None => {
                            return;
                        }
                    };
                }
                EventType::Exit => {
                    panic!("Saw exit event before any entry events");
                }
            }
        }
        assert!(self.g[prev_node].variant == EventType::Entry);
        context.push(prev_node);
        loop {
            let next_node = match self.next_node(prev_node) {
                Some(n) => n,
                None => break,
            };
            match context.last() {
                Some(&nidx) => {
                    if self.g[nidx].tracepoint_id != self.g[next_node].tracepoint_id {
                        self.g.add_edge(
                            nidx,
                            next_node,
                            HierarchicalEdge {
                                variant: EdgeType::Hierarchical,
                            },
                        );
                    }
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
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct SearchSpace {
    paths: HashMap<String, HierarchicalCriticalPath>, // key is the hash of the critical path
    occurances: HashMap<String, usize>,
    pub added_paths: usize,
    entry_points: HashSet<TracepointID>,
    synchronization_points: HashSet<TracepointID>,
}

impl SearchSpace {
    pub fn trace_points(&self) -> HashSet<TracepointID> {
        self.paths
            .iter()
            .map(|(_, v)| v.g.node_references().map(|(_, w)| w.tracepoint_id))
            .flatten()
            .collect::<HashSet<_>>()
    }

    pub fn path_lengths(&self) -> Vec<usize> {
        self.paths.iter().map(|(_, v)| v.len()).collect()
    }

    pub fn path_count(&self) -> usize {
        self.paths.len()
    }

    pub fn find_matches(&self, group: &Group, silent: bool) -> Vec<&HierarchicalCriticalPath> {
        let now = Instant::now();
        let mut matching_hashes = self
            .paths
            .iter()
            .filter(|&(_, v)| v.contains(group))
            .map(|(k, _)| k)
            .collect::<Vec<&String>>();
        matching_hashes.sort_by(|&a, &b| {
            self.occurances
                .get(b)
                .unwrap()
                .cmp(&self.occurances.get(a).unwrap())
        });
        if !silent {
            eprintln!(
                "Finding {} matching groups out of {} took {}, group size {}",
                matching_hashes.len(),
                self.paths.len(),
                now.elapsed().as_micros(),
                group.g.node_count()
            );
        }
        matching_hashes
            .iter()
            .map(|&h| self.paths.get(h).unwrap())
            .collect()
    }

    pub fn add_trace(&mut self, trace: &Trace, verbose: bool) {
        eprintln!("Adding {}", trace.base_id);
        let mut count = 0;
        let mut overlaps = 0;
        let mut added = 0;
        if verbose {
            eprintln!("Counting paths...");
            eprintln!(
                "Starting to process {} paths",
                CriticalPath::count_possible_paths(trace)
            );
        }
        for node in trace.g.node_references() {
            let in_neighbors = trace
                .g
                .neighbors_directed(node.0, Direction::Incoming)
                .collect::<Vec<_>>();
            if in_neighbors.len() > 1 {
                self.synchronization_points
                    .insert(trace.g[node.0].tracepoint_id);
                for &n in &in_neighbors {
                    self.synchronization_points.insert(trace.g[n].tracepoint_id);
                }
            }
        }
        for path in HierarchicalCriticalPath::all_possible_paths(trace) {
            self.added_paths += 1;
            self.entry_points
                .insert(path.g[path.start_node].tracepoint_id);
            self.entry_points
                .insert(path.g[path.end_node].tracepoint_id);
            let mut occurances = 1;
            if self.paths.get(path.hash()).is_none() {
                let mut add_path = true;
                let mut paths_to_remove: Vec<String> = Vec::new();
                for p in self.paths.values() {
                    if p.len() < path.len() {
                        if path.contains(p) {
                            paths_to_remove.push(p.hash().to_string());
                            occurances += self.occurances.get(p.hash()).unwrap();
                        }
                    } else if path.len() < p.len() {
                        if p.contains(&path) {
                            add_path = false;
                            *self.occurances.get_mut(p.hash()).unwrap() += 1;
                        }
                    }
                }
                for p in paths_to_remove {
                    self.paths.remove(&p);
                    self.occurances.remove(&p);
                    added -= 1;
                    overlaps += 1;
                }
                if add_path {
                    self.paths.insert(path.hash().to_string(), path.clone());
                    self.occurances.insert(path.hash().to_string(), occurances);
                    added += 1;
                } else {
                    overlaps += 1;
                }
            }
            count += 1;
            if verbose && (count % 1000 == 0) {
                eprintln!("Added {}/{} paths, overlaps = {}", added, count, overlaps);
            }
        }
        eprintln!(
            "Added {}/{} paths, removed {} overlaps",
            added, count, overlaps
        );
    }

    pub fn get_entry_points(&self) -> Vec<TracepointID> {
        self.entry_points.iter().cloned().collect()
    }

    pub fn get_synchronization_points(&self) -> Vec<TracepointID> {
        self.synchronization_points.iter().cloned().collect()
    }
}

impl Display for SearchSpace {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        lazy_static! {
            static ref RE: Regex = Regex::new("label=\"Hierarchical\"").unwrap();
        }
        for (hash, path) in self.paths.iter() {
            write!(
                f,
                "{} x {}:\n{}",
                hash,
                self.occurances.get(hash).unwrap(),
                RE.replace_all(&format!("{}", Dot::new(&path.g)), "style=\"dashed\"")
            )?;
        }
        Ok(())
    }
}

impl Path for HierarchicalCriticalPath {
    fn get_hash(&self) -> &str {
        &self.hash
    }

    fn set_hash(&mut self, hash: &str) {
        self.hash = hash.to_string()
    }

    fn start_node(&self) -> NodeIndex {
        self.start_node
    }

    fn at(&self, idx: NodeIndex) -> TracepointID {
        self.g[idx].tracepoint_id
    }

    fn next_node(&self, nidx: NodeIndex) -> Option<NodeIndex> {
        let visitor =
            EdgeFiltered::from_fn(&self.g, |e| e.weight().variant == EdgeType::HappensBefore);
        let mut matches = visitor.neighbors_directed(nidx, Direction::Outgoing);
        let result = matches.next();
        assert!(matches.next().is_none());
        result
    }

    fn prev_node(&self, nidx: NodeIndex) -> Option<NodeIndex> {
        let visitor =
            EdgeFiltered::from_fn(&self.g, |e| e.weight().variant == EdgeType::HappensBefore);
        let mut matches = visitor.neighbors_directed(nidx, Direction::Incoming);
        let result = matches.next();
        assert!(matches.next().is_none());
        result
    }

    fn len(&self) -> usize {
        self.g.node_count()
    }
}

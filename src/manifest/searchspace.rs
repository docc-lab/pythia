//! This module has the search space without the complexity of
//! supporting multiple request types.

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

use pythia_common::RequestType;

use crate::critical::CriticalPath;
use crate::critical::Path;
use crate::grouping::Group;
use crate::trace::DAGEdge;
use crate::trace::EventType;
use crate::trace::Trace;
use crate::trace::TraceNode;
use crate::trace::TracepointID;

/// Maybe we don't need this, but in case we want to add more stuff to the edge.
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

/// This is the search space described in the paper.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HierarchicalCriticalPath {
    /// This graph contains both happens-before and hierarchical edges.
    pub g: StableGraph<TraceNode, HierarchicalEdge>,
    pub start_node: NodeIndex,
    pub end_node: NodeIndex,
    /// Trace points at the top of the hierarchy
    pub hierarchy_starts: HashSet<NodeIndex>,
    pub request_type: RequestType,
    hash: String,
}

impl HierarchicalCriticalPath {
    /// This is an iterator, so it creates the paths one at a time, to avoid running out of memory.
    ///
    /// If we put all paths in a vector, we run out of memory for some traces.
    pub fn all_possible_paths<'a>(trace: &'a Trace) -> impl Iterator<Item = Self> + 'a {
        CriticalPath::all_possible_paths(trace).map(|x| HierarchicalCriticalPath::from_path(&x))
    }

    /// Copies critical path and then adds hierarchical edges
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
            hierarchy_starts: HashSet::new(),
        };
        result.add_hierarchical_edges();
        result.calculate_hash();
        result
    }

    /// Hierarchical children of a node. The node needs to be a span start.
    pub fn child_nodes(&self, nidx: NodeIndex) -> Vec<NodeIndex> {
        EdgeFiltered::from_fn(&self.g, |e| e.weight().variant == EdgeType::Hierarchical)
            .neighbors_directed(nidx, Direction::Outgoing)
            .collect()
    }

    fn add_hierarchical_edges(&mut self) {
        // Right now, all nodes in the graph have a happens before relationship.

        // Initialize stack to keep track of of hierarchical calls (like a call stack)
        // Note that we're only keeping track of Entry events! This means that when we have
        // an entry event as the most recent, any next ones we see before the corresponding Exit must be
        // hierarchically "below" the last one.
        let mut context = Vec::new();
        // Start with the first node.
        let mut prev_node = self.start_node;

        // Go through nodes from the start_node until we find the first entry node.
        // If we just find Annotations, there's nothing to do.
        // If we find an exit before any entry events, there's an issue.
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
        // Not sure if we need this assert since we're always finding an Entry, but OK
        assert!(self.g[prev_node].variant == EventType::Entry);

        // We know this node is one of the ones at the top of the hierarchy.
        self.hierarchy_starts.insert(prev_node);

        // Add first item to the stack, since it's an Entry event.
        context.push(prev_node);

        loop {
            // Find the next node in the trace
            let next_node = match self.next_node(prev_node) {
                Some(n) => n,
                None => break,
            };
            match context.last() {
                Some(&nidx) => {
                    // If the most recent hierarchical node differs from the next node, add a
                    // node -> next node hierarchical relationship.
                    // This makes sense since anything following an Entry is "below" it in the hierarchy.
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
                // If there's nothing in the stack, this means that we just had an exit event that matched the original
                // entry call and popped it, so we're back at the top of the hierarchy again.
                None => {
                    self.hierarchy_starts.insert(next_node);
                }
            }
            match self.g[next_node].variant {
                EventType::Entry => {
                    // If it's a new Entry, that means we're going one level deeper in the hierarchy and can check for its
                    // child calls next.
                    context.push(next_node);
                }
                EventType::Exit => {
                    // Reached the end of this call. Pop and make sure we have an Entry to match our Exit, otherwise
                    // we have some imbalanced Exit/Entry graph.
                    let last = context.pop().unwrap();
                    assert!(self.g[last].variant == EventType::Entry);
                }
                // Annotations can't have children, so ignore here.
                EventType::Annotation => {}
            }
            prev_node = next_node;
        }
    }

    // Helper function to tell whether before_node happens before (in the Lampert sense) after_node in this
    // path. For example, if the path is A -> B -> C, happens_before(A, B) would be true, but happens_before(B, A) would
    // return false.
    // Looks through the whole path, so is O(n).
    pub fn happens_before(&self, before_node: &TraceNode, after_node: &TraceNode) -> bool {
        // First check for the same node, as one cannot happen before oneself
        if before_node == after_node {
            return false;
        }

        // Initialize flag to tell if we've seen the before node already
        let mut before_node_seen = false;
        // Start with the root
        let mut current_node = self.start_node;

        loop {
            // If we haven't seen the before node, check the current node to see if we've reached the before_node
            // or if we've reached the after_node first.
            if !before_node_seen {
                // If the current node is the before node, we can mark that we've found it.
                if self.g[current_node] == *before_node {
                    before_node_seen = true;
                }
                // If we've reached the later node before finding the before node, we know it can't happen before it
                if self.g[current_node] == *after_node {
                    return false;
                }
            } else {
                // If we saw the before_node in a previous node, that means we're down the happens-before path, and if
                // we see the after_node, we know it must occur after.
                if self.g[current_node] == *after_node {
                    return true;
                }
                // If we've reached the last node without finding the after_node, that means we didn't find it in the
                // path. This shouldn't happen, as all children will be in the path, but we can include a check for
                // completion's sake.
                if current_node == self.end_node {
                    return false;
                }
            }
            // We'll always want to get the next node while we can.
            current_node = match self.next_node(current_node) {
                Some(n) => n,
                None => break,
            };
        }

        return false;
    }
}

/// Collection of all HierarchicalCriticalPaths
///
/// Also contains more information that is pre-calculated from these paths
#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub struct SearchSpace {
    /// Key is the hash of the critical path
    pub paths: HashMap<String, HierarchicalCriticalPath>,
    occurances: HashMap<String, usize>,
    pub added_paths: usize,
    entry_points: HashSet<TracepointID>,
    /// List of tracepoints where multiple branches of execution joined, and the last tracepoint of each
    /// branch of execution.
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

    /// Add a new offline profiling trace to the existing search space
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

    pub fn get_top_hierarchy(&self) -> Vec<TracepointID> {
        let mut result = HashSet::new();
        for p in self.paths.values() {
            for &tp in &p.hierarchy_starts {
                result.insert(p.g[tp].tracepoint_id);
            }
        }
        result.drain().collect()
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

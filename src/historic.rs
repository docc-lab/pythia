use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Display;
use std::time::Duration;

use indexmap::set::IndexSet;
use petgraph::graph::EdgeIndex;
use petgraph::visit::EdgeRef;
use stats::variance;

use crate::grouping::Group;
use crate::manifest::Manifest;
use crate::poset::PosetNode;
use crate::search::SearchState;
use crate::search::SearchStrategy;
use crate::trace::Trace;

struct Edge {
    start: PosetNode,
    end: PosetNode,
    latencies: Vec<Duration>,
}

impl Edge {
    fn new(source: &PosetNode, target: &PosetNode, duration: Duration) -> Self {
        let mut latencies = Vec::new();
        latencies.push(duration);
        Edge {
            start: source.clone(),
            end: target.clone(),
            latencies: latencies,
        }
    }

    fn add_duration(&mut self, duration: Duration) {
        self.latencies.push(duration);
    }
}

pub struct Historic {
    edges: Vec<Edge>,
    entry_points: HashSet<String>,
    edge_map: HashMap<PosetNode, HashMap<PosetNode, usize>>,
    tried_tracepoints: RefCell<HashSet<String>>,
    manifest: Manifest,
}

impl Display for Historic {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for edge in &self.edges {
            write!(
                f,
                "({} -> {}): {:?},\n",
                edge.start, edge.end, edge.latencies
            )?
        }
        Ok(())
    }
}

impl Historic {
    pub fn new(m: Manifest) -> Historic {
        Historic {
            edges: Vec::new(),
            entry_points: HashSet::new(),
            edge_map: HashMap::new(),
            tried_tracepoints: RefCell::new(HashSet::new()),
            manifest: m,
        }
    }

    fn add_trace(&mut self, trace: &Trace) {
        // Breadth-first search over all nodes, add outgoing edges to manifest
        let mut visited = HashSet::new();
        let mut to_visit = IndexSet::new();
        to_visit.insert(trace.start_node);
        self.entry_points
            .insert(trace.g[trace.start_node].tracepoint_id.clone());
        for nidx in trace.possible_end_nodes() {
            self.entry_points
                .insert(trace.g[nidx].tracepoint_id.clone());
        }
        while let Some(nidx) = to_visit.pop() {
            let source = PosetNode::from_event(&trace.g[nidx]);
            let inner_map = self
                .edge_map
                .entry(source.clone())
                .or_insert(HashMap::new());
            for edge in trace.g.edges(nidx) {
                assert_eq!(nidx, edge.source());
                let target = PosetNode::from_event(&trace.g[edge.target()]);
                match inner_map.get(&target) {
                    Some(&idx) => self.edges[idx].add_duration(edge.weight().duration),
                    None => {
                        self.edges
                            .push(Edge::new(&source, &target, edge.weight().duration));
                        inner_map.insert(target.clone(), self.edges.len() - 1);
                    }
                }
                if visited.get(&edge.target()).is_none() {
                    to_visit.insert(edge.target());
                }
            }
            visited.insert(nidx);
        }
        self.edges.sort_by(|a, b| {
            variance(b.latencies.iter().map(|x| x.as_nanos()))
                .partial_cmp(&variance(a.latencies.iter().map(|x| x.as_nanos())))
                .unwrap()
        });
    }

    fn get_entry_points(&self) -> Vec<&String> {
        self.entry_points.iter().collect()
    }
}

impl SearchStrategy for Historic {
    fn search(
        &self,
        _group: &Group,
        _edge: EdgeIndex,
        budget: usize,
    ) -> (Vec<&String>, SearchState) {
        if budget == 0 {
            panic!("The historic method cannot be used without a budget");
        }
        let mut result = HashSet::new();
        let mut index = 0;
        let mut at_start = true;
        let mut tried_tracepoints = self.tried_tracepoints.borrow_mut();
        while result.len() < budget {
            if index > self.edges.len() {
                return (result.drain().collect(), SearchState::NextEdge);
            }
            let edge = &self.edges[index];
            if at_start {
                if tried_tracepoints.get(&edge.start.tracepoint_id).is_none() {
                    tried_tracepoints.insert(edge.start.tracepoint_id.clone());
                    result.insert(&edge.start.tracepoint_id);
                }
                at_start = false;
            } else {
                if tried_tracepoints.get(&edge.end.tracepoint_id).is_none() {
                    tried_tracepoints.insert(edge.end.tracepoint_id.clone());
                    result.insert(&edge.end.tracepoint_id);
                }
                at_start = true;
                index += 1;
            }
        }
        (result.drain().collect(), SearchState::DepletedBudget)
    }
}

impl Default for Historic {
    fn default() -> Self {
        Historic {
            edges: Vec::new(),
            edge_map: HashMap::new(),
            entry_points: HashSet::new(),
            tried_tracepoints: RefCell::new(HashSet::new()),
            manifest: Manifest::new(),
        }
    }
}

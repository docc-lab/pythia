use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Display;
use std::time::Duration;

use indexmap::set::IndexSet;
use petgraph::graph::EdgeIndex;
use petgraph::visit::EdgeRef;
use serde::{Deserialize, Serialize};

use grouping::Group;
use manifest::SearchSpace;
use osprofiler::OSProfilerDAG;
use poset::PosetNode;

#[derive(Serialize, Deserialize)]
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

#[derive(Serialize, Deserialize)]
pub struct Historic {
    edges: Vec<Edge>,
    edge_map: HashMap<PosetNode, HashMap<PosetNode, usize>>,
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

impl SearchSpace for Historic {
    fn new() -> Self {
        Historic {
            edges: Vec::new(),
            edge_map: HashMap::new(),
        }
    }

    fn add_trace(&mut self, trace: &OSProfilerDAG) {
        // Breadth-first search over all nodes, add outgoing edges to manifest
        let mut visited = HashSet::new();
        let mut to_visit = IndexSet::new();
        to_visit.insert(trace.start_node);
        while let Some(nidx) = to_visit.pop() {
            let source = PosetNode::from_event(&trace.g[nidx].span);
            let inner_map = self
                .edge_map
                .entry(source.clone())
                .or_insert(HashMap::new());
            for edge in trace.g.edges(nidx) {
                assert_eq!(nidx, edge.source());
                let target = PosetNode::from_event(&trace.g[edge.target()].span);
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
    }

    fn get_entry_points(&self) -> Vec<&String> {
        Vec::new()
    }

    fn search(&self, _group: &Group, _edge: EdgeIndex) -> Vec<&String> {
        Vec::new()
    }
}

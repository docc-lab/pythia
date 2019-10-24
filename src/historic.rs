use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::time::Duration;

use petgraph::graph::EdgeIndex;
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

#[derive(Serialize, Deserialize)]
pub struct Historic {
    edges: Vec<Edge>,
    edge_map: HashMap<(PosetNode, PosetNode), usize>,
}

impl Display for Historic {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for edge in &self.edges {
            write!(
                f,
                "({} -> {}): {:?}, ",
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

    }

    fn get_entry_points(&self) -> Vec<&String> {
        Vec::new()
    }

    fn search(&self, _group: &Group, _edge: EdgeIndex) -> Vec<&String> {
        Vec::new()
    }
}

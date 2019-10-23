use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;

use petgraph::graph::EdgeIndex;
use serde::{Deserialize, Serialize};

use grouping::Group;
use manifest::SearchSpace;
use osprofiler::OSProfilerDAG;

#[derive(Serialize, Deserialize)]
pub struct Historic {
    tracepoints: HashMap<String, f64>,
}

impl Display for Historic {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.tracepoints)
    }
}

impl SearchSpace for Historic {
    fn new() -> Self {
        Historic {
            tracepoints: HashMap::new(),
        }
    }

    fn add_trace(&mut self, trace: &OSProfilerDAG) {}

    fn get_entry_points(&self) -> Vec<&String> {
        Vec::new()
    }

    fn search(&self, _group: &Group, _edge: EdgeIndex) -> Vec<&String> {
        Vec::new()
    }
}

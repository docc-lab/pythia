use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::osprofiler::OSProfilerDAG;

#[derive(Serialize, Deserialize)]
pub struct HierarchicalCriticalPath {}

impl HierarchicalCriticalPath {
    pub fn all_possible_paths(trace: &OSProfilerDAG) -> Vec<Self> {
        Vec::new()
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
            // self.entry_points
            //     .insert(path.g.g[path.start_node].span.tracepoint_id.clone());
            // self.entry_points
            //     .insert(path.g.g[path.end_node].span.tracepoint_id.clone());
            // match self.paths.get(&path.hash()) {
            //     Some(_) => {}
            //     None => {
            //         self.paths.insert(path.hash().clone(), path.clone());
            //     }
            // }
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

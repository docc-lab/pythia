use std::fmt;
use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::osprofiler::OSProfilerDAG;

#[derive(Serialize, Deserialize)]
pub struct SearchSpace {}

impl SearchSpace {
    pub fn new() -> SearchSpace {
        SearchSpace {}
    }
    pub fn add_trace(&mut self, trace: &OSProfilerDAG) {}
    pub fn get_entry_points(&self) -> Vec<&String> {
        Vec::new()
    }
}

impl Display for SearchSpace {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "This is a representation",)?;
        Ok(())
    }
}

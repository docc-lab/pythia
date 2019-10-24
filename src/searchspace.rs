use std::fmt::Display;

use petgraph::graph::EdgeIndex;

use crate::grouping::Group;
use crate::osprofiler::OSProfilerDAG;

#[typetag::serde(tag = "type")]
pub trait SearchSpace: Display {
    fn add_trace(&mut self, trace: &OSProfilerDAG);
    fn get_entry_points<'a>(&'a self) -> Vec<&'a String>;
    fn search<'a>(&'a self, group: &Group, edge: EdgeIndex) -> Vec<&'a String>;
}

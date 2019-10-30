use std::fmt::Display;

use petgraph::graph::EdgeIndex;

use crate::grouping::Group;
use crate::osprofiler::OSProfilerDAG;

#[derive(Clone, Copy)]
pub enum SearchState {
    NextEdge,
    DepletedBudget,
}

#[typetag::serde(tag = "type")]
pub trait SearchStrategy: Display {
    fn add_trace(&mut self, trace: &OSProfilerDAG);
    fn get_entry_points(&self) -> Vec<&String>;
    fn search(&self, group: &Group, edge: EdgeIndex, budget: usize) -> (Vec<&String>, SearchState);
}

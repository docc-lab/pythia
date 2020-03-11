use std::fmt::Display;

use petgraph::graph::EdgeIndex;

use crate::grouping::Group;
use crate::searchspace::SearchSpace;

#[derive(Clone, Copy)]
pub enum SearchState {
    NextEdge,
    DepletedBudget,
}

#[typetag::serde(tag = "type")]
pub trait SearchStrategy: Display {
    fn search(
        &self,
        group: &Group,
        edge: EdgeIndex,
        budget: usize,
    ) -> (Vec<&String>, SearchState);
}

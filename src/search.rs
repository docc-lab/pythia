use std::fmt::Display;

use petgraph::graph::EdgeIndex;

use crate::grouping::Group;
use crate::trace::TracepointID;

#[derive(Clone, Copy)]
pub enum SearchState {
    NextEdge,
    DepletedBudget,
}

pub trait SearchStrategy: Display {
    fn search(
        &self,
        group: &Group,
        edge: EdgeIndex,
        budget: usize,
    ) -> (Vec<TracepointID>, SearchState);
}

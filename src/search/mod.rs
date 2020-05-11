mod flat;

use std::fmt::Display;

use petgraph::graph::EdgeIndex;

use crate::grouping::Group;
use crate::search::flat::FlatSearch;
use crate::settings::Settings;
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

#[derive(Debug)]
pub enum SearchStrategyType {
    Flat,
}

pub fn strategy_from_settings(settings: &Settings) -> Box<dyn SearchStrategy> {
    match &settings.search_strategy {
        SearchStrategyType::Flat => Box::new(FlatSearch::from_settings(settings)),
    }
}

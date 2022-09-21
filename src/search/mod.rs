/*
This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.

Copyright (c) 2022, Diagnosis and Control of Clouds Laboratory
All rights reserved.
*/

//! This includes search strategies.
//!
//! The trait should be implemented by the search strategy.

mod flat;
mod hierarchical;
mod historic;

use petgraph::graph::EdgeIndex;

use crate::controller::Controller;
use crate::grouping::Group;
use crate::manifest::Manifest;
use crate::search::flat::FlatSearch;
use crate::search::hierarchical::HierarchicalSearch;
use crate::search::historic::HistoricSearch;
use crate::settings::Settings;
use crate::trace::TracepointID;

pub trait SearchStrategy {
    /// Simply return a list of tracepoints to enable. The number of trace points should be <= the
    /// budget
    fn search(&self, group: &Group, edge: EdgeIndex, budget: usize) -> Vec<TracepointID>;
}

#[derive(Debug)]
pub enum SearchStrategyType {
    Flat,
    Hierarchical,
    Historic,
}

/// Constructor for search strategy
pub fn get_strategy(
    s: &Settings,
    m: &'static Manifest,
    c: &'static Box<dyn Controller>,
) -> Box<dyn SearchStrategy> {
    match &s.search_strategy {
        SearchStrategyType::Flat => Box::new(FlatSearch::new(s, m, c)),
        SearchStrategyType::Hierarchical => Box::new(HierarchicalSearch::new(s, m, c)),
        SearchStrategyType::Historic => Box::new(HistoricSearch::new(s, m, c)),
    }
}

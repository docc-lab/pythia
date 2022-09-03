/*
BSD 2-Clause License

Copyright (c) 2022, Diagnosis and Control of Clouds Laboratory
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

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

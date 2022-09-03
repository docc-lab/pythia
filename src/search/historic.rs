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

use std::collections::HashMap;
use std::collections::HashSet;

use petgraph::graph::EdgeIndex;
use rand::seq::IteratorRandom;

use pythia_common::RequestType;

use crate::controller::Controller;
use crate::grouping::Group;
use crate::manifest::Manifest;
use crate::search::SearchStrategy;
use crate::settings::Settings;
use crate::trace::TracepointID;

/// This strategy returns a random selection of trace points to enable
pub struct HistoricSearch {
    controller: &'static Box<dyn Controller>,
    per_request_types: HashMap<RequestType, HashSet<TracepointID>>,
}

impl SearchStrategy for HistoricSearch {
    fn search(&self, group: &Group, _edge: EdgeIndex, budget: usize) -> Vec<TracepointID> {
        let mut rng = rand::thread_rng();
        self.per_request_types
            .get(&group.request_type)
            .unwrap()
            .iter()
            .filter(|&tp| !self.controller.is_enabled(&(*tp, Some(group.request_type))))
            .cloned()
            .choose_multiple(&mut rng, budget)
    }
}

impl HistoricSearch {
    pub fn new(_s: &Settings, m: &'static Manifest, c: &'static Box<dyn Controller>) -> Self {
        HistoricSearch {
            controller: c,
            per_request_types: m.get_per_request_types(),
        }
    }
}

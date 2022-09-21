/*
This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.

Copyright (c) 2022, Diagnosis and Control of Clouds Laboratory
All rights reserved.
*/

use std::collections::HashSet;
use std::time::Instant;

use petgraph::graph::EdgeIndex;

use crate::controller::Controller;
use crate::critical::Path;
use crate::grouping::Group;
use crate::manifest::HierarchicalCriticalPath;
use crate::manifest::Manifest;
use crate::search::SearchStrategy;
use crate::settings::Settings;
use crate::trace::TracepointID;

pub struct FlatSearch {
    controller: &'static Box<dyn Controller>,
    manifest: &'static Manifest,
}

impl SearchStrategy for FlatSearch {
    fn search(&self, group: &Group, edge: EdgeIndex, budget: usize) -> Vec<TracepointID> {
        let matches = self.manifest.find_matches(group);
        let mut result = HashSet::new();
        for m in matches {
            let now = Instant::now();
            let remaining_budget = budget - result.len();
            result.extend(
                self.split_group_by_n(m, group, edge, remaining_budget)
                    .iter()
                    .take(remaining_budget),
            );
            eprintln!("Finding middle took {}", now.elapsed().as_micros(),);
            result = result
                .into_iter()
                .filter(|&x| !self.controller.is_enabled(&(x, Some(group.request_type))))
                .collect();
        }
        result.drain().collect()
    }
}

impl FlatSearch {
    pub fn new(_s: &Settings, m: &'static Manifest, c: &'static Box<dyn Controller>) -> Self {
        FlatSearch {
            controller: c,
            manifest: m,
        }
    }

    /// Find n tracepoints that equally separate the edge according to the path
    fn split_group_by_n(
        &self,
        path: &HierarchicalCriticalPath, // Contains full search space
        group: &Group,
        edge: EdgeIndex,
        n: usize,
    ) -> Vec<TracepointID> {
        let mut result = Vec::new();
        let (source, target) = group.g.edge_endpoints(edge).unwrap();
        let mut path_source = path.start_node;
        let path_target;
        let mut nodes_between = 0;
        let mut cur_path_idx = path.start_node;
        let mut cur_group_idx = group.start_node;
        loop {
            if path.g[cur_path_idx] == group.g[cur_group_idx] {
                if cur_group_idx == source {
                    path_source = cur_path_idx;
                    nodes_between = 0;
                }
                if cur_group_idx == target {
                    path_target = cur_path_idx;
                    nodes_between -= 1;
                    break;
                }
                cur_group_idx = group.next_node(cur_group_idx).unwrap();
            }
            cur_path_idx = path.next_node(cur_path_idx).unwrap();
            nodes_between += 1;
        }
        if nodes_between == 0 {
            println!("The matching nodes are consecutive");
        }
        let mut gaps = Vec::new();
        if nodes_between <= n {
            for _ in 0..nodes_between {
                gaps.push(0);
            }
        } else {
            for _ in 0..n {
                gaps.push(nodes_between / (n + 1));
            }
        }
        cur_path_idx = path.next_node(path_source).unwrap();
        for i in gaps {
            for _ in 0..i {
                cur_path_idx = path.next_node(cur_path_idx).unwrap();
                if cur_path_idx == path_target {
                    eprintln!("Reached target prematurely");
                    break;
                }
            }
            if self
                .controller
                .is_enabled(&(path.g[cur_path_idx].tracepoint_id, Some(path.request_type)))
            {
                match path.next_node(cur_path_idx) {
                    Some(nidx) => {
                        cur_path_idx = nidx;
                        if cur_path_idx == path_target {
                            cur_path_idx = path.prev_node(cur_path_idx).unwrap();
                            cur_path_idx = path.prev_node(cur_path_idx).unwrap();
                        }
                    }
                    None => {
                        cur_path_idx = path.prev_node(cur_path_idx).unwrap();
                    }
                };
                if cur_path_idx == path_source {
                    println!("Couldn't find not enabled nodes in between");
                    continue;
                }
            }
            if cur_path_idx == path_target {
                println!("Already reached target node, breaking");
                continue;
            }
            result.push(path.g[cur_path_idx].tracepoint_id);
            if cur_path_idx == path_target || cur_path_idx == path_source {
                eprintln!("Some old assertions failed");
                break;
            }
            cur_path_idx = path.next_node(cur_path_idx).unwrap();
        }
        result
    }
}

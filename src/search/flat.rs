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
        // Get all of the HierarchicalCriticalPaths that contain this group
        let matches = self.manifest.find_matches(group);
        let mut result = HashSet::new();
        for m in matches {
            let now = Instant::now();
            // Note: it looks like this is greedy, so it'll take as many as possible from the first match!
            // So the later matches might be ignored. I wonder if the order of find_matches should matter?
            let remaining_budget = budget - result.len();
            result.extend(
                self.split_group_by_n(m, group, edge, remaining_budget)
                    .iter()
                    .take(remaining_budget),
            );
            // note: no guarantee (that I saw) that we actually get N=remaining_budget items from the group, but it
            // doesn't really matter because budget is updated in pythia_controller.rs.
            // We may be getting under the budget (due to rounding)
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
        // path "contains full search space"
        // Alex note: really the path is the full possible trace path with all tracepoints
        path: &HierarchicalCriticalPath,
        group: &Group,
        edge: EdgeIndex,
        // Alex note: n is approximate. Due to rounding (etc), we may add n-1.
        // For example, with nodes_between and n=3, we only add 2 trace points because of gap math.
        n: usize,
    ) -> Vec<TracepointID> {
        let mut result = Vec::new();
        let (source, target) = group.g.edge_endpoints(edge).unwrap();
        let mut path_source = path.start_node;
        let path_target;
        let mut nodes_between = 0;
        let mut cur_path_idx = path.start_node;
        let mut cur_group_idx = group.start_node;
        // Figure out how many nodes are in between the source node and target node
        loop {
            if path.g[cur_path_idx] == group.g[cur_group_idx] {
                if cur_group_idx == source {
                    // We're already setting cur_path_idx to path.start_node so not sure why we need the next line
                    path_source = cur_path_idx;
                    nodes_between = 0;
                }
                if cur_group_idx == target {
                    // path_target ends up as the target
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
        // Keep a vector of... gaps?
        let mut gaps = Vec::new();
        if nodes_between <= n {
            for _ in 0..nodes_between {
                // If we have fewer nodes between than nodes we want to turn on, we don't need any gaps (AKA gaps of 0)
                // e.g. between A and D in A->B->C->D and we want to turn on 3. We can just turn on B and C.
                gaps.push(0);
            }
        } else {
            for _ in 0..n {
                // Otherwise we just need n gaps of length / n + 1
                // e.g. between A and G in
                // A -> B -> C -> D -> E -> F -> G
                // nodes_between = 5
                // If n=3, we wants 3 gaps of 1 (5 / 3 = 1 with integer division)
                gaps.push(nodes_between / (n + 1));
            }
        }
        // This sets the cur_path_idx to one after the path source. (probably because of assertion at bottom).
        // e.g. between A and G in
        // A -> B -> C -> D -> E -> F -> G
        // cur_path_idx would be B. I guess potentially missing one is better than double counting first.
        cur_path_idx = path.next_node(path_source).unwrap();
        // For all of the gaps:
        for i in gaps {
            for _ in 0..i {
                // Walk through the path for the gap number
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
            // Note: I think we'll reach this after the "reached prematurely" check above (that break is only out of 0..i loop)
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

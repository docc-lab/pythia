use std::collections::HashSet;
use std::time::Instant;

use petgraph::graph::EdgeIndex;

use crate::controller::OSProfilerController;
use crate::critical::Path;
use crate::grouping::Group;
use crate::manifest::HierarchicalCriticalPath;
use crate::manifest::Manifest;
use crate::search::SearchStrategy;
use crate::settings::Settings;
use crate::trace::TracepointID;

pub struct FlatSearch {
    controller: &'static OSProfilerController,
    manifest: &'static Manifest,
}

impl SearchStrategy for FlatSearch {
    fn search(&self, group: &Group, edge: EdgeIndex, budget: usize) -> Vec<TracepointID> {
        let now = Instant::now();
        let matches = self.manifest.find_matches(group);
        eprintln!(
            "Finding {} matching groups took {}, group size {}",
            matches.len(),
            now.elapsed().as_micros(),
            group.g.node_count()
        );
        if matches.len() == 0 {
            println!("No critical path matches the group {}", group);
        }
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
        }
        result.drain().collect()
    }
}

impl FlatSearch {
    pub fn new(_s: &Settings, m: &'static Manifest, c: &'static OSProfilerController) -> Self {
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
                assert_ne!(cur_path_idx, path_target);
            }
            if self
                .controller
                .is_enabled(&(path.g[cur_path_idx].tracepoint_id, Some(path.request_type)))
            {
                cur_path_idx = path.next_node(cur_path_idx).unwrap();
                if cur_path_idx == path_target {
                    cur_path_idx = path.prev_node(cur_path_idx).unwrap();
                    cur_path_idx = path.prev_node(cur_path_idx).unwrap();
                    if cur_path_idx == path_source {
                        println!("Couldn't find not enabled nodes in between");
                        continue;
                    }
                }
            }
            if cur_path_idx == path_target {
                println!("Already reached target node, breaking");
                continue;
            }
            result.push(path.g[cur_path_idx].tracepoint_id);
            assert_ne!(cur_path_idx, path_target);
            assert_ne!(cur_path_idx, path_source);
            cur_path_idx = path.next_node(cur_path_idx).unwrap();
        }
        for tp in &result {
            assert!(!self
                .controller
                .is_enabled(&(tp.clone(), Some(path.request_type))));
        }
        result
    }
}

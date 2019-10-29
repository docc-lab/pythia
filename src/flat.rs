use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Display;

use petgraph::dot::Dot;
use petgraph::graph::EdgeIndex;
use serde::{Deserialize, Serialize};

use crate::critical::CriticalPath;
use crate::grouping::Group;
use crate::osprofiler::OSProfilerDAG;
use crate::searchspace::SearchSpace;
use crate::searchspace::SearchState;

#[derive(Serialize, Deserialize)]
pub struct FlatSpace {
    paths: HashMap<String, CriticalPath>, // key is the hash of the critical path
    entry_points: HashSet<String>,
    occurances: HashMap<String, usize>,
}

#[typetag::serde]
impl SearchSpace for FlatSpace {
    fn add_trace(&mut self, trace: &OSProfilerDAG) {
        for path in &CriticalPath::all_possible_paths(trace) {
            self.add_path(path);
        }
    }

    fn get_entry_points(&self) -> Vec<&String> {
        self.entry_points.iter().collect()
    }

    fn search(&self, group: &Group, edge: EdgeIndex, budget: usize) -> (Vec<&String>, SearchState) {
        let mut matching_hashes = self
            .paths
            .iter()
            .filter(|&(_, v)| self.is_match(group, v))
            .map(|(k, _)| k)
            .collect::<Vec<&String>>();
        matching_hashes.sort_by(|&a, &b| {
            self.occurances
                .get(b)
                .unwrap()
                .cmp(&self.occurances.get(a).unwrap())
        });
        return match matching_hashes.len() {
            0 => panic!("No critical path matches the group {}", Dot::new(&group.g)),
            1 => {
                let result = self.split_group_by_n(
                    self.paths.get(matching_hashes[0]).unwrap(),
                    group,
                    edge,
                    budget,
                );
                let state = if result.len() < budget {
                    SearchState::NextEdge
                } else {
                    SearchState::DepletedBudget
                };
                (result, state)
            }
            _ => {
                let mut result = Vec::new();
                for i in matching_hashes {
                    let tracepoints =
                        self.split_group_by_n(self.paths.get(i).unwrap(), group, edge, 1);
                    assert_eq!(tracepoints.len(), 1);
                    result.push(tracepoints[0]);
                    if result.len() >= budget {
                        break;
                    }
                }
                (result, SearchState::DepletedBudget)
            }
        };
    }
}

impl Default for FlatSpace {
    fn default() -> Self {
        FlatSpace {
            paths: HashMap::new(),
            entry_points: HashSet::new(),
            occurances: HashMap::new(),
        }
    }
}

impl FlatSpace {
    /// Find n tracepoints that equally separate the edge according to the path
    fn split_group_by_n<'a>(
        &self,
        path: &'a CriticalPath, // Contains full search space
        group: &Group,
        edge: EdgeIndex,
        n: usize,
    ) -> Vec<&'a String> {
        let mut result = Vec::new();
        let (source, target) = group.g.edge_endpoints(edge).unwrap();
        let mut path_source = path.start_node;
        let path_target;
        let mut nodes_between = 0;
        let mut cur_path_idx = path.start_node;
        let mut cur_group_idx = group.start_node;
        loop {
            if path.g.g[cur_path_idx] == group.g[cur_group_idx] {
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
        let mut gaps = Vec::new();
        if nodes_between <= n {
            for _ in 0..n {
                gaps.push(0);
            }
        } else {
            for _ in 0..n {
                gaps.push(nodes_between / (n + 1));
            }
        }
        cur_path_idx = path_source;
        for i in gaps {
            for _ in 0..i {
                cur_path_idx = path.next_node(cur_path_idx).unwrap();
                assert_ne!(cur_path_idx, path_target);
            }
            result.push(&path.g.g[cur_path_idx].span.tracepoint_id);
            cur_path_idx = path.next_node(cur_path_idx).unwrap();
            assert_ne!(cur_path_idx, path_target);
        }
        result
    }

    /// Check if group is a subset of path
    fn is_match(&self, group: &Group, path: &CriticalPath) -> bool {
        let mut cur_path_idx = path.start_node;
        let mut cur_group_idx = group.start_node;
        loop {
            if path.g.g[cur_path_idx] == group.g[cur_group_idx] {
                cur_group_idx = match group.next_node(cur_group_idx) {
                    Some(nidx) => nidx,
                    None => return true,
                }
            }
            cur_path_idx = match path.next_node(cur_path_idx) {
                Some(nidx) => nidx,
                None => return false,
            }
        }
    }

    fn add_path(&mut self, path: &CriticalPath) {
        self.entry_points
            .insert(path.g.g[path.start_node].span.tracepoint_id.clone());
        self.entry_points
            .insert(path.g.g[path.end_node].span.tracepoint_id.clone());
        match self.paths.get(&path.hash()) {
            Some(_) => {}
            None => {
                self.paths.insert(path.hash().clone(), path.clone());
                self.occurances.insert(path.hash().clone(), 0);
            }
        }
        *self.occurances.get_mut(&path.hash()).unwrap() += 1;
    }
}

impl Display for FlatSpace {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Occurances: {:?}\n", self.occurances)?;
        for (i, (h, p)) in self.paths.iter().enumerate() {
            write!(f, "Path {}: {}\n{}", i, h, Dot::new(&p.g.g))?;
        }
        Ok(())
    }
}

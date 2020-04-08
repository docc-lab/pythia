use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Display;
use std::time::Instant;

use petgraph::dot::Dot;
use petgraph::graph::EdgeIndex;

use crate::critical::CriticalPath;
use crate::critical::Path;
use crate::grouping::Group;
use crate::manifest::Manifest;
use crate::search::SearchState;
use crate::search::SearchStrategy;
use crate::trace::Trace;
use crate::trace::TracepointID;

pub struct FlatSpace {
    paths: HashMap<String, CriticalPath>, // key is the hash of the critical path
    entry_points: HashSet<TracepointID>,
    occurances: HashMap<String, usize>,
    tried_groups: RefCell<HashSet<String>>,
    enabled_tracepoints: RefCell<HashSet<TracepointID>>,
    manifest: Manifest,
}

impl FlatSpace {
    fn add_trace(&mut self, trace: &Trace) {
        for path in CriticalPath::all_possible_paths(trace) {
            self.add_path(&path);
        }
    }

    fn get_entry_points(&self) -> Vec<TracepointID> {
        self.entry_points.iter().cloned().collect()
    }
}

impl SearchStrategy for FlatSpace {
    fn search(
        &self,
        group: &Group,
        edge: EdgeIndex,
        budget: usize,
    ) -> (Vec<TracepointID>, SearchState) {
        let now = Instant::now();
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
        eprintln!(
            "Finding {} matching groups took {}, group size {}",
            self.paths.len(),
            now.elapsed().as_micros(),
            group.g.node_count()
        );
        let mut tried_groups = self.tried_groups.borrow_mut();
        let now = Instant::now();
        return match matching_hashes.len() {
            0 => {
                println!("No critical path matches the group {}", Dot::new(&group.g));
                return (Vec::new(), SearchState::NextEdge);
            }
            1 => {
                let mut current_hash = matching_hashes[0];
                for h in matching_hashes {
                    if tried_groups.get(h).is_none() {
                        current_hash = h;
                        break;
                    }
                }
                let result = self.split_group_by_n(
                    self.paths.get(current_hash).unwrap(),
                    group,
                    edge,
                    budget,
                );
                for i in &result {
                    let mut enabled_tracepoints = self.enabled_tracepoints.borrow_mut();
                    enabled_tracepoints.insert(*i);
                }
                tried_groups.insert(current_hash.clone());
                eprintln!("Finding middle took {}", now.elapsed().as_micros(),);
                if result.len() < budget {
                    tried_groups.clear();
                    (result, SearchState::NextEdge)
                } else {
                    (result, SearchState::DepletedBudget)
                }
            }
            _ => {
                let mut result = HashSet::new();
                let mut split_count = 1;
                loop {
                    for i in &matching_hashes {
                        if !tried_groups.get(*i).is_none() {
                            continue;
                        }
                        let tracepoints = self.split_group_by_n(
                            self.paths.get(*i).unwrap(),
                            group,
                            edge,
                            split_count,
                        );
                        tried_groups.insert(i.to_string());
                        for t in &tracepoints {
                            result.insert(*t);
                            if result.len() >= budget {
                                break;
                            }
                        }
                    }
                    if result.len() >= budget {
                        break;
                    }
                    split_count += 1;
                    if split_count > budget {
                        for i in &result {
                            let mut enabled_tracepoints = self.enabled_tracepoints.borrow_mut();
                            enabled_tracepoints.insert(*i);
                        }
                        eprintln!("Finding middle took {}", now.elapsed().as_micros(),);
                        tried_groups.clear();
                        return (result.drain().collect(), SearchState::NextEdge);
                    }
                }
                for i in &result {
                    let mut enabled_tracepoints = self.enabled_tracepoints.borrow_mut();
                    enabled_tracepoints.insert(*i);
                }
                eprintln!("Finding middle took {}", now.elapsed().as_micros(),);
                if split_count > budget {
                    tried_groups.clear();
                    (result.drain().collect(), SearchState::NextEdge)
                } else {
                    (result.drain().collect(), SearchState::DepletedBudget)
                }
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
            tried_groups: RefCell::new(HashSet::new()),
            enabled_tracepoints: RefCell::new(HashSet::new()),
            manifest: Manifest::new(),
        }
    }
}

impl FlatSpace {
    pub fn new(m: Manifest) -> Self {
        FlatSpace {
            paths: HashMap::new(),
            entry_points: HashSet::new(),
            occurances: HashMap::new(),
            tried_groups: RefCell::new(HashSet::new()),
            enabled_tracepoints: RefCell::new(HashSet::new()),
            manifest: m,
        }
    }

    /// Find n tracepoints that equally separate the edge according to the path
    fn split_group_by_n(
        &self,
        path: &CriticalPath, // Contains full search space
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
            if !self
                .enabled_tracepoints
                .borrow()
                .get(&path.g.g[cur_path_idx].tracepoint_id)
                .is_none()
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
            result.push(path.g.g[cur_path_idx].tracepoint_id);
            assert_ne!(cur_path_idx, path_target);
            assert_ne!(cur_path_idx, path_source);
            cur_path_idx = path.next_node(cur_path_idx).unwrap();
        }
        result
    }

    /// Check if group is a subset of path
    fn is_match(&self, group: &Group, path: &CriticalPath) -> bool {
        let mut cur_path_idx = path.start_node;
        let mut cur_group_idx = group.start_node;
        let mut matches = 0;
        let result;
        loop {
            if path.g.g[cur_path_idx] == group.g[cur_group_idx] {
                matches += 1;
                cur_group_idx = match group.next_node(cur_group_idx) {
                    Some(nidx) => nidx,
                    None => {
                        result = true;
                        break;
                    }
                }
            }
            cur_path_idx = match path.next_node(cur_path_idx) {
                Some(nidx) => nidx,
                None => {
                    result = false;
                    break;
                }
            }
        }
        println!("Match score: {}", matches);
        return result;
    }

    fn add_path(&mut self, path: &CriticalPath) {
        self.entry_points
            .insert(path.g.g[path.start_node].tracepoint_id.clone());
        self.entry_points
            .insert(path.g.g[path.end_node].tracepoint_id.clone());
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

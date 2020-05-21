use std::collections::HashSet;

use petgraph::graph::{EdgeIndex, NodeIndex};
use rand::seq::SliceRandom;

use crate::controller::Controller;
use crate::critical::Path;
use crate::grouping::Group;
use crate::manifest::HierarchicalCriticalPath;
use crate::manifest::Manifest;
use crate::search::SearchStrategy;
use crate::settings::Settings;
use crate::trace::EventType;
use crate::trace::TracepointID;

pub struct HierarchicalSearch {
    controller: &'static Box<dyn Controller>,
    manifest: &'static Manifest,
}

impl SearchStrategy for HierarchicalSearch {
    fn search(&self, group: &Group, edge: EdgeIndex, budget: usize) -> Vec<TracepointID> {
        let mut rng = &mut rand::thread_rng();
        let (source, target) = group.g.edge_endpoints(edge).unwrap();
        let source_context = self.get_context(group, source);
        let target_context = self.get_context(group, target);
        let mut common_context = Vec::new();
        let mut idx = 0;
        loop {
            if idx >= source_context.len() || idx >= target_context.len() {
                break;
            } else if source_context[idx] == target_context[idx] {
                common_context.push(source_context[idx]);
                idx += 1;
            } else {
                break;
            }
        }
        println!("Common context for the search: {:?}", common_context);
        let matches = self.manifest.find_matches(group);
        let mut result = self.search_context(&matches, common_context);
        result = result
            .into_iter()
            .filter(|&x| !self.controller.is_enabled(&(x, Some(group.request_type))))
            .collect();
        result = result.choose_multiple(&mut rng, budget).cloned().collect();
        result
    }
}

impl HierarchicalSearch {
    pub fn new(_s: &Settings, m: &'static Manifest, c: &'static Box<dyn Controller>) -> Self {
        HierarchicalSearch {
            controller: c,
            manifest: m,
        }
    }

    fn search_context(
        &self,
        matches: &Vec<&HierarchicalCriticalPath>,
        context: Vec<TracepointID>,
    ) -> Vec<TracepointID> {
        let mut possible_child_nodes = Vec::new();
        for m in matches {
            let mut possible_next_nodes: Vec<(usize, Option<NodeIndex>)> = vec![(0, None)];
            while !possible_next_nodes.is_empty() {
                let to_eval = possible_next_nodes.pop().unwrap();
                if to_eval.0 == context.len() {
                    possible_child_nodes.push((
                        m,
                        match to_eval.1 {
                            Some(nidx) => nidx,
                            None => m.start_node,
                        },
                    ));
                    continue;
                }
                let tracepoint = context[to_eval.0];
                let nidx = to_eval.1;
                match nidx {
                    None => {
                        if m.g[m.start_node].tracepoint_id == tracepoint {
                            possible_next_nodes.push((to_eval.0 + 1, Some(m.start_node)))
                        }
                    }
                    Some(nidx) => {
                        for candidate in m
                            .child_nodes(nidx)
                            .iter()
                            .filter(|&a| m.g[*a].tracepoint_id == tracepoint)
                        {
                            possible_next_nodes.push((to_eval.0 + 1, Some(*candidate)));
                        }
                    }
                }
            }
        }
        let mut result = HashSet::new();
        for (path, nidx) in possible_child_nodes {
            for child in path.child_nodes(nidx) {
                result.insert(path.g[child].tracepoint_id);
            }
        }
        result.drain().collect()
    }

    fn get_context(&self, group: &Group, node: NodeIndex) -> Vec<TracepointID> {
        let mut result = Vec::new();
        let mut nidx = group.start_node;
        loop {
            match group.g[nidx].variant {
                EventType::Annotation => {
                    if nidx == node {
                        result.push(group.g[nidx].tracepoint_id);
                        break;
                    }
                }
                EventType::Exit => {
                    if nidx == node {
                        break;
                    }
                    assert_eq!(result.pop().unwrap(), group.g[nidx].tracepoint_id);
                }
                EventType::Entry => {
                    result.push(group.g[nidx].tracepoint_id);
                    if nidx == node {
                        break;
                    }
                }
            }
            nidx = group.next_node(nidx).unwrap();
        }
        result
    }
}

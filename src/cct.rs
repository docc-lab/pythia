use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Display;
use std::path::Path;

use petgraph::dot::Dot;
use petgraph::graph::{EdgeIndex, NodeIndex};
use petgraph::{Direction, Graph};
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};

use crate::critical::CriticalPath;
use crate::grouping::Group;
use crate::osprofiler::OSProfilerDAG;
use crate::search::SearchState;
use crate::search::SearchStrategy;
use crate::trace::EventEnum;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CCT {
    pub g: Graph<String, u32>, // Nodes indicate tracepoint id, edges don't matter
    pub entry_points: HashMap<String, NodeIndex>,
    #[serde(skip)]
    enabled_tracepoints: RefCell<HashSet<String>>,
}

#[typetag::serde]
impl SearchStrategy for CCT {
    fn add_trace(&mut self, trace: &OSProfilerDAG) {
        for path in CriticalPath::all_possible_paths(trace) {
            self.add_path_to_manifest(&path);
        }
    }

    fn get_entry_points(&self) -> Vec<&String> {
        self.entry_points.keys().collect()
    }

    fn search<'a>(
        &'a self,
        group: &Group,
        edge: EdgeIndex,
        budget: usize,
    ) -> (Vec<&'a String>, SearchState) {
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
        let mut result = self.search_context(common_context);
        let result_state = if result.len() > budget {
            SearchState::DepletedBudget
        } else {
            SearchState::NextEdge
        };
        if budget != 0 {
            result = result.choose_multiple(&mut rng, budget).cloned().collect();
        }
        for i in &result {
            let mut enabled_tracepoints = self.enabled_tracepoints.borrow_mut();
            enabled_tracepoints.insert(i.to_string());
        }
        (result, result_state)
    }
}

impl Default for CCT {
    fn default() -> Self {
        CCT {
            g: Graph::<String, u32>::new(),
            entry_points: HashMap::<String, NodeIndex>::new(),
            enabled_tracepoints: RefCell::new(HashSet::new()),
        }
    }
}

impl CCT {
    pub fn new() -> CCT {
        CCT {
            g: Graph::<String, u32>::new(),
            entry_points: HashMap::<String, NodeIndex>::new(),
            enabled_tracepoints: RefCell::new(HashSet::new()),
        }
    }

    pub fn from_trace_list(list: Vec<OSProfilerDAG>) -> CCT {
        let mut cct = CCT::new();
        println!("Creating manifest from {} traces", list.len());
        let mut counter = 0;
        let mut node_counter = 0;
        let mut path_node_counter = 0;
        for trace in &list {
            node_counter += trace.g.node_count();
            for path in CriticalPath::all_possible_paths(trace) {
                path_node_counter += path.g.g.node_count();
                cct.add_path_to_manifest(&path);
                counter += 1;
            }
        }
        println!("Used a total of {} paths", counter);
        println!("Total {} nodes in traces", node_counter);
        println!("Total {} nodes in paths", path_node_counter);
        cct
    }

    fn search_context<'a>(&'a self, context: Vec<&String>) -> Vec<&'a String> {
        let mut nidx: Option<NodeIndex> = None;
        for tracepoint in context {
            nidx = match nidx {
                None => {
                    let result = self.entry_points.get(tracepoint);
                    Some(*result.unwrap())
                }
                Some(nidx) => {
                    let result: Vec<NodeIndex> = self
                        .g
                        .neighbors_directed(nidx, Direction::Outgoing)
                        .filter(|a| self.g[*a] == *tracepoint)
                        .collect();
                    if result.len() == 0 {
                        // We are at a child node, look at the parent for more trace points to
                        // enable
                        continue;
                    }
                    assert!(result.len() == 1);
                    Some(result[0])
                }
            }
        }
        match nidx {
            None => Vec::new(),
            Some(nidx) => self
                .g
                .neighbors_directed(nidx, Direction::Outgoing)
                .map(|x| &self.g[x])
                .filter(|&a| self.enabled_tracepoints.borrow().get(a).is_none())
                .collect(),
        }
    }

    fn get_context<'a>(&self, group: &'a Group, node: NodeIndex) -> Vec<&'a String> {
        let mut result = Vec::new();
        let mut nidx = group.start_node;
        loop {
            match group.g[nidx].variant {
                EventEnum::Annotation => {
                    if nidx == node {
                        result.push(&group.g[nidx].tracepoint_id);
                        break;
                    }
                }
                EventEnum::Exit => {
                    if nidx == node {
                        break;
                    }
                    assert_eq!(*result.pop().unwrap(), group.g[nidx].tracepoint_id);
                }
                EventEnum::Entry => {
                    result.push(&group.g[nidx].tracepoint_id);
                    if nidx == node {
                        break;
                    }
                }
            }
            nidx = group.next_node(nidx).unwrap();
        }
        result
    }

    pub fn from_file(file: &Path) -> Option<CCT> {
        let reader = match std::fs::File::open(file) {
            Ok(x) => x,
            Err(_) => return None,
        };
        Some(serde_json::from_reader(reader).unwrap())
    }

    pub fn to_file(&self, file: &Path) {
        let writer = std::fs::File::create(file).unwrap();
        serde_json::to_writer(writer, self).expect("Failed to manifest to cache");
    }

    fn add_path_to_manifest(&mut self, path: &CriticalPath) {
        assert!(path.is_hypothetical);
        let mut cur_path_nidx = path.start_node;
        let mut cur_manifest_nidx = None;
        loop {
            let cur_span = &path.g.g[cur_path_nidx].span;
            if cur_span.tracepoint_id == "/opt/stack/neutron/neutron/agent/dhcp/agent.py:580:neutron.agent.dhcp.agent.DhcpAgent.port_create_end" {
                if cur_manifest_nidx.is_none() {
                    println!("At that node, trace_id: {}", path.g.base_id.to_hyphenated().to_string());
                }
            }
            match cur_span.variant {
                EventEnum::Entry => {
                    let next_nidx = match cur_manifest_nidx {
                        Some(nidx) => self.add_child_if_necessary(nidx, &cur_span.tracepoint_id),
                        None => match self.entry_points.get(&cur_span.tracepoint_id) {
                            Some(nidx) => *nidx,
                            None => {
                                let new_nidx = self.g.add_node(cur_span.tracepoint_id.clone());
                                self.entry_points
                                    .insert(cur_span.tracepoint_id.clone(), new_nidx);
                                new_nidx
                            }
                        },
                    };
                    cur_manifest_nidx = Some(next_nidx);
                }
                EventEnum::Annotation => {
                    self.add_child_if_necessary(
                        cur_manifest_nidx.unwrap(),
                        &cur_span.tracepoint_id,
                    );
                }
                EventEnum::Exit => {
                    let mut parent_nidx = self.find_parent(cur_manifest_nidx.unwrap());
                    if cur_span.tracepoint_id == self.g[cur_manifest_nidx.unwrap()] {
                        cur_manifest_nidx = parent_nidx;
                    } else {
                        loop {
                            match parent_nidx {
                                Some(nidx) => {
                                    if self.g[nidx] == cur_span.tracepoint_id {
                                        break;
                                    }
                                    parent_nidx = self.find_parent(nidx);
                                }
                                None => {
                                    panic!("Couldn't find parent");
                                }
                            }
                        }
                    }
                }
            }
            cur_path_nidx = match path.next_node(cur_path_nidx) {
                Some(nidx) => nidx,
                None => break,
            };
        }
    }

    fn add_child_if_necessary(&mut self, parent: NodeIndex, node: &str) -> NodeIndex {
        match self.find_child(parent, node) {
            Some(child_nidx) => child_nidx,
            None => self.add_child(parent, node),
        }
    }

    fn add_child(&mut self, parent: NodeIndex, node: &str) -> NodeIndex {
        let nidx = self.g.add_node(String::from(node));
        self.g.add_edge(parent, nidx, 1);
        nidx
    }

    fn find_parent(&mut self, node: NodeIndex) -> Option<NodeIndex> {
        let mut matches = self.g.neighbors_directed(node, Direction::Incoming);
        let result = matches.next();
        assert!(matches.next().is_none());
        result
    }

    fn find_child(&mut self, parent: NodeIndex, node: &str) -> Option<NodeIndex> {
        let mut matches = self
            .g
            .neighbors_directed(parent, Direction::Outgoing)
            .filter(|&a| self.g[a] == node);
        let result = matches.next();
        assert!(matches.next().is_none());
        result
    }
}

impl Display for CCT {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", Dot::new(&self.g))
    }
}

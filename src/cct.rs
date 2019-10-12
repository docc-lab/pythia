use std::collections::HashMap;
use std::path::Path;

use petgraph::{Graph, Direction};
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};

use trace::EventEnum;
use osprofiler::OSProfilerDAG;
use critical::CriticalPath;



#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CCT {
    pub g: Graph<String, u32>, // Nodes indicate tracepoint id, edges don't matter
    pub entry_points: HashMap<String, NodeIndex>
}

impl CCT {
    pub fn new() -> CCT {
        CCT{
            g: Graph::<String, u32>::new(),
            entry_points: HashMap::<String, NodeIndex>::new()
        }
    }

    pub fn add_trace(&mut self, trace: &OSProfilerDAG) {
        for path in CriticalPath::all_possible_paths(trace) {
            self.add_path_to_manifest(&path);
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

    pub fn from_file(file: &Path) -> Option<CCT> {
        let reader = match std::fs::File::open(file) {
            Ok(x) => x,
            Err(_) => return None
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
                        Some(nidx) => {
                            self.add_child_if_necessary(nidx, &cur_span.tracepoint_id)
                        },
                        None => {
                            match self.entry_points.get(&cur_span.tracepoint_id) {
                                Some(nidx) => *nidx,
                                None => {
                                    let new_nidx = self.g.add_node(cur_span.tracepoint_id.clone());
                                    self.entry_points.insert(cur_span.tracepoint_id.clone(), new_nidx);
                                    new_nidx
                                }
                            }
                        }
                    };
                    cur_manifest_nidx = Some(next_nidx);
                },
                EventEnum::Annotation => {
                    self.add_child_if_necessary(
                        cur_manifest_nidx.unwrap(), &cur_span.tracepoint_id);
                },
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
                                },
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
                None => break
            };
        }
    }

    fn add_child_if_necessary(&mut self, parent: NodeIndex, node: &str) -> NodeIndex {
        match self.find_child(parent, node) {
            Some(child_nidx) => child_nidx,
            None => self.add_child(parent, node)
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
        let mut matches = self.g.neighbors_directed(parent, Direction::Outgoing)
            .filter(|&a| self.g[a] == node);
        let result = matches.next();
        assert!(matches.next().is_none());
        result
    }
}

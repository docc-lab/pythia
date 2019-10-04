extern crate redis;
extern crate serde_json;
extern crate serde;
extern crate uuid;
extern crate chrono;
extern crate petgraph;
extern crate single;

pub mod trace;
pub mod osprofiler;
pub mod options;
pub mod critical;

use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;

use petgraph::{Graph, dot::Dot, Direction};

use trace::Event;
use trace::EventEnum;
use osprofiler::OSProfilerDAG;
use options::LINE_WIDTH;
use options::ManifestMethod;
use options::MANIFEST_METHOD;
use critical::CriticalPath;

pub fn get_manifest(manfile: &str) {
    let trace_ids = std::fs::read_to_string(manfile).unwrap();
    let mut traces = Vec::new();
    for id in trace_ids.split('\n') {
        if id.len() <= 1 {
            continue;
        }
        println!("Working on {:?}", id);
        let trace = OSProfilerDAG::from_base_id(id);
        traces.push(trace);
    }
    match MANIFEST_METHOD {
        ManifestMethod::Poset => {
            // let manifest = Poset::from_trace_list(traces);
            // println!("{}", Dot::new(&manifest.g));
        },
        ManifestMethod::CCT => {
            let manifest = CCT::from_trace_list(traces);
            println!("{}", Dot::new(&manifest.g));
        }
    };
}

pub fn get_trace(trace_id: &str) {
    let trace = OSProfilerDAG::from_base_id(trace_id);
    println!("{}", Dot::new(&trace.g));
}

pub fn get_crit(trace_id: &str) {
    let trace = OSProfilerDAG::from_base_id(trace_id);
    let crit = CriticalPath::from_trace(&trace);
    println!("{}", Dot::new(&crit.g.g));
}

use petgraph::graph::NodeIndex;

struct CCT {
    g: Graph<String, u32>, // Nodes indicate tracepoint id, edges don't matter
    entry_points: HashMap<String, NodeIndex>
}

impl CCT {
    fn from_trace_list(list: Vec<OSProfilerDAG>) -> CCT {
        let mut cct = CCT{ g: Graph::<String, u32>::new(),
                           entry_points: HashMap::<String, NodeIndex>::new() };
        for trace in &list {
            for mut path in CriticalPath::all_possible_paths(trace) {
                path.filter_incomplete_spans();
                cct.add_to_manifest(&path);
            }
        }
        cct
    }

    fn add_to_manifest(&mut self, path: &CriticalPath) {
        assert!(path.is_hypothetical);
        let mut cur_path_nidx = path.start_node;
        let mut cur_manifest_nidx = None;
        loop {
            let cur_span = &path.g.g[cur_path_nidx].span;
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
                        let nidx_to_move;
                        loop {
                            match parent_nidx {
                                Some(nidx) => {
                                    if self.g[nidx] == cur_span.tracepoint_id {
                                        nidx_to_move = nidx;
                                        parent_nidx = self.find_parent(nidx);
                                        break;
                                    }
                                    parent_nidx = self.find_parent(nidx);
                                },
                                None => {
                                    nidx_to_move = cur_manifest_nidx.unwrap();
                                    break;
                                }
                            }
                        }
                        self.move_to_parent(nidx_to_move, parent_nidx);
                    }
                }
            }
            cur_path_nidx = match path.next_node(cur_path_nidx) {
                Some(nidx) => nidx,
                None => break
            };
        }
    }

    fn move_to_parent(&mut self, node: NodeIndex, new_parent: Option<NodeIndex>) {
        let cur_parent = self.find_parent(node);
        match cur_parent {
            Some(p) => {
                let edge = self.g.find_edge(p, node).unwrap();
                self.g.remove_edge(edge);
            },
            None => {}
        }
        if !new_parent.is_none() {
            self.g.add_edge(new_parent.unwrap(), node, 1);
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

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct ManifestNode {
    pub tracepoint_id: String,
    pub variant: EventEnum
}

impl ManifestNode {
    fn _from_event(span: &Event) -> ManifestNode {
        ManifestNode { tracepoint_id: span.tracepoint_id.clone(), variant: span.variant }
    }
}

impl Display for ManifestNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Break the tracepoint id into multiple lines so that the graphs look prettier
        let mut result = String::with_capacity(self.tracepoint_id.len() + 10);
        let mut written = 0;
        while written <= self.tracepoint_id.len() {
            if written + LINE_WIDTH <= self.tracepoint_id.len() {
                result.push_str(&self.tracepoint_id[written..written + LINE_WIDTH]);
                result.push_str("-\n");
            } else {
                result.push_str(&self.tracepoint_id[written..self.tracepoint_id.len()]);
            }
            written += LINE_WIDTH;
        }
        match self.variant {
            EventEnum::Entry => result.push_str(": S"),
            EventEnum::Exit => result.push_str(": E"),
            EventEnum::Annotation => result.push_str(": A")
        };
        write!(f, "{}", result)
    }
}

// struct Poset {
//     g: Graph<ManifestNode, u32> // Edge weights indicate number of occurance of an ordering.
// }

// impl Poset {
//     fn from_trace_list(list: Vec<OSProfilerDAG>) -> Poset {
//         let mut dag = Graph::<ManifestNode, u32>::new();
//         let mut node_index_map = HashMap::new();
//         for trace in &list {
//             for nid in trace.g.raw_nodes() {
//                 let node = ManifestNode::from_event(&nid.weight.span);
//                 match node_index_map.get(&node) {
//                     Some(_) => {},
//                     None => {
//                         node_index_map.insert(node.clone(), dag.add_node(node));
//                     }
//                 }
//             }
//         }
//         for trace in &list {
//             for edge in trace.g.raw_edges() {
//                 let source = *node_index_map.get(&ManifestNode::from_event(&trace.g[edge.source()].span)).unwrap();
//                 let target = *node_index_map.get(&ManifestNode::from_event(&trace.g[edge.target()].span)).unwrap();
//                 match dag.find_edge(source, target) {
//                     Some(idx) => {
//                         dag[idx] += 1;
//                     },
//                     None => {
//                         dag.add_edge(source, target, 1);
//                     }
//                 }
//             }
//         }
//         Poset{g: dag}
//     }
// }


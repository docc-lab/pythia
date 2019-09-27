extern crate redis;
extern crate serde_json;
extern crate serde;
extern crate uuid;
extern crate chrono;
extern crate petgraph;

pub mod trace;
pub mod osprofiler;
pub mod options;

use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::time::Duration;

use petgraph::{Graph, dot::Dot, Direction};

use trace::Event;
use trace::EventEnum;
use osprofiler::OSProfilerDAG;
use options::LINE_WIDTH;
use options::ManifestMethod;
use options::MANIFEST_METHOD;

pub fn redis_main() {
    // let event_list = get_matches("ffd1560e-7928-437c-87e9-a712c85ed2ac").unwrap();
    // let trace = create_dag(event_list);
    // println!("{}", Dot::new(&trace));
    // return;
    let trace_ids = std::fs::read_to_string("/opt/stack/requests.txt").unwrap();
    for id in trace_ids.split('\n') {
        if id.len() <= 1 {
            continue;
        }
        println!("Working on {:?}", id);
        let trace = OSProfilerDAG::from_base_id(id);
        println!("{}", Dot::new(&trace.g));
        let crit = CriticalPath::from_trace(trace);
        println!("{:?}", crit);
    }
}

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
            let manifest = Poset::from_trace_list(traces);
            println!("{}", Dot::new(&manifest.g));
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
    let crit = CriticalPath::from_trace(trace);
    println!("{}", Dot::new(&crit.g.g));
}

use petgraph::graph::NodeIndex;
use uuid::Uuid;

struct CCT {
    g: Graph<String, u32>, // Nodes indicate tracepoint id, edges don't matter
    entry_points: HashMap<String, NodeIndex>
}

impl CCT {
    fn from_trace_list(list: Vec<OSProfilerDAG>) -> CCT {
        let mut cct = CCT{ g: Graph::<String, u32>::new(),
                           entry_points: HashMap::<String, NodeIndex>::new() };
        for trace in &list {
            for path in HypotheticalCriticalPath::from_trace(trace) {
                cct.add_to_manifest(&path);
            }
        }
        cct
    }

    fn add_to_manifest(&mut self, path: &HypotheticalCriticalPath) {
        let mut stack = Vec::<(NodeIndex, Uuid)>::new();
        let mut next_nidx = path.start_node;
        let mut next_node = path.g.g[path.start_node];
        loop {
            let next_tracepoint_id = &next_node.span.tracepoint_id;
            match next_node.span.variant {
                EventEnum::Entry => {
            let idx = match self.entry_points.get(&next_node.span.tracepoint_id) {
                Some(&i) => i,
                None => self.g.add_node(next_node.span.tracepoint_id.clone())
            };
            stack.push((idx, next_node.span.trace_id));
                    let mut matches = self.g.neighbors_directed(stack.last().unwrap().0, Direction::Outgoing)
                        .filter(|&nidx| self.g[nidx] == *cur_tracepoint_id);
                    let first = matches.next();
                    assert!(matches.next() == None);
                    let new_idx = match first {
                        Some(nidx) => nidx,
                        None => self.g.add_node(cur_tracepoint_id.clone())
                    };
                    stack.push((new_idx, trace.g[trace_idx].span.trace_id));
                },
                EventEnum::Exit => {
                    let mut self_idx = NodeIndex::end();
                    let mut self_trace_idx;
                    let mut found_self = false;
                    let mut index_to_remove = 0;
                    for (idx, i) in stack.iter().enumerate().rev() {
                        if found_self {
                            let parent_idx = i.0;
                            match self.g.find_edge(parent_idx, self_idx) {
                                Some(_) => {},
                                None => {self.g.add_edge(parent_idx, self_idx, 1);}
                            }
                            break;
                        }
                        self_idx = i.0;
                        self_trace_idx = i.1;
                        if *cur_tracepoint_id == self.g[self_idx] {
                            assert_eq!(self_trace_idx, trace.g[trace_idx].span.trace_id);
                            index_to_remove = idx;
                            found_self = true;
                        }
                    }
                    // Remove self from the stack
                    stack.remove(index_to_remove);
                },
                EventEnum::Annotation => {
                    let matches: Vec<_> = self.g
                        .neighbors_directed(stack.last().unwrap().0, Direction::Outgoing)
                        .filter(|&nidx| self.g[nidx] == *cur_tracepoint_id).collect();
                    if matches.len() == 1 {
                        matches[0]
                    } else if matches.len() == 0 {
                        let nidx = self.g.add_node(cur_tracepoint_id.clone());
                        self.g.add_edge(stack.last().unwrap().0, nidx, 1);
                        nidx
                    } else {
                        panic!("Too many matches");
                    };
                }
            };
            let next_nodes: Vec<_> = path.g.g.neighbors_directed(next_nidx, Direction::Outgoing).collect();
            assert!(next_nodes.len() == 1);
            let next_node = path.g.g[next_nodes[0]];
        }
    }

    fn manifest_helper(&mut self, path: &HypotheticalCriticalPath, trace_idx: NodeIndex,
                       stack: &mut Vec<(NodeIndex, Uuid)>) {
        // This function traverses the new trace and the manifest concurrently,
        // and adds new nodes as needed
        // trace_idx is the node to be added from the trace, stack.last() is the
        // parent of that node
    }
}

struct Poset {
    g: Graph<ManifestNode, u32> // Edge weights indicate number of occurance of an ordering.
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct ManifestNode {
    pub tracepoint_id: String,
    pub variant: EventEnum
}

impl ManifestNode {
    fn from_event(span: &Event) -> ManifestNode {
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

impl Poset {
    fn from_trace_list(list: Vec<OSProfilerDAG>) -> Poset {
        let mut dag = Graph::<ManifestNode, u32>::new();
        let mut node_index_map = HashMap::new();
        for trace in &list {
            for nid in trace.g.raw_nodes() {
                let node = ManifestNode::from_event(&nid.weight.span);
                match node_index_map.get(&node) {
                    Some(_) => {},
                    None => {
                        node_index_map.insert(node.clone(), dag.add_node(node));
                    }
                }
            }
        }
        for trace in &list {
            for edge in trace.g.raw_edges() {
                let source = *node_index_map.get(&ManifestNode::from_event(&trace.g[edge.source()].span)).unwrap();
                let target = *node_index_map.get(&ManifestNode::from_event(&trace.g[edge.target()].span)).unwrap();
                match dag.find_edge(source, target) {
                    Some(idx) => {
                        dag[idx] += 1;
                    },
                    None => {
                        dag.add_edge(source, target, 1);
                    }
                }
            }
        }
        Poset{g: dag}
    }
}

#[derive(Debug)]
struct CriticalPath {
    g: OSProfilerDAG,
    duration: Duration,
}

impl CriticalPath {
    fn from_trace(dag: OSProfilerDAG) -> CriticalPath {
        let mut path = CriticalPath {
            duration: Duration::new(0, 0),
            g: OSProfilerDAG::new()
        };
        let mut cur_node = dag.end_node;
        let mut end_nidx = path.g.g.add_node(dag.g[cur_node].clone());
        loop {
            let next_node = dag.g.neighbors_directed(cur_node, Direction::Incoming).max_by_key(|&nidx| dag.g[nidx].span.timestamp).unwrap();
            let start_nidx = path.g.g.add_node(dag.g[next_node].clone());
            path.g.g.add_edge(start_nidx, end_nidx, dag.g[dag.g.find_edge(next_node, cur_node).unwrap()].clone());
            if next_node == dag.start_node { break; }
            cur_node = next_node;
            end_nidx = start_nidx;
        }
        path
    }
}

#[derive(Debug, Clone)]
struct HypotheticalCriticalPath {
    /// A HypotheticalCriticalPath is a possible critical path that
    /// is generated by splitting the critical path into two at every
    /// concurrent part of the trace.
    g: OSProfilerDAG,
    start_node: NodeIndex,
    end_node: NodeIndex
}

impl HypotheticalCriticalPath {
    fn from_trace(dag: &OSProfilerDAG) -> Vec<HypotheticalCriticalPath> {
        let mut result = Vec::new();
        for end_node in dag.possible_end_nodes() {
            let mut path = HypotheticalCriticalPath {
                g: OSProfilerDAG::new(),
                start_node: NodeIndex::end(),
                end_node: NodeIndex::end()
            };
            let cur_node = end_node;
            let end_nidx = path.g.g.add_node(dag.g[cur_node].clone());
            path.end_node = end_nidx;
            result.extend(HypotheticalCriticalPath::from_trace_helper(dag, cur_node, end_nidx, path));
        }
        result
    }

    fn from_trace_helper(dag: &OSProfilerDAG, cur_node: NodeIndex, end_nidx: NodeIndex,
        mut path: HypotheticalCriticalPath) -> Vec<HypotheticalCriticalPath> {
        let next_nodes: Vec<_> = dag.g.neighbors_directed(cur_node, Direction::Incoming).collect();
        if next_nodes.len() == 0 {
            panic!("Path finished too early");
        } else if next_nodes.len() == 1 {
            let next_node = next_nodes[0];
            let start_nidx = path.g.g.add_node(dag.g[next_node].clone());
            path.g.g.add_edge(start_nidx, end_nidx, dag.g[dag.g.find_edge(next_node, cur_node).unwrap()].clone());
            if next_node == dag.start_node {
                path.start_node = start_nidx;
                vec![path]
            } else {
                HypotheticalCriticalPath::from_trace_helper(dag, next_node, start_nidx, path)
            }
        } else {
            let mut result = Vec::new();
            for next_node in next_nodes {
                let mut new_path = path.clone();
                let start_nidx = new_path.g.g.add_node(dag.g[next_node].clone());
                new_path.g.g.add_edge(start_nidx, end_nidx, dag.g[dag.g.find_edge(next_node, cur_node).unwrap()].clone());
                if next_node == dag.start_node {
                    path.start_node = start_nidx;
                    result.push(new_path);
                } else {
                    result.extend(HypotheticalCriticalPath::from_trace_helper(dag, next_node, start_nidx, new_path));
                }
            }
            result
        }
    }
}

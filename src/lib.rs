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
    let manifest = Poset::from_trace_list(traces);
    println!("{}", Dot::new(&manifest.g));
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

struct Poset {
    g: Graph<ManifestNode, u32>
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

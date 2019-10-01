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

use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::time::Duration;

use petgraph::{Graph, dot::Dot, Direction};

use trace::Event;
use trace::EventEnum;
use trace::DAGEdge;
use trace::DAGNode;
use trace::EdgeType;
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
        let crit = CriticalPath::from_trace(&trace);
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
        println!("Adding this path: {}", Dot::new(&path.g.g));
        let mut cur_path_nidx = path.start_node;
        let mut cur_manifest_nidx = None;
        loop {
            let cur_span = &path.g.g[cur_path_nidx].span;
            match cur_manifest_nidx {
                None => println!("Cur node: None"),
                Some(nidx) => println!("Cur node: {:?}", self.g[nidx])
            }
            println!("Adding {:?}", cur_span);
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
                                        parent_nidx = self.find_parent(nidx);
                                        break;
                                    }
                                    parent_nidx = self.find_parent(nidx);
                                },
                                None => break
                            }
                        }
                        match parent_nidx {
                            Some(nidx) => {
                                self.move_to_parent(cur_manifest_nidx.unwrap(), nidx);
                                cur_manifest_nidx = Some(nidx);
                            },
                            None => {
                                self.entry_points.insert(cur_span.tracepoint_id.clone(), cur_manifest_nidx.unwrap());
                                cur_manifest_nidx = None;
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

    fn move_to_parent(&mut self, node: NodeIndex, new_parent: NodeIndex) {
        let cur_parent = self.find_parent(node);
        match cur_parent {
            Some(p) => {
                let edge = self.g.find_edge(p, node).unwrap();
                self.g.remove_edge(edge);
            },
            None => {}
        }
        self.g.add_edge(new_parent, node, 1);
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

#[derive(Debug, Clone)]
struct CriticalPath {
    g: OSProfilerDAG,
    start_node: NodeIndex,
    end_node: NodeIndex,
    duration: Duration,
    is_hypothetical: bool
}

impl CriticalPath {
    fn from_trace(dag: &OSProfilerDAG) -> CriticalPath {
        let mut path = CriticalPath {
            duration: Duration::new(0, 0),
            g: OSProfilerDAG::new(),
            start_node: NodeIndex::end(),
            end_node: NodeIndex::end(),
            is_hypothetical: false
        };
        let mut cur_node = dag.end_node;
        let mut end_nidx = path.g.g.add_node(dag.g[cur_node].clone());
        path.end_node = end_nidx;
        loop {
            let next_node = dag.g.neighbors_directed(cur_node, Direction::Incoming).max_by_key(|&nidx| dag.g[nidx].span.timestamp).unwrap();
            let start_nidx = path.g.g.add_node(dag.g[next_node].clone());
            path.g.g.add_edge(start_nidx, end_nidx, dag.g[dag.g.find_edge(next_node, cur_node).unwrap()].clone());
            if next_node == dag.start_node {
                path.start_node = start_nidx;
                break;
            }
            cur_node = next_node;
            end_nidx = start_nidx;
        }
        path.add_synthetic_nodes(dag);
        path
    }

    /// This method returns all possible critical paths that
    /// are generated by splitting the critical path into two at every
    /// concurrent part of the trace.
    fn all_possible_paths(dag: &OSProfilerDAG) -> Vec<CriticalPath> {
        let mut result = Vec::new();
        for end_node in dag.possible_end_nodes() {
            let mut path = CriticalPath {
                g: OSProfilerDAG::new(),
                start_node: NodeIndex::end(),
                end_node: NodeIndex::end(),
                duration: Duration::new(0, 0),
                is_hypothetical: true
            };
            let cur_node = end_node;
            let end_nidx = path.g.g.add_node(dag.g[cur_node].clone());
            path.end_node = end_nidx;
            result.extend(CriticalPath::possible_paths_helper(dag, cur_node, end_nidx, path));
        }
        for i in &mut result {
            i.add_synthetic_nodes(dag);
        }
        result
    }

    fn possible_paths_helper(dag: &OSProfilerDAG, cur_node: NodeIndex, end_nidx: NodeIndex,
        mut path: CriticalPath) -> Vec<CriticalPath> {
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
                CriticalPath::possible_paths_helper(dag, next_node, start_nidx, path)
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
                    result.extend(CriticalPath::possible_paths_helper(dag, next_node, start_nidx, new_path));
                }
            }
            result
        }
    }

    fn filter_incomplete_spans(&mut self) {
        let mut cur_node = self.start_node;
        let mut span_map = HashMap::new();
        loop {
            match self.g.g[cur_node].span.variant {
                EventEnum::Entry => {
                    span_map.insert(self.g.g[cur_node].span.trace_id, cur_node);
                },
                EventEnum::Annotation => {},
                EventEnum::Exit => {
                    match &span_map.get(&self.g.g[cur_node].span.trace_id) {
                        Some(_) => {
                           span_map.remove(&self.g.g[cur_node].span.trace_id);
                        },
                        None => {
                            self.remove_node(cur_node);
                        }
                    }
                }
            }
            cur_node = match self.next_node(cur_node) {
                Some(nidx) => nidx,
                None => break
            }
        }
        for (_, nidx) in span_map {
            self.remove_node(nidx);
        }
    }

    /// We add synthetic nodes for spans with exit nodes off the critical path
    /// e.g.,
    /// A_start -> B_start -> C_start -> C_end -> ... rest of the path
    ///                   \-> D_start -> B_end -> A_end
    /// We add B_end and A_end (in that order) right before C_start
    fn add_synthetic_nodes(&mut self, dag: &OSProfilerDAG) {
        let mut cur_nidx = self.start_node;
        let mut cur_dag_nidx = dag.start_node;
        let mut active_spans = Vec::new();
        loop {
            let cur_node = &self.g.g[cur_nidx];
            let cur_dag_node = &dag.g[cur_dag_nidx];
            match cur_node.span.variant {
                EventEnum::Entry => {
                    active_spans.push(cur_dag_node.span.clone());
                },
                EventEnum::Annotation => {},
                EventEnum::Exit => {
                    let to_remove = active_spans.iter()
                        .rposition(|span| span.trace_id == cur_node.span.trace_id)
                        .unwrap();
                    active_spans.remove(to_remove);
                }
            }
            let next_nidx = match self.next_node(cur_nidx) {
                Some(nidx) => nidx,
                None => {
                    assert!(active_spans.is_empty());
                    break
                }
            };
            let next_dag_nodes = dag.g.neighbors_directed(cur_dag_nidx, Direction::Outgoing).collect::<Vec<_>>();
            if next_dag_nodes.len() == 1 {
                cur_dag_nidx = next_dag_nodes[0];
            } else {
                assert!(next_dag_nodes.len() != 0);
                let mut next_dag_nidx = next_dag_nodes.iter()
                    .filter(|&nidx| dag.g[*nidx].span.trace_id == self.g.g[next_nidx].span.trace_id);
                cur_dag_nidx = *next_dag_nidx.next().unwrap();
                assert!(next_dag_nidx.next().is_none());
                let unfinished_spans = self.find_unfinished(&active_spans, next_nidx);
                for span in unfinished_spans.iter().rev() {
                    self.add_node_after(cur_nidx, span);
                }
            }
            cur_nidx = next_nidx;
        }
    }

    fn find_unfinished(&self, spans: &Vec<Event>, nidx: NodeIndex) -> Vec<Event> {
        let mut result = spans.clone();
        let mut cur_nidx = nidx;
        loop {
            let mut to_remove = None;
            for (idx, span) in result.iter().enumerate() {
                if span.trace_id == self.g.g[cur_nidx].span.trace_id {
                    to_remove = Some(idx);
                }
            }
            match to_remove {
                Some(idx) => {result.remove(idx);},
                None => {}
            }
            cur_nidx = match self.next_node(cur_nidx) {
                Some(nidx) => nidx,
                None => break
            };
        }
        result
    }

    fn next_node(&self, nidx: NodeIndex) -> Option<NodeIndex> {
        let mut matches = self.g.g.neighbors_directed(nidx, Direction::Outgoing);
        let result = matches.next();
        assert!(matches.next().is_none());
        result
    }

    fn prev_node(&self, nidx: NodeIndex) -> Option<NodeIndex> {
        let mut matches = self.g.g.neighbors_directed(nidx, Direction::Incoming);
        let result = matches.next();
        assert!(matches.next().is_none());
        result
    }

    fn remove_node(&mut self, nidx: NodeIndex) {
        let next_node = self.next_node(nidx);
        let prev_node = self.prev_node(nidx);
        match next_node {
            Some(next_nidx) => {
                self.g.g.remove_edge(self.g.g.find_edge(nidx, next_nidx).unwrap());
                match prev_node {
                    Some(prev_nidx) => {
                        self.g.g.remove_edge(self.g.g.find_edge(prev_nidx, nidx).unwrap());
                        self.g.g.add_edge(prev_nidx, next_nidx, DAGEdge{
                            duration: (self.g.g[next_nidx].span.timestamp
                                       - self.g.g[prev_nidx].span.timestamp).to_std().unwrap(),
                            variant: EdgeType::ChildOf});
                    },
                    None => {
                        self.start_node = next_nidx;
                    }
                }
            },
            None => {
                match prev_node {
                    Some(prev_nidx) => {
                        self.g.g.remove_edge(self.g.g.find_edge(prev_nidx, nidx).unwrap());
                        self.end_node = prev_nidx;
                    },
                    None => {
                        panic!("Something went wrong here");
                    }
                }
            }
        }
        self.g.g.remove_node(nidx);
    }

    fn add_node_after(&mut self, after: NodeIndex, node: &Event) {
        let next_node = self.next_node(after);
        let new_node = self.g.g.add_node(DAGNode{span: node.clone()});
        self.g.g.add_edge(after, new_node, DAGEdge{
            duration: Duration::new(0, 0), variant: EdgeType::ChildOf});
        match next_node {
            Some(next_nidx) => {
                let old_edge = self.g.g.find_edge(after, next_nidx).unwrap();
                let old_duration = self.g.g[old_edge].duration;
                self.g.g.remove_edge(old_edge);
                self.g.g.add_edge(new_node, next_nidx, DAGEdge{
                    duration: old_duration, variant: EdgeType::ChildOf});
            },
            None => {
                self.end_node = new_node;
            }
        }
    }
}

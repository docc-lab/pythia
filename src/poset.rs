use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;

use petgraph::dot::Dot;
use petgraph::graph::EdgeIndex;
use petgraph::graph::Graph;
use petgraph::graph::NodeIndex;
use petgraph::Direction;
use serde::{Deserialize, Serialize};

use critical::CriticalPath;
use grouping::Group;
use manifest::SearchSpace;
use osprofiler::OSProfilerDAG;
use trace::Event;
use trace::EventEnum;

#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
struct PosetNode {
    pub tracepoint_id: String,
    pub variant: EventEnum,
}

impl PosetNode {
    fn from_event(span: &Event) -> PosetNode {
        PosetNode {
            tracepoint_id: span.tracepoint_id.clone(),
            variant: span.variant,
        }
    }
}

impl Display for PosetNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        const LINE_WIDTH: usize = 75;
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
            EventEnum::Annotation => result.push_str(": A"),
        };
        write!(f, "{}", result)
    }
}

#[derive(Serialize, Deserialize)]
pub struct Poset {
    g: Graph<PosetNode, u32>, // Edge weights indicate number of occurance of an ordering.
    entry_points: HashMap<PosetNode, NodeIndex>,
}

impl SearchSpace for Poset {
    fn new() -> Self {
        Poset {
            g: Graph::<PosetNode, u32>::new(),
            entry_points: HashMap::new(),
        }
    }

    fn add_trace(&mut self, trace: &OSProfilerDAG) {
        for path in &CriticalPath::all_possible_paths(trace) {
            self.add_path(path);
        }
    }

    fn get_entry_points(&self) -> Vec<&String> {
        Vec::new()
    }

    fn search(&self, _group: &Group, _edge: EdgeIndex) -> Vec<&String> {
        Vec::new()
    }
}

impl Poset {
    fn add_path(&mut self, path: &CriticalPath) {
        let mut cur_path_nidx = path.start_node;
        let new_node = PosetNode::from_event(&path.g.g[cur_path_nidx].span);
        let mut cur_nidx = match self.entry_points.get(&new_node) {
            Some(nidx) => *nidx,
            None => {
                let nidx = self.g.add_node(new_node.clone());
                self.entry_points.insert(new_node, nidx);
                nidx
            }
        };
        loop {
            let next_path_nidx = match path.next_node(cur_path_nidx) {
                Some(nidx) => nidx,
                None => break,
            };
            let new_node = PosetNode::from_event(&path.g.g[next_path_nidx].span);
            let next_nidx = match self
                .g
                .neighbors_directed(cur_nidx, Direction::Outgoing)
                .find(|&a| self.g[a] == new_node)
            {
                Some(nidx) => nidx,
                None => {
                    let nidx = self.g.add_node(new_node.clone());
                    self.entry_points.insert(new_node, nidx);
                    nidx
                }
            };
            match self.g.find_edge(cur_nidx, next_nidx) {
                Some(edge_idx) => {
                    self.g[edge_idx] += 1;
                }
                None => {
                    self.g.add_edge(cur_nidx, next_nidx, 1);
                }
            }
            cur_path_nidx = next_path_nidx;
            cur_nidx = next_nidx;
        }
    }
}

impl Display for Poset {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", Dot::new(&self.g))
    }
}

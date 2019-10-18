use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;

use petgraph::dot::Dot;
use petgraph::graph::EdgeIndex;
use petgraph::graph::Graph;
use petgraph::graph::NodeIndex;
use petgraph::visit::IntoNodeReferences;
use serde::{Deserialize, Serialize};

use grouping::Group;
use manifest::SearchSpace;
use osprofiler::OSProfilerDAG;
use trace::Event;
use trace::EventEnum;

#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
struct ManifestNode {
    pub tracepoint_id: String,
    pub variant: EventEnum,
}

impl ManifestNode {
    fn from_event(span: &Event) -> ManifestNode {
        ManifestNode {
            tracepoint_id: span.tracepoint_id.clone(),
            variant: span.variant,
        }
    }
}

impl Display for ManifestNode {
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
    g: Graph<ManifestNode, u32>, // Edge weights indicate number of occurance of an ordering.
    node_index_map: HashMap<ManifestNode, NodeIndex>,
}

impl SearchSpace for Poset {
    fn new() -> Self {
        Poset {
            g: Graph::<ManifestNode, u32>::new(),
            node_index_map: HashMap::new(),
        }
    }

    fn add_trace(&mut self, trace: &OSProfilerDAG) {
        for nid in trace.g.node_references() {
            let node = ManifestNode::from_event(&trace.g[nid.0].span);
            match self.node_index_map.get(&node) {
                Some(_) => {}
                None => {
                    self.node_index_map
                        .insert(node.clone(), self.g.add_node(node));
                }
            }
        }
        for edge in trace.g.edge_indices() {
            let source = *self
                .node_index_map
                .get(&ManifestNode::from_event(
                    &trace.g[trace.g.edge_endpoints(edge).unwrap().0].span,
                ))
                .unwrap();
            let target = *self
                .node_index_map
                .get(&ManifestNode::from_event(
                    &trace.g[trace.g.edge_endpoints(edge).unwrap().1].span,
                ))
                .unwrap();
            match self.g.find_edge(source, target) {
                Some(idx) => {
                    self.g[idx] += 1;
                }
                None => {
                    self.g.add_edge(source, target, 1);
                }
            }
        }
    }

    fn get_entry_points(&self) -> Vec<&String> {
        Vec::new()
    }

    fn search(&self, group: &Group, edge: EdgeIndex) -> Vec<&String> {
        Vec::new()
    }
}

impl Display for Poset {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", Dot::new(&self.g))
    }
}

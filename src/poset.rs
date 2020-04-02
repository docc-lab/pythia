use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Display;

use petgraph::dot::Dot;
use petgraph::graph::EdgeIndex;
use petgraph::graph::NodeIndex;
use petgraph::stable_graph::StableGraph;
use petgraph::Direction;

use crate::critical::CriticalPath;
use crate::critical::Path;
use crate::grouping::Group;
use crate::manifest::Manifest;
use crate::search::SearchState;
use crate::search::SearchStrategy;
use crate::trace::Event;
use crate::trace::EventType;
use crate::trace::Trace;
use crate::trace::TRACEPOINT_ID_MAP;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct PosetNode {
    pub tracepoint_id: usize,
    pub variant: EventType,
}

impl PosetNode {
    pub fn from_event(span: &Event) -> PosetNode {
        PosetNode {
            tracepoint_id: span.tracepoint_id,
            variant: span.variant,
        }
    }
}

impl Display for PosetNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        const LINE_WIDTH: usize = 75;
        // Break the tracepoint id into multiple lines so that the graphs look prettier
        let tracepoint_id = TRACEPOINT_ID_MAP
            .lock()
            .unwrap()
            .get_by_right(&self.tracepoint_id)
            .unwrap().clone();
        let mut result = String::with_capacity(tracepoint_id.len() + 10);
        let mut written = 0;
        while written <= tracepoint_id.len() {
            if written + LINE_WIDTH <= tracepoint_id.len() {
                result.push_str(&tracepoint_id[written..written + LINE_WIDTH]);
            // result.push_str("-\n");
            } else {
                result.push_str(&tracepoint_id[written..tracepoint_id.len()]);
            }
            written += LINE_WIDTH;
        }
        match self.variant {
            EventType::Entry => result.push_str(": S"),
            EventType::Exit => result.push_str(": E"),
            EventType::Annotation => result.push_str(": A"),
        };
        write!(f, "{}", result)
    }
}

pub struct Poset {
    g: StableGraph<PosetNode, u32>, // Edge weights indicate number of occurance of an ordering.
    entry_points: HashMap<PosetNode, NodeIndex>,
    exit_points: HashMap<PosetNode, NodeIndex>,
    manifest: Manifest,
}

impl Poset {
    pub fn new(m: Manifest) -> Poset {
        Poset {
            g: StableGraph::new(),
            entry_points: HashMap::new(),
            exit_points: HashMap::new(),
            manifest: m,
        }
    }

    fn add_trace(&mut self, trace: &Trace) {
        for path in &CriticalPath::all_possible_paths(trace) {
            self.add_path(path);
        }
    }

    fn get_entry_points(&self) -> Vec<&usize> {
        let mut result = HashSet::new();
        result.extend(self.entry_points.keys().map(|x| &x.tracepoint_id));
        result.extend(self.exit_points.keys().map(|x| &x.tracepoint_id));
        result.drain().collect()
    }
}

impl SearchStrategy for Poset {
    fn search(
        &self,
        _group: &Group,
        _edge: EdgeIndex,
        _budget: usize,
    ) -> (Vec<usize>, SearchState) {
        (Vec::new(), SearchState::NextEdge)
    }
}

impl Default for Poset {
    fn default() -> Self {
        Poset {
            manifest: Manifest::new(),
            g: StableGraph::<PosetNode, u32>::new(),
            entry_points: HashMap::new(),
            exit_points: HashMap::new(),
        }
    }
}

impl Poset {
    fn add_path(&mut self, path: &CriticalPath) {
        let mut cur_path_nidx = path.start_node;
        let new_node = PosetNode::from_event(&path.g.g[cur_path_nidx]);
        let (mut merging, mut cur_nidx) = match self.entry_points.get(&new_node) {
            Some(&nidx) => (true, nidx),
            None => {
                let nidx = self.g.add_node(new_node.clone());
                self.entry_points.insert(new_node, nidx);
                (false, nidx)
            }
        };
        loop {
            let next_path_nidx = match path.next_node(cur_path_nidx) {
                Some(nidx) => nidx,
                None => {
                    match self.exit_points.get(&self.g[cur_nidx]) {
                        Some(&exit) => {
                            self.merge_endings(cur_nidx, exit, path, cur_path_nidx);
                        }
                        None => {
                            self.exit_points.insert(self.g[cur_nidx].clone(), cur_nidx);
                        }
                    }
                    break;
                }
            };
            let new_node = PosetNode::from_event(&path.g.g[next_path_nidx]);
            let next_nidx = match self
                .g
                .neighbors_directed(cur_nidx, Direction::Outgoing)
                .find(|&a| self.g[a] == new_node)
            {
                Some(nidx) => {
                    merging = true;
                    nidx
                }
                None => {
                    if merging {
                        let (temp_merging, nidx) =
                            self.fix_merge_conflict(path, new_node, cur_path_nidx, cur_nidx);
                        merging = temp_merging;
                        nidx
                    } else {
                        self.g.add_node(new_node.clone())
                    }
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
            cur_nidx = next_nidx;
            cur_path_nidx = next_path_nidx;
        }
    }
}

impl Poset {
    /// When adding a new path, if the new path or old path has extra nodes, find which one has
    /// extra nodes and add shortcut edge to after the optional path.
    ///
    /// The remainder of the code adds an edge from cur_nidx to the nidx returned.
    ///
    /// Returns boolean indicating if we're still merging or started a new branch, and the new node
    /// index.
    fn fix_merge_conflict(
        &mut self,
        _path: &CriticalPath,
        new_node: PosetNode,
        _next_path_nidx: NodeIndex,
        _cur_nidx: NodeIndex,
    ) -> (bool, NodeIndex) {
        (false, self.g.add_node(new_node))
    }

    fn merge_endings(
        &mut self,
        added_nidx: NodeIndex,
        exit_nidx: NodeIndex,
        path: &CriticalPath,
        path_nidx: NodeIndex,
    ) {
        let mut cur_orig_nidx = *self.exit_points.get(&self.g[added_nidx]).unwrap();
        assert_eq!(cur_orig_nidx, exit_nidx);
        assert_eq!(self.g[exit_nidx], self.g[added_nidx]);
        let mut cur_added_nidx = added_nidx;
        let mut cur_path_nidx = path_nidx;
        // let mut counter = 0;
        // println!(
        //     "Start: added_nidx: {}, path_nidx: {}, orig_nidx: {}, fresh result: {}",
        //     self.g[cur_added_nidx],
        //     PosetNode::from_event(&path.g.g[cur_path_nidx].span),
        //     self.g[cur_orig_nidx],
        //     self.g[*self.exit_points.get(&self.g[added_nidx]).unwrap()]
        // );
        // println!("Exit points: {:?}", self.exit_points);
        loop {
            // println!(
            //     "Loop {}: added_nidx: {}, path_nidx: {}, orig_nidx: {}",
            //     counter,
            //     self.g[cur_added_nidx],
            //     PosetNode::from_event(&path.g.g[cur_path_nidx].span),
            //     self.g[cur_orig_nidx]
            // );
            // counter += 1;
            let prev_path_nidx = match path.prev_node(cur_path_nidx) {
                Some(nidx) => nidx,
                None => break,
            };
            let prev_path_node = PosetNode::from_event(&path.g.g[prev_path_nidx]);
            let prev_added_nidx: NodeIndex = match self
                .g
                .neighbors_directed(cur_added_nidx, Direction::Incoming)
                .find(|&y| self.g[y] == prev_path_node)
            {
                Some(nidx) => nidx,
                None => panic!(
                    "Couldn't find previous added nidx {} of cur_added_nidx {}, cur_path_node: {}, cur_orig_nidx: {}",
                    prev_path_node,
                    self.g[cur_added_nidx],
                    PosetNode::from_event(&path.g.g[cur_path_nidx]),
                    self.g[cur_orig_nidx]
                ),
            };
            if cur_added_nidx == cur_orig_nidx {
                break;
            }
            match self.merge_node(cur_added_nidx, cur_orig_nidx) {
                Ok(_) => {}
                Err(_) => {
                    break;
                }
            };
            cur_added_nidx = prev_added_nidx;
            cur_orig_nidx = match self
                .g
                .neighbors_directed(cur_orig_nidx, Direction::Incoming)
                .find(|&y| self.g[y] == prev_path_node)
            {
                Some(nidx) => nidx,
                None => break,
            };
            cur_path_nidx = prev_path_nidx;
        }
    }

    fn merge_node(&mut self, source: NodeIndex, target: NodeIndex) -> Result<(), ()> {
        // println!("Merging nodes");
        assert_eq!(self.g[source], self.g[target]);
        let mut result = Ok(());
        for prev_neighbor in self
            .g
            .neighbors_directed(source, Direction::Incoming)
            .collect::<Vec<NodeIndex>>()
        {
            let prev_target_neighbor = match self
                .g
                .neighbors_directed(target, Direction::Incoming)
                .find(|&i| self.g[i] == self.g[prev_neighbor])
            {
                Some(nidx) => nidx,
                None => {
                    self.g.add_edge(prev_neighbor, target, 0);
                    result = Err(());
                    prev_neighbor
                }
            };
            let edge_idx = self.g.find_edge(prev_target_neighbor, target).unwrap();
            self.g[edge_idx] += 1;
        }
        assert!(self.exit_points.values().find(|&&i| i == source).is_none());
        self.g.remove_node(source);
        result
    }
}

impl Display for Poset {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", Dot::new(&self.g))
    }
}

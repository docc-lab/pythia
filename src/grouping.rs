use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::time::Duration;

use petgraph::graph::EdgeIndex;
use petgraph::graph::NodeIndex;
use petgraph::stable_graph::StableGraph;
use petgraph::Direction;
use stats::variance;

use crate::critical::CriticalPath;
use crate::critical::Path;
use crate::trace::Event;
use crate::trace::EventType;
use crate::trace::RequestType;
use crate::trace::TracepointID;

#[derive(Clone, Debug)]
pub struct Group {
    pub g: StableGraph<GroupNode, GroupEdge>,
    hash: String,
    pub start_node: NodeIndex,
    pub request_type: RequestType,
    pub traces: Vec<CriticalPath>,
    pub variance: f64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GroupNode {
    pub tracepoint_id: TracepointID,
    pub variant: EventType,
}

impl GroupNode {
    fn from_event(e: &Event) -> GroupNode {
        GroupNode {
            tracepoint_id: e.tracepoint_id,
            variant: e.variant,
        }
    }
}

impl Display for GroupNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.tracepoint_id).ok();
        match self.variant {
            EventType::Annotation => Ok(()),
            EventType::Entry => write!(f, " start"),
            EventType::Exit => write!(f, " end"),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct GroupEdge {
    pub duration: Vec<Duration>,
}

impl Display for GroupEdge {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Edge({} elements, {:?} max, {:?} min)",
            self.duration.len(),
            self.duration.iter().max().unwrap(),
            self.duration.iter().min().unwrap()
        )
    }
}

impl Group {
    pub fn from_critical_paths(paths: Vec<CriticalPath>) -> Vec<Group> {
        let mut hash_map = HashMap::<String, Group>::new();
        for path in paths {
            match hash_map.get_mut(&path.hash()) {
                Some(v) => v.add_trace(&path),
                None => {
                    hash_map.insert(path.hash().to_string(), Group::new(path));
                }
            }
        }
        for (_, group) in hash_map.iter_mut() {
            group.calculate_variance();
        }
        hash_map.values().cloned().collect::<Vec<Group>>()
    }

    fn new(path: CriticalPath) -> Group {
        let mut dag = StableGraph::<GroupNode, GroupEdge>::new();
        let mut cur_node = path.start_node;
        let mut prev_node = None;
        let mut prev_dag_nidx = None;
        let mut start_node = None;
        loop {
            let dag_nidx = dag.add_node(GroupNode::from_event(&path.g.g[cur_node]));
            if prev_node.is_none() {
                start_node = Some(dag_nidx);
            } else {
                match path.g.g.find_edge(prev_node.unwrap(), cur_node) {
                    Some(edge) => {
                        dag.add_edge(
                            prev_dag_nidx.unwrap(),
                            dag_nidx,
                            GroupEdge {
                                duration: vec![path.g.g[edge].duration],
                            },
                        );
                    }
                    None => panic!("No edge?"),
                }
            }
            prev_dag_nidx = Some(dag_nidx);
            prev_node = Some(cur_node);
            cur_node = match path.next_node(cur_node) {
                Some(node) => node,
                None => break,
            };
        }
        Group {
            g: dag,
            start_node: start_node.unwrap(),
            hash: path.hash().to_string(),
            request_type: path.request_type,
            traces: vec![path],
            variance: 0.0,
        }
    }

    /// Returns all edges sorted by variance.
    pub fn problem_edges(&self) -> Vec<EdgeIndex> {
        let mut edge_variances = HashMap::<EdgeIndex, f64>::new();
        let mut cur_node = self.start_node;
        let mut prev_node = None;
        loop {
            if !prev_node.is_none() {
                match self.g.find_edge(prev_node.unwrap(), cur_node) {
                    Some(edge) => {
                        edge_variances.insert(
                            edge,
                            variance(self.g[edge].duration.iter().map(|d| d.as_secs_f64())),
                        );
                    }
                    None => panic!("No edge?"),
                }
            }
            prev_node = Some(cur_node);
            cur_node = match self.next_node(cur_node) {
                Some(node) => node,
                None => break,
            };
        }
        let mut result = edge_variances
            .into_iter()
            .collect::<Vec<(EdgeIndex, f64)>>();
        result.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        result.iter().map(|a| a.0).collect()
    }

    fn add_trace(&mut self, path: &CriticalPath) {
        self.traces.push(path.clone());
        let mut cur_node = path.start_node;
        let mut prev_node = None;
        let mut cur_dag_nidx = self.start_node;
        let mut prev_dag_nidx = None;
        loop {
            if !prev_dag_nidx.is_none() {
                match path.g.g.find_edge(prev_node.unwrap(), cur_node) {
                    Some(edge) => {
                        let dag_edge = self
                            .g
                            .find_edge(prev_dag_nidx.unwrap(), cur_dag_nidx)
                            .unwrap();
                        self.g[dag_edge].duration.push(path.g.g[edge].duration);
                    }
                    None => panic!("No edge?"),
                }
            }
            prev_dag_nidx = Some(cur_dag_nidx);
            prev_node = Some(cur_node);
            cur_node = match path.next_node(cur_node) {
                Some(node) => node,
                None => break,
            };
            cur_dag_nidx = self.next_node(cur_dag_nidx).unwrap();
        }
    }

    pub fn next_node(&self, nidx: NodeIndex) -> Option<NodeIndex> {
        let mut matches = self.g.neighbors_directed(nidx, Direction::Outgoing);
        let result = matches.next();
        assert!(matches.next().is_none());
        result
    }

    fn calculate_variance(&mut self) {
        self.variance = variance(self.traces.iter().map(|x| x.duration.as_nanos()));
        println!("Set variance of {} to {}", self.hash, self.variance);
    }
}

#[derive(Debug)]
pub struct GroupManager {
    groups: HashMap<String, Group>,
}

impl GroupManager {
    pub fn new() -> Self {
        GroupManager {
            groups: HashMap::new(),
        }
    }

    pub fn update(&mut self, paths: &Vec<CriticalPath>) {
        let mut updated_groups = Vec::new();
        for path in paths {
            match self.groups.get_mut(&path.hash()) {
                Some(v) => v.add_trace(&path),
                None => {
                    self.groups
                        .insert(path.hash().to_string(), Group::new(path.clone()));
                }
            }
            updated_groups.push(path.hash().clone());
        }
        for h in updated_groups {
            self.groups.get_mut(&h).unwrap().calculate_variance();
        }
    }
}

impl Display for Group {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Group<{}: {}, {:?}>",
            self.request_type,
            self.traces.len(),
            self.hash
        )
    }
}

impl Display for GroupManager {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for (_, g) in &self.groups {
            write!(f, "{}, ", g)?;
        }
        Ok(())
    }
}

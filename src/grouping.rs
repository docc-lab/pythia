use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::time::Duration;

use petgraph::graph::EdgeIndex;
use petgraph::graph::NodeIndex;
use petgraph::stable_graph::StableGraph;
use petgraph::Direction;
use stats::variance;

use critical::CriticalPath;
use trace::Event;
use trace::EventEnum;

#[derive(Clone)]
pub struct Group {
    pub g: StableGraph<GroupNode, GroupEdge>,
    hash: String,
    start_node: NodeIndex,
    pub traces: Vec<CriticalPath>,
    pub variance: f64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GroupNode {
    pub tracepoint_id: String,
    pub variant: EventEnum,
}

impl GroupNode {
    fn from_event(e: &Event) -> GroupNode {
        GroupNode {
            tracepoint_id: e.tracepoint_id.clone(),
            variant: e.variant,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct GroupEdge {
    pub duration: Vec<Duration>,
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
            let dag_nidx = dag.add_node(GroupNode::from_event(&path.g.g[cur_node].span));
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
            traces: vec![path],
            variance: 0.0,
        }
    }

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

    fn next_node(&self, nidx: NodeIndex) -> Option<NodeIndex> {
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

impl Display for Group {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Group<{}, {:?}>", self.traces.len(), self.hash)
    }
}

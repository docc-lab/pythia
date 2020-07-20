//! Code related to grouping critical paths

use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::time::Duration;

use petgraph::dot::Dot;
use petgraph::graph::EdgeIndex;
use petgraph::graph::NodeIndex;
use petgraph::stable_graph::StableGraph;
use petgraph::Direction;
use stats::variance;

use pythia_common::RequestType;

use crate::critical::CriticalPath;
use crate::critical::Path;
use crate::trace;
use crate::trace::TraceNode;
use crate::trace::TracepointID;
use crate::trace::Value;
//use crate::trace::Event;

/// A group of critical paths
#[derive(Clone, Debug)]
pub struct Group {
    /// Representative path and the relevant latency etc. statistics
    pub g: StableGraph<TraceNode, GroupEdge>,
    hash: String,
    pub start_node: NodeIndex,
    pub end_node: NodeIndex,
    pub request_type: RequestType,
    /// The raw critical paths that this group was constructed from
    pub traces: Vec<CriticalPath>,
    pub variance: f64,
}

#[derive(Debug, Clone)]
pub struct GroupEdge {
    /// These are the durations of the individual paths.
    pub duration: Vec<Duration>,
    pub key_value: HashMap<String, Vec<Value>>,
}

impl PartialEq for GroupEdge {
    fn eq(&self, other: &Self) -> bool {
        self.duration == other.duration
    }
}
impl Display for GroupEdge {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Edge({} elements, {:?} min, {:?} max, {:?} variance)",
            self.duration.len(),
            self.duration.iter().min().unwrap(),
            self.duration.iter().max().unwrap(),
            variance(self.duration.iter().map(|&x| x.as_nanos())),
        )
    }
}

impl Group {
    pub fn dot(&self) -> String {
        format!("{}", Dot::new(&self.g))
    }

    pub fn from_critical_paths(paths: Vec<CriticalPath>) -> Vec<Group> {
        let mut hash_map = HashMap::<String, Group>::new();
        for path in paths {
            match hash_map.get_mut(path.hash()) {
                Some(v) => v.add_trace(&path),
                None => {
                    hash_map.insert(path.hash().to_string(), Group::new(path));
                }
            }
        }
        let mut zeros = 0;
        for (_, group) in hash_map.iter_mut() {
            group.calculate_variance();
            if group.variance == 0.0 {
                zeros += 1;
            }
        }
        println!("{} groups had 0 variance", zeros);
        hash_map.values().cloned().collect::<Vec<Group>>()
    }

    fn new(path: CriticalPath) -> Group {
        let mut dag = StableGraph::<TraceNode, GroupEdge>::new();
        let mut cur_node = path.start_node;
        let mut prev_node = None;
        let mut prev_dag_nidx = None;
        let mut start_node = None;
        let mut end_node;
        /*  let mut map = HashMap::new();
        let mut vec_value: Vec<Value> = Vec::new();
        let mut vec_host: Vec<Value> = Vec::new();
        for node in dag.node_indices()
        {
            vec_value.push_back(node.get_maps());
        }
        map.insert("value".to_string(), vec_value);*/
        loop {
            let dag_nidx = dag.add_node(TraceNode::from_event(&path.g.g[cur_node]));
            end_node = dag_nidx;
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
                                key_value: HashMap::new(),
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
            end_node: end_node,
            hash: path.hash().to_string(),
            request_type: path.request_type,
            traces: vec![path],
            variance: 0.0,
        }
    }

    /// After we use a group for diagnosis, we reset the group. This function is incomplete, and we
    /// should ideally modify the edges as well.
    pub fn used(&mut self) {
        self.traces = Vec::new();
        self.variance = 0.0;
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

    fn calculate_variance(&mut self) {
        self.variance = variance(self.traces.iter().map(|x| x.duration.as_nanos()));
        if self.variance != 0.0 {
            println!("Set variance of {} to {}", self.hash, self.variance);
        }
    }
}

/*impl TraceNode {
    pub fn get_maps(&self) -> HashMap<String, Vec<Value>> {
    return self.key_value_pair;
    }
}*/

impl Path for Group {
    fn get_hash(&self) -> &str {
        &self.hash
    }

    fn set_hash(&mut self, hash: &str) {
        self.hash = hash.to_string()
    }

    fn start_node(&self) -> NodeIndex {
        self.start_node
    }

    fn at(&self, idx: NodeIndex) -> TracepointID {
        self.g[idx].tracepoint_id
    }

    fn next_node(&self, nidx: NodeIndex) -> Option<NodeIndex> {
        let mut matches = self.g.neighbors_directed(nidx, Direction::Outgoing);
        let result = matches.next();
        assert!(matches.next().is_none());
        result
    }

    fn prev_node(&self, nidx: NodeIndex) -> Option<NodeIndex> {
        let mut matches = self.g.neighbors_directed(nidx, Direction::Incoming);
        let result = matches.next();
        assert!(matches.next().is_none());
        result
    }

    fn len(&self) -> usize {
        self.g.node_count()
    }
}

/// This manages the grouping etc. and stores a collection of groups
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

    /// Add new paths to the appropriate groups
    pub fn update(&mut self, paths: &Vec<CriticalPath>) {
        let mut updated_groups = Vec::new();
        for path in paths {
            match self.groups.get_mut(path.hash()) {
                Some(v) => v.add_trace(&path),
                None => {
                    self.groups
                        .insert(path.hash().to_string(), Group::new(path.clone()));
                }
            }
            updated_groups.push(path.hash().clone());
        }
        for h in updated_groups {
            self.groups.get_mut(h).unwrap().calculate_variance();
        }
    }

    /// Return groups filtered based on occurance and sorted by variance
    pub fn problem_groups(&self) -> Vec<&Group> {
        let mut sorted_groups: Vec<&Group> = self
            .groups
            .values()
            .filter(|&g| g.variance != 0.0)
            .filter(|&g| g.traces.len() > 3)
            .collect();
        sorted_groups.sort_by(|a, b| b.variance.partial_cmp(&a.variance).unwrap());
        sorted_groups
    }

    /// Mark a group as "used": reset its performance data
    pub fn used(&mut self, group: &str) {
        self.groups.get_mut(group).unwrap().used();
    }
}

impl Display for Group {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Group<{} {:?} traces>",
            self.traces.len(),
            self.request_type,
        )
    }
}

impl Display for GroupManager {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut groups: Vec<&Group> = self
            .groups
            .values()
            .filter(|&g| g.traces.len() != 0)
            .collect();
        groups.sort_by(|a, b| b.variance.partial_cmp(&a.variance).unwrap());
        for g in &groups {
            write!(f, "{}, ", g)?;
        }
        Ok(())
    }
}

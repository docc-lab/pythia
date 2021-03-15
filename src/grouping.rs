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
use stats::mean;

use pythia_common::RequestType;

use crate::critical::CriticalPath;
use crate::critical::Path;
use crate::trace::TraceNode;
//use crate::trace::TraceNode::key_value_pair;
use crate::trace::TracepointID;
use crate::trace::Value;

use histogram::Histogram;
use lazy_static::lazy_static;
use std::sync::Mutex;

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
   // pub key_value_pairs: HashMap<String, Vec<Value>>,
   // tsl: Group means to calculate CVs
   pub mean: f64,
   pub is_used: bool,


    //   //tsl: Disable strategy - if a groups stops being problematic, disable all the tracepoints for that
    // pub enabled_tps : Vec<(TracepointID, Option<RequestType>)>,

   // tsl: Group coefficient of variance
  // pub cv: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupEdge {
    /// These are the durations of the individual paths.
    pub duration: Vec<Duration>,
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

    fn get_hash(&self) -> &str {
        &self.hash
    }
    pub fn dot(&self) -> String {
        format!("{}", Dot::new(&self.g))
    }

        /// tsl: add enabled tracepoints for the groups
    // pub fn update_enabled_tracepoints(&mut self, decisions: &Vec<(TracepointID, Option<RequestType>)>) {
        
    //     for decision in decisions {
    //         self.enabled_tps.push(&decision);
    //     }
    // }

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
            group.calculate_mean();
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
            mean: 0.0,
            is_used: false,
            // enabled_tps: Vec<(TracepointID, Option<RequestType>)> = Vec::new(),
            //cv: 0.0,
          //  key_value_pairs: TraceNode::get_key_values(),
        }
    }

    /// After we use a group for diagnosis, we reset the group. This function is incomplete, and we
    /// should ideally modify the edges as well.
    pub fn used(&mut self) {
        self.traces = Vec::new();
        self.variance = 0.0;
        self.is_used = true;
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
        // tsl : edge variances are here; so maybe; sum them up and divide them by the total variance
        let mut result = edge_variances
            .into_iter()
            .collect::<Vec<(EdgeIndex, f64)>>();
        result.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
         //tsl: let's see
        let sum: f64 = result.iter().map(|a| a.1).sum();
        println!("*New Metric: hash {:?}, reqtype {:?}, total var {:?}, edge_total: {:?}", self.hash, self.request_type, self.variance, sum);
        result.iter().map(|a| a.0).collect()

       
    }

    fn add_trace(&mut self, path: &CriticalPath) {
        println!("**** A trace {:?} added to group{:?}",path.g.base_id, self.hash);
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
    // tsl: calculate mean of the group
    fn calculate_mean(&mut self) {
        // change below variance to mean
        self.mean = mean(self.traces.iter().map(|x| x.duration.as_nanos()));
        if self.mean != 0.0 {
            println!("Set mean of {:?} - {} to {}", self.request_type, self.hash, self.mean);
        }
    }
    fn calculate_variance(&mut self) {
        println!(
            "Duration of each trace: {:?}",
                self.traces.iter().
                map(|x| x.duration.as_nanos())
                .collect::<Vec<_>>()
        );
        self.variance = variance(self.traces.iter().map(|x| x.duration.as_nanos()));
        if self.variance != 0.0 {
            println!("Set variance of {:?} - {} to {}", self.request_type, self.hash, self.variance);
        }
    }
}

// # key value = hostname = client | server  ---> Append trace_id 0000> 
// 1231-123_hostname = "client" , 1233331-123_hostname = "client"

// 2222-123_hostname = "client"

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
    trees: HashMap<String,  Node>
}

impl GroupManager {
    pub fn new() -> Self {
        GroupManager {
            groups: HashMap::new(),
            trees: HashMap::new()
        }
    }

    /// Add new paths to the appropriate groups
    pub fn update(&mut self, paths: &Vec<CriticalPath>) {
        let mut updated_groups = Vec::new();
        for path in paths {
            // check if path matches to a group
            match self.groups.get_mut(path.hash()) {
                Some(v) => v.add_trace(&path), // yes matches, so assign path to a group
                None => { // no, here we create a new group
                    println!("**** A trace {:?} created a group{:?}",path.g.base_id, path.hash().to_string());
                    self.groups
                        .insert(path.hash().to_string(), Group::new(path.clone()));
                    // trees add new group
                    let mut group_now = self.groups.get_mut(path.hash()).unwrap();
                    println!("+group_now now: {:?}, {:?}", group_now.get_hash(), group_now.request_type);
                    
                    let mut req_type_now = group_now.request_type;
                    println!("+req_type_now now: {:?}", req_type_now);
                    if req_type_now == RequestType::Unknown{
                        println!("skipping null req type");
                        // continue;
                    }
                    else{
                        match self.trees.get_mut(&group_now.request_type.to_string()) {
                            // if there exists a tree by that req type -> add group to that
                            Some(v) => v.add_group(group_now),
                            None => { // if not, create new tree
                                let mut new_tree = Node { val: group_now.request_type.to_string(), group_ids:[group_now.get_hash().to_string()].to_vec(),trace_ids:vec![], l: None, r: None };
                                println!("+Tree created now now: {:?}", new_tree);
                                self.trees.insert(group_now.request_type.to_string(), new_tree);
                                
                            } 
                        }
                    }

                }
            }
            updated_groups.push(path.hash().clone());
        }
        for h in updated_groups {
            self.groups.get_mut(h).unwrap().calculate_variance();
            self.groups.get_mut(h).unwrap().calculate_mean();
        }
    }

    // enable tps on behalf of group id
    pub fn enable_tps(&mut self, points: &Vec<(TracepointID, Option<RequestType>)>, group_id: &String) {
        eprintln!("Enabling {:?}", points);
        // let mut enabled_tracepoints = self.enabled_tracepoints.lock().unwrap();
        let mut vec = Vec::new();
        let mut req_type_now = RequestType::Unknown;
        for p in points {
            println!("+ Point: {:?}",p);
            vec.push(p.0);
            req_type_now = p.1.unwrap();
        }
        println!("+ type: {:?} points:{:?}",req_type_now, vec);
        self.trees.get_mut(&req_type_now.to_string()).unwrap().enable_tps_for_group(group_id, &vec);

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
    /// tsl: Return groups filtered based on coefficient of variance
    pub fn problem_groups_cv(&self, cv_threshold: f64) -> Vec<&Group> {
        // println!("Groups in CV Analaysis: {}", groups);
        let mut sorted_groups: Vec<&Group> = self
            .groups
            .values()
            .filter(|&g| g.is_used != true) // TODO: what happens to used groups?
            .filter(|&g| g.variance != 0.0)
            .filter(|&g| (g.variance.sqrt()/g.mean) > cv_threshold) // tsl: g.CV > Threshold
            .filter(|&g| g.traces.len() > 3)
            .collect();
        sorted_groups.sort_by(|a, b| b.variance.partial_cmp(&a.variance).unwrap());
        // println!("\n**Groups sorted in CV Analaysis: {}", sorted_groups);
        sorted_groups

    }

    /// tsl: Return groups filtered based on mean distribution -- consistently slow groups
    pub fn problem_groups_slow(&self, percentile: f64) -> Vec<&Group> {
        let mut histogram = Histogram::new();
        let mut groups_vec: Vec<&Group> = self
            .groups
            .values()
            .collect();

        println!("Populate histogram");
        for val in groups_vec.iter() {
           // print!("{:?},  ",(val.mean.round() as f64) / (1000000000 as f64) );
            //histogram.increment((( val.mean.round() as f64) / (1000000000 as f64)) as u64);
            histogram.increment(val.mean.round() as u64);
        }
        // get P percentile mean
        let mean_threshold  = histogram.percentile(percentile).unwrap();
        println!("**Get value {} for given P: {:?}", mean_threshold, percentile);

        let mut sorted_groups: Vec<&Group> = self
            .groups
            .values()
            .filter(|&g| g.mean > mean_threshold as f64)
            .filter(|&g| g.traces.len() > 3)
            .collect();
        sorted_groups.sort_by(|a, b| b.mean.partial_cmp(&a.mean).unwrap());
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
            "Group<{} {:?} traces, mean: {:?}, var: {:?}, cv:{:?}, hash: {:?}, durations: {:?}>",
            self.traces.len(),
            self.request_type,
            self.mean/1000000.0,
            self.variance,
            self.variance.sqrt()/self.mean,
            self.hash,
            self.traces.iter().map(|x| x.duration.as_nanos()).collect::<Vec<_>>()
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

        let mut trees: Vec<&Node> = self
            .trees
            .values()
            // .filter(|&g| g.traces.len() != 0)
            .collect();
        for t in &trees {
            write!(f, "{:?}, ", t)?;
        }
        Ok(())
    }
}

#[derive(PartialEq)]
#[derive(Debug)]
struct Node {
    val: String,
    // treenode: &'a TreeNode,
    trace_ids: Vec<TracepointID>,
    group_ids: Vec<String>,
    l: Option<Box<Node>>,
    r: Option<Box<Node>>,
}
impl Node {
    pub fn enable_tps_for_group(&mut self, group_id: &str, trace_ids: &Vec<TracepointID>) {

        // if we are at correct node --> add the child node (left with tracepoints contain rel., right with no tracepoints -- only parents tps for right)
        if self.group_ids.iter().any(|i| i==group_id) {
            let target_node_left =  &mut self.l;
            match target_node_left {
                &mut Some(ref mut subnode) => println!("Do nothing and check right child for group id"),//panic!("Has a child LEFT :/"),
                &mut None => {
                    println!("Adding to the left of {:?}",self.val);
                    let mut tps = Vec::new();
                    // tps = trace_ids.iter().map(|x| x.to_string()).collect();
                    // println!();
                    tps = trace_ids.clone(); // add newly enabled tracepoints
                    // tps.extend(self.trace_ids.clone()); // add parent's tracepoints

                    let new_node = Node { val: "TPS".to_string(), trace_ids:tps, group_ids:Vec::new() , l: None, r: None }; //group_ids:vec![group_id.to_string()]
                    let boxed_node = Some(Box::new(new_node));
                    *target_node_left = boxed_node;
                }
            }

            let target_node_right =  &mut self.r;
            match target_node_right {
                &mut Some(ref mut subnode) => {
                    println!("Inner traversing right");
                    subnode.enable_tps_for_group(group_id,trace_ids);
                },
                
                //panic!("Has a child Right :/"),
                &mut None => {
                    println!("Adding to the right of {:?}",self.val);
                    let mut tps = Vec::new();
                    tps = self.trace_ids.clone(); // only parent's traceids
                    // let t2:&'static str = "123";
                    // let together = format!("{}{}", new_val, "-NO");

                    let mut gids = Vec::new();
                    gids = self.group_ids.clone();

                    let new_node = Node { val: "NO".to_string(), trace_ids:tps, group_ids:gids, l: None, r: None };
                    let boxed_node = Some(Box::new(new_node));
                    *target_node_right = boxed_node;
                }
            }

            return
        }
        let target_node_right =  &mut self.r;
        let target_node_left  = &mut self.l;
        // println!("{:?} target node", target_node);
        match target_node_left {
            &mut Some(ref mut subnode) => {
                println!("traversing left");
                subnode.enable_tps_for_group(group_id,trace_ids)
            },
            &mut None => {
                println!("none Left ended");
            }
        }

        // println!("akiko");
        match target_node_right {
            &mut Some(ref mut subnode) => {
                println!("traversing right");
                subnode.enable_tps_for_group( group_id,trace_ids)
            },
            &mut None => {
                println!("none right ended");
            }
        }


    }

    pub fn add_group(&mut self,   group: &Group) {// group_id: &'a str) {
        println!("Iterating : {:?}", self.val);
        println!("\n");

        let target_node_left = &mut self.l;
        let target_node_right = &mut self.r;
        match target_node_left {
            &mut Some(ref mut subnode) => {
                println!("+node: {:?} has left child",self.val);
                subnode.add_group( &group);
            },
            &mut None => {
                println!("+node: {:?} is at leaf",self.val);
                 for tp in &self.trace_ids {
                     println!("+ Check TP2 {:?}", tp);
                     // IF contain any of the tps then append group_ids
                     if group.traces[0].contains_tp(tp) {
                         
                         println!("+{:?}",ARRAY.lock().unwrap());

                         if ARRAY.lock().unwrap().iter().any(|i| i==group.get_hash()){
                             println!("+***Found");
                         }
                         else{
                             println!("+Not found so adding");
                             ARRAY.lock().unwrap().push(group.get_hash().to_string());
                             self.group_ids.push(group.get_hash().to_string());
                         }

                         println!("+evet left");
                         return
                     }


                }

            }
        }
        println!("u1");
        match target_node_right {
            &mut Some(ref mut subnode) => {
                println!("+node: {:?} has right child",self.val);
                subnode.add_group( &group);},
            &mut None => {
                println!("+We are at the RIGHT*** leaves, so let's check tps included.. if so append group");
                 for tp in &self.trace_ids {
                     println!("+ Check TP {:?}", tp);
                     // TODO: if contain any ! then append group_ids
                     if group.traces[0].contains_tp(tp){
                         println!("+checking now");
                         println!("+{:?}",ARRAY.lock().unwrap());

                         if ARRAY.lock().unwrap().iter().any(|i| i==group.get_hash()){
                             println!("+***Found");
                         }
                         self.group_ids.push(group.get_hash().to_string());
                         println!("+evet right");
                         return;
                     }
                }

            }
        }


    }
}


// Prevent double adding the group id
lazy_static! {
    static ref ARRAY: Mutex<Vec<String>> = Mutex::new(vec![]);
}

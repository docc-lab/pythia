/// Stuff related to working with osprofiler
///
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;

use chrono::NaiveDateTime;
use petgraph::graph::NodeIndex;
use redis::Commands;
use redis::Connection;
use regex::RegexSet;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::reader::Reader;
use crate::rpclib::get_events_from_client;
use crate::settings::Settings;
use crate::trace::Event;
use crate::trace::EventType;
use crate::trace::RequestType;
use crate::trace::Trace;
use crate::trace::TracepointID;
use crate::trace::{DAGEdge, EdgeType};

pub struct OSProfilerReader {
    connection: Connection,
    client_list: Vec<String>,
    prev_traces: HashMap<String, Duration>,
}

impl Reader for OSProfilerReader {
    fn get_recent_traces(&mut self) -> Vec<Trace> {
        let mut traces = Vec::new();
        let mut first_trace = None;
        loop {
            let id: String = match self.connection.lpop("osprofiler_traces") {
                Ok(i) => i,
                Err(_) => {
                    break;
                }
            };
            match &first_trace {
                Some(i) => {
                    if *i == id {
                        let () = self.connection.rpush("osprofiler_traces", &id).unwrap();
                        break;
                    }
                }
                None => {
                    first_trace = Some(id.clone());
                }
            }
            match self.get_trace_from_base_id(&id) {
                Some(t) => {
                    // Keep traces for one cycle, use them only when the duration becomes stable
                    // (i.e., request has finished)
                    let stable = match self.prev_traces.get(&id) {
                        Some(&d) => {
                            if d == t.duration {
                                true
                            } else {
                                false
                            }
                        }
                        None => false,
                    };
                    if stable {
                        traces.push(t);
                        self.prev_traces.remove(&id);
                    } else {
                        let () = self.connection.rpush("osprofiler_traces", &id).unwrap();
                        self.prev_traces.insert(id, t.duration);
                    }
                }
                None => {
                    let () = self.connection.rpush("osprofiler_traces", &id).unwrap();
                }
            }
        }
        traces
    }

    fn read_file(&mut self, file: &str) -> Trace {
        let reader = std::fs::File::open(file).unwrap();
        let t: Vec<OSProfilerSpan> = serde_json::from_reader(reader).unwrap();
        let mut dag = self.from_event_list(Uuid::nil(), t);
        dag.prune();
        dag
    }

    fn read_dir(&mut self, _foldername: &str) -> Vec<Trace> {
        Vec::new()
    }

    /*
    pub fn get_key_value_pairs(&mut self, id: &str) -> HashMap<String, String> {
        let base_id = Uuid::parse_str(id).ok().unwrap();
        let mut event_list = self.get_matches_(&base_id).unwrap();
        sort_event_list(&mut event_list);
        let mut tracepoint_id_map: HashMap<Uuid, String> = HashMap::new();
        for event in event_list.iter_mut() {
            event.tracepoint_id = event.get_tracepoint_id(&mut tracepoint_id_map);
        }
        let mut result = HashMap::new();
        for event in &event_list {
            result.insert(
                format!("{}::project", event.tracepoint_id),
                event.project.clone(),
            );
            result.insert(format!("{}::name", event.tracepoint_id), event.name.clone());
            result.insert(
                format!("{}::service", event.tracepoint_id),
                event.service.clone(),
            );
            match &event.variant {
                OSProfilerEnum::WaitAnnotation(a) => {
                    result.insert(
                        format!("{}::host", event.tracepoint_id),
                        a.info.host.clone(),
                    );
                    result.insert(
                        format!("{}::function::name", event.tracepoint_id),
                        a.info.function.name.clone(),
                    );
                    result.insert(
                        format!("{}::function::args", event.tracepoint_id),
                        a.info.function.args.clone(),
                    );
                    result.insert(
                        format!("{}::function::kwargs", event.tracepoint_id),
                        a.info.function.kwargs.clone(),
                    );
                }
                OSProfilerEnum::Annotation(a) => {
                    result.insert(
                        format!("{}::host", event.tracepoint_id),
                        a.info.host.clone(),
                    );
                }
                OSProfilerEnum::FunctionEntry(a) => {
                    result.insert(
                        format!("{}::host", event.tracepoint_id),
                        a.info.host.clone(),
                    );
                    result.insert(
                        format!("{}::function::name", event.tracepoint_id),
                        a.info.function.name.clone(),
                    );
                    result.insert(
                        format!("{}::function::args", event.tracepoint_id),
                        a.info.function.args.clone(),
                    );
                    result.insert(
                        format!("{}::function::kwargs", event.tracepoint_id),
                        a.info.function.kwargs.clone(),
                    );
                }
                OSProfilerEnum::FunctionExit(a) => {
                    result.insert(
                        format!("{}::host", event.tracepoint_id),
                        a.info.host.clone(),
                    );
                    result.insert(
                        format!("{}::function::result", event.tracepoint_id),
                        a.info.function.result.clone(),
                    );
                }
                OSProfilerEnum::RequestEntry(a) => {
                    result.insert(
                        format!("{}::request::path", event.tracepoint_id),
                        a.info.request.path.clone(),
                    );
                    result.insert(
                        format!("{}::request::scheme", event.tracepoint_id),
                        a.info.request.scheme.clone(),
                    );
                    result.insert(
                        format!("{}::request::method", event.tracepoint_id),
                        a.info.request.method.clone(),
                    );
                    result.insert(
                        format!("{}::request::query", event.tracepoint_id),
                        a.info.request.query.clone(),
                    );
                }
                OSProfilerEnum::RequestExit(a) => {
                    result.insert(
                        format!("{}::host", event.tracepoint_id),
                        a.info.host.clone(),
                    );
                }
            }
            println!("{:?}", event);
        }
        result
    }
    */

    fn get_trace_from_base_id(&mut self, id: &str) -> Option<Trace> {
        println!("Working on {}", id);
        let mut result = match Uuid::parse_str(id) {
            Ok(uuid) => {
                let event_list = self.get_all_matches(&uuid);
                if event_list.len() == 0 {
                    eprintln!("No traces match the uuid {}", uuid);
                    return None;
                }
                let dag = self.from_event_list(Uuid::parse_str(id).unwrap(), event_list);
                dag
            }
            Err(_) => {
                panic!("Malformed UUID received as base ID: {}", id);
            }
        };
        if let RequestType::Unknown = result.request_type {
            eprintln!("Warning: couldn't get type for request {}", id);
        }
        result.duration = (result.g[result.end_node].timestamp
            - result.g[result.start_node].timestamp)
            .to_std()
            .unwrap();
        result.prune();
        Some(result)
    }
}

impl OSProfilerReader {
    pub fn from_settings(settings: &Settings) -> OSProfilerReader {
        let redis_url = &settings.redis_url;
        let client = redis::Client::open(&redis_url[..]).unwrap();
        let con = client.get_connection().unwrap();
        OSProfilerReader {
            connection: con,
            client_list: settings.pythia_clients.clone(),
            prev_traces: HashMap::new(),
        }
    }

    /// Get matching events from all redis instances
    fn get_all_matches(&mut self, span_id: &Uuid) -> Vec<OSProfilerSpan> {
        let mut event_list = Vec::new();
        for node in self.client_list.iter() {
            event_list.extend(get_events_from_client(node, span_id.clone()));
        }
        event_list
    }

    /// Public wrapper for get_matches_ that accepts string input and does not return RedisResult
    pub fn get_matches(&mut self, span_id: &str) -> Vec<OSProfilerSpan> {
        match Uuid::parse_str(span_id) {
            Ok(uuid) => self.get_matches_(&uuid).unwrap(),
            Err(_) => panic!("Malformed UUID as base id: {}", span_id),
        }
    }

    /// Get matching events from local redis instance
    fn get_matches_(&mut self, span_id: &Uuid) -> redis::RedisResult<Vec<OSProfilerSpan>> {
        let to_parse: String = match self
            .connection
            .get("osprofiler:".to_string() + &span_id.to_hyphenated().to_string())
        {
            Ok(to_parse) => to_parse,
            Err(_) => {
                return Ok(Vec::new());
            }
        };
        let mut result = Vec::new();
        for dict_string in to_parse[1..to_parse.len() - 1].split("}{") {
            match parse_field(&("{".to_string() + dict_string + "}")) {
                Ok(span) => {
                    result.push(span);
                }
                Err(e) => panic!("Problem while parsing {}: {}", dict_string, e),
            }
        }
        Ok(result)
    }

    fn from_event_list(&mut self, id: Uuid, mut event_list: Vec<OSProfilerSpan>) -> Trace {
        let mut mydag = Trace::new(&id);
        self.add_events(&mut mydag, &mut event_list);
        mydag
    }

    fn add_events(
        &mut self,
        mut dag: &mut Trace,
        event_list: &mut Vec<OSProfilerSpan>,
    ) -> Option<NodeIndex> {
        if event_list.len() == 0 {
            return None;
        }
        sort_event_list(event_list);
        let base_id = event_list[0].base_id;
        let start_time = event_list[0].timestamp;
        let mut tracepoint_id_map: HashMap<Uuid, String> = HashMap::new();
        // Latest event with the same id, end if event already finished, start if it didn't
        let mut id_map = HashMap::new();
        let mut active_spans = HashMap::new();
        // The latest completed children span for each parent id
        let mut children_per_parent = HashMap::<Uuid, Option<Uuid>>::new();
        children_per_parent.insert(event_list[0].base_id, None);
        // Map of asynchronous traces that start from this DAG -> parent node in DAG
        let mut async_traces = HashMap::new();
        let mut waiters = HashMap::<Uuid, NodeIndex>::new();
        let mut wait_spans = HashSet::<Uuid>::new();
        let mut add_next_to_waiters = false;
        let mut wait_for = Vec::<Uuid>::new();
        let mut nidx = None;
        let mut prev_time = start_time;
        for (idx, event) in event_list.iter().enumerate() {
            assert!(event.base_id == base_id);
            assert!(prev_time <= event.timestamp);
            prev_time = event.timestamp;
            let mut mynode = Event::from_osp_span(event);
            let current_tracepoint_id = event.get_tracepoint_id(&mut tracepoint_id_map);
            mynode.tracepoint_id = TracepointID::from_str(&current_tracepoint_id);
            if mynode.variant == EventType::Entry {
                let matches: Vec<usize> = REQUEST_TYPE_REGEXES
                    .matches(&current_tracepoint_id)
                    .iter()
                    .collect();
                if matches.len() > 0 {
                    assert!(matches.len() == 1);
                    assert!(dag.request_type == RequestType::Unknown);
                    dag.request_type = REQUEST_TYPES[matches[0]];
                }
            }
            // Don't add asynch_wait into the DAGs
            nidx = match &event.info {
                OSProfilerEnum::Annotation(AnnotationEnum::WaitFor(w)) => {
                    wait_for.push(w.wait_for);
                    None
                }
                _ => {
                    if wait_spans.contains(&mynode.trace_id) {
                        None
                    } else {
                        let nidx = dag.g.add_node(mynode);
                        id_map.insert(event.trace_id, nidx);
                        if dag.start_node == NodeIndex::end() {
                            dag.start_node = nidx;
                        }
                        Some(nidx)
                    }
                }
            };
            if let OSProfilerEnum::Annotation(s) = &event.info {
                match &s {
                    AnnotationEnum::WaitFor(_) => {
                        wait_spans.insert(event.trace_id);
                    }
                    AnnotationEnum::Child(c) => {
                        async_traces.insert(c.child_id, nidx.unwrap());
                    }
                    _ => {}
                }
            }
            if add_next_to_waiters && !nidx.is_none() {
                for waiter in wait_for.iter() {
                    waiters.insert(*waiter, nidx.unwrap());
                }
                wait_for = vec![];
                add_next_to_waiters = false;
            }
            match &event.info {
                OSProfilerEnum::FunctionEntry(_) | OSProfilerEnum::RequestEntry(_) => {
                    active_spans.insert(event.trace_id, nidx.unwrap());
                    children_per_parent.insert(event.trace_id, None);
                    if event.parent_id == event.base_id {
                        match children_per_parent.get(&event.parent_id).unwrap() {
                            Some(sibling_id) => {
                                let sibling_node = id_map.get(sibling_id).unwrap();
                                dag.g.add_edge(
                                    *sibling_node,
                                    nidx.unwrap(),
                                    DAGEdge {
                                        duration: (event.timestamp
                                            - dag.g[*sibling_node].timestamp)
                                            .to_std()
                                            .unwrap(),
                                        variant: EdgeType::ChildOf,
                                    },
                                );
                            }
                            None => {
                                if idx != 0 {
                                    panic!("I don't know when this happens");
                                }
                            }
                        }
                    } else {
                        match children_per_parent.get(&event.parent_id) {
                            Some(result) => match result {
                                Some(sibling_id) => {
                                    let sibling_node = id_map.get(sibling_id).unwrap();
                                    dag.g.add_edge(
                                        *sibling_node,
                                        nidx.unwrap(),
                                        DAGEdge {
                                            duration: (event.timestamp
                                                - dag.g[*sibling_node].timestamp)
                                                .to_std()
                                                .unwrap(),
                                            variant: EdgeType::ChildOf,
                                        },
                                    );
                                }
                                None => {
                                    let parent_node = id_map.get(&event.parent_id).unwrap();
                                    dag.g.add_edge(
                                        *parent_node,
                                        nidx.unwrap(),
                                        DAGEdge {
                                            duration: (event.timestamp
                                                - dag.g[*parent_node].timestamp)
                                                .to_std()
                                                .unwrap(),
                                            variant: EdgeType::ChildOf,
                                        },
                                    );
                                }
                            },
                            None => {
                                // Parent has finished execution before child starts - shouldn't happen
                                let parent_node = &dag.g[match id_map.get(&event.parent_id) {
                                    Some(&nidx) => nidx,
                                    None => {
                                        panic!("Warning: Parent of node {:?} not found. Silently ignoring this event", event);
                                    }
                                }];
                                assert!(event.timestamp > parent_node.timestamp);
                                panic!("Parent of node {:?} not found: {:?}", event, parent_node);
                            }
                        }
                    }
                }
                OSProfilerEnum::Annotation(_) => {
                    match nidx {
                        None => {
                            // Don't add wait for annotations
                        }
                        Some(nidx) => match children_per_parent.get(&event.parent_id).unwrap() {
                            Some(sibling_id) => {
                                let sibling_node = id_map.get(sibling_id).unwrap();
                                dag.g.add_edge(
                                    *sibling_node,
                                    nidx,
                                    DAGEdge {
                                        duration: (event.timestamp
                                            - dag.g[*sibling_node].timestamp)
                                            .to_std()
                                            .unwrap(),
                                        variant: EdgeType::ChildOf,
                                    },
                                );
                            }
                            None => {
                                // If idx == 0, annotation is the first node and the edge is added in
                                // add_async
                                if idx != 0 {
                                    let parent_node = id_map.get(&event.parent_id).unwrap();
                                    dag.g.add_edge(
                                        *parent_node,
                                        nidx,
                                        DAGEdge {
                                            duration: (event.timestamp
                                                - dag.g[*parent_node].timestamp)
                                                .to_std()
                                                .unwrap(),
                                            variant: EdgeType::ChildOf,
                                        },
                                    );
                                }
                            }
                        },
                    }
                }
                OSProfilerEnum::Exit(_) => {
                    if nidx.is_none() {
                        add_next_to_waiters = true;
                    } else {
                        let start_span = active_spans.remove(&event.trace_id).unwrap();
                        match children_per_parent.remove(&event.trace_id).unwrap() {
                            Some(child_id) => {
                                let child_node = id_map.get(&child_id).unwrap();
                                dag.g.add_edge(
                                    *child_node,
                                    nidx.unwrap(),
                                    DAGEdge {
                                        duration: (event.timestamp - dag.g[*child_node].timestamp)
                                            .to_std()
                                            .unwrap(),
                                        variant: EdgeType::ChildOf,
                                    },
                                );
                            }
                            None => {
                                dag.g.add_edge(
                                    start_span,
                                    nidx.unwrap(),
                                    DAGEdge {
                                        duration: (event.timestamp - dag.g[start_span].timestamp)
                                            .to_std()
                                            .unwrap(),
                                        variant: EdgeType::ChildOf,
                                    },
                                );
                            }
                        }
                    }
                }
            }
            if !nidx.is_none() {
                children_per_parent.insert(event.parent_id, Some(event.trace_id));
            }
        }
        dag.end_node = match nidx {
            Some(nid) => nid,
            None => dag.start_node,
        };
        for (trace_id, parent) in async_traces.iter() {
            let last_node = self.add_asynch(&mut dag, trace_id, *parent);
            if last_node.is_none() {
                continue;
            }
            let last_node = last_node.unwrap();
            if dag.g[last_node].timestamp > dag.g[dag.end_node].timestamp {
                dag.end_node = last_node;
            }
            match &waiters.get(trace_id) {
                Some(parent) => {
                    dag.g.add_edge(
                        last_node,
                        **parent,
                        DAGEdge {
                            duration: (dag.g[**parent].timestamp - dag.g[last_node].timestamp)
                                .to_std()
                                .unwrap(),
                            variant: EdgeType::FollowsFrom,
                        },
                    );
                }
                None => {}
            }
        }
        nidx
    }

    fn add_asynch(
        &mut self,
        mut dag: &mut Trace,
        trace_id: &Uuid,
        parent: NodeIndex,
    ) -> Option<NodeIndex> {
        let mut event_list = self.get_all_matches(trace_id);
        if event_list.len() == 0 {
            return None;
        }
        let last_node = self.add_events(&mut dag, &mut event_list);
        let first_event = event_list
            .iter()
            .fold(None, |min, x| match min {
                None => Some(x),
                Some(y) => Some(if x.timestamp < y.timestamp { x } else { y }),
            })
            .unwrap();
        let first_node = dag
            .g
            .node_indices()
            .find(|idx| dag.g[*idx].trace_id == first_event.trace_id)
            .unwrap();
        dag.g.add_edge(
            parent,
            first_node,
            DAGEdge {
                duration: (first_event.timestamp - dag.g[parent].timestamp)
                    .to_std()
                    .unwrap(),
                variant: EdgeType::FollowsFrom,
            },
        );
        last_node
    }
}

fn sort_event_list(event_list: &mut Vec<OSProfilerSpan>) {
    // Sorts events by timestamp
    event_list.sort_by(|a, b| {
        if a.timestamp == b.timestamp {
            match a.info {
                OSProfilerEnum::FunctionEntry(_) | OSProfilerEnum::RequestEntry(_) => {
                    Ordering::Less
                }
                _ => Ordering::Greater,
            }
        } else {
            a.timestamp.cmp(&b.timestamp)
        }
    });
}

fn parse_field(field: &String) -> Result<OSProfilerSpan, String> {
    let result: OSProfilerSpan = match serde_json::from_str(field) {
        Ok(a) => a,
        Err(e) => {
            return Err(e.to_string());
        }
    };
    if result.name == "asynch_request" || result.name == "asynch_wait" {
        return match result.info {
            OSProfilerEnum::Annotation(_) => Ok(result),
            _ => {
                println!("{:?}", result);
                Err("".to_string())
            }
        };
    }
    Ok(result)
}

impl Event {
    fn from_osp_span(event: &OSProfilerSpan) -> Event {
        Event {
            trace_id: event.trace_id,
            tracepoint_id: TracepointID::from_str(&event.tracepoint_id),
            timestamp: event.timestamp,
            variant: match event.info {
                OSProfilerEnum::FunctionEntry(_) | OSProfilerEnum::RequestEntry(_) => {
                    EventType::Entry
                }
                OSProfilerEnum::Exit(_) => EventType::Exit,
                OSProfilerEnum::Annotation(_) => EventType::Annotation,
            },
            is_synthetic: false,
        }
    }
}

impl OSProfilerSpan {
    pub fn get_tracepoint_id(&self, map: &mut HashMap<Uuid, String>) -> String {
        // The map needs to be initialized and passed to it from outside :(
        match &self.info {
            OSProfilerEnum::FunctionEntry(_) | OSProfilerEnum::RequestEntry(_) => {
                map.insert(self.trace_id, self.tracepoint_id.clone());
                self.tracepoint_id.clone()
            }
            OSProfilerEnum::Annotation(_) => self.tracepoint_id.clone(),
            OSProfilerEnum::Exit(_) => match map.remove(&self.trace_id) {
                Some(s) => s,
                None => {
                    if self.name.starts_with("asynch_wait") {
                        self.tracepoint_id.clone()
                    } else {
                        panic!("Couldn't find trace id for {:?}", self);
                    }
                }
            },
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct OSProfilerSpan {
    pub trace_id: Uuid,
    pub parent_id: Uuid,
    project: String,
    pub name: String,
    pub base_id: Uuid,
    service: String,
    pub tracepoint_id: String,
    #[serde(with = "serde_timestamp")]
    pub timestamp: NaiveDateTime,
    info: OSProfilerEnum,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(untagged)]
enum OSProfilerEnum {
    Annotation(AnnotationEnum),
    FunctionEntry(FunctionEntryInfo),
    RequestEntry(RequestEntryInfo),
    Exit(ExitEnum),
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(untagged)]
enum AnnotationEnum {
    WaitFor(WaitAnnotationInfo),
    Child(ChildAnnotationInfo),
    Plain(PlainAnnotationInfo),
    Log(LogAnnotationInfo),
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct WaitAnnotationInfo {
    function: FunctionEntryFunction,
    thread_id: u64,
    host: String,
    tracepoint_id: String,
    pid: u64,
    wait_for: Uuid,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct LogAnnotationInfo {
    thread_id: u64,
    host: String,
    tracepoint_id: String,
    pid: u64,
    msg: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct PlainAnnotationInfo {
    thread_id: u64,
    host: String,
    tracepoint_id: String,
    pid: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct ChildAnnotationInfo {
    thread_id: u64,
    host: String,
    tracepoint_id: String,
    child_id: Uuid,
    pid: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct RequestEntryInfo {
    request: RequestEntryRequest,
    thread_id: u64,
    host: String,
    tracepoint_id: String,
    pid: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct RequestEntryRequest {
    path: String,
    scheme: String,
    method: String,
    query: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(untagged)]
enum ExitEnum {
    Normal(NormalExitInfo),
    Error(ErrorExitInfo),
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct NormalExitInfo {
    host: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct ErrorExitInfo {
    etype: String,
    message: String,
    host: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct FunctionEntryInfo {
    function: FunctionEntryFunction,
    thread_id: u64,
    host: String,
    tracepoint_id: String,
    pid: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct FunctionEntryFunction {
    name: String,
}

lazy_static! {
    // The ordering of the below two structures should match each other
    pub static ref REQUEST_TYPE_REGEXES: RegexSet = RegexSet::new(&[
        r"openstackclient\.compute\.v2\.server\.CreateServer\.take_action",
        r"openstackclient\.compute\.v2\.server\.ListServer\.take_action",
        r"openstackclient\.compute\.v2\.server\.DeleteServer\.take_action"
    ])
    .unwrap();
    static ref REQUEST_TYPES: Vec<RequestType> = vec![
        RequestType::ServerCreate,
        RequestType::ServerList,
        RequestType::ServerDelete,
    ];
}

pub mod serde_timestamp {
    use chrono::NaiveDateTime;
    use serde::de;
    use serde::ser;
    use std::fmt;

    pub fn deserialize<'de, D>(d: D) -> Result<NaiveDateTime, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        d.deserialize_str(NaiveDateTimeVisitor)
    }

    pub fn serialize<S>(t: &NaiveDateTime, s: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        s.serialize_str(&t.format("%Y-%m-%dT%H:%M:%S%.6f").to_string())
    }

    struct NaiveDateTimeVisitor;

    impl<'de> de::Visitor<'de> for NaiveDateTimeVisitor {
        type Value = NaiveDateTime;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            write!(formatter, "a string represents chrono::NaiveDateTime")
        }

        fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            match NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.6f") {
                Ok(t) => Ok(t),
                Err(_) => Err(de::Error::invalid_value(de::Unexpected::Str(s), &self)),
            }
        }
    }
}

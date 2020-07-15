/// Stuff related to working with osprofiler
///
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;
use std::time::Duration;

use petgraph::graph::NodeIndex;
use redis::Commands;
use redis::Connection;
use uuid::Uuid;

use pythia_common::osprofiler::ExitEnum;
use pythia_common::AnnotationEnum;
use pythia_common::OSProfilerEnum;
use pythia_common::OSProfilerSpan;
use pythia_common::RequestType;
use pythia_common::REQUEST_TYPES;
use pythia_common::REQUEST_TYPE_REGEXES;

use crate::critical::CriticalPath;
use crate::reader::Reader;

use crate::rpclib::free_keys;
use crate::rpclib::get_events_from_client;
use crate::settings::Settings;
use crate::trace::Event;
use crate::trace::EventType;
use crate::trace::Trace;
use crate::trace::TracepointID;
use crate::trace::{DAGEdge, EdgeType};
use crate::PythiaError;

use crate::trace::Value::Int;
use crate::trace::Value::Str;

pub struct OSProfilerReader {
    connection: Connection,
    client_list: Vec<String>,
    prev_traces: HashMap<String, Duration>,
    trace_error_count: HashMap<String, usize>,
    for_searchspace: bool,
    free_keys: bool,
}

impl Reader for OSProfilerReader {
    fn for_searchspace(&mut self) {
        self.for_searchspace = true;
    }

    fn reset_state(&mut self) {
        if self.free_keys {
            redis::cmd("flushall")
                .query::<()>(&mut self.connection)
                .ok();
        } else {
            loop {
                match self.connection.lpop::<_, String>("osprofiler_traces") {
                    Ok(_) => {}
                    Err(_) => {
                        break;
                    }
                };
            }
        }
    }

    fn get_recent_traces(&mut self) -> Vec<Trace> {
        let mut ids = Vec::new();
        loop {
            let id: String = match self.connection.lpop("osprofiler_traces") {
                Ok(i) => i,
                Err(_) => {
                    break;
                }
            };
            ids.push(id);
        }
        for id in self.prev_traces.keys() {
            ids.push(id.clone());
        }
        let mut traces = Vec::new();
        let mut keys = Vec::new();
        for id in &ids {
            match self.trace_error_count.get(id) {
                Some(&i) => {
                    if i > 5 {
                        self.prev_traces.remove(id);
                        self.trace_error_count.remove(id);
                        eprintln!("Giving up on {}", id);
                        continue;
                    }
                }
                None => {
                    self.trace_error_count.insert(id.clone(), 0);
                }
            }
            match self.get_trace_from_base_id(&id) {
                Ok(t) => {
                    // Keep traces for one cycle, use them only when the duration becomes stable
                    // (i.e., request has finished)
                    let stable = match self.prev_traces.get(id) {
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
                        match CriticalPath::from_trace(&t) {
                            Ok(_) => {
                                keys.extend(t.keys.iter().cloned());
                                traces.push(t);
                                self.prev_traces.remove(id);
                                self.trace_error_count.remove(id);
                            }
                            Err(_) => {
                                *self.trace_error_count.get_mut(id).unwrap() += 1;
                                self.prev_traces.insert(id.clone(), t.duration);
                            }
                        }
                    } else {
                        self.prev_traces.insert(id.clone(), t.duration);
                    }
                }
                Err(_) => {
                    *self.trace_error_count.get_mut(id).unwrap() += 1;
                }
            }
        }
        if self.free_keys {
            for node in self.client_list.iter() {
                free_keys(node, keys.clone());
            }
        }
        traces
    }

    fn read_file(&mut self, file: &str) -> Trace {
        let reader = std::fs::File::open(file).unwrap();
        let t: Vec<OSProfilerSpan> = serde_json::from_reader(reader).unwrap();
        self.from_event_list(Uuid::nil(), t).unwrap()
    }

    fn read_dir(&mut self, _foldername: &str) -> Vec<Trace> {
        Vec::new()
    }
    /*

     //pub fn get_key_value_pairs(&mut self, id: &str) -> HashMap<String, String> {

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
     }*/

    fn get_trace_from_base_id(&mut self, id: &str) -> Result<Trace, Box<dyn Error>> {
        eprintln!("Working on {}", id);
        let mut result = match Uuid::parse_str(id) {
            Ok(uuid) => {
                let event_list = self.get_all_matches(&uuid);
                if event_list.len() == 0 {
                    return Err(Box::new(PythiaError(
                        format!("No traces match the uuid {}", uuid).into(),
                    )));
                }
                let dag = self.from_event_list(Uuid::parse_str(id).unwrap(), event_list)?;
                dag
            }
            Err(_) => {
                panic!("Malformed UUID received as base ID: {}", id);
            }
        };
        if result.request_type == RequestType::Unknown {
            eprintln!("Warning: couldn't get type for request {}", id);
        }
        result.duration = (result.g[result.end_node].timestamp
            - result.g[result.start_node].timestamp)
            .to_std()
            .unwrap();
        Ok(result)
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
            trace_error_count: HashMap::new(),
            for_searchspace: false,
            free_keys: settings.free_keys,
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

    fn from_event_list(
        &mut self,
        id: Uuid,
        mut event_list: Vec<OSProfilerSpan>,
    ) -> Result<Trace, Box<dyn Error>> {
        let mut mydag = Trace::new(&id);
        self.add_events(&mut mydag, &mut event_list, None)?;
        Ok(mydag)
    }

    fn add_events(
        &mut self,
        mut dag: &mut Trace,
        event_list: &mut Vec<OSProfilerSpan>,
        mut parent_of_trace: Option<NodeIndex>,
    ) -> Result<Option<NodeIndex>, Box<dyn Error>> {
        if event_list.len() == 0 {
            return Ok(None);
        }
        sort_event_list(event_list);
        let base_id = event_list[0].base_id;
        dag.keys.push(format!("osprofiler:{}", base_id));
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
        let mut prev_nidx = None;
        let mut prev_time = start_time;
        for (idx, event) in event_list.iter().enumerate() {
            assert!(event.base_id == base_id);
            assert!(prev_time <= event.timestamp);
            prev_time = event.timestamp;
            let mut mynode = Event::from_osp_span(event);
            let current_tracepoint_id = event.get_tracepoint_id(&mut tracepoint_id_map)?;
            mynode.tracepoint_id = TracepointID::from_str(&current_tracepoint_id);
            if mynode.variant == EventType::Entry {
                let matches: Vec<usize> = REQUEST_TYPE_REGEXES
                    .matches(&current_tracepoint_id)
                    .iter()
                    .collect();
                if matches.len() > 0 {
                    assert!(matches.len() == 1);
                    dag.request_type = REQUEST_TYPES[matches[0]];
                }
            }
            // Don't add asynch_wait into the DAGs
            nidx = match &event.info {
                OSProfilerEnum::Annotation(AnnotationEnum::WaitFor(w)) => {
                    wait_for.push(w.wait_for);
                    None
                }
                OSProfilerEnum::Annotation(AnnotationEnum::Child(_)) => None,
                _ => {
                    if wait_spans.contains(&mynode.trace_id) {
                        None
                    } else {
                        let nidx = dag.g.add_node(mynode);
                        id_map.insert(event.trace_id, nidx);
                        if dag.start_node == NodeIndex::end() {
                            dag.start_node = nidx;
                            dag.end_node = nidx;
                        }
                        Some(nidx)
                    }
                }
            };
            match nidx {
                Some(nid) => {
                    if dag.g[nid].timestamp > dag.g[dag.end_node].timestamp {
                        dag.end_node = nid;
                    }
                }
                None => {}
            }
            if let OSProfilerEnum::Annotation(s) = &event.info {
                match &s {
                    AnnotationEnum::WaitFor(_) => {
                        wait_spans.insert(event.trace_id);
                    }
                    AnnotationEnum::Child(c) => {
                        async_traces.insert(
                            c.child_id,
                            match prev_nidx {
                                Some(i) => i,
                                None => match parent_of_trace {
                                    Some(nidx) => nidx,
                                    None => {
                                        return Err(Box::new(PythiaError(
                                            "No parent, start with annotation".into(),
                                        )))
                                    }
                                },
                            },
                        );
                    }
                    _ => {}
                }
            }
            if !nidx.is_none() && !parent_of_trace.is_none() {
                dag.g.add_edge(
                    parent_of_trace.unwrap(),
                    nidx.unwrap(),
                    DAGEdge {
                        duration: (event.timestamp - dag.g[parent_of_trace.unwrap()].timestamp)
                            .to_std()
                            .unwrap(),
                        variant: EdgeType::FollowsFrom,
                    },
                );
                parent_of_trace = None;
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
                            None => {}
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
            if !nidx.is_none() {
                prev_nidx = nidx;
            }
        }
        for (trace_id, parent) in async_traces.iter() {
            let last_node = self.add_asynch(&mut dag, trace_id, *parent)?;
            if last_node.is_none() {
                continue;
            }
            let last_node = last_node.unwrap();
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
        Ok(nidx)
    }

    fn add_asynch(
        &mut self,
        mut dag: &mut Trace,
        trace_id: &Uuid,
        parent: NodeIndex,
    ) -> Result<Option<NodeIndex>, Box<dyn Error>> {
        let mut event_list = self.get_all_matches(trace_id);
        if event_list.len() == 0 {
            return Ok(None);
        }
        self.add_events(&mut dag, &mut event_list, Some(parent))
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

impl Event {
    fn from_osp_span(event: &OSProfilerSpan) -> Event {
        let mut map = HashMap::new();
        if let OSProfilerEnum::Annotation(AnnotationEnum::KeyValue(key_value_annotation_info)) =
            &event.info
        {
            let val = Int(key_value_annotation_info.value);
            map.insert("value".to_string(), val);
        }
        match &event.info {
            OSProfilerEnum::FunctionEntry(function_entry_info) => {
                let host = Str((&function_entry_info.host).to_string());
                map.insert("host".to_string(), host);
            }
            OSProfilerEnum::RequestEntry(request_entry_info) => {
                let host = Str((&request_entry_info.host).to_string());
                map.insert("host".to_string(), host);
            }
            OSProfilerEnum::Exit(ExitEnum::Normal(normal_entry_info)) => {
                let host = Str((&normal_entry_info.host).to_string());
                map.insert("host".to_string(), host);
            }
            OSProfilerEnum::Exit(ExitEnum::Error(error_entry_info)) => {
                let host = Str((&error_entry_info.host).to_string());
                map.insert("host".to_string(), host);
            }
            OSProfilerEnum::Annotation(AnnotationEnum::KeyValue(key_value_annotation_info)) => {
                let host = Str((&key_value_annotation_info.host).to_string());
                map.insert("host".to_string(), host);
            }
            OSProfilerEnum::Annotation(AnnotationEnum::WaitFor(wait_for_annotation_info)) => {
                let host = Str((&wait_for_annotation_info.host).to_string());
                map.insert("host".to_string(), host);
            }
            OSProfilerEnum::Annotation(AnnotationEnum::Child(child_annotation_info)) => {
                let host = Str((&child_annotation_info.host).to_string());
                map.insert("host".to_string(), host);
            }
            OSProfilerEnum::Annotation(AnnotationEnum::Plain(plain_annotation_info)) => {
                let host = Str((&plain_annotation_info.host).to_string());
                map.insert("host".to_string(), host);
            }
            OSProfilerEnum::Annotation(AnnotationEnum::Log(log_annotation_info)) => {
                let host = Str((&log_annotation_info.host).to_string());
                map.insert("host".to_string(), host);
            }
        }
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
            key_value_pair: map,
        }
    }
}

use std::collections::HashMap;
use std::convert::TryInto;
use std::error::Error;
use std::fmt;

use chrono::Duration;
use chrono::NaiveDateTime;
use petgraph::algo::connected_components;
use petgraph::graph::{Graph, NodeIndex};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::reader::HexID;
use crate::reader::Reader;
use crate::settings::Settings;
use crate::trace::Event;
use crate::trace::EventType;
use crate::trace::Trace;
use crate::trace::TracepointID;
use crate::trace::{DAGEdge, EdgeType};

#[derive(Debug)]
struct UberParseError(String);

impl fmt::Display for UberParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Uber error: {}", self.0)
    }
}

impl Error for UberParseError {}

fn raise(s: &str) -> Box<dyn Error> {
    // panic!(s.to_string());
    Box::new(UberParseError(s.into()))
}

pub struct UberReader {}

impl Reader for UberReader {
    fn for_searchspace(&mut self) {}
    fn reset_state(&mut self) {}
    fn read_file(&mut self, filename: &str) -> Trace {
        self.try_read_file(filename).unwrap()
    }

    fn get_trace_from_base_id(&mut self, _id: &str) -> Option<Trace> {
        None
    }

    fn get_recent_traces(&mut self) -> Vec<Trace> {
        Vec::new()
    }

    fn read_dir(&mut self, foldername: &str) -> Vec<Trace> {
        let mut results = Vec::new();
        for entry in std::fs::read_dir(foldername).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            eprintln!("Reading {}", path.to_str().unwrap());
            match self.try_read_file(&path.to_str().unwrap()) {
                Ok(t) => results.push(t),
                Err(e) => {
                    eprintln!("Parsing failed with {:?}", e);
                }
            }
        }
        results
    }
}

fn convert_uber_timestamp(start_time: u64, duration: i64) -> (NaiveDateTime, NaiveDateTime) {
    let duration = Duration::microseconds(duration);

    let seconds: i64 = (start_time / 1000000).try_into().unwrap();
    let nanos: u32 = ((start_time % 1000000) * 1000).try_into().unwrap();
    let start_time = NaiveDateTime::from_timestamp(seconds, nanos);
    (start_time, start_time + duration)
}

struct UberParsingState {
    nidx: NodeIndex,
    event: UberEvent,
    active_spans: HashMap<Uuid, NodeIndex>,
    children_per_parent: HashMap<Uuid, NodeIndex>,
    last_nidx: Option<NodeIndex>,
}

impl UberReader {
    pub fn from_settings(_settings: &Settings) -> Self {
        UberReader {}
    }

    fn try_read_file(&mut self, filename: &str) -> Result<Trace, Box<dyn Error>> {
        let reader = std::fs::File::open(filename).unwrap();
        match serde_json::from_reader(reader) {
            // We either have a saved file, or saved xtrace output
            Ok(v) => Ok(v),
            Err(_) => {
                let reader = std::fs::File::open(filename).unwrap();
                let mut t: UberTrace = serde_json::from_reader(reader).unwrap();
                self.from_json(&mut t)
                // trace.prune();
            }
        }
    }

    fn to_events_edges(&self, spans: &Vec<UberSpan>) -> Result<Vec<UberEvent>, Box<dyn Error>> {
        let mut events = Vec::new();
        for span in spans {
            let parent = if span.references.len() == 1 {
                let r = &span.references[0];
                if r.trace_id != span.trace_id {
                    return Err(raise(&format!(
                        "Mismatch on trace ids {:?} and {:?}",
                        r.trace_id, span.trace_id
                    )));
                }
                Some(r.span_id)
            } else if span.references.len() == 0 {
                None
            } else {
                return Err(raise(&format!("Got {} references", span.references.len())));
            };
            let (start_time, end_time) = convert_uber_timestamp(span.start_time, span.duration);
            events.push(UberEvent {
                e: Event {
                    trace_id: span.span_id.to_uuid(),
                    tracepoint_id: TracepointID::from_str(&span.operation_name.to_string()),
                    timestamp: start_time,
                    is_synthetic: false,
                    variant: EventType::Entry,
                },
                parent_id: parent,
            });
            events.push(UberEvent {
                e: Event {
                    trace_id: span.span_id.to_uuid(),
                    tracepoint_id: TracepointID::from_str(&span.operation_name.to_string()),
                    timestamp: end_time,
                    is_synthetic: false,
                    variant: EventType::Exit,
                },
                parent_id: parent,
            });
        }
        Ok(events)
    }

    fn from_json(&self, data: &mut UberTrace) -> Result<Trace, Box<dyn Error>> {
        assert!(data.data.len() == 1);
        let trace = &data.data[0];
        let mut mydag = Trace::new(&trace.trace_id.to_uuid());
        let mut event_list = self.to_events_edges(&trace.spans)?;
        event_list.sort_by(|a, b| a.e.timestamp.cmp(&b.e.timestamp));
        let mut state = UberParsingState {
            active_spans: HashMap::new(),
            children_per_parent: HashMap::new(),
            last_nidx: None,
            event: event_list[0].clone(),
            nidx: NodeIndex::end(),
        };
        let mut deferred_events: Vec<UberEvent> = Vec::new();
        let mut deferred_timestamp = event_list[0].e.timestamp.clone();
        for event in event_list.iter() {
            state.event = event.clone();
            if event.e.timestamp > deferred_timestamp {
                let mut num_tries = deferred_events.len() + 1;
                while deferred_events.len() != 0 {
                    if num_tries == 0 {
                        return Err(raise("Could not add deferred nodes"));
                    }
                    num_tries -= 1;
                    state.event = deferred_events.pop().unwrap();
                    state.nidx = mydag.g.add_node(event.e.clone());
                    match self.try_add_node(&mut mydag, &mut state) {
                        Ok(_) => {
                            state.last_nidx = Some(state.nidx);
                        }
                        Err(_) => {
                            mydag.g.remove_node(state.nidx);
                            deferred_events.insert(0, state.event.clone());
                        }
                    }
                }
            }
            state.nidx = mydag.g.add_node(event.e.clone());
            match self.try_add_node(&mut mydag, &mut state) {
                Ok(_) => {
                    state.last_nidx = Some(state.nidx);
                }
                Err(_) => {
                    mydag.g.remove_node(state.nidx);
                    deferred_events.push(event.clone());
                    deferred_timestamp = event.e.timestamp.clone();
                }
            }
        }
        mydag.end_node = state.last_nidx.unwrap();
        mydag.duration = (mydag.g[mydag.end_node].timestamp - mydag.g[mydag.start_node].timestamp)
            .to_std()
            .unwrap();
        let g: Graph<Event, DAGEdge> = mydag.g.clone().into();
        if connected_components(&g) > 1 {
            Err(raise("Too many connected components"))
        } else {
            Ok(mydag)
        }
    }

    fn try_add_node(
        &self,
        mydag: &mut Trace,
        s: &mut UberParsingState,
    ) -> Result<(), Box<dyn Error>> {
        if mydag.g.node_count() <= 1 {
            mydag.start_node = s.nidx;
            if !s.event.parent_id.is_none() {
                return Err(raise("Trace does not start with root span"));
            }
        }
        let prev_sibling = match &s.event.parent_id {
            Some(id) => {
                let result = match s.children_per_parent.get(&id.to_uuid()) {
                    Some(&id) => Some(id.clone()),
                    None => None,
                };
                s.children_per_parent.insert(id.to_uuid(), s.nidx);
                result
            }
            None => None,
        };
        match prev_sibling {
            Some(i) => {
                mydag.g.add_edge(
                    i,
                    s.nidx,
                    DAGEdge {
                        duration: (s.event.e.timestamp - mydag.g[i].timestamp).to_std()?,
                        variant: EdgeType::ChildOf,
                    },
                );
            }
            None => {
                let prev_nidx = match &s.event.parent_id {
                    Some(p) => match s.active_spans.get(&p.to_uuid()) {
                        Some(&id) => Some(id),
                        None => {
                            return Err(raise("Parent did not start before current node"));
                        }
                    },
                    None => {
                        if mydag.g.node_count() > 1 {
                            match mydag.g[s.last_nidx.unwrap()].variant {
                                EventType::Exit => {}
                                _ => {
                                    if mydag.g[s.last_nidx.unwrap()].trace_id != s.event.e.trace_id
                                    {
                                        return Err(raise(
                                            "Last node not exit and got parentless node",
                                        ));
                                    }
                                }
                            }
                            s.last_nidx
                        } else {
                            None
                        }
                    }
                };
                match prev_nidx {
                    Some(p) => {
                        mydag.g.add_edge(
                            p,
                            s.nidx,
                            DAGEdge {
                                duration: (s.event.e.timestamp - mydag.g[p].timestamp).to_std()?,
                                variant: EdgeType::ChildOf,
                            },
                        );
                    }
                    None => {}
                }
            }
        }
        s.active_spans.insert(s.event.e.trace_id, s.nidx);
        s.children_per_parent.insert(s.event.e.trace_id, s.nidx);
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct UberEvent {
    e: Event,
    parent_id: Option<HexID>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct UberTrace {
    pub data: Vec<UberData>,
    total: u64,
    limit: u64,
    offset: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct UberData {
    #[serde(rename = "traceID")]
    trace_id: HexID,
    spans: Vec<UberSpan>,
    #[serde(skip_deserializing)]
    processes: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UberSpan {
    #[serde(rename = "traceID")]
    trace_id: HexID,
    #[serde(rename = "spanID")]
    span_id: HexID,
    #[serde(default)]
    flags: u64,
    operation_name: HexID,
    references: Vec<UberReference>,
    start_time: u64,
    duration: i64,
    tags: Vec<UberTag>,
    logs: Vec<String>,
    process: UberProcess,
    warnings: Option<String>,
    #[serde(rename = "processID")]
    process_id: HexID,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct UberEdge {
    parent: HexID,
    child: HexID,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UberReference {
    ref_type: String,
    #[serde(rename = "traceID")]
    trace_id: HexID,
    #[serde(rename = "spanID")]
    span_id: HexID,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UberProcess {
    service_name: HexID,
    tags: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct UberTag {
    key: String,
    #[serde(rename = "type")]
    type_of: String,
    value: String,
}

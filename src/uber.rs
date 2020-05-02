use std::collections::HashMap;
use std::convert::TryInto;
use std::error::Error;
use std::fmt;

use byteorder::BigEndian;
use byteorder::ByteOrder;
use chrono::Duration;
use chrono::NaiveDateTime;
use futures::future;
use futures::future::Future;
use futures::stream::Stream;
use futures::Async;
use hex;
use hyper::rt;
use hyper::Client;
use petgraph::graph::NodeIndex;
use serde::de;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::hdfs::HDFSID;
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
    Box::new(UberParseError(s.into()))
}

pub struct UberReader {}

impl Reader for UberReader {
    fn read_file(&mut self, filename: &str) -> Trace {
        let reader = std::fs::File::open(filename).unwrap();
        match serde_json::from_reader(reader) {
            // We either have a saved file, or saved xtrace output
            Ok(v) => v,
            Err(_) => {
                let reader = std::fs::File::open(filename).unwrap();
                let mut t: UberTrace = serde_json::from_reader(reader).unwrap();
                self.from_json(&mut t).unwrap()
                // trace.prune();
            }
        }
    }

    fn get_trace_from_base_id(&mut self, id: &str) -> Option<Trace> {
        None
    }

    fn get_recent_traces(&mut self) -> Vec<Trace> {
        Vec::new()
    }
}

fn convert_uber_timestamp(start_time: u64, duration: i64) -> (NaiveDateTime, NaiveDateTime) {
    let duration = Duration::microseconds(duration);

    let seconds: i64 = (start_time / 1000000).try_into().unwrap();
    let nanos: u32 = ((start_time % 1000000) * 1000).try_into().unwrap();
    let start_time = NaiveDateTime::from_timestamp(seconds, nanos);
    (start_time, start_time + duration)
}

impl UberReader {
    pub fn from_settings(settings: &Settings) -> Self {
        UberReader {}
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
        let mut active_spans = HashMap::new();
        let mut children_per_parent: HashMap<HDFSID, NodeIndex> = HashMap::new();
        let mut last_nidx: Option<NodeIndex> = None;
        let mut event_list = self.to_events_edges(&trace.spans)?;
        event_list.sort_by(|a, b| a.e.timestamp.cmp(&b.e.timestamp));
        for (idx, event) in event_list.iter().enumerate() {
            let nidx = mydag.g.add_node(event.e.clone());
            if idx == 0 {
                mydag.start_node = nidx;
                if !event.parent_id.is_none() {
                    return Err(raise("Trace does not start with root span"));
                }
            }
            match children_per_parent.get(&event.parent_id) {
                Some(&i) => {
                    mydag.g.add_edge(
                        i,
                        nidx,
                        DAGEdge {
                            duration: (event.e.timestamp - mydag.g[i].timestamp).to_std().unwrap(),
                            variant: EdgeType::ChildOf,
                        },
                    );
                }
                None => {
                    let prev_nidx = match event.parent_id {
                        Some(p) => *active_spans.get(&p.to_uuid()).unwrap(),
                        None => {
                            match mydag.g[last_nidx.unwrap()].variant {
                                EventType::Exit => {}
                                _ => {
                                    return Err(raise(
                                        "Last node not exit and got parentless node",
                                    ));
                                }
                            }
                            last_nidx.unwrap()
                        }
                    };
                    mydag.g.add_edge(
                        prev_nidx,
                        nidx,
                        DAGEdge {
                            duration: (event.e.timestamp - mydag.g[prev_nidx].timestamp)
                                .to_std()
                                .unwrap(),
                            variant: EdgeType::ChildOf,
                        },
                    );
                }
            }
            match &event.e.variant {
                EventType::Entry => {
                    active_spans.insert(event.e.trace_id, nidx);
                }
                EventType::Exit => {
                    let start_nidx = match active_spans.get(&event.e.trace_id) {
                        Some(i) => i,
                        None => {
                            return Err(raise(&format!(
                                "Span {} started before ending",
                                event.e.trace_id
                            )));
                        }
                    };
                }
                EventType::Annotation => {
                    return Err(raise("Uber does not support annotations"));
                }
            }
            last_nidx = Some(nidx);
        }
        // mydag.end_node = nidx;
        // mydag.duration = (mydag.g[mydag.end_node].timestamp - mydag.g[mydag.start_node].timestamp)
        //     .to_std()
        //     .unwrap();
        Ok(mydag)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct UberEvent {
    e: Event,
    parent_id: Option<HDFSID>,
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
    trace_id: HDFSID,
    spans: Vec<UberSpan>,
    #[serde(skip_deserializing)]
    processes: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UberSpan {
    #[serde(rename = "traceID")]
    trace_id: HDFSID,
    #[serde(rename = "spanID")]
    span_id: HDFSID,
    #[serde(default)]
    flags: u64,
    operation_name: HDFSID,
    references: Vec<UberReference>,
    start_time: u64,
    duration: i64,
    tags: Vec<UberTag>,
    logs: Vec<String>,
    process: UberProcess,
    warnings: Option<String>,
    #[serde(rename = "processID")]
    process_id: HDFSID,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct UberEdge {
    parent: HDFSID,
    child: HDFSID,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UberReference {
    ref_type: String,
    #[serde(rename = "traceID")]
    trace_id: HDFSID,
    #[serde(rename = "spanID")]
    span_id: HDFSID,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UberProcess {
    service_name: HDFSID,
    tags: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct UberTag {
    key: String,
    #[serde(rename = "type")]
    type_of: String,
    value: String,
}

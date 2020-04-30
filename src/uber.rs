use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt;

use byteorder::BigEndian;
use byteorder::ByteOrder;
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
                let mut trace = self.from_json(&mut t);
                trace.prune();
                trace
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

impl UberReader {
    pub fn from_settings(settings: &Settings) -> Self {
        UberReader {}
    }

    fn to_events_edges(&self, spans: &Vec<UberSpan>) -> (Vec<Event>, Vec<UberEdge>) {
        let mut events = Vec::new();
        let mut edges = Vec::new();
        (events, edges)
    }

    fn from_json(&self, data: &mut UberTrace) -> Trace {
        assert!(data.data.len() == 1);
        let mut trace = data.data[0];
        let mut mydag = Trace::new(&trace.traceID.to_uuid());
        let mut event_id_map = HashMap::new();
        let mut nidx = NodeIndex::end();
        let (mut event_list, edge_list) = self.to_events_edges(&trace.spans);
        event_list.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        for (idx, event) in event_list.iter().enumerate() {
            nidx = mydag.g.add_node(event.clone());
            event_id_map.insert(event.trace_id.clone(), nidx);
            if idx == 0 {
                mydag.start_node = nidx;
            } else {
                for parent in event.parent_event_id.iter() {
                    match event_id_map.get(parent) {
                        Some(&parent_nidx) => {
                            mydag.g.add_edge(
                                parent_nidx,
                                nidx,
                                DAGEdge {
                                    duration: (mynode.timestamp - mydag.g[parent_nidx].timestamp)
                                        .to_std()
                                        .unwrap(),
                                    variant: EdgeType::ChildOf,
                                },
                            );
                        }
                        None => {
                            panic!("Couldn't find parent node {}", parent);
                        }
                    }
                }
            }
        }
        mydag.end_node = nidx;
        mydag.duration = (mydag.g[mydag.end_node].timestamp - mydag.g[mydag.start_node].timestamp)
            .to_std()
            .unwrap();
        mydag
    }
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
    traceID: HDFSID,
    spans: Vec<UberSpan>,
    processes: HashMap<HDFSID, UberProcess>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct UberSpan {
    traceID: HDFSID,
    spanID: HDFSID,
    flags: u64,
    operationName: HDFSID,
    references: Vec<UberReference>,
    startTime: u64,
    duration: u64,
    tags: Vec<UberTag>,
    logs: Vec<String>,
    process: UberProcess,
    warnings: String,
    processID: HDFSID,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct UberEdge {
    parent: HDFSID,
    child: HDFSID,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct UberReference {
    refType: String,
    traceID: HDFSID,
    spanID: HDFSID,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct UberProcess {
    serviceName: HDFSID,
    tags: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct UberTag {
    key: String,
    #[serde(rename = "type")]
    type_of: String,
    value: String,
}

use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt;

use byteorder::BigEndian;
use byteorder::ByteOrder;
use chrono::NaiveDateTime;
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

use crate::reader::Reader;
use crate::settings::Settings;
use crate::trace::Event;
use crate::trace::EventType;
use crate::trace::Trace;
use crate::trace::{DAGEdge, EdgeType};

pub struct HDFSReader {}

impl Reader for HDFSReader {
    fn get_recent_traces(&mut self) -> Vec<Trace> {
        Vec::new()
    }

    fn get_trace_from_base_id(&mut self, id: &str) -> Option<Trace> {
        assert!(id.len() != 0);
        let (tx, mut rx) = futures::sync::mpsc::unbounded();
        let client = Client::new();

        let fut = client
            .get(
                "http://localhost:4080/interactive/reports/5c924fe2264cf827"
                    .parse()
                    .unwrap(),
            )
            .and_then(|res| res.into_body().concat2())
            .and_then(move |body| {
                let s = ::std::str::from_utf8(&body).expect("httpbin sends utf-8 JSON");

                tx.unbounded_send(s.to_string()).unwrap();
                Ok(())
            })
            .map_err(|e| eprintln!("RPC Client error: {:?}", e));
        rt::run(fut);
        let mut result = "".to_string();
        loop {
            match rx.poll() {
                Ok(Async::Ready(Some(s))) => {
                    result = s;
                }
                Ok(Async::NotReady) => {}
                Ok(Async::Ready(None)) => {
                    break;
                }
                Err(e) => panic!("Got error from poll: {:?}", e),
            }
        }
        println!("{}", result);
        None
    }

    fn read_file(&mut self, file: &str) -> Trace {
        let reader = std::fs::File::open(file).unwrap();
        let mut t: Vec<HDFSTrace> = serde_json::from_reader(reader).unwrap();
        assert!(t.len() == 1);
        self.from_json(&mut t[0])
    }
}

impl HDFSReader {
    pub fn from_settings(_settings: &Settings) -> Self {
        HDFSReader {}
    }

    fn from_json(&self, data: &mut HDFSTrace) -> Trace {
        let mut mydag = Trace::new(&data.id.to_uuid());
        let mut event_id_map = HashMap::new();
        let mut nidx = NodeIndex::end();
        sort_event_list(&mut data.reports);
        for (idx, event) in data.reports.iter().enumerate() {
            let mynode = Event::from_hdfs_node(event);
            nidx = mydag.g.add_node(mynode.clone());
            event_id_map.insert(event.event_id.clone(), nidx);
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
        mydag.duration = (mydag.g[mydag.end_node].timestamp - mydag.g[mydag.start_node].timestamp).to_std().unwrap();
        mydag
    }
}

#[derive(Serialize, Debug, Clone, Eq, PartialEq, Copy)]
pub struct HDFSID {
    id: Option<[u8; 8]>,
}

impl HDFSID {
    fn to_uuid(&self) -> Uuid {
        let mut buf: [u8; 16] = [0; 16];
        match self.id {
            Some(bytes) => {
                buf[..8].copy_from_slice(&bytes);
            }
            None => {}
        }
        Uuid::from_bytes(buf)
    }
}

fn eventid_to_uuid(id: &String) -> Uuid {
    let id = id.parse::<i64>().unwrap();
    let mut buf = [0; 16];
    BigEndian::write_i64(&mut buf, id);
    Uuid::from_bytes(buf)
}

fn convert_hdfs_timestamp(timestamp: u64, _hrt: u64) -> NaiveDateTime {
    let seconds: i64 = (timestamp / 1000).try_into().unwrap();
    let nanos: u32 = ((timestamp % 1000) * 1000000).try_into().unwrap();
    NaiveDateTime::from_timestamp(seconds, nanos)
}

impl<'de> Deserialize<'de> for HDFSID {
    fn deserialize<D>(deserializer: D) -> Result<HDFSID, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_str(HDFSIDVisitor)
    }
}

struct HDFSIDVisitor;

impl<'de> de::Visitor<'de> for HDFSIDVisitor {
    type Value = HDFSID;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string representing HDFSID")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value == "0" {
            return Ok(HDFSID { id: None });
        }
        let decoded = hex::decode(value).unwrap();
        let mut result = [0; 8];
        let decoded = &decoded[..result.len()];
        result.copy_from_slice(decoded);
        Ok(HDFSID { id: Some(result) })
    }
}

fn sort_event_list(event_list: &mut Vec<HDFSEvent>) {
    // Sorts events by timestamp
    event_list.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
}

impl Event {
    fn from_hdfs_node(event: &HDFSEvent) -> Event {
        Event {
            trace_id: eventid_to_uuid(&event.event_id),
            tracepoint_id: event.label.clone(),
            timestamp: convert_hdfs_timestamp(event.timestamp, event.hrt),
            variant: EventType::Annotation,
            is_synthetic: false,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct HDFSTrace {
    pub id: HDFSID,
    pub reports: Vec<HDFSEvent>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct HDFSEvent {
    agent: String,
    process_name: String,
    #[serde(rename = "TaskID")]
    task_id: HDFSID,
    #[serde(rename = "ParentEventID")]
    parent_event_id: Vec<String>,
    label: String,
    title: String,
    host: String,
    #[serde(rename = "HRT")]
    hrt: u64,
    timestamp: u64,
    #[serde(rename = "ThreadID")]
    thread_id: u64,
    thread_name: String,
    #[serde(rename = "EventID")]
    event_id: String,
    #[serde(rename = "ProcessID")]
    process_id: u64,
    #[serde(flatten)]
    variant: HDFSEnum,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(untagged)]
pub enum HDFSEnum {
    Type1(Type1Event),
    Type2(Type2Event),
    Type3(Type3Event),
    Type4(Type4Event),
    Type5(Type5Event),
    Type6(Type6Event),
    Type7(Type7Event),
    Type8(Type8Event),
    Type9(Type9Event),
    Type10(Type10Event),
    Type11(Type11Event),
    Type12(Type12Event),
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
#[serde(deny_unknown_fields)]
pub struct Type1Event {
    tag: Vec<String>,
    source: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
#[serde(deny_unknown_fields)]
pub struct Type2Event {
    operation: String,
    cycles: u64,
    source: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
#[serde(deny_unknown_fields)]
pub struct Type3Event {
    operation: String,
    cycles: u64,
    file: String,
    duration: String,
    source: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
#[serde(deny_unknown_fields)]
pub struct Type4Event {
    cycles: u64,
    tag: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
#[serde(deny_unknown_fields)]
pub struct Type5Event {
    cycles: u64,
    source: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
#[serde(deny_unknown_fields)]
pub struct Type6Event {
    cycles: u64,
    connection: String,
    duration: String,
    operation: String,
    source: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
#[serde(deny_unknown_fields)]
pub struct Type7Event {
    cycles: u64,
    source: String,
    duration: String,
    operation: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
#[serde(deny_unknown_fields)]
pub struct Type8Event {
    cycles: u64,
    connection: String,
    duration: String,
    operation: String,
    bytes: String,
    source: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
#[serde(deny_unknown_fields)]
pub struct Type9Event {
    cycles: u64,
    connection: String,
    duration: String,
    operation: String,
    bytes: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
#[serde(deny_unknown_fields)]
pub struct Type10Event {
    cycles: u64,
    queue: String,
    source: String,
    operation: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
#[serde(deny_unknown_fields)]
pub struct Type11Event {
    cycles: u64,
    queue: String,
    source: String,
    operation: String,
    queue_duration: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
#[serde(deny_unknown_fields)]
pub struct Type12Event {
    cycles: u64,
    source: String,
    file: String,
    bytes: String,
    duration: String,
    operation: String,
}
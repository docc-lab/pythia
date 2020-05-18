use std::collections::HashMap;
use std::convert::TryInto;
use std::error::Error;

use byteorder::BigEndian;
use byteorder::ByteOrder;
use chrono::NaiveDateTime;
use futures::future;
use futures::future::Future;
use futures::stream::Stream;
use futures::Async;
use hyper::rt;
use hyper::Client;
use petgraph::graph::NodeIndex;
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

pub struct HDFSReader {
    xtrace_url: String,
    for_searchspace: bool,
}

impl Reader for HDFSReader {
    fn for_searchspace(&mut self) {
        self.for_searchspace = true;
    }

    fn reset_state(&mut self) {}
    fn get_recent_traces(&mut self) -> Vec<Trace> {
        Vec::new()
    }

    fn get_trace_from_base_id(&mut self, id: &str) -> Result<Trace, Box<dyn Error>> {
        assert!(id.len() != 0);
        let urn: String = format!("{}/interactive/reports/{}", self.xtrace_url, id);

        let (tx, mut rx) = futures::sync::mpsc::unbounded();

        let fut = future::lazy(move || {
            Client::new()
                .get(urn.parse().unwrap())
                .and_then(|res| res.into_body().concat2())
                .and_then(move |body| {
                    let s = ::std::str::from_utf8(&body).expect("httpbin sends utf-8 JSON");
                    tx.unbounded_send(s.to_string()).unwrap();
                    Ok(())
                })
                .map_err(|e| eprintln!("RPC Client error: {:?}", e))
        });
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
        let mut t: Vec<HDFSTrace> = serde_json::from_str(&result)?;
        assert!(t.len() == 1);
        let mut trace = self.from_json(&mut t[0]);
        if self.for_searchspace {
            trace.prune();
        }
        Ok(trace)
    }

    fn read_file(&mut self, file: &str) -> Trace {
        let reader = std::fs::File::open(file).unwrap();
        match serde_json::from_reader(reader) {
            // We either have a saved file, or saved xtrace output
            Ok(v) => v,
            Err(_) => {
                let reader = std::fs::File::open(file).unwrap();
                let mut t: Vec<HDFSTrace> = serde_json::from_reader(reader).unwrap();
                assert!(t.len() == 1);
                let mut trace = self.from_json(&mut t[0]);
                if self.for_searchspace {
                    trace.prune();
                }
                trace
            }
        }
    }

    fn read_dir(&mut self, _foldername: &str) -> Vec<Trace> {
        Vec::new()
    }
}

impl HDFSReader {
    pub fn from_settings(settings: &Settings) -> Self {
        HDFSReader {
            xtrace_url: settings.xtrace_url.clone(),
            for_searchspace: false,
        }
    }

    fn should_skip_edge(&self, mynode: &Event, parent: &Event) -> bool {
        (mynode.tracepoint_id == TracepointID::from_str("Client.java:1076")
            && parent.tracepoint_id == TracepointID::from_str("Client.java:1044"))
            || (mynode.tracepoint_id == TracepointID::from_str("DFSOutputStream.java:441")
                && parent.tracepoint_id == TracepointID::from_str("DFSOutputStream.java:1669"))
            || (mynode.tracepoint_id == TracepointID::from_str("BlockReceiver.java:1322")
                && parent.tracepoint_id == TracepointID::from_str("BlockReceiver.java:903"))
            || (mynode.tracepoint_id == TracepointID::from_str("DFSOutputStream.java:441")
                && parent.tracepoint_id == TracepointID::from_str("SocketOutputStream.java:63"))
            || (mynode.tracepoint_id == TracepointID::from_str("PacketHeader.java:164")
                && parent.tracepoint_id == TracepointID::from_str("SocketInputStream.java:57"))
            || (mynode.tracepoint_id == TracepointID::from_str("BlockReceiver.java:1322")
                && parent.tracepoint_id == TracepointID::from_str("SocketOutputStream.java:63"))
            || (mynode.tracepoint_id == TracepointID::from_str("PipelineAck.java:257")
                && parent.tracepoint_id == TracepointID::from_str("SocketInputStream.java:57"))
            || (mynode.tracepoint_id == TracepointID::from_str("DFSOutputStream.java:2271")
                && parent.tracepoint_id == TracepointID::from_str("DFSOutputStream.java:1805"))
    }

    fn should_skip_node(&self, node: &HDFSEvent, event: &Event) -> bool {
        node.label == "waited"
            || event.tracepoint_id == TracepointID::from_str("DFSOutputStream.java:387")
            || event.tracepoint_id == TracepointID::from_str("BlockReceiver.java:1280")
    }

    fn from_json(&self, data: &mut HDFSTrace) -> Trace {
        let mut mydag = Trace::new(&data.id.to_uuid());
        eprintln!("Working on {}", mydag.base_id);
        let mut event_id_map = HashMap::new();
        let mut nidx = NodeIndex::end();
        let mut start_node = None;
        let mut wait_parents: HashMap<String, Vec<String>> = HashMap::new();
        sort_event_list(&mut data.reports);
        for (_idx, event) in data.reports.iter().enumerate() {
            let mynode = Event::from_hdfs_node(event);
            if self.should_skip_node(&event, &mynode) {
                let mut parents = Vec::new();
                let mut potential_parents = event.parent_event_id.clone();
                while !potential_parents.is_empty() {
                    let p = potential_parents.pop().unwrap();
                    match event_id_map.get(&p) {
                        None => {
                            for p2 in wait_parents.get(&p).unwrap() {
                                potential_parents.push(p2.clone());
                            }
                        }
                        Some(_) => {
                            parents.push(p);
                        }
                    }
                }
                wait_parents.insert(event.event_id.clone(), parents);
                continue;
            }
            nidx = mydag.g.add_node(mynode.clone());
            event_id_map.insert(event.event_id.clone(), nidx);
            if start_node.is_none() {
                mydag.start_node = nidx;
                start_node = Some(nidx);
            } else {
                for parent in event.parent_event_id.iter() {
                    match event_id_map.get(parent) {
                        Some(&parent_nidx) => {
                            // Skip this edge, since it's not used.
                            if self.should_skip_edge(&mynode, &mydag.g[parent_nidx]) {
                                continue;
                            }
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
                            // Must have deleted the parent
                            for p2 in wait_parents.get(parent).unwrap() {
                                let &parent_nidx = event_id_map.get(p2).unwrap();
                                if self.should_skip_edge(&mynode, &mydag.g[parent_nidx]) {
                                    continue;
                                }
                                mydag.g.add_edge(
                                    parent_nidx,
                                    nidx,
                                    DAGEdge {
                                        duration: (mynode.timestamp
                                            - mydag.g[parent_nidx].timestamp)
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
        }
        mydag.end_node = nidx;
        mydag.duration = (mydag.g[mydag.end_node].timestamp - mydag.g[mydag.start_node].timestamp)
            .to_std()
            .unwrap();
        mydag
    }
}

fn eventid_to_uuid(id: &String) -> Uuid {
    let id = id.parse::<i64>().unwrap();
    let mut buf = [0; 16];
    BigEndian::write_i64(&mut buf, id);
    Uuid::from_bytes(buf)
}

fn convert_hdfs_timestamp(_timestamp: u64, hrt: u64) -> NaiveDateTime {
    let seconds: i64 = (hrt / 1000).try_into().unwrap();
    let nanos: u32 = ((hrt % 1000) * 1000000).try_into().unwrap();
    NaiveDateTime::from_timestamp(seconds, nanos)
}

fn sort_event_list(event_list: &mut Vec<HDFSEvent>) {
    // Sorts events by timestamp
    event_list.sort_by(|a, b| a.hrt.cmp(&b.hrt));
}

impl Event {
    fn from_hdfs_node(event: &HDFSEvent) -> Event {
        Event {
            trace_id: eventid_to_uuid(&event.event_id),
            tracepoint_id: TracepointID::from_str(match &event.variant {
                HDFSEnum::WithSource(s) => &s.source,
                HDFSEnum::WithoutSource(_) => &event.label,
            }),
            timestamp: convert_hdfs_timestamp(event.timestamp, event.hrt),
            variant: EventType::Annotation,
            is_synthetic: false,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct HDFSTrace {
    pub id: HexID,
    pub reports: Vec<HDFSEvent>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct HDFSEvent {
    agent: String,
    process_name: String,
    #[serde(rename = "TaskID")]
    task_id: HexID,
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
    WithSource(EventWithSource),
    WithoutSource(EventWithoutSource),
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct EventWithSource {
    source: String,
    #[serde(flatten)]
    variant: WithSourceEnum,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(untagged)]
pub enum WithSourceEnum {
    Type1(Type1Event),
    Type2(Type2Event),
    Type3(Type3Event),
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
#[serde(untagged)]
pub enum EventWithoutSource {
    Type4(Type4Event),
    Type9(Type8Event),
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
#[serde(deny_unknown_fields)]
pub struct Type1Event {
    tag: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
#[serde(deny_unknown_fields)]
pub struct Type2Event {
    operation: String,
    cycles: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
#[serde(deny_unknown_fields)]
pub struct Type3Event {
    operation: String,
    cycles: u64,
    file: String,
    duration: String,
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
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
#[serde(deny_unknown_fields)]
pub struct Type6Event {
    cycles: u64,
    connection: String,
    duration: String,
    operation: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
#[serde(deny_unknown_fields)]
pub struct Type7Event {
    cycles: u64,
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
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
#[serde(deny_unknown_fields)]
pub struct Type9Event {
    cycles: u64,
    bytes: String,
    duration: String,
    operation: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
#[serde(deny_unknown_fields)]
pub struct Type10Event {
    cycles: u64,
    queue: String,
    operation: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
#[serde(deny_unknown_fields)]
pub struct Type11Event {
    cycles: u64,
    queue: String,
    operation: String,
    queue_duration: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
#[serde(deny_unknown_fields)]
pub struct Type12Event {
    cycles: u64,
    file: String,
    bytes: String,
    duration: String,
    operation: String,
}

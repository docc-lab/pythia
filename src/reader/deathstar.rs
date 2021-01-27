use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::TryInto;
use std::error::Error;
use std::time::Duration;

use byteorder::BigEndian;
use byteorder::ByteOrder;
use chrono::offset::Local;
use chrono::NaiveDateTime;
use futures::future;
use futures::future::Future;
use futures::stream::Stream;
use futures::Async;
use hyper::rt;
use hyper::Client;
use petgraph::graph::NodeIndex;
use regex::Regex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use std::path::PathBuf;

use crate::reader::HexID;
use crate::reader::Reader;
use crate::settings::Settings;
use crate::trace::Event;
use crate::trace::EventType;
use crate::trace::Trace;
use crate::trace::TracepointID;
use crate::trace::Value::SignedInt;
use crate::trace::Value::Str;
use crate::trace::Value::UnsignedInt;
use crate::trace::{DAGEdge, EdgeType};
use crate::PythiaError;
//use crate::trace::Value::float;

pub struct DEATHSTARReader {
    xtrace_url: String,
    jiffy: Duration,
    processed_traces: HashSet<String>,
    for_searchspace: bool,
    simplify_trace: bool,
    DEATHSTAR_trace_dir: PathBuf
}

impl Reader for DEATHSTARReader {
    fn for_searchspace(&mut self) {
        self.for_searchspace = true;
    }

    fn reset_state(&mut self) {}

    /// This function parses an xtrace webpage to get all requests executed from
    /// shell (with FsShell tag) and those with high enough elapsed time since last update
    fn get_recent_traces(&mut self) -> Vec<Trace> {
        let re1 = Regex::new(r"<td>").unwrap();
        let re2 = Regex::new(r"tag/").unwrap();
        let exclude1 = Regex::new(r"offset=").unwrap();
        let exclude2 = Regex::new(r"<form").unwrap();
        let xtrace_page = self
            .download_webpage(format!("{}/tag/FsShell?length=100", self.xtrace_url))
            .unwrap();
        let mut result = Vec::new();
        let mut trace_id: Option<String> = None;
        let mut date_passed = false;
        let main_re = Regex::new(r"tag/main").unwrap();
        let delete_all = self.processed_traces.len() == 0;
        for (idx, line) in xtrace_page
            .lines()
            .filter(|&s| re1.is_match(s) || re2.is_match(s))
            .filter(|&s| !exclude1.is_match(s))
            .filter(|&s| !exclude2.is_match(s))
            .enumerate()
        {
            if idx % 10 == 0 {
                trace_id = Some(line.split("\"").nth(5).unwrap().to_string());
            } else if idx % 10 == 5 {
                let date =
                    NaiveDateTime::parse_from_str(line.trim(), "<td>%b %d %Y, %H:%M:%S</td>")
                        .unwrap();
                date_passed = (Local::now().naive_local() - date).to_std().unwrap() > self.jiffy;
            } else if idx % 10 == 8 {
                if date_passed
                    && main_re.is_match(line)
                    && self
                        .processed_traces
                        .get(&trace_id.clone().unwrap())
                        .is_none()
                {
                    self.processed_traces.insert(trace_id.clone().unwrap());
                    if !delete_all {
                        result.push(trace_id.clone().unwrap());
                    }
                }
            }
        }
        result
            .iter()
            .map(|id| self.get_trace_from_base_id(id))
            .filter(|x| x.is_ok())
            .map(|x| x.unwrap())
            .collect()
    }

    // fn get_trace_from_base_id(&mut self, id: &str) -> Result<Trace, Box<dyn Error>> {
    //     assert!(id.len() != 0);
    //     let urn: String = format!("{}/interactive/reports/{}", self.xtrace_url, id);
    //     let result = self.download_webpage(urn)?;
    //     let mut t: Vec<DEATHSTARTrace> = serde_json::from_str(&result)?;
    //     assert!(t.len() == 1);
    //     let mut trace = self.from_json(&mut t[0]);
    //     // if self.for_searchspace {
    //     //     trace.prune();
    //     // }
    //     Ok(trace)
    // }

    fn get_trace_from_base_id(&mut self, id: &str) -> Result<Trace, Box<dyn Error>> {
        let mut path = self.DEATHSTAR_trace_dir.clone();
        path.push(id);
        path.set_extension("json");
        eprintln!("Reading {}", path.to_str().unwrap());
        let mut trace = self.read_file(&path.to_str().unwrap());
        Ok(trace)
    }

    fn read_file(&mut self, file: &str) -> Trace {
        let reader = std::fs::File::open(file).unwrap();
        match serde_json::from_reader(reader) {
            // We either have a saved file, or saved xtrace output
            Ok(v) => v,
            Err(_) => {
                let reader = std::fs::File::open(file).unwrap();
                // println!({})
                println!("Reading  mert{:?}", reader);
                let mut t: Vec<DEATHSTARTrace> = serde_json::from_reader(reader).unwrap();
                assert!(t.len() == 1);
                let mut trace = self.from_json(&mut t[0]);
                // if self.for_searchspace {
                //     trace.prune();
                // }
                trace
            }
        }
    }

    fn read_dir(&mut self, _foldername: &str) -> Vec<Trace> {
        Vec::new()
    }
}

impl DEATHSTARReader {
    pub fn from_settings(settings: &Settings) -> Self {
        DEATHSTARReader {
            xtrace_url: settings.xtrace_url.clone(),
            DEATHSTAR_trace_dir: settings.DEATHSTAR_trace_dir.clone(),
            jiffy: settings.jiffy,
            processed_traces: HashSet::new(),
            for_searchspace: false,
            simplify_trace: false,
        }
    }

    // fn try_read_file(&mut self, filename: &str) -> Result<Trace, Box<dyn Error>> {
    //     let reader = std::fs::File::open(filename).unwrap();
    //     match serde_json::from_reader(reader) {
    //         // We either have a saved file, or saved xtrace output
    //         Ok(v) => Ok(v),
    //         Err(_) => {
    //             let reader = std::fs::File::open(filename).unwrap();
    //             let mut t: DEATHSTARTrace = serde_json::from_reader(reader).unwrap();
    //             self.from_json(&mut t)
    //             // trace.prune();
    //         }
    //     }
    // }

    fn download_webpage(&self, urn: String) -> Result<String, Box<dyn Error>> {
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
                Err(_) => {
                    return Err(Box::new(PythiaError("Poll got us Err".into())));
                }
            }
        }
        Ok(result)
    }

    fn should_skip_edge(&self, mynode: &Event, parent: &Event) -> bool {
        if self.simplify_trace {
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
        } else {
            false
        }
    }

    fn should_skip_node(&self, node: &DEATHSTAREvent, event: &Event) -> bool {
        if self.simplify_trace {
            node.label == "waited"
                || event.tracepoint_id == TracepointID::from_str("DFSOutputStream.java:387")
                || event.tracepoint_id == TracepointID::from_str("BlockReceiver.java:1280")
        } else {
            false
        }
    }

    fn from_json(&self, data: &mut DEATHSTARTrace) -> Trace {
        println!("ukucu1");
        let mut mydag = Trace::new(&data.id.to_uuid());
        eprintln!("Working on {}", mydag.base_id);
        let mut event_id_map = HashMap::new();
        let mut nidx = NodeIndex::end();
        let mut start_node = None;
        let mut wait_parents: HashMap<String, Vec<String>> = HashMap::new();
        println!("ukucu12");
        sort_event_list(&mut data.reports);
        for (_idx, event) in data.reports.iter().enumerate() {
            let mynode = Event::from_DEATHSTAR_node(event);
            if self.should_skip_node(&event, &mynode) {
                println!("ukucu1222");
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
            println!("ukucu22212121");
            nidx = mydag.g.add_node(mynode.clone());
            event_id_map.insert(event.event_id.clone(), nidx);
            if start_node.is_none() {
                mydag.start_node = nidx;
                start_node = Some(nidx);
            } else {
                println!("ukucu3");
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
        println!("ukucu4");
        mydag.end_node = nidx;
        mydag.duration = (mydag.g[mydag.end_node].timestamp - mydag.g[mydag.start_node].timestamp)
            .to_std()
            .unwrap();
        mydag
    }
}

fn eventid_to_uuid(id: &String) -> Uuid {
    // let id = id.parse::<i64>().unwrap();
    // let mut buf = [0u8; 65536];
    // BigEndian::write_i64(&mut buf, id);
    // Uuid::from_bytes(buf)
    // Uuid::parse_str(id)
    // write!(buf, "{}", id).unwrap();
    // Uuid::parse_str(id).unwrap()
    // Uuid::from_str(id)
    // let mut buf = [0u8; 16];
    // let mut s = String::new();
    // s.push_str(id);
    // write!(buf, "{}", id).unwrap();
    // buf.write(id);
    // Uuid::from_bytes(id.into_bytes())
    let mut buf: [u8; 8] = [0; 8];
    let decoded = hex::decode(id).unwrap();
    for i in 0..8 {
        buf[i] = decoded[i];
    }
    HexID { id: Some(buf) }.to_uuid()
    // id.to_uuid()
    // Uuid::from_bytes(buf)
}

fn convert_DEATHSTAR_timestamp(_timestamp: u64, hrt: u64) -> NaiveDateTime {
    let seconds: i64 = (hrt / 1000000000).try_into().unwrap();
    let nanos: u32 = (hrt % 1000000000).try_into().unwrap();
    NaiveDateTime::from_timestamp(seconds, nanos)
}

fn sort_event_list(event_list: &mut Vec<DEATHSTAREvent>) {
    // Sorts events by timestamp
    event_list.sort_by(|a, b| a.hrt.cmp(&b.hrt));
}

impl Event {
    fn from_DEATHSTAR_node(event: &DEATHSTAREvent) -> Event {
       println!("ukumert");
       let mut map = HashMap::new();
       map.insert("Agent".to_string(), Str(event.agent.to_string()));
       map.insert("Process Name".to_string(), Str(event.process_name.to_string()));
       map.insert("Host".to_string(), Str(event.host.to_string()));
       //map.insert("hrt".to_string(), Int(event.hrt));
     //  map.insert("Thread id".to_string(), Int(event.thread_id));
       map.insert("Thread Name".to_string(), Str(event.process_id.to_string()));
       //map.insert("Process ID:".to_string(), Int(event.process_id));
       println!("ukumert1");
    //    if let DEATHSTAREnum::WithSource(s) = &event.variant{
    //        println!("ukumert0");
    //        if let WithSourceEnum::Type13(foo) = &s.variant{
    //            map.insert("Name".to_string(), Str(foo.name.to_string()));
    //         } else if let WithSourceEnum::Type14(foo) = &s.variant{
    //         map.insert("Read Size".to_string(), Str(foo.readsize.to_string()));
    //         } else if let WithSourceEnum::Type14(foo) = &s.variant{
    //         map.insert("Read Size".to_string(), Str(foo.readsize.to_string()));
    //         } else if let WithSourceEnum::Type15(foo) = &s.variant{
    //             map.insert("Write Size".to_string(), Str(foo.writesize.to_string()));
    //         } else if let WithSourceEnum::Type1(foo) = &s.variant{
    //             map.insert("Tag".to_string(), Str(foo.tag[0].to_string()));
    //         } else if let WithSourceEnum::Type16(foo) = &s.variant{
    //             map.insert("Replication".to_string(), Str(foo.replication.to_string()));
    //         }
    //     }
    //     else if let DEATHSTAREnum::WithoutSource(s) = &event.variant{
    //         println!("ukumert010");
    //         if let EventWithoutSource::Type4(foo) = &s{
    //             let white_space = foo.tag[0].find(" ");
    //             if let Some(number) = white_space{
    //                 map.insert("Command".to_string(), Str(foo.tag[0][1..number].to_string()));
    //             }
    //         }
    //     }
        println!("ukumert2");
        Event {
            trace_id: eventid_to_uuid(&event.event_id),
            // trace_id: event.event_id,
            // tracepoint_id: TracepointID::from_str(match &event.variant {
            //     DEATHSTAREnum::WithSource(s) => &s.source,
            //     DEATHSTAREnum::WithoutSource(_) => &event.label,
            // }),

            tracepoint_id: TracepointID::from_str(&event.source),
            timestamp: convert_DEATHSTAR_timestamp(event.timestamp, event.hrt),
            variant: EventType::Annotation,
            is_synthetic: false,
            key_value_pair: map,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct DEATHSTARTrace {
    pub id: HexID,
    pub reports: Vec<DEATHSTAREvent>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct DEATHSTAREvent {
    agent: String,
    process_name: String,
    #[serde(rename = "TaskID")]
    task_id: HexID,
    #[serde(rename = "ParentEventID")]
    parent_event_id: Vec<String>,
    label: String, 
    source: String, 
    title: String,
    host: String,
    #[serde(rename = "HRT")]
    hrt: u64,
    timestamp: u64,
    #[serde(rename = "ThreadID")]
    thread_id: i64,
    // thread_name: String,
    #[serde(rename = "EventID")]
    event_id: String,
    #[serde(rename = "ProcessID")]
    process_id: u64,
    // #[serde(flatten)]
    // variant: DEATHSTAREnum,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(untagged)]
pub enum DEATHSTAREnum {
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
    Type13(Type13Event),
    Type14(Type14Event),
    Type15(Type15Event),
    Type16(Type16Event),
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

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
#[serde(deny_unknown_fields)]
pub struct Type13Event {
    cycles: u64,
    name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
#[serde(deny_unknown_fields)]
pub struct Type14Event {
    cycles: u64,
    readsize: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
#[serde(deny_unknown_fields)]
pub struct Type15Event {
    cycles: u64,
    writesize: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
#[serde(deny_unknown_fields)]
pub struct Type16Event {
    cycles: u64,
    replication: String,
}
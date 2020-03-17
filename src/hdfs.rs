use std::collections::HashMap;
use std::fmt;

use hex;
use petgraph::{graph::NodeIndex, stable_graph::StableGraph};
use serde::de;
use serde::{Deserialize, Serialize};

use crate::trace::Event;
use crate::trace::EventType;
use crate::trace::{DAGEdge, EdgeType};

pub struct HDFSReader {}

impl HDFSReader {
    pub fn from_settings(settings: &HashMap<String, String>) -> Self {
        HDFSReader {}
    }

    pub fn read_file(&self, file: &str) -> HDFSDAG {
        let reader = std::fs::File::open(file).unwrap();
        let t: Vec<HDFSTrace> = serde_json::from_reader(reader).unwrap();
        assert!(t.len() == 1);
        HDFSDAG::from_json(&t[0])
    }
}

#[derive(Serialize, Debug, Clone, Eq, PartialEq, Copy)]
pub struct HDFSID {
    id: Option<[u8; 8]>,
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HDFSDAG {
    pub g: StableGraph<Event, DAGEdge>,
    pub base_id: HDFSID,
    pub start_node: NodeIndex,
    pub end_node: NodeIndex,
}

impl HDFSDAG {
    pub fn new(base_id: HDFSID) -> Self {
        HDFSDAG {
            g: StableGraph::new(),
            base_id: base_id.clone(),
            start_node: NodeIndex::end(),
            end_node: NodeIndex::end(),
        }
    }

    fn from_json(data: &HDFSTrace) -> HDFSDAG {
        let mut mydag = HDFSDAG::new(data.id);
        mydag.add_events(&data.reports);
        mydag
    }

    fn add_events(&mut self, data: &Vec<HDFSEvent>) {
        // let mut event_id_map = HashMap::new();
        // for (idx, event) in data.iter().enumerate() {
        //     let mut mynode = Event::from_hdfs_node(event);
        // }
    }
}

impl Event {
    // fn from_hdfs_node(event: &HDFSEvent) -> Event {
    //     Event {}
    // }
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

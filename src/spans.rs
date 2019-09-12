use serde::{Deserialize, Serialize};
use uuid::Uuid;
use chrono::NaiveDateTime;
use serde::de;
use std::fmt;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct Event {
    pub trace_id: Uuid,
    pub parent_id: Uuid,
    pub tracepoint_id: String,
    pub timestamp: NaiveDateTime,
    pub variant: EventEnum
}

#[derive(Serialize, Deserialize, Hash, Debug, Clone, Copy, Eq, PartialEq)]
pub enum EventEnum {
    Entry,
    Exit,
    Annotation
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct OSProfilerSpan {
    pub trace_id: Uuid,
    pub parent_id: Uuid,
    project: String,
    pub name: String,
    pub base_id: Uuid,
    service: String,
    #[serde(skip_deserializing)]
    pub tracepoint_id: String,
    #[serde(deserialize_with = "from_osp_timestamp")]
    pub timestamp: NaiveDateTime,
    #[serde(flatten)]
    pub variant: OSProfilerEnum
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(untagged)]
pub enum OSProfilerEnum {
    FunctionEntry(FunctionEntrySpan),
    FunctionExit(FunctionExitSpan),
    RequestEntry(RequestEntrySpan),
    Annotation(AnnotationSpan),
    RequestExit(RequestExitSpan),
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct AnnotationSpan {
    pub info: AnnotationInfo,
    pub tracepoint_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct AnnotationInfo {
    thread_id: u64,
    host: String,
    pub tracepoint_id: String,
    pub child_id: Uuid,
    pid: u64
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct RequestEntrySpan {
    info: RequestEntryInfo,
    pub tracepoint_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
struct RequestEntryInfo {
    request: RequestEntryRequest,
    thread_id: u64,
    host: String,
    pub tracepoint_id: String,
    pid: u64
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
struct RequestEntryRequest {
    path: String,
    scheme: String,
    method: String,
    query: String
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct RequestExitSpan {
    info: RequestExitInfo,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
struct RequestExitInfo { host: String }

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct FunctionExitSpan {
    info: FunctionExitInfo,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
struct FunctionExitInfo {
    function: FunctionExitFunction,
    host: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
struct FunctionExitFunction { result: String }

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct FunctionEntrySpan {
    info: FunctionEntryInfo,
    pub tracepoint_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
struct FunctionEntryInfo {
    function: FunctionEntryFunction,
    thread_id: u64,
    host: String,
    tracepoint_id: String,
    pid: u64
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
struct FunctionEntryFunction { name: String, args: String, kwargs: String }

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
        match NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S.%f") {
            Ok(t) => Ok(t),
            Err(_) => Err(de::Error::invalid_value(de::Unexpected::Str(s), &self)),
        }
    }
}

fn from_osp_timestamp<'de, D>(d: D) -> Result<NaiveDateTime, D::Error>
where
    D: de::Deserializer<'de>,
{
    d.deserialize_str(NaiveDateTimeVisitor)
}

use serde::Deserialize;
use uuid::Uuid;
use chrono::NaiveDateTime;
use serde::de;
use std::fmt;

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum OSProfilerSpan {
    FunctionEntry(FunctionEntrySpan),
    FunctionExit(FunctionExitSpan),
    RequestEntry(RequestEntrySpan),
    RequestExit(RequestExitSpan)
}

#[derive(Deserialize, Debug)]
pub struct RequestEntrySpan {
    info: RequestEntryInfo,
    parent_id: Uuid,
    project: String,
    name: String,
    base_id: Uuid,
    #[serde(deserialize_with = "from_osp_timestamp")]
    timestamp: NaiveDateTime,
    service: String,
    tracepoint_id: String,
    trace_id: Uuid
}

#[derive(Deserialize, Debug)]
struct RequestEntryInfo {
    request: RequestEntryRequest,
    thread_id: u64,
    host: String,
    tracepoint_id: String,
    pid: u64
}

#[derive(Deserialize, Debug)]
struct RequestEntryRequest {
    path: String,
    scheme: String,
    method: String,
    query: String
}

#[derive(Deserialize, Debug)]
pub struct RequestExitSpan {
    info: RequestExitInfo,
    parent_id: Uuid,
    project: String,
    name: String,
    base_id: Uuid,
    #[serde(deserialize_with = "from_osp_timestamp")]
    timestamp: NaiveDateTime,
    service: String,
    trace_id: Uuid
}

#[derive(Deserialize, Debug)]
struct RequestExitInfo { host: String }

#[derive(Deserialize, Debug)]
pub struct FunctionExitSpan {
    info: FunctionExitInfo,
    parent_id: Uuid,
    project: String,
    name: String,
    base_id: Uuid,
    #[serde(deserialize_with = "from_osp_timestamp")]
    timestamp: NaiveDateTime,
    service: String,
    trace_id: Uuid
}

#[derive(Deserialize, Debug)]
struct FunctionExitInfo {
    function: FunctionExitFunction,
    host: String,
}

#[derive(Deserialize, Debug)]
struct FunctionExitFunction { result: String }

#[derive(Deserialize, Debug)]
pub struct FunctionEntrySpan {
    info: FunctionEntryInfo,
    parent_id: Uuid,
    project: String,
    name: String,
    base_id: Uuid,
    #[serde(deserialize_with = "from_osp_timestamp")]
    timestamp: NaiveDateTime,
    service: String,
    tracepoint_id: String,
    trace_id: Uuid
}

#[derive(Deserialize, Debug)]
struct FunctionEntryInfo {
    function: FunctionEntryFunction,
    thread_id: u64,
    host: String,
    tracepoint_id: String,
    pid: u64
}

#[derive(Deserialize, Debug)]
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

use serde::Deserialize;
use uuid::Uuid;
use chrono::NaiveDateTime;
use serde::de;
use std::fmt;

#[derive(Deserialize, Debug, Clone)]
pub struct OSProfilerSpan {
    pub trace_id: Uuid,
    pub parent_id: Uuid,
    project: String,
    pub name: String,
    pub base_id: Uuid,
    service: String,
    #[serde(deserialize_with = "from_osp_timestamp")]
    pub timestamp: NaiveDateTime,
    #[serde(flatten)]
    pub variant: OSProfilerEnum
}

#[derive(Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum OSProfilerEnum {
    FunctionEntry(FunctionEntrySpan),
    FunctionExit(FunctionExitSpan),
    RequestEntry(RequestEntrySpan),
    Annotation(AnnotationSpan),
    RequestExit(RequestExitSpan),
}

#[derive(Deserialize, Debug, Clone)]
pub struct AnnotationSpan {
    pub info: AnnotationInfo,
    pub tracepoint_id: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct AnnotationInfo {
    thread_id: u64,
    host: String,
    pub tracepoint_id: String,
    pub child_id: Uuid,
    pid: u64
}

#[derive(Deserialize, Debug, Clone)]
pub struct RequestEntrySpan {
    info: RequestEntryInfo,
    pub tracepoint_id: String,
}

#[derive(Deserialize, Debug, Clone)]
struct RequestEntryInfo {
    request: RequestEntryRequest,
    thread_id: u64,
    host: String,
    pub tracepoint_id: String,
    pid: u64
}

#[derive(Deserialize, Debug, Clone)]
struct RequestEntryRequest {
    path: String,
    scheme: String,
    method: String,
    query: String
}

#[derive(Deserialize, Debug, Clone)]
pub struct RequestExitSpan {
    info: RequestExitInfo,
}

#[derive(Deserialize, Debug, Clone)]
struct RequestExitInfo { host: String }

#[derive(Deserialize, Debug, Clone)]
pub struct FunctionExitSpan {
    info: FunctionExitInfo,
}

#[derive(Deserialize, Debug, Clone)]
struct FunctionExitInfo {
    function: FunctionExitFunction,
    host: String,
}

#[derive(Deserialize, Debug, Clone)]
struct FunctionExitFunction { result: String }

#[derive(Deserialize, Debug, Clone)]
pub struct FunctionEntrySpan {
    info: FunctionEntryInfo,
    pub tracepoint_id: String,
}

#[derive(Deserialize, Debug, Clone)]
struct FunctionEntryInfo {
    function: FunctionEntryFunction,
    thread_id: u64,
    host: String,
    tracepoint_id: String,
    pid: u64
}

#[derive(Deserialize, Debug, Clone)]
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

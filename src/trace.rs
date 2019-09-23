/// General trace implementation
///

use std::fmt;
use std::fmt::Display;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use uuid::Uuid;
use chrono::NaiveDateTime;

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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DAGNode {
    pub span: Event
}

impl Display for DAGNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.span.variant {
            EventEnum::Entry => {
                write!(f, "{} start: {}", self.span.trace_id, self.span.tracepoint_id)
            },
            EventEnum::Annotation => {
                write!(f, "{} start: {}", self.span.trace_id, self.span.tracepoint_id)
            },
            EventEnum::Exit => {
                write!(f, "{} end", self.span.trace_id)
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct DAGEdge {
    pub duration: Duration,
    pub variant: EdgeType
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum EdgeType {
    ChildOf,
    FollowsFrom
}

impl Display for DAGEdge {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.variant {
            EdgeType::ChildOf => write!(f, "{}: C", self.duration.as_nanos()),
            EdgeType::FollowsFrom => write!(f, "{}: F", self.duration.as_nanos())
        }
    }
}

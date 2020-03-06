/// General trace implementation
///
use std::fmt;
use std::fmt::Display;
use std::time::Duration;

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::grouping::GroupNode;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct Event {
    pub trace_id: Uuid,
    pub parent_id: Uuid,
    pub tracepoint_id: String,
    pub timestamp: NaiveDateTime,
    pub variant: EventType,
}

#[derive(Serialize, Deserialize, Hash, Debug, Clone, Copy, Eq, PartialEq)]
pub enum EventType {
    Entry,
    Exit,
    Annotation,
}

impl Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.variant {
            EventType::Entry => write!(f, "{} start: {}", self.trace_id, self.tracepoint_id),
            EventType::Annotation => write!(f, "{} start: {}", self.trace_id, self.tracepoint_id),
            EventType::Exit => write!(f, "{} end", self.trace_id),
        }
    }
}

impl PartialEq<GroupNode> for Event {
    fn eq(&self, other: &GroupNode) -> bool {
        self.tracepoint_id == other.tracepoint_id && self.variant == other.variant
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct DAGEdge {
    pub duration: Duration,
    pub variant: EdgeType,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub enum EdgeType {
    ChildOf,
    FollowsFrom,
}

impl Display for DAGEdge {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.variant {
            EdgeType::ChildOf => write!(f, "{}: C", self.duration.as_nanos()),
            EdgeType::FollowsFrom => write!(f, "{}: F", self.duration.as_nanos()),
        }
    }
}

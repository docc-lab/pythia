mod searchspace;

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Display;
use std::path::Path;

use petgraph::visit::IntoNodeReferences;
use petgraph::visit::NodeRef;
use serde::{Deserialize, Serialize};

use pythia_common::RequestType;

use crate::grouping::Group;
use crate::manifest::searchspace::SearchSpace;
use crate::reader::REQUEST_TYPE_REGEXES;
use crate::trace::Trace;
use crate::trace::TracepointID;

pub use crate::manifest::searchspace::HierarchicalCriticalPath;

#[derive(Serialize, Deserialize, Debug)]
pub struct Manifest {
    pub per_request_type: HashMap<RequestType, SearchSpace>,
    request_type_tracepoints: Vec<TracepointID>,
}

impl Manifest {
    pub fn find_matches<'a>(&'a self, group: &Group) -> Vec<&'a HierarchicalCriticalPath> {
        match self.per_request_type.get(&group.request_type) {
            Some(ss) => ss.find_matches(group),
            None => {
                panic!(
                    "Request type {:?} not present in manifest",
                    group.request_type
                );
            }
        }
    }

    pub fn new() -> Manifest {
        Manifest {
            per_request_type: HashMap::new(),
            request_type_tracepoints: Vec::new(),
        }
    }

    pub fn try_constructing(trace: &Trace) -> Manifest {
        let mut map = HashMap::<RequestType, SearchSpace>::new();
        match map.get_mut(&trace.request_type) {
            Some(space) => {
                space.add_trace(&trace, true);
            }
            None => {
                let mut space = SearchSpace::default();
                space.add_trace(&trace, true);
                map.insert(trace.request_type, space);
            }
        }
        Manifest {
            per_request_type: map,
            request_type_tracepoints: Vec::new(),
        }
    }

    pub fn from_trace_list(traces: &Vec<Trace>) -> Manifest {
        let mut map = HashMap::<RequestType, SearchSpace>::new();
        for trace in traces {
            match map.get_mut(&trace.request_type) {
                Some(space) => {
                    space.add_trace(&trace, false);
                }
                None => {
                    let mut space = SearchSpace::default();
                    space.add_trace(&trace, false);
                    map.insert(trace.request_type, space);
                }
            }
        }
        let mut result = Manifest {
            per_request_type: map,
            request_type_tracepoints: Vec::new(),
        };
        result.add_request_type_tracepoints(traces);
        result
    }

    fn add_request_type_tracepoints(&mut self, traces: &Vec<Trace>) {
        for trace in traces {
            self.request_type_tracepoints.extend(
                trace
                    .g
                    .node_references()
                    .map(|x| x.weight().tracepoint_id.to_string())
                    .filter(|x: &String| REQUEST_TYPE_REGEXES.is_match(x))
                    .map(|x| TracepointID::from_str(&x)),
            );
        }
    }

    pub fn to_file(&self, file: &Path) {
        let writer = std::fs::File::create(file).unwrap();
        serde_json::to_writer(writer, self).ok();
    }

    pub fn from_file(file: &Path) -> Option<Manifest> {
        let reader = std::fs::File::open(file).unwrap();
        serde_json::from_reader(reader).unwrap()
    }

    pub fn entry_points(&self) -> Vec<TracepointID> {
        let mut result = HashSet::new();
        for cct in self.per_request_type.values() {
            for tracepoint in cct.get_entry_points() {
                result.insert(tracepoint);
            }
        }
        result.iter().cloned().collect()
    }
}

impl Display for Manifest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Manifest:").unwrap();
        for (request_type, inner) in &self.per_request_type {
            write!(f, "{:?} manifest:\n{}", request_type, inner).unwrap();
        }
        Ok(())
    }
}

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Display;
use std::path::Path;

use petgraph::dot::Dot;
use petgraph::graph::EdgeIndex;
use serde::{Deserialize, Serialize};

use cct::CCT;
use grouping::Group;
use osprofiler::OSProfilerDAG;
use osprofiler::RequestType;

#[derive(Serialize, Deserialize)]
pub struct Manifest {
    pub per_request_type: HashMap<RequestType, CCT>,
}

impl Manifest {
    pub fn from_trace_list(traces: Vec<OSProfilerDAG>) -> Manifest {
        let mut map = HashMap::<RequestType, CCT>::new();
        for trace in traces {
            match map.get_mut(&trace.request_type.unwrap()) {
                Some(cct) => {
                    cct.add_trace(&trace);
                }
                None => {
                    let mut cct = CCT::new();
                    cct.add_trace(&trace);
                    map.insert(trace.request_type.unwrap(), cct);
                }
            }
        }
        Manifest {
            per_request_type: map,
        }
    }

    pub fn to_file(&self, file: &Path) {
        let writer = std::fs::File::create(file).unwrap();
        serde_json::to_writer(writer, self).expect("Failed to manifest to cache");
    }

    pub fn from_file(file: &Path) -> Option<Manifest> {
        let reader = match std::fs::File::open(file) {
            Ok(x) => x,
            Err(_) => return None,
        };
        Some(serde_json::from_reader(reader).unwrap())
    }

    pub fn entry_points(&self) -> Vec<&String> {
        let mut result = HashSet::new();
        for cct in self.per_request_type.values() {
            for (tracepoint, _) in &cct.entry_points {
                result.insert(tracepoint);
            }
        }
        result.drain().collect()
    }

    pub fn search<'a>(&'a self, group: &Group, edge: EdgeIndex) -> Vec<&'a String> {
        self.per_request_type
            .get(&group.request_type)
            .unwrap()
            .search(group, edge)
    }
}

impl Display for Manifest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Manifest:").unwrap();
        for (request_type, cct) in &self.per_request_type {
            write!(f, "{:?} dot:\n{}", request_type, Dot::new(&cct.g)).unwrap();
        }
        Ok(())
    }
}

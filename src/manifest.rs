use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Display;
use std::path::Path;

use petgraph::graph::EdgeIndex;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use grouping::Group;
use osprofiler::OSProfilerDAG;
use osprofiler::RequestType;

pub trait SearchSpace {
    fn new() -> Self;
    fn add_trace(&mut self, &OSProfilerDAG);
    fn get_entry_points<'a>(&'a self) -> Vec<&'a String>;
    fn search<'a>(&'a self, &Group, EdgeIndex) -> Vec<&'a String>;
}

#[derive(Serialize, Deserialize)]
pub struct Manifest<M: SearchSpace> {
    pub per_request_type: HashMap<RequestType, M>,
}

impl<M> Manifest<M>
where
    M: SearchSpace,
    M: Serialize,
    M: DeserializeOwned,
{
    pub fn from_trace_list(traces: Vec<OSProfilerDAG>) -> Manifest<M> {
        let mut map = HashMap::<RequestType, M>::new();
        for trace in traces {
            match map.get_mut(&trace.request_type.unwrap()) {
                Some(cct) => {
                    cct.add_trace(&trace);
                }
                None => {
                    let mut cct = M::new();
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

    pub fn from_file(file: &Path) -> Option<Manifest<M>> {
        let reader = match std::fs::File::open(file) {
            Ok(x) => x,
            Err(_) => return None,
        };
        Some(serde_json::from_reader(reader).unwrap())
    }

    pub fn entry_points(&self) -> Vec<&String> {
        let mut result = HashSet::new();
        for cct in self.per_request_type.values() {
            for tracepoint in cct.get_entry_points() {
                result.insert(tracepoint);
            }
        }
        result.drain().collect()
    }

    pub fn search<'a>(
        &'a self,
        group: &Group,
        edge: EdgeIndex,
    ) -> Vec<(&'a String, Option<RequestType>)> {
        self.per_request_type
            .get(&group.request_type)
            .unwrap()
            .search(group, edge)
            .iter()
            .map(|&a| (a, Some(group.request_type)))
            .collect()
    }
}

impl<M> Display for Manifest<M>
where
    M: Display,
    M: SearchSpace,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Manifest:").unwrap();
        for (request_type, inner) in &self.per_request_type {
            write!(f, "{:?} manifest:\n{}", request_type, inner).unwrap();
        }
        Ok(())
    }
}

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Display;
use std::path::Path;

use petgraph::visit::IntoNodeReferences;
use petgraph::visit::NodeRef;
use serde::{Deserialize, Serialize};

use crate::osprofiler::REQUEST_TYPE_REGEXES;
use crate::searchspace::SearchSpace;
use crate::trace::RequestType;
use crate::trace::Trace;
use crate::trace::TracepointID;

#[derive(Serialize, Deserialize, Debug)]
pub struct Manifest {
    pub per_request_type: HashMap<RequestType, SearchSpace>,
    request_type_tracepoints: Vec<TracepointID>,
}

impl Manifest {
    pub fn new() -> Manifest {
        Manifest {
            per_request_type: HashMap::new(),
            request_type_tracepoints: Vec::new(),
        }
    }

    pub fn from_trace_list(traces: &Vec<Trace>) -> Manifest {
        let mut map = HashMap::<RequestType, SearchSpace>::new();
        for trace in traces {
            match map.get_mut(&trace.request_type) {
                Some(space) => {
                    space.add_trace(&trace);
                }
                None => {
                    let mut space = SearchSpace::default();
                    space.add_trace(&trace);
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

    fn ingest_dir(&mut self, file: &Path) -> std::io::Result<()> {
        for entry in std::fs::read_dir(file)? {
            let entry = entry?;
            let path = entry.path();
            let request_type = RequestType::from_str(path.file_stem().unwrap().to_str().unwrap())
                .expect("Couldn't parse request type");
            let reader = std::fs::File::open(path)?;
            self.per_request_type
                .insert(request_type, serde_json::from_reader(reader).unwrap());
        }
        Ok(())
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

    // pub fn search<'a>(
    //     &'a self,
    //     group: &Group,
    //     edge: EdgeIndex,
    //     budget: usize,
    // ) -> (Vec<(&'a String, Option<RequestType>)>, SearchState) {
    //     let (tracepoints, state) = self.strategy.search(
    //         self.per_request_type.get(&group.request_type).unwrap(),
    //         group,
    //         edge,
    //         budget,
    //     );
    //     (
    //         tracepoints
    //             .iter()
    //             .map(|&a| (a, Some(group.request_type)))
    //             .collect(),
    //         state,
    //     )
    // }
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

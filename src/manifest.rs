use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Display;
use std::path::Path;
use std::path::PathBuf;

use petgraph::graph::EdgeIndex;
// use serde::Serialize;

use crate::cct::CCT;
use crate::grouping::Group;
use crate::historic::Historic;
use crate::osprofiler::OSProfilerDAG;
use crate::osprofiler::RequestType;
use crate::poset::Poset;
use crate::searchspace::SearchSpace;

pub struct Manifest {
    pub per_request_type: HashMap<RequestType, Box<dyn SearchSpace>>,
    manifest_type: String,
}

impl Manifest {
    pub fn from_trace_list(manifest_type: &str, traces: Vec<OSProfilerDAG>) -> Manifest {
        let mut map = HashMap::<RequestType, Box<dyn SearchSpace>>::new();
        for trace in traces {
            match map.get_mut(&trace.request_type.unwrap()) {
                Some(cct) => {
                    cct.add_trace(&trace);
                }
                None => {
                    let mut cct = Self::get_new_inner(manifest_type);
                    cct.add_trace(&trace);
                    map.insert(trace.request_type.unwrap(), cct);
                }
            }
        }
        Manifest {
            per_request_type: map,
            manifest_type: String::from(manifest_type),
        }
    }

    fn get_new_inner(manifest_type: &str) -> Box<dyn SearchSpace> {
        match manifest_type {
            "CCT" => Box::new(CCT::default()),
            "Poset" => Box::new(Poset::default()),
            "Historic" => Box::new(Historic::default()),
            _ => panic!("Unsupported manifest method"),
        }
    }

    pub fn to_file(&self, file: &Path) {
        for (request_type, inner) in self.per_request_type.iter() {
            let mut newfile = PathBuf::from(file);
            newfile.push(request_type.to_string());
            newfile.set_extension("json");
            let writer = std::fs::File::create(newfile).unwrap();
            serde_json::to_writer(writer, inner.as_ref()).expect("Failed to manifest to cache");
        }
    }

    pub fn from_file(manifest_type: &str, file: &Path) -> Option<Manifest> {
        None
        // let reader = match std::fs::File::open(file) {
        //     Ok(x) => x,
        //     Err(_) => return None,
        // };
        // Some(serde_json::from_reader(reader).unwrap())
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

impl Display for Manifest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Manifest:").unwrap();
        for (request_type, inner) in &self.per_request_type {
            write!(f, "{:?} manifest:\n{}", request_type, inner).unwrap();
        }
        Ok(())
    }
}

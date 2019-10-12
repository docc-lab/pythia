use std::fmt;
use std::fmt::Display;
use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use cct::CCT;
use osprofiler::RequestType;
use osprofiler::OSProfilerDAG;

#[derive(Serialize, Deserialize)]
pub struct Manifest {
    per_request_type: HashMap<RequestType,CCT>
}

impl Manifest {
    pub fn from_trace_list(traces: Vec<OSProfilerDAG>) -> Manifest {
        let mut map = HashMap::<RequestType, CCT>::new();
        Manifest {per_request_type: map}
    }

    pub fn to_file(&self, file: &Path) {
        let writer = std::fs::File::create(file).unwrap();
        serde_json::to_writer(writer, self).expect("Failed to manifest to cache");
    }
}

impl Display for Manifest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Manifest")
    }
}

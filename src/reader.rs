use std::collections::HashMap;

use crate::hdfs::HDFSReader;
use crate::osprofiler::OSProfilerReader;
use crate::trace::Trace;

pub trait Reader {
    fn read_file(&mut self, filename: &str) -> Trace;
    fn get_trace_from_base_id(&mut self, id: &str) -> Option<Trace>;

    fn read_trace_file(&mut self, tracefile: &str) -> Vec<Trace> {
        let trace_ids = std::fs::read_to_string(tracefile).unwrap();
        let mut traces = Vec::new();
        for id in trace_ids.split('\n') {
            if id.len() <= 1 {
                continue;
            }
            println!("Working on {:?}", id);
            let trace = self.get_trace_from_base_id(id).unwrap();
            traces.push(trace);
        }
        traces
    }
}

pub fn reader_from_settings(settings: &HashMap<String, String>) -> Box<dyn Reader> {
    match settings.get("application") {
        Some(s) => {
            if s == "OpenStack" {
                return Box::new(OSProfilerReader::from_settings(settings));
            } else if s == "HDFS" {
                return Box::new(HDFSReader::from_settings(settings));
            }
        }
        None => {}
    }
    panic!("Please choose application in Settings.toml")
}

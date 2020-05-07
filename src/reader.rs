use pythia_common::ApplicationType;
use pythia_common::Settings;

use crate::hdfs::HDFSReader;
use crate::osprofiler::OSProfilerReader;
use crate::trace::Trace;
use crate::uber::UberReader;

pub trait Reader {
    fn read_file(&mut self, filename: &str) -> Trace;
    fn get_trace_from_base_id(&mut self, id: &str) -> Option<Trace>;
    fn get_recent_traces(&mut self) -> Vec<Trace>;
    fn read_dir(&mut self, foldername: &str) -> Vec<Trace>;

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

pub fn reader_from_settings(settings: &Settings) -> Box<dyn Reader> {
    match &settings.application {
        ApplicationType::OpenStack => Box::new(OSProfilerReader::from_settings(settings)),
        ApplicationType::HDFS => Box::new(HDFSReader::from_settings(settings)),
        ApplicationType::Uber => Box::new(UberReader::from_settings(settings)),
    }
}

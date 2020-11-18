//! Controller has an API for sending control signals. OSProfilerController sends the orders to
//! agents while HDFSController writes the control signals to a local file. TestController does nothing.

mod hdfs;
mod osprofiler;

use pythia_common::RequestType;

use crate::controller::hdfs::HDFSController;
use crate::controller::osprofiler::OSProfilerController;
use crate::settings::ApplicationType;
use crate::settings::Settings;
use crate::trace::TracepointID;


use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use pythia_common::RequestType;


pub trait Controller: Send + Sync {
    fn enable(&self, points: &Vec<(TracepointID, Option<RequestType>)>);
    fn disable(&self, points: &Vec<(TracepointID, Option<RequestType>)>);
    fn is_enabled(&self, point: &(TracepointID, Option<RequestType>)) -> bool;
    fn disable_all(&self);
    fn enable_all(&self);
    fn enabled_tracepoints(&self) -> Vec<(TracepointID, Option<RequestType>)>;

    fn disable_by_name(&self, point: &str) {
        self.disable(&vec![(TracepointID::from_str(point), None)]);
    }
}

pub fn controller_from_settings(settings: &Settings) -> Box<dyn Controller> {
    match &settings.application {
        ApplicationType::OpenStack => Box::new(OSProfilerController::from_settings(settings)),
        ApplicationType::HDFS => Box::new(HDFSController::from_settings(settings)),
        ApplicationType::Uber => panic!("Can't control uber"),
    }
}

pub struct TestController {

enabled_tracepoints: Arc<Mutex<HashSet<(TracepointID, Option<RequestType>)>>>,

}

impl TestController {
    pub fn new() -> Self {
        Self{enabled_tracepoints: Arc::new(Mutex::new(HashSet::new())),}
        
    }
}

impl Controller for TestController {
    fn enable(&self, _: &Vec<(TracepointID, Option<RequestType>)>) {}
    fn disable(&self, _: &Vec<(TracepointID, Option<RequestType>)>) {}
    fn is_enabled(&self, _: &(TracepointID, Option<RequestType>)) -> bool {
        false
    }
    fn disable_all(&self) {}
    fn enable_all(&self) {}
    fn enabled_tracepoints(&self) -> Vec<(TracepointID, Option<RequestType>)> {
        self.enabled_tracepoints
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect()
    }
}

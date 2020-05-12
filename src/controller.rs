use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use pythia_common::RequestType;

use crate::rpclib::set_all_client_tracepoints;
use crate::rpclib::set_client_tracepoints;
use crate::settings::Settings;
use crate::trace::TracepointID;

pub struct OSProfilerController {
    client_list: Vec<String>,

    enabled_tracepoints: Arc<Mutex<HashSet<(TracepointID, Option<RequestType>)>>>,
    // This should only be valid after disable_all is called
}

impl OSProfilerController {
    pub fn from_settings(settings: &Settings) -> OSProfilerController {
        OSProfilerController {
            client_list: settings.pythia_clients.clone(),
            enabled_tracepoints: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub fn enable(&self, points: &Vec<(TracepointID, Option<RequestType>)>) {
        let mut enabled_tracepoints = self.enabled_tracepoints.lock().unwrap();
        for p in points {
            if p.1 == Some(RequestType::Unknown) {
                enabled_tracepoints.insert((p.0, None));
            } else {
                enabled_tracepoints.insert(p.clone());
            }
        }
        self.write_to_tracepoints(points, b"1");
    }

    pub fn disable(&self, points: &Vec<(TracepointID, Option<RequestType>)>) {
        let mut enabled_tracepoints = self.enabled_tracepoints.lock().unwrap();
        for p in points {
            if p.1 == Some(RequestType::Unknown) {
                enabled_tracepoints.remove(&(p.0, None));
            } else {
                enabled_tracepoints.remove(p);
            }
        }
        self.write_to_tracepoints(points, b"0");
    }

    pub fn disable_by_name(&self, point: &str) {
        self.disable(&vec![(TracepointID::from_str(point), None)]);
    }

    pub fn is_enabled(&self, point: &(TracepointID, Option<RequestType>)) -> bool {
        let enabled_tracepoints = self.enabled_tracepoints.lock().unwrap();
        // A tracepoint is enabled either globally or for a request type
        !enabled_tracepoints.get(point).is_none()
            && !enabled_tracepoints.get(&(point.0, None)).is_none()
    }

    fn write_to_tracepoints(
        &self,
        points: &Vec<(TracepointID, Option<RequestType>)>,
        to_write: &[u8; 1],
    ) {
        for client in self.client_list.iter() {
            set_client_tracepoints(
                client,
                points
                    .iter()
                    .map(|(x, y)| ((*x).clone(), y.clone(), to_write.clone()))
                    .collect(),
            );
        }
    }

    fn set_all_tracepoints(&self, to_write: &[u8; 1]) {
        for client in self.client_list.iter() {
            set_all_client_tracepoints(client, *to_write);
        }
    }

    /// Also removes request-type-specific controllers
    pub fn disable_all(&self) {
        self.set_all_tracepoints(b"0");
    }

    /// Also removes request-type-specific controllers
    pub fn enable_all(&self) {
        self.set_all_tracepoints(b"1");
    }
}

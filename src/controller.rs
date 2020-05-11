use std::collections::HashSet;

use pythia_common::RequestType;

use crate::rpclib::set_all_client_tracepoints;
use crate::rpclib::set_client_tracepoints;
use crate::settings::Settings;
use crate::trace::TracepointID;

pub struct OSProfilerController {
    client_list: Vec<String>,

    enabled_tracepoints: HashSet<(TracepointID, RequestType)>,
    // This should only be valid after disable_all is called
}

impl OSProfilerController {
    pub fn from_settings(settings: &Settings) -> OSProfilerController {
        OSProfilerController {
            client_list: settings.pythia_clients.clone(),
            enabled_tracepoints: HashSet::new(),
        }
    }

    pub fn enable(&mut self, points: &Vec<(TracepointID, Option<RequestType>)>) {
        for p in points {
            if p.1.is_none() {
                eprintln!("Someone enabled a tracepoint without request type, tracking does not work anymore");
                continue;
            }
            self.enabled_tracepoints.insert((p.0, p.1.unwrap()));
        }
        self.write_to_tracepoints(points, b"1");
    }

    pub fn disable(&mut self, points: &Vec<(TracepointID, Option<RequestType>)>) {
        for p in points {
            if p.1.is_none() {
                eprintln!("Someone disabled a tracepoint without request type, tracking does not work anymore");
                continue;
            }
            self.enabled_tracepoints.remove(&(p.0, p.1.unwrap()));
        }
        self.write_to_tracepoints(points, b"0");
    }

    pub fn disable_by_name(&self, point: &str) {
        eprintln!("Someone disabled a tracepoint by name, tracking does not work anymore");
        self.write_to_tracepoints(&vec![(TracepointID::from_str(point), None)], b"0");
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
    pub fn diable_all(&self) {
        self.set_all_tracepoints(b"0");
    }

    /// Also removes request-type-specific controllers
    pub fn enable_all(&self) {
        self.set_all_tracepoints(b"1");
    }
}

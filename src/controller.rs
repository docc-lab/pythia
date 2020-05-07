use std::fs::File;
use std::io::prelude::*;
use std::path::PathBuf;

use pythia_common::RequestType;
use pythia_common::Settings;

use crate::rpclib::set_all_client_tracepoints;
use crate::rpclib::set_client_tracepoints;
use crate::trace::TracepointID;

pub struct OSProfilerController {
    manifest_root: PathBuf,
    client_list: Vec<String>,
}

impl OSProfilerController {
    pub fn from_settings(settings: &Settings) -> OSProfilerController {
        OSProfilerController {
            manifest_root: settings.manifest_root.clone(),
            client_list: settings.pythia_clients.clone(),
        }
    }

    pub fn get_disabled(
        &self,
        points: &Vec<(TracepointID, Option<RequestType>)>,
    ) -> Vec<(TracepointID, Option<RequestType>)> {
        let mut result = Vec::new();
        for point in points {
            if self.is_disabled(point) {
                result.push((point.0, point.1));
            }
        }
        result
    }

    fn is_disabled(&self, point: &(TracepointID, Option<RequestType>)) -> bool {
        !self.read_tracepoint(point.0, &point.1)
    }

    pub fn enable(&self, points: &Vec<(TracepointID, Option<RequestType>)>) {
        self.write_to_tracepoints(points, b"1");
    }

    pub fn disable_by_name(&self, point: &str) {
        self.write_to_tracepoint(point, &None, b"0");
    }

    pub fn disable(&self, points: &Vec<(TracepointID, Option<RequestType>)>) {
        self.write_to_tracepoints(points, b"0");
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

    fn read_tracepoint(
        &self,
        tracepoint: TracepointID,
        request_type: &Option<RequestType>,
    ) -> bool {
        let contents = match std::fs::read_to_string(
            self.get_path(&tracepoint.to_string(), request_type),
        )
        .ok()
        {
            Some(x) => x,
            None => return false,
        };
        contents.parse::<i32>().unwrap() == 1
    }

    fn write_to_tracepoint(
        &self,
        tracepoint: &str,
        request_type: &Option<RequestType>,
        to_write: &[u8; 1],
    ) {
        let path = self.get_path(tracepoint, request_type);
        match File::create(&path) {
            Ok(mut f) => {
                f.write_all(to_write).unwrap();
            }
            Err(e) => eprintln!("Problem creating file {:?}: {}", path, e),
        }
    }

    fn get_path(&self, tracepoint: &str, request_type: &Option<RequestType>) -> PathBuf {
        let mut result = self.manifest_root.clone();
        if tracepoint.chars().nth(0).unwrap() == '/' {
            result.push(&tracepoint[1..]);
        } else {
            result.push(tracepoint);
        }
        match request_type {
            Some(t) => {
                let mut newname = result.file_name().unwrap().to_os_string();
                newname.push(":");
                newname.push(t.to_string());
                result.set_file_name(newname);
            }
            None => {}
        }
        result
    }
}

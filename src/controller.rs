use std::collections::HashMap;
use std::fs::{read_dir, File};
use std::io::prelude::*;
use std::path::{Path, PathBuf};

use crate::trace::RequestType;
use crate::rpclib::set_all_client_tracepoints;
use crate::rpclib::set_client_tracepoints;

pub struct OSProfilerController {
    manifest_root: PathBuf,
}

impl OSProfilerController {
    pub fn from_settings(settings: &HashMap<String, String>) -> OSProfilerController {
        OSProfilerController {
            manifest_root: PathBuf::from(settings.get("manifest_root").unwrap()),
        }
    }

    pub fn get_disabled<'a>(
        &self,
        points: &Vec<(&'a String, Option<RequestType>)>,
    ) -> Vec<(&'a String, Option<RequestType>)> {
        let mut result = Vec::new();
        for point in points {
            if self.is_disabled(point) {
                result.push((point.0, point.1));
            }
        }
        result
    }

    fn is_disabled(&self, point: &(&String, Option<RequestType>)) -> bool {
        !self.read_tracepoint(point.0, &point.1)
    }

    pub fn enable(&self, points: &Vec<(&String, Option<RequestType>)>) {
        self.write_to_tracepoints(points, b"1");
    }

    pub fn disable(&self, points: &Vec<(&String, Option<RequestType>)>) {
        self.write_to_tracepoints(points, b"0");
    }

    fn write_to_tracepoints(
        &self,
        points: &Vec<(&String, Option<RequestType>)>,
        to_write: &[u8; 1],
    ) {
        for client in vec!["http://cp-1:3030"] {
            set_client_tracepoints(
                client,
                points
                    .iter()
                    .map(|(x, y)| ((*x).clone(), y.clone(), to_write.clone()))
                    .collect(),
            );
        }
        for (tracepoint, request_type) in points {
            self.write_to_tracepoint(tracepoint, request_type, to_write);
        }
    }

    fn set_all_tracepoints(&self, to_write: &[u8; 1]) {
        self.write_dir(self.manifest_root.as_path(), to_write);
        for client in vec!["http://cp-1:3030"] {
            set_all_client_tracepoints(client, *to_write);
        }
    }

    pub fn write_client_dir(&self, to_write: &[u8; 1]) {
        self.write_dir(self.manifest_root.as_path(), to_write);
    }

    /// Also removes request-type-specific controllers
    pub fn diable_all(&self) {
        self.set_all_tracepoints(b"0");
    }

    /// Also removes request-type-specific controllers
    pub fn enable_all(&self) {
        self.set_all_tracepoints(b"1");
    }

    pub fn apply_settings(&self, settings: Vec<(String, Option<RequestType>, [u8; 1])>) {
        for (tracepoint, request_type, to_write) in settings.iter() {
            self.write_to_tracepoint(tracepoint, request_type, to_write);
        }
    }

    fn write_dir(&self, dir: &Path, to_write: &[u8; 1]) {
        for f in read_dir(dir).unwrap() {
            let path = f.unwrap().path();
            if path.is_dir() {
                self.write_dir(&path, to_write);
            } else {
                if RequestType::from_str(
                    path.file_name()
                        .unwrap()
                        .to_string_lossy()
                        .rsplit(":")
                        .next()
                        .unwrap(),
                )
                .is_ok()
                {
                    std::fs::remove_file(path).ok();
                } else {
                    let mut file = File::create(path).unwrap();
                    file.write_all(to_write).unwrap();
                }
            }
        }
    }

    fn read_tracepoint(&self, tracepoint: &String, request_type: &Option<RequestType>) -> bool {
        let contents = match std::fs::read_to_string(self.get_path(tracepoint, request_type)).ok() {
            Some(x) => x,
            None => return false,
        };
        contents.parse::<i32>().unwrap() == 1
    }

    fn write_to_tracepoint(
        &self,
        tracepoint: &String,
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

    fn get_path(&self, tracepoint: &String, request_type: &Option<RequestType>) -> PathBuf {
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

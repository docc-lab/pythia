use std::collections::HashMap;
use std::fs::{read_dir, File};
use std::io::prelude::*;
use std::path::{Path, PathBuf};

use crate::osprofiler::RequestType;

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
        for (tracepoint, request_type) in points {
            self.write_to_tracepoint(tracepoint, request_type, b"1");
        }
    }

    pub fn disable(&self, points: &Vec<(&String, Option<RequestType>)>) {
        for (tracepoint, request_type) in points {
            self.write_to_tracepoint(tracepoint, request_type, b"0");
        }
    }

    pub fn diable_all(&self) {
        self.write_dir(self.manifest_root.as_path(), b"0");
    }

    pub fn enable_all(&self) {
        self.write_dir(self.manifest_root.as_path(), b"1");
    }

    fn write_dir(&self, dir: &Path, to_write: &[u8]) {
        for f in read_dir(dir).unwrap() {
            let path = f.unwrap().path();
            if path.is_dir() {
                self.write_dir(&path, to_write);
            } else {
                let mut file = File::create(path).unwrap();
                file.write_all(to_write).unwrap();
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
        to_write: &[u8],
    ) {
        let mut file = File::create(self.get_path(tracepoint, request_type)).unwrap();
        file.write_all(to_write).unwrap();
    }

    fn get_path(&self, tracepoint: &String, request_type: &Option<RequestType>) -> PathBuf {
        let mut result = self.manifest_root.clone();
        if tracepoint.chars().nth(0).unwrap() == '/' {
            result.push(&tracepoint[1..]);
        } else {
            result.push(tracepoint);
        }
        match request_type {
            Some(t) => result.push(":".to_owned() + &t.to_string()),
            None => {}
        }
        result
    }
}

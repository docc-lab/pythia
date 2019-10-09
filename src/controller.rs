use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::fs::{File, read_dir};
use std::collections::HashMap;

pub struct OSProfilerController {
    manifest_root: PathBuf
}

impl OSProfilerController {
    pub fn from_settings(settings: &HashMap<String,String>) -> OSProfilerController {
        OSProfilerController{
            manifest_root: PathBuf::from(settings.get("manifest_root").unwrap())
        }
    }

    pub fn enable(&self, points: Vec<String>) {
        for tracepoint in points {
            self.write_to_tracepoint(tracepoint, b"1");
        }
    }

    pub fn disable(&self, points: Vec<String>) {
        for tracepoint in points {
            self.write_to_tracepoint(tracepoint, b"0");
        }
    }

    pub fn diable_all(&self) {
        fn disable_dir(dir: &Path) {
            for f in read_dir(dir).unwrap() {
                let path = f.unwrap().path();
                if path.is_dir() {
                    disable_dir(&path);
                } else {
                    let mut file = File::create(path).unwrap();
                    file.write_all(b"0").unwrap();
                }
            }
        }
        disable_dir(self.manifest_root.as_path());
    }

    fn write_to_tracepoint(&self, tracepoint: String, to_write: &[u8]) {
        let mut file = File::create(self.get_path(tracepoint)).unwrap();
        file.write_all(to_write).unwrap();
    }

    fn get_path(&self, tracepoint: String) -> PathBuf {
        let mut result = self.manifest_root.clone();
        if tracepoint.chars().nth(0).unwrap() == '/' {
            result.push(&tracepoint[1..]);
        } else {
            result.push(tracepoint);
        }
        result
    }
}

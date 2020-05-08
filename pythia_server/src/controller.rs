use std::fs::{read_dir, File};
use std::io::prelude::*;
use std::path::{Path, PathBuf};

use pythia_common::RequestType;

use crate::settings::Settings;

pub struct OSProfilerController {
    manifest_root: PathBuf,
}

impl OSProfilerController {
    pub fn from_settings(settings: &Settings) -> OSProfilerController {
        OSProfilerController {
            manifest_root: settings.manifest_root.clone(),
        }
    }

    pub fn write_client_dir(&self, to_write: &[u8; 1]) {
        self.write_dir(self.manifest_root.as_path(), to_write);
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

//! This module contains a Reader trait, which reads traces.

mod hdfs;
mod osprofiler;
mod uber;

use std::error::Error;
use std::fmt;

use hex;
use itertools::Itertools;
use serde::de;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::reader::hdfs::HDFSReader;
use crate::reader::osprofiler::OSProfilerReader;
use crate::reader::uber::UberReader;
use crate::settings::ApplicationType;
use crate::settings::Settings;
use crate::trace::Trace;

pub trait Reader {
    /// The file can contain a trace json, written by serde or by the tracing
    /// infrastructure
    fn read_file(&mut self, filename: &str) -> Trace;
    /// The folder contains files that may include trace jsons, written by serde
    /// or by the tracing infrastructure
    fn read_dir(&mut self, foldername: &str) -> Vec<Trace>;
    fn get_trace_from_base_id(&mut self, id: &str) -> Result<Trace, Box<dyn Error>>;

    /// This function collects new traces that have finished.
    ///
    /// It is called multiple times for OpenStack, which collects traces in the first
    /// call and returns traces whose duration did not change in the second call.
    fn get_recent_traces(&mut self) -> Vec<Trace>;

    /// Used before get_recent_traces, so we know what is *recent*.
    fn reset_state(&mut self);
    /// Some readers have different behavior when reading traces for the search
    /// space (e.g., pruning normally vs. not pruning for search space). Calling
    /// this function indicates this Reader will be used for search space
    fn for_searchspace(&mut self);

    /// Read a file with one request ID per line
    fn read_trace_file(&mut self, tracefile: &str) -> Vec<Trace> {
        let trace_ids = std::fs::read_to_string(tracefile).unwrap();
        let mut traces = Vec::new();
        for id in trace_ids.split('\n') {
            if id.len() <= 1 {
                continue;
            }
            println!("Working on {:?}", id);
            match self.get_trace_from_base_id(id) {
                Ok(t) => {
                    traces.push(t);
                }
                Err(e) => {
                    eprintln!("Failed with {:?}", e);
                }
            }
        }
        traces
    }
}

/// Constructor for Reader
pub fn reader_from_settings(settings: &Settings) -> Box<dyn Reader> {
    match &settings.application {
        ApplicationType::OpenStack => Box::new(OSProfilerReader::from_settings(settings)),
        ApplicationType::HDFS => Box::new(HDFSReader::from_settings(settings)),
        ApplicationType::Uber => Box::new(UberReader::from_settings(settings)),
    }
}

#[derive(Serialize, Debug, Clone, Eq, PartialEq, Copy, Hash)]
pub struct HexID {
    id: Option<[u8; 8]>,
}

impl HexID {
    pub fn to_uuid(&self) -> Uuid {
        let mut buf: [u8; 16] = [0; 16];
        match self.id {
            Some(bytes) => {
                buf[..8].copy_from_slice(&bytes);
            }
            None => {}
        }
        Uuid::from_bytes(buf)
    }

    pub fn to_string(&self) -> String {
        format!("{:02x}", self.id.unwrap().iter().format(""))
    }

    pub fn from_str(id: &str) -> Self {
        let mut buf: [u8; 8] = [0; 8];
        let decoded = hex::decode(id).unwrap();
        for i in 0..8 {
            buf[i] = decoded[i];
        }
        HexID { id: Some(buf) }
    }
}

impl<'de> Deserialize<'de> for HexID {
    fn deserialize<D>(deserializer: D) -> Result<HexID, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_str(HexIDVisitor)
    }
}

struct HexIDVisitor;

impl<'de> de::Visitor<'de> for HexIDVisitor {
    type Value = HexID;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string representing HexID")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value == "0" {
            return Ok(HexID { id: None });
        }
        let decoded = hex::decode(value).unwrap();
        let mut result = [0; 8];
        let decoded = &decoded[..result.len()];
        result.copy_from_slice(decoded);
        Ok(HexID { id: Some(result) })
    }
}

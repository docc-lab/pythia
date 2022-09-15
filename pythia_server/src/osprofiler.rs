/*
This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.

Copyright (c) 2022, Diagnosis and Control of Clouds Laboratory
All rights reserved.
*/

//! Stuff related to reading data from osprofiler
//!
use redis::Commands;
use redis::Connection;
use redis::FromRedisValue;
use redis::Value;
use uuid::Uuid;

//use pythia_common::OSProfilerEnum;
use pythia_common::osprofiler;
use pythia_common::OSProfilerSpan;
//mod pythia_common::osprofiler;
use crate::settings::Settings;

pub struct OSProfilerReader {
    redis_url: String,
    connection: Connection,
}

impl OSProfilerReader {
    pub fn from_settings(settings: &Settings) -> OSProfilerReader {
        let redis_url = &settings.redis_url;
        let client = redis::Client::open(&redis_url[..]).unwrap();
        let con = client.get_connection().unwrap();
        OSProfilerReader {
            redis_url: settings.redis_url.clone(),
            connection: con,
        }
    }

    pub fn free_keys(&mut self, keys: Vec<String>) {
        self.connection.del::<_, ()>(keys).ok();
    }

    fn restart_connection(&mut self) {
        let client = redis::Client::open(&self.redis_url[..]).unwrap();
        self.connection = client.get_connection().unwrap();
    }

    pub fn get_stats(&mut self) -> (f32, u32) {
        eprintln!("MERT get stats");
        let info = redis::cmd("INFO")
            .query::<redis::InfoDict>(&mut self.connection)
            .unwrap();
        (
            info.get("instantaneous_input_kbps").unwrap(),
            info.get("used_memory_dataset").unwrap(),
        )
    }

    /// Public wrapper for get_matches_ that accepts string input and does not return RedisResult
    pub fn get_matches(&mut self, span_id: &str) -> Vec<OSProfilerSpan> {
        match Uuid::parse_str(span_id) {
            Ok(uuid) => self.get_matches_(&uuid).unwrap(),
            Err(_) => panic!("Malformed UUID as base id: {}", span_id),
        }
    }

    /// Get matching events from local redis instance
    fn get_matches_(&mut self, span_id: &Uuid) -> redis::RedisResult<Vec<OSProfilerSpan>> {
        let mut trials = 0;
        let mut to_parse: Option<String> = None;
        while to_parse.is_none() && trials < 2 {
            to_parse = match self
                .connection
                .get("osprofiler:".to_string() + &span_id.to_hyphenated().to_string())
            {
                Ok(to_parse) => match &to_parse {
                    Value::Nil => {
                        return Ok(Vec::new());
                    }
                    Value::Data(_) => Some(FromRedisValue::from_redis_value(&to_parse).unwrap()),
                    _ => {
                        eprintln!("Got {:?} as reply", to_parse);
                        return Ok(Vec::new());
                    }
                },
                Err(e) => {
                    self.restart_connection();
                    eprintln!("Got error {} for {}", e, span_id);
                    None
                }
            };
            trials += 1;
        }
        let mut result = Vec::new();
        let to_parse = to_parse.unwrap();
        for dict_string in to_parse[1..to_parse.len() - 1].split("}{") {
            match osprofiler::parse_field(&("{".to_string() + dict_string + "}")) {
                Ok(span) => {
                    result.push(span);
                }
                Err(e) => panic!("Problem while parsing {}: {}", dict_string, e),
            }
        }
        Ok(result)
    }
}

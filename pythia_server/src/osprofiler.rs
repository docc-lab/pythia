/// Stuff related to working with osprofiler
///
use redis::Commands;
use redis::Connection;
use redis::FromRedisValue;
use redis::Value;
use uuid::Uuid;


use pythia_common::OSProfilerEnum;
use pythia_common::OSProfilerSpan;

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
            match parse_field(&("{".to_string() + dict_string + "}")) {
                Ok(span) => {
                    result.push(span);
                }
                Err(e) => panic!("Problem while parsing {}: {}", dict_string, e),
            }
        }
        Ok(result)
    }
}

fn parse_field(field: &String) -> Result<OSProfilerSpan, String> {
    let result: OSProfilerSpan = match serde_json::from_str(field) {
        Ok(a) => a,
        Err(e) => {
            return Err(e.to_string());
        }
    };
    if result.name == "asynch_request" || result.name == "asynch_wait" {
        return match result.info {
            OSProfilerEnum::Annotation(_) => Ok(result),
            _ => {
                println!("{:?}", result);
                Err("".to_string())
            }
        };
    }
    Ok(result)
}
/*
#[cfg(test)]
mod tests {
/*use redis::Commands;
use redis::Connection;
use redis::FromRedisValue;
use redis::Value;
use uuid::Uuid;
use std::collections::HashMap;
use std::fmt; */

use chrono::naive::{NaiveDate, NaiveTime, NaiveDateTime};

use pythia_common::OSProfilerEnum;
use pythia_common::OSProfilerSpan;

use crate::settings::Settings;

    use super::*;

    #[test]
        fn test_parse() {
         let mut value = String::new();

        let d = NaiveDate::from_ymd(2015, 6, 3);
        let t = NaiveTime::from_hms_milli(12, 34, 56, 789);

       let dt = NaiveDateTime::new(d, t);
       let y:u64= 293402358;

    let current_info = OSProfilerEnum {value: y, tracepoint_id: "nova/usr/local".to_string(), host: "cloudlab".to_string(), thread_id: 5743728237, pid: 4771};
      let my_uuid = Uuid::parse_str("936DA01F9ABD4d9d80C702AF85C822A8").unwrap();
            let testStruct = OSProfilerSpan {trace_id: my_uuid, parent_id: my_uuid, project: "nova".to_string(), name:"build_instance".to_string(), base_id: my_uuid, service: "nova".to_string(), tracepoint_id: "nova/manager.py".to_string(), timestamp: dt, info: current_info};


            assert_eq!(parse_field({trace_id: my_uuid, parent_id: my_uuid, project: "nova", name:"build_instance", base_id: my_uuid, service: "nova", tracepoint_id: "nova/manager.py", timestamp: dt , info: {"value": y, tracepoint_id: "nova/usr/local", host: "cloudlab", thread_id: 5743728237, pid: 4771}}),Ok(testStruct));
    }
}
*/

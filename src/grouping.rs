use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;

use critical::CriticalPath;
use stats::variance;

#[derive(Clone)]
pub struct Group {
    hash: String,
    traces: Vec<CriticalPath>,
    pub variance: f64
}

impl Group {
    pub fn from_critical_paths(paths: Vec<CriticalPath>) -> Vec<Group> {
        let mut hash_map = HashMap::<String, Group>::new();
        for path in paths {
            match hash_map.get_mut(&path.hash()) {
                Some(v) => v.add_trace(path),
                None => {
                    hash_map.insert(path.hash().to_string(),
                        Group{hash: path.hash().to_string(),
                              traces:vec![path],
                              variance: 0.0
                        });
                },
            }
        }
        for (_, group) in hash_map.iter_mut() {
            group.calculate_variance();
        }
        hash_map.values().cloned().collect::<Vec<Group>>()
    }

    fn add_trace(&mut self, path: CriticalPath) {
        self.traces.push(path);
    }

    fn calculate_variance(&mut self) {
        self.variance = variance(self.traces.iter().map(|x| x.duration.as_nanos()));
        println!("Set variance of {} to {}", self.hash, self.variance);
    }
}

impl Display for Group {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Group<{}, {:?}>", self.traces.len(), self.hash)
    }
}

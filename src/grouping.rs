use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;

use critical::CriticalPath;

pub struct Group {
    traces: HashMap<String, Vec<CriticalPath>>
}

impl Group {
    pub fn from_critical_paths(paths: Vec<CriticalPath>) -> Group {
        let mut hash_map = HashMap::<String, Vec<CriticalPath>>::new();
        for path in paths {
            match hash_map.get_mut(&path.hash()) {
                Some(v) => v.push(path),
                None => {
                    hash_map.insert(path.hash().to_string(), vec![path]);
                },
            }
        }
        Group {
            traces: hash_map
        }
    }
}

impl Display for Group {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Group<{:?}>", self.traces.values().map(|v| v.len()).collect::<Vec<_>>())
    }
}

use critical::CriticalPath;

#[derive (Debug)]
pub struct Group {
}

impl Group {
    pub fn from_critical_paths(paths: Vec<CriticalPath>) -> Group {
        Group {}
    }
}

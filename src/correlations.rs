use std::collections::HashMap;
use std::collections::HashSet;

use indexmap::set::IndexSet;
use petgraph::visit::EdgeRef;

use crate::osprofiler::OSProfilerDAG;
use crate::osprofiler::OSProfilerEnum;

pub fn get_key_value_pairs(trace: &OSProfilerDAG) -> HashMap<String, String> {
    let mut result = HashMap::new();
    // Breadth-first search over all nodes
    let mut visited = HashSet::new();
    let mut to_visit = IndexSet::new();
    to_visit.insert(trace.start_node);
    while let Some(nidx) = to_visit.pop() {
        let source = &trace.g[nidx].span;
        result.extend(source.key_value_pairs().drain());
        for edge in trace.g.edges(nidx) {
            assert_eq!(nidx, edge.source());
            if visited.get(&edge.target()).is_none() {
                to_visit.insert(edge.target());
            }
        }
        visited.insert(nidx);
    }
    result
}

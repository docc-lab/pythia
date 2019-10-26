use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Display;
use std::time::Duration;

use indexmap::set::IndexSet;
use petgraph::graph::EdgeIndex;
use petgraph::visit::EdgeRef;
use serde::{Deserialize, Serialize};
use serde::{Deserializer, Serializer};

use crate::grouping::Group;
use crate::osprofiler::OSProfilerDAG;
use crate::poset::PosetNode;
use crate::searchspace::SearchSpace;

#[derive(Serialize, Deserialize)]
struct Edge {
    start: PosetNode,
    end: PosetNode,
    latencies: Vec<Duration>,
}

impl Edge {
    fn new(source: &PosetNode, target: &PosetNode, duration: Duration) -> Self {
        let mut latencies = Vec::new();
        latencies.push(duration);
        Edge {
            start: source.clone(),
            end: target.clone(),
            latencies: latencies,
        }
    }

    fn add_duration(&mut self, duration: Duration) {
        self.latencies.push(duration);
    }
}

fn serialize_historic<S: Serializer>(
    map: &HashMap<PosetNode, HashMap<PosetNode, usize>>,
    s: S,
) -> Result<S::Ok, S::Error> {
    map.iter()
        .map(|(a, b)| {
            (
                a.clone(),
                b.iter()
                    .map(|(x, y)| (x.clone(), y.clone()))
                    .collect::<Vec<(_, _)>>(),
            )
        })
        .collect::<Vec<(_, _)>>()
        .serialize(s)
}

fn deserialize_historic<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<HashMap<PosetNode, HashMap<PosetNode, usize>>, D::Error> {
    let vec = <Vec<(PosetNode, Vec<(PosetNode, usize)>)>>::deserialize(d)?;
    let mut map = HashMap::new();
    for (k, v) in vec {
        let mut inner = HashMap::new();
        for (x, y) in v {
            inner.insert(x, y);
        }
        map.insert(k, inner);
    }
    Ok(map)
}

#[derive(Serialize, Deserialize)]
pub struct Historic {
    edges: Vec<Edge>,
    #[serde(serialize_with = "serialize_historic")]
    #[serde(deserialize_with = "deserialize_historic")]
    edge_map: HashMap<PosetNode, HashMap<PosetNode, usize>>,
}

impl Display for Historic {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for edge in &self.edges {
            write!(
                f,
                "({} -> {}): {:?},\n",
                edge.start, edge.end, edge.latencies
            )?
        }
        Ok(())
    }
}

#[typetag::serde]
impl SearchSpace for Historic {
    fn add_trace(&mut self, trace: &OSProfilerDAG) {
        // Breadth-first search over all nodes, add outgoing edges to manifest
        let mut visited = HashSet::new();
        let mut to_visit = IndexSet::new();
        to_visit.insert(trace.start_node);
        while let Some(nidx) = to_visit.pop() {
            let source = PosetNode::from_event(&trace.g[nidx].span);
            let inner_map = self
                .edge_map
                .entry(source.clone())
                .or_insert(HashMap::new());
            for edge in trace.g.edges(nidx) {
                assert_eq!(nidx, edge.source());
                let target = PosetNode::from_event(&trace.g[edge.target()].span);
                match inner_map.get(&target) {
                    Some(&idx) => self.edges[idx].add_duration(edge.weight().duration),
                    None => {
                        self.edges
                            .push(Edge::new(&source, &target, edge.weight().duration));
                        inner_map.insert(target.clone(), self.edges.len() - 1);
                    }
                }
                if visited.get(&edge.target()).is_none() {
                    to_visit.insert(edge.target());
                }
            }
            visited.insert(nidx);
        }
    }

    fn get_entry_points(&self) -> Vec<&String> {
        Vec::new()
    }

    fn search(&self, _group: &Group, _edge: EdgeIndex, budget: usize) -> Vec<&String> {
        Vec::new()
    }
}

impl Default for Historic {
    fn default() -> Self {
        Historic {
            edges: Vec::new(),
            edge_map: HashMap::new(),
        }
    }
}

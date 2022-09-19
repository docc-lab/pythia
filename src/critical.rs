/*
This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.

Copyright (c) 2022, Diagnosis and Control of Clouds Laboratory
All rights reserved.
*/

//! Critical path-related stuff

use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;

use crypto::digest::Digest;
use crypto::sha2::Sha256;
use genawaiter::{rc::gen, yield_};
use petgraph::visit::EdgeRef;
use petgraph::{dot::Dot, graph::NodeIndex, Direction};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use pythia_common::RequestType;

use crate::trace::DAGEdge;
use crate::trace::EdgeType;
use crate::trace::Event;
use crate::trace::EventType;
use crate::trace::Trace;
use crate::trace::TracepointID;
use crate::PythiaError;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CriticalPath {
    /// This is the actual critical path
    pub g: Trace,
    pub start_node: NodeIndex,
    pub end_node: NodeIndex,
    pub duration: Duration,
    /// A hypothetical critical path is just a path which wasn't critical
    pub is_hypothetical: bool,
    pub request_type: RequestType,
    /// The hash is lazily calculated at first access
    hash: String,
}

impl CriticalPath {
    pub fn from_trace(dag: &Trace) -> Result<CriticalPath, Box<dyn Error>> {
        let mut path = CriticalPath {
            duration: Duration::new(0, 0),
            g: Trace::new(&dag.base_id),
            start_node: NodeIndex::end(),
            end_node: NodeIndex::end(),
            is_hypothetical: false,
            hash: "".to_string(),
            request_type: dag.request_type,
        };
        let mut cur_node = dag.end_node;
        let mut end_nidx = path.g.g.add_node(dag.g[cur_node].clone());
        path.end_node = end_nidx;
        loop {
            let next_node = match dag
                .g
                .neighbors_directed(cur_node, Direction::Incoming)
                .max_by_key(|&nidx| dag.g[nidx].timestamp)
            {
                Some(nidx) => nidx,
                None => {
                    return Err(Box::new(PythiaError(
                        format!("Disjoint trace {}", dag.base_id).into(),
                    )))
                }
            };
            let start_nidx = path.g.g.add_node(dag.g[next_node].clone());
            path.g.g.add_edge(
                start_nidx,
                end_nidx,
                dag.g[dag.g.find_edge(next_node, cur_node).unwrap()].clone(),
            );
            if next_node == dag.start_node {
                path.start_node = start_nidx;
                break;
            }
            cur_node = next_node;
            end_nidx = start_nidx;
        }
        path.add_synthetic_nodes(dag)?;
        path.duration = (path.g.g[path.end_node].timestamp - path.g.g[path.start_node].timestamp)
            .to_std()
            .unwrap();
        path.filter_incomplete_spans()?;
        path.calculate_hash();
        Ok(path)
    }

    pub fn count_possible_paths(dag: &Trace) -> u64 {
        let mut count = 0;
        let mut remaining_nodes = vec![dag.start_node];
        while !remaining_nodes.is_empty() {
            let mut cur_node = remaining_nodes.pop().unwrap();
            loop {
                let mut next_nodes: Vec<_> = dag
                    .g
                    .neighbors_directed(cur_node, Direction::Outgoing)
                    .collect();
                if next_nodes.len() == 0 {
                    break;
                }
                let next_node = next_nodes.pop().unwrap();
                for node in next_nodes {
                    remaining_nodes.push(node);
                }
                cur_node = next_node;
            }
            count += 1;
        }
        count
    }

    /// Lazily return each path separately. If we try to return `Vec<CriticalPath>`, we run out of
    /// memory for HDFS.
    pub fn all_possible_paths<'a>(dag: &'a Trace) -> impl Iterator<Item = CriticalPath> + 'a {
        gen!({
            let mut p = CriticalPath {
                g: Trace::new(&dag.base_id),
                start_node: NodeIndex::end(),
                end_node: NodeIndex::end(),
                duration: Duration::new(0, 0),
                is_hypothetical: true,
                hash: "".to_string(),
                request_type: dag.request_type,
            };
            let mut remaining_nodes = vec![(dag.start_node, dag.start_node, p.g.start_node, p)];
            while !remaining_nodes.is_empty() {
                let (mut prev_node, mut cur_node, mut cur_path_node, mut p) =
                    remaining_nodes.pop().unwrap();
                loop {
                    let next_nidx = p.g.g.add_node(dag.g[cur_node].clone());
                    p.end_node = next_nidx;
                    if cur_path_node == NodeIndex::end() {
                        p.start_node = next_nidx;
                    } else {
                        p.g.g.add_edge(
                            cur_path_node,
                            next_nidx,
                            dag.g[dag.g.find_edge(prev_node, cur_node).unwrap()].clone(),
                        );
                    }
                    let mut next_nodes: Vec<_> = dag
                        .g
                        .neighbors_directed(cur_node, Direction::Outgoing)
                        .collect();
                    if next_nodes.len() == 0 {
                        break;
                    }
                    let next_node = next_nodes.pop().unwrap();
                    for node in next_nodes {
                        remaining_nodes.push((cur_node, node, next_nidx, p.clone()));
                    }
                    cur_path_node = next_nidx;
                    prev_node = cur_node;
                    cur_node = next_node;
                }
                match p.add_synthetic_nodes(&dag) {
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("Path extraction failed with {:?}, skipping.", e);
                        continue;
                    }
                }
                match p.filter_incomplete_spans() {
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("Incomplete span filtering failed with {:?}, skipping.", e);
                        continue;
                    }
                }
                p.calculate_hash();
                yield_!(p);
            }
        })
        .into_iter()
    }

    /// Remove spans that have a start but no end and vice versa, and also extra nodes of spans
    /// that have multiple starts/endings.
    pub fn filter_incomplete_spans(&mut self) -> Result<(), Box<dyn Error>> {
        let mut cur_node = self.start_node;
        let mut span_map = HashMap::new();
        let mut nodes_to_remove = Vec::new();
        let mut exits = HashMap::<Uuid, NodeIndex>::new();
        loop {
            let cur_trace_id = self.g.g[cur_node].trace_id;
            let existing_node_id = span_map.get(&cur_trace_id);
            match self.g.g[cur_node].variant {
                EventType::Entry => match existing_node_id {
                    Some(_) => {
                        nodes_to_remove.push(cur_node.clone());
                        assert!(exits.get(&cur_trace_id).is_none());
                    }
                    None => {
                        span_map.insert(cur_trace_id.clone(), cur_node.clone());
                    }
                },
                EventType::Annotation => {}
                EventType::Exit => {
                    match existing_node_id {
                        Some(_) => {
                            span_map.remove(&cur_trace_id);
                        }
                        None => match exits.get(&cur_trace_id) {
                            Some(node) => {
                                nodes_to_remove.push(node.clone());
                            }
                            None => {
                                // println!(
                                //     "Current path with base_id {}:\n{}",
                                //     self.g.base_id,
                                //     Dot::new(&self.g.g)
                                // );
                                // println!("Node to remove: {:?}", cur_node);
                                return Err(Box::new(PythiaError(
                                    format!(
                                        "We shouldn't have any incomplete spans, in trace {}",
                                        self.g.base_id,
                                    )
                                    .into(),
                                )));
                            }
                        },
                    }
                    exits.insert(cur_trace_id.clone(), cur_node.clone());
                }
            }
            cur_node = match self.next_node(cur_node) {
                Some(nidx) => nidx,
                None => break,
            }
        }
        for nidx in nodes_to_remove {
            self.remove_node(nidx);
        }
        Ok(())
    }

    /// We add synthetic nodes for spans with exit nodes off the critical path
    /// e.g.,
    /// ```text
    /// A_start -> B_start -> C_start -> C_end -> ... rest of the path
    ///                   \-> D_start -> B_end -> A_end
    /// ```
    /// We add `B_end` and `A_end` (in that order) right before `C_start`
    fn add_synthetic_nodes(&mut self, dag: &Trace) -> Result<(), Box<dyn Error>> {
        let mut cur_nidx = self.start_node;
        let mut cur_dag_nidx = dag.start_node;
        let mut active_spans = Vec::new();
        loop {
            let cur_node = &self.g.g[cur_nidx];
            let cur_dag_node = &dag.g[cur_dag_nidx];
            assert!(cur_node.trace_id == cur_dag_node.trace_id);
            match cur_node.variant {
                EventType::Entry => {
                    active_spans.push(cur_dag_node.clone());
                }
                EventType::Annotation => {}
                EventType::Exit => {
                    match active_spans
                        .iter()
                        .rposition(|span| span.trace_id == cur_node.trace_id)
                    {
                        Some(idx) => {
                            active_spans.remove(idx);
                        }
                        None => {
                            self.add_synthetic_start_node(cur_nidx, cur_dag_nidx, dag)?;
                        }
                    };
                }
            }
            let next_nidx = match self.next_node(cur_nidx) {
                Some(nidx) => nidx,
                None => break,
            };
            let next_dag_nodes = dag
                .g
                .neighbors_directed(cur_dag_nidx, Direction::Outgoing)
                .collect::<Vec<_>>();
            if next_dag_nodes.len() == 1 {
                cur_dag_nidx = next_dag_nodes[0];
            } else {
                assert!(next_dag_nodes.len() != 0);
                let mut unfinished_spans = Vec::new();
                for next_dag_nidx in next_dag_nodes {
                    if dag.g[next_dag_nidx].trace_id == self.g.g[next_nidx].trace_id {
                        cur_dag_nidx = next_dag_nidx;
                    } else {
                        unfinished_spans.extend(self.get_unfinished(
                            &active_spans,
                            next_nidx,
                            next_dag_nidx,
                            dag,
                        ));
                    }
                }
                for span in unfinished_spans.iter().rev() {
                    self.add_node_after(cur_nidx, span);
                    cur_nidx = self.next_node(cur_nidx).unwrap();
                }
            }
            cur_nidx = next_nidx;
        }
        Ok(())
    }

    /// We encountered an end node for a span that did not start on our critical path
    /// We should go back, and add a corresponding synthetic start node after the correct
    /// synchronization point
    fn add_synthetic_start_node(
        &mut self,
        start_nidx: NodeIndex,
        start_dag_nidx: NodeIndex,
        dag: &Trace,
    ) -> Result<(), Box<dyn Error>> {
        let span_to_add = self.g.g[start_nidx].clone();
        // Find synch. point
        let mut cur_nidx = start_nidx;
        let mut cur_dag_nidx = start_dag_nidx;
        loop {
            assert!(dag.g[cur_dag_nidx].trace_id == self.g.g[cur_nidx].trace_id);
            let prev_dag_nodes = dag
                .g
                .neighbors_directed(cur_dag_nidx, Direction::Incoming)
                .collect::<Vec<_>>();
            let mut prev_nidx = cur_nidx;
            loop {
                prev_nidx = match self.prev_node(prev_nidx) {
                    Some(id) => id,
                    None => {
                        return Err(Box::new(PythiaError(
                            format!(
                                "Failed to find prev node in trace {}\n{}",
                                dag.base_id,
                                Dot::new(&dag.g)
                            )
                            .into(),
                        )));
                    }
                };
                if !self.g.g[prev_nidx].is_synthetic {
                    break;
                }
            }
            if prev_dag_nodes.len() == 1 {
                cur_dag_nidx = prev_dag_nodes[0];
            // We may have added other synthetic nodes to self, so iterate self
            // until we find matches in the dag
            } else {
                assert!(!prev_dag_nodes.is_empty());
                let mut found_start = false;
                for prev_dag_nidx in prev_dag_nodes {
                    if dag.g[prev_dag_nidx].trace_id == self.g.g[prev_nidx].trace_id {
                        cur_dag_nidx = prev_dag_nidx;
                    } else {
                        if self.find_start_node(&span_to_add, prev_dag_nidx, dag) {
                            found_start = true;
                        }
                    }
                }
                if found_start {
                    self.add_node_after(prev_nidx, &span_to_add);
                    return Ok(());
                }
            }
            cur_nidx = prev_nidx;
        }
    }

    fn find_start_node(&self, span: &Event, start_nidx: NodeIndex, dag: &Trace) -> bool {
        let mut cur_dag_nidx = start_nidx;
        loop {
            if dag.g[cur_dag_nidx].trace_id == span.trace_id {
                return true;
            }
            let prev_dag_nodes = dag
                .g
                .neighbors_directed(cur_dag_nidx, Direction::Incoming)
                .collect::<Vec<_>>();
            if prev_dag_nodes.len() == 1 {
                cur_dag_nidx = prev_dag_nodes[0];
            } else {
                if prev_dag_nodes.is_empty() {
                    return false;
                }
                for prev_node in prev_dag_nodes {
                    if self.find_start_node(span, prev_node, dag) {
                        return true;
                    }
                }
                return false;
            }
        }
    }

    /// Get all of the active spans that are not finished in the rest of the critical path.
    /// A synthetic node will be added after all unfinished spans.
    ///
    /// The end of the unfinished span needs to be accessible through `dag_nidx`, otherwise we
    /// would be adding an erroneous edge
    fn get_unfinished(
        &self,
        spans: &Vec<Event>,
        nidx: NodeIndex,
        dag_nidx: NodeIndex,
        dag: &Trace,
    ) -> Vec<Event> {
        let mut unfinished = spans.clone();
        let mut cur_nidx = nidx;
        loop {
            for (idx, span) in unfinished.iter().enumerate() {
                if span.trace_id == self.g.g[cur_nidx].trace_id {
                    unfinished.remove(idx);
                    break;
                }
            }
            cur_nidx = match self.next_node(cur_nidx) {
                Some(nidx) => nidx,
                None => break,
            };
        }
        unfinished.retain(|span| dag.can_reach_from_node(span.trace_id, dag_nidx));
        unfinished
    }

    /// This is not used
    pub fn next_real_node(&self, nidx: NodeIndex) -> Option<NodeIndex> {
        let mut result;
        loop {
            let mut matches = self.g.g.edges(nidx);
            result = matches.next();
            assert!(matches.next().is_none());
            if result.is_none() {
                return None;
            }
            if result.unwrap().weight().duration > Duration::new(0, 1) {
                break;
            }
        }
        Some(result.unwrap().target())
    }

    fn remove_node(&mut self, nidx: NodeIndex) {
        let next_node = self.next_node(nidx);
        let prev_node = self.prev_node(nidx);
        match next_node {
            Some(next_nidx) => {
                self.g
                    .g
                    .remove_edge(self.g.g.find_edge(nidx, next_nidx).unwrap());
                match prev_node {
                    Some(prev_nidx) => {
                        self.g
                            .g
                            .remove_edge(self.g.g.find_edge(prev_nidx, nidx).unwrap());
                        self.g.g.add_edge(
                            prev_nidx,
                            next_nidx,
                            DAGEdge {
                                duration: (self.g.g[next_nidx].timestamp
                                    - self.g.g[prev_nidx].timestamp)
                                    .to_std()
                                    .unwrap(),
                                variant: EdgeType::ChildOf,
                            },
                        );
                    }
                    None => {
                        self.start_node = next_nidx;
                    }
                }
            }
            None => match prev_node {
                Some(prev_nidx) => {
                    self.g
                        .g
                        .remove_edge(self.g.g.find_edge(prev_nidx, nidx).unwrap());
                    self.end_node = prev_nidx;
                }
                None => {
                    panic!("Something went wrong here");
                }
            },
        }
        self.g.g.remove_node(nidx);
    }

    /// Modifies the span to be entry/exit if input is exit/entry, and changes timestamp to be
    /// +1 ns.
    fn add_node_after(&mut self, after: NodeIndex, node: &Event) {
        let next_node = self.next_node(after);
        let new_node = self.g.g.add_node(Event {
            tracepoint_id: node.tracepoint_id.clone(),
            variant: match node.variant {
                EventType::Entry => EventType::Exit,
                EventType::Exit => EventType::Entry,
                EventType::Annotation => panic!("don't give me annotation"),
            },
            trace_id: node.trace_id,
            timestamp: self.g.g[after].timestamp + chrono::Duration::nanoseconds(1),
            is_synthetic: true,
            key_value_pair: HashMap::new(),
        });
        self.g.g.add_edge(
            after,
            new_node,
            DAGEdge {
                duration: Duration::new(0, 1),
                variant: EdgeType::ChildOf,
            },
        );
        match next_node {
            Some(next_nidx) => {
                let old_edge = self.g.g.find_edge(after, next_nidx).unwrap();
                let old_duration = self.g.g[old_edge].duration;
                self.g.g.remove_edge(old_edge);
                self.g.g.add_edge(
                    new_node,
                    next_nidx,
                    DAGEdge {
                        duration: old_duration,
                        variant: EdgeType::ChildOf,
                    },
                );
            }
            None => {
                self.end_node = new_node;
            }
        }
    }
}

/// Common methods that a Path has
pub trait Path {
    fn get_hash(&self) -> &str;
    fn set_hash(&mut self, hash: &str);
    fn start_node(&self) -> NodeIndex;
    fn at(&self, idx: NodeIndex) -> TracepointID;
    fn next_node(&self, idx: NodeIndex) -> Option<NodeIndex>;
    fn prev_node(&self, idx: NodeIndex) -> Option<NodeIndex>;
    fn len(&self) -> usize;

    fn hash(&self) -> &str {
        self.get_hash()
    }

    fn calculate_hash(&mut self) {
        let mut hasher = Sha256::new();
        let mut cur_node = self.start_node();
        loop {
            hasher.input(&self.at(cur_node).bytes());
            cur_node = match self.next_node(cur_node) {
                Some(node) => node,
                None => break,
            };
        }
        self.set_hash(&hasher.result_str());
    }

    /// This is the matching code. A path contains another path if it can be constructed by adding
    /// nodes to the other path.
    fn contains(&self, other: &dyn Path) -> bool {
        let mut cur_self_idx = self.start_node();
        let mut cur_other_idx = other.start_node();
        let result;
        loop {
            if self.at(cur_self_idx) == other.at(cur_other_idx) {
                cur_other_idx = match other.next_node(cur_other_idx) {
                    Some(nidx) => nidx,
                    None => {
                        result = true;
                        break;
                    }
                }
            }
            cur_self_idx = match self.next_node(cur_self_idx) {
                Some(nidx) => nidx,
                None => {
                    result = false;
                    break;
                }
            }
        }
        return result;
    }
}

impl Path for CriticalPath {
    fn get_hash(&self) -> &str {
        &self.hash
    }

    fn set_hash(&mut self, hash: &str) {
        self.hash = hash.to_string();
    }

    fn start_node(&self) -> NodeIndex {
        self.start_node
    }

    fn at(&self, idx: NodeIndex) -> TracepointID {
        self.g.g[idx].tracepoint_id
    }

    fn next_node(&self, nidx: NodeIndex) -> Option<NodeIndex> {
        let mut matches = self.g.g.neighbors_directed(nidx, Direction::Outgoing);
        let result = matches.next();
        assert!(matches.next().is_none());
        result
    }

    fn prev_node(&self, nidx: NodeIndex) -> Option<NodeIndex> {
        let mut matches = self.g.g.neighbors_directed(nidx, Direction::Incoming);
        let result = matches.next();
        assert!(matches.next().is_none());
        result
    }

    fn len(&self) -> usize {
        self.g.g.node_count()
    }
}

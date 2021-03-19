use std::collections::HashSet;

use petgraph::graph::{EdgeIndex, NodeIndex};
use rand::seq::SliceRandom;

use crate::controller::Controller;
use crate::critical::Path;
use crate::grouping::Group;
use crate::manifest::HierarchicalCriticalPath;
use crate::manifest::Manifest;
use crate::search::SearchStrategy;
use crate::settings::Settings;
use crate::trace::EventType;
use crate::trace::TracepointID;

pub struct HierarchicalSearch {
    controller: &'static Box<dyn Controller>,
    manifest: &'static Manifest,
}

impl SearchStrategy for HierarchicalSearch {
    fn search(&self, group: &Group, edge: EdgeIndex, budget: usize) -> Vec<TracepointID> {
        let mut rng = &mut rand::thread_rng();
        let (source, target) = group.g.edge_endpoints(edge).unwrap();
        // Get context for source and target, aka the hierarchical nodes that come before them.
        let source_context = self.get_context(group, source);
        let target_context = self.get_context(group, target);
        // Build a common context of the shared prefix between those two contexts.
        // e.g.
        // source_context = [1, 2, 3, 4]
        // target_context = [1, 2]
        // common_context = [1, 2]
        let mut common_context = Vec::new();
        let mut idx = 0;
        loop {
            if idx >= source_context.len() || idx >= target_context.len() {
                break;
            } else if source_context[idx] == target_context[idx] {
                common_context.push(source_context[idx]);
                idx += 1;
            } else {
                break;
            }
        }
        println!("Common context for the search: {:?}", common_context);
        let matches = self.manifest.find_matches(group);
        let mut result = self.search_context(&matches, common_context);
        result = result
            .into_iter()
            .filter(|&x| !self.controller.is_enabled(&(x, Some(group.request_type))))
            .collect();
        result = result.choose_multiple(&mut rng, budget).cloned().collect();
        result
    }
}

impl HierarchicalSearch {
    pub fn new(_s: &Settings, m: &'static Manifest, c: &'static Box<dyn Controller>) -> Self {
        HierarchicalSearch {
            controller: c,
            manifest: m,
        }
    }

    fn search_context(
        &self,
        matches: &Vec<&HierarchicalCriticalPath>,
        context: Vec<TracepointID>,
    ) -> Vec<TracepointID> {
        // Really we're looking at possible new parents that are at the correct hierarchy
        // depth. Once we have them, we'll return THEIR children as the search result.
        let mut possible_child_nodes = Vec::new();
        // We want to iterate through every path that contains the group in question.
        for m in matches {
            // We want to keep a running list of (hierarchy depth, possible node) tuples
            // Initialize with 0 and no possible node. Note that the first in last out order doesn't matter, as there
            // aren't any shared variables between the entries.
            let mut possible_next_nodes: Vec<(usize, Option<NodeIndex>)> = vec![(0, None)];
            while !possible_next_nodes.is_empty() {
                let to_eval = possible_next_nodes.pop().unwrap();
                // If we've reached the depth in the hierarchy of the shared context, that means the possible
                // node we have is at the current deepest level in the hierarchy, and thus we can add it to the possible
                // new parent nodes.
                if to_eval.0 == context.len() {
                    possible_child_nodes.push((
                        m,
                        match to_eval.1 {
                            Some(nidx) => nidx,
                            None => m.start_node,
                        },
                    ));
                    continue;
                }
                // If we're not at the depth of the shared context, we want to recursively add this next node's children.
                // Kind of like doing BFS, add it to the queue.

                // First, get the tracepoint at this point in the shared context. This will ensure that we're following
                // the shared context as we go hierarchically deeper into the traces.
                // Note that this is either the first one (for the first iteration) or the next one
                let tracepoint = context[to_eval.0];
                let nidx = to_eval.1;
                match nidx {
                    None => {
                        // I think this is only matched at the beginning, when the possible node is None. In this case,
                        // we want to add the node at the beginning of the hierarchy that is the first element in
                        // the shared context.
                        for &nidx in m.hierarchy_starts.iter() {
                            if m.g[nidx].tracepoint_id == tracepoint {
                                possible_next_nodes.push((to_eval.0 + 1, Some(nidx)))
                            }
                        }
                    }
                    Some(nidx) => {
                        // We want to explore the child nodes of the current node (in the hierarchical path match) that
                        // match the tracepoint. This is how we add the next layer to the queue as we go down the
                        // hierarchy, and we have the tracepoint id match to make sure we're following the context.
                        for candidate in m
                            .child_nodes(nidx)
                            .iter()
                            .filter(|&a| m.g[*a].tracepoint_id == tracepoint)
                        {
                            possible_next_nodes.push((to_eval.0 + 1, Some(*candidate)));
                        }
                    }
                }
            }
        }
        // We now have a list of all of the vanguard nodes, if you will, at the current deepest level of the hierarchy.
        // We now want to add all of their children to the results, which we get from child_nodes.
        let mut result = HashSet::new();
        for (path, nidx) in possible_child_nodes {
            for child in path.child_nodes(nidx) {
                result.insert(path.g[child].tracepoint_id);
            }
        }
        result.drain().collect()
    }

    // get_context walks through the group's nodes and builds a list of the hierarchical tracepoints that come before the node.
    // For example, with group:
    // A Entry -> B Entry -> C Entry -> C Exit -> D Entry
    // and node: (D entry)
    // Would build context for tracepoints from (A Entry, B Entry).
    fn get_context(&self, group: &Group, node: NodeIndex) -> Vec<TracepointID> {
        let mut result = Vec::new();
        let mut nidx = group.start_node;
        loop {
            match group.g[nidx].variant {
                EventType::Annotation => {
                    if nidx == node {
                        result.push(group.g[nidx].tracepoint_id);
                        break;
                    }
                }
                EventType::Exit => {
                    if nidx == node {
                        break;
                    }
                    assert_eq!(result.pop().unwrap(), group.g[nidx].tracepoint_id);
                }
                EventType::Entry => {
                    result.push(group.g[nidx].tracepoint_id);
                    if nidx == node {
                        break;
                    }
                }
            }
            nidx = group.next_node(nidx).unwrap();
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use crate::controller::Controller;
    use crate::controller::TestController;
    use crate::manifest::HierarchicalCriticalPath;
    use crate::manifest::Manifest;
    use crate::search::HierarchicalSearch;
    use crate::settings::Settings;
    use crate::trace::TracepointID;

    use pythia_common::RequestType;

    lazy_static! {
        static ref SETTINGS: Settings = Settings::read();
        static ref CONTROLLER: Box<dyn Controller> = Box::new(TestController::new());
        static ref MANIFEST: Manifest = Manifest::from_file(&SETTINGS.manifest_file.as_path())
            .expect("Couldn't read manifest from cache");
    }

    #[test]
    fn it_works() {
        CONTROLLER.disable_all();
        let search = HierarchicalSearch::new(&SETTINGS, &MANIFEST, &CONTROLLER);
        let mut manifest = MANIFEST.clone();
        let mut paths: Vec<HierarchicalCriticalPath> = manifest
            .per_request_type
            .get_mut(&RequestType::ServerCreate)
            .unwrap()
            .paths
            .values()
            .cloned()
            .collect();
        for path in paths.iter_mut() {
            path.hierarchy_starts.insert(path.start_node);
        }
        println!(
            "{:?}",
            search.search_context(
                &paths.iter().map(|x| x).collect(), vec![
                TracepointID::from_str("emreates/usr/lib/python3/dist-packages/cliff/app.py:363:openstackclient.shell.App.run_subcommand"),
                TracepointID::from_str("emreates/usr/local/lib/python3.6/dist-packages/openstackclient/compute/v2/server.py:662:openstackclient.compute.v2.server.CreateServer.take_action")
                ])
            );
    }
}

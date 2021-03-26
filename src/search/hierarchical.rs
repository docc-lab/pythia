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
        let mut result = self.search_context(
            &matches,
            common_context,
            group.g[source].tracepoint_id,
            group.g[target].tracepoint_id,
            budget,
        );
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
        source_tracepoint: TracepointID,
        target_tracepoint: TracepointID,
        budget: usize,
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
        // Let's start by being super greedy with the budget.
        let mut budget_left = budget;
        for (path, nidx) in possible_child_nodes {
            // If we're out of budget, we can just stop early.
            if budget_left == 0 {
                break;
            }
            //  Get all of the child nodes
            let all_children = path.child_nodes(nidx);
            // We want to filter these to just those that happen between source and target (via happens-before)

            // Question: must there exist at least one that happens directly after the source node?
            // For now we can do just the dumber approach
            let mut valid_child_tracepoints = Vec::new();

            // TODO(alex): path.happens_before is O(n) on the size of the trace, so if we're calling this for a ton of
            // children, this could end up being expensive. We can definitely speed this up.
            for child in all_children {
                let child_tracepoint = path.g[child].tracepoint_id;
                let child_happens_after_source =
                    path.happens_before(source_tracepoint, child_tracepoint);
                let child_happens_before_target =
                    path.happens_before(child_tracepoint, target_tracepoint);
                if child_happens_after_source && child_happens_before_target {
                    valid_child_tracepoints.push(child_tracepoint);
                }
            }

            // Now that we have all the valid nodes at the next level, we want to split them by our budget.
            // For example, if budget is 1, pick the node in the middle.
            // TODO(alex): we can probably refactor out this functionality from flat.rs and share a helper.
            // If we have enough for all of the tracepoints, just add them
            let num_valid_child_tracepoints = valid_child_tracepoints.len();
            if budget_left >= num_valid_child_tracepoints {
                for child in valid_child_tracepoints {
                    result.insert(child);
                }
                budget_left = budget_left - num_valid_child_tracepoints;
            } else {
                // Calculate the spacing n between every node we want to turn on, then turn on every n'th.
                // E.g. for [A, B, C, D, E] and budget 2, we want to turn on B and D. That means we want to turn on
                // indices 1 and 3.
                let spacing = valid_child_tracepoints.len() / (budget_left + 1);
                for i in 0..valid_child_tracepoints.len() {
                    // `+ 1` to each of these to help split roughly evenly and prefer center nodes over start/end of
                    // vector. See https://replit.com/join/gzypktqi-lxls.
                    if (i + 1) % (spacing + 1) == 0 {
                        if budget_left > 0 {
                            result.insert(valid_child_tracepoints[i]);
                            budget_left = budget_left - 1;
                        } else {
                            break;
                        }
                    }
                }
            }
        }
        println!("Total number of search results from search_context: {:?}", result.len());
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
        let search_result =
            search.search_context(
                /*matches=*/ &paths.iter().map(|x| x).collect(),
                /*context=*/ vec![
                    TracepointID::from_str("emreates/usr/lib/python3/dist-packages/cliff/app.py:363:openstackclient.shell.App.run_subcommand"),
                    TracepointID::from_str("emreates/usr/local/lib/python3.6/dist-packages/openstackclient/compute/v2/server.py:662:openstackclient.compute.v2.server.CreateServer.take_action")
                ],
                /*source_tracepoint=*/ TracepointID::from_str("emreates/usr/lib/python3/dist-packages/cliff/app.py:363:openstackclient.shell.App.run_subcommand"),
                /*target_tracepoint=*/ TracepointID::from_str("emreates/usr/local/lib/python3.6/dist-packages/openstackclient/compute/v2/server.py:662:openstackclient.compute.v2.server.CreateServer.take_action"),
                /*budget=*/ 1
            );
        println!("{:?}", search_result);
        assert_eq!(
            search_result,
            [TracepointID::from_str("keystone/v3/auth/tokens:POST")]
        )
    }
    #[test]
    fn it_works_across_hierarchy_levels() {
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
        assert_eq!(
            search.search_context(
                /*matches=*/ &paths.iter().map(|x| x).collect(),
                /*context=*/ vec![
                    TracepointID::from_str("emreates/usr/lib/python3/dist-packages/cliff/app.py:363:openstackclient.shell.App.run_subcommand"),
                ],
                /*source_tracepoint=*/ TracepointID::from_str("emreates/usr/lib/python3/dist-packages/cliff/app.py:363:openstackclient.shell.App.run_subcommand"),
                /*target_tracepoint=*/ TracepointID::from_str("emreates/usr/local/lib/python3.6/dist-packages/openstackclient/compute/v2/server.py:662:openstackclient.compute.v2.server.CreateServer.take_action"),
                /*budget=*/ 1
            ),
            [TracepointID::from_str("keystone/v3/auth/tokens:POST")]);
    }
    #[test]
    fn it_works_with_higher_budget() {
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
        // Sort both by strings so we can ignore order (since we're getting values from a set in search_context)
        assert_eq!(
            search.search_context(
                /*matches=*/ &paths.iter().map(|x| x).collect(),
                /*context=*/ vec![
                    TracepointID::from_str("emreates/usr/lib/python3/dist-packages/cliff/app.py:363:openstackclient.shell.App.run_subcommand"),
                ],
                /*source_tracepoint=*/ TracepointID::from_str("emreates/usr/lib/python3/dist-packages/cliff/app.py:363:openstackclient.shell.App.run_subcommand"),
                /*target_tracepoint=*/ TracepointID::from_str("emreates/usr/local/lib/python3.6/dist-packages/openstackclient/compute/v2/server.py:662:openstackclient.compute.v2.server.CreateServer.take_action"),
                /*budget=*/ 3
            ).sort_by(|a, b| a.to_string().cmp(&b.to_string())),
            [
                TracepointID::from_str("keystone/v3:GET"),
                TracepointID::from_str("emreates/usr/local/lib/python3.6/dist-packages/openstackclient/compute/v2/server.py:486:openstackclient.compute.v2.server.CreateServer.get_parser"),
                TracepointID::from_str("keystone/v3/auth/tokens:POST"),
            ].sort_by(|a, b| a.to_string().cmp(&b.to_string()))
        );
    }
}

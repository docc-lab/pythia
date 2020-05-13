#[macro_use]
extern crate lazy_static;

pub mod controller;
pub mod critical;
pub mod grouping;
pub mod manifest;
pub mod reader;
pub mod rpclib;
pub mod search;
pub mod settings;
pub mod trace;

use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io::stdin;
use std::io::{self, BufRead};

use itertools::Itertools;
use procinfo::pid::statm_self;

use pythia_common::RequestType;

use crate::controller::OSProfilerController;
use crate::critical::CriticalPath;
use crate::grouping::Group;
use crate::manifest::Manifest;
use crate::reader::reader_from_settings;
use crate::settings::Settings;
use crate::trace::Trace;

pub fn make_decision(_epoch_file: &str, _dry_run: bool, _budget: usize) {
    panic!("This broke while transitioning to continuously running loop");
}
// use rand::seq::SliceRandom;
// use crate::cct::CCT;
// use crate::flat::FlatSpace;
// use crate::historic::Historic;
// use crate::poset::Poset;
// use crate::search::SearchState;
// use crate::search::SearchStrategy;
// use crate::settings::ManifestMethod;
//
// /// Make a single instrumentation decision.
// pub fn make_decision(epoch_file: &str, dry_run: bool, budget: usize) {
//     let settings = Settings::read();
//     let mut budget = budget;
//     let mut rng = &mut rand::thread_rng();
//     let controller = OSProfilerController::from_settings(&settings);
//     let manifest_file = &settings.manifest_file;
//     let manifest =
//         Manifest::from_file(manifest_file.as_path()).expect("Couldn't read manifest from cache");
//     let strategy: Box<dyn SearchStrategy> = match &settings.manifest_method {
//         ManifestMethod::CCT => Box::new(CCT::new(manifest)),
//         ManifestMethod::Poset => Box::new(Poset::new(manifest)),
//         ManifestMethod::Flat => Box::new(FlatSpace::new(manifest)),
//         ManifestMethod::Historic => Box::new(Historic::new(manifest)),
//     };
//     let mut reader = reader_from_settings(&settings);
//     let now = Instant::now();
//     let traces = reader.read_trace_file(epoch_file);
//     eprintln!("Reading traces took {}us", now.elapsed().as_micros());
//     let critical_paths = traces
//         .iter()
//         .map(|t| CriticalPath::from_trace(t).unwrap())
//         .collect();
//     let now = Instant::now();
//     let mut groups = Group::from_critical_paths(critical_paths);
//     eprintln!(
//         "Extracting critical paths took {}us",
//         now.elapsed().as_micros()
//     );
//     groups.sort_by(|a, b| b.variance.partial_cmp(&a.variance).unwrap()); // descending order
//     println!("\n\nGroups sorted by variance:\n");
//     for group in &groups {
//         println!("Group is: {}", group);
//     }
//     let mut group_index = 0;
//     let mut converged = false;
//     let mut old_tracepoints = None;
//     let now = Instant::now();
//     while !converged {
//         if group_index >= groups.len() {
//             panic!("Could not find any tracepoints to enable");
//         }
//         println!("\n\nEdges sorted by variance:\n");
//         let problem_edges = groups[group_index].problem_edges();
//         for edge in &problem_edges {
//             let endpoints = groups[group_index].g.edge_endpoints(*edge).unwrap();
//             println!(
//                 "({} -> {}): {}",
//                 groups[group_index].g[endpoints.0],
//                 groups[group_index].g[endpoints.1],
//                 groups[group_index].g[*edge]
//             );
//         }
//         let mut index = 0;
//         while !converged {
//             if index >= problem_edges.len() {
//                 break;
//             }
//             let problem_edge = problem_edges[index];
//             println!("\n\nTry {}: Next tracepoints to enable:\n", index);
//             let (tracepoints, state) = strategy.search(&groups[group_index], problem_edge, budget);
//             match old_tracepoints {
//                 Some(list) => {
//                     if list == tracepoints {
//                         println!("It seems like we entered an infinite loop");
//                     }
//                 }
//                 None => {}
//             }
//             println!("{:?}", tracepoints);
//             let mut to_enable = controller.get_disabled(
//                 &tracepoints
//                     .iter()
//                     .map(|&x| (x, Some(groups[group_index].request_type)))
//                     .collect(),
//             );
//             if to_enable.len() != 0 {
//                 if budget == 0 {
//                     converged = true;
//                 } else if to_enable.len() > budget {
//                     to_enable = to_enable
//                         .choose_multiple(&mut rng, budget)
//                         .cloned()
//                         .collect();
//                     budget = 0;
//                     converged = true;
//                 } else {
//                     budget -= to_enable.len();
//                     if budget == 0 {
//                         converged = true;
//                     }
//                 }
//                 if !dry_run {
//                     controller.enable(&to_enable);
//                     println!("Enabled tracepoints.");
//                 }
//             }
//             match state {
//                 SearchState::NextEdge => {
//                     index += 1;
//                 }
//                 SearchState::DepletedBudget => {}
//             }
//             old_tracepoints = Some(tracepoints);
//         }
//         group_index += 1;
//     }
//     eprintln!(
//         "Searching plus enabling took {}us",
//         now.elapsed().as_micros()
//     );
// }

pub fn disable_all() {
    let settings = Settings::read();
    let controller = OSProfilerController::from_settings(&settings);
    controller.disable_all();
}

pub fn enable_all() {
    let settings = Settings::read();
    let controller = OSProfilerController::from_settings(&settings);
    controller.enable_all();
}

pub fn disable_tracepoint(t: &str) {
    let settings = Settings::read();
    let controller = OSProfilerController::from_settings(&settings);
    controller.disable_by_name(t);
}

pub fn enable_skeleton() {
    let settings = Settings::read();
    let manifest_file = &settings.manifest_file;
    let manifest =
        Manifest::from_file(manifest_file.as_path()).expect("Couldn't read manifest from cache");
    let controller = OSProfilerController::from_settings(&settings);
    controller.disable_all();
    let to_enable = manifest.skeleton();
    controller.enable(&to_enable.iter().map(|&a| (a.clone(), None)).collect());
    println!("Enabled following tracepoints: {:?}", to_enable);
}

pub fn manifest_stats() {
    let settings = Settings::read();
    let manifest_file = settings.manifest_file;
    let prev_stats = statm_self().unwrap();
    let manifest =
        Manifest::from_file(manifest_file.as_path()).expect("Couldn't read manifest from cache");
    let after_stats = statm_self().unwrap();
    eprintln!(
        "Memory footprint (in pages):\nsize: {}, resident: {}, share: {}, text: {}, data: {}",
        after_stats.size - prev_stats.size,
        after_stats.resident - prev_stats.resident,
        after_stats.share - prev_stats.share,
        after_stats.text - prev_stats.text,
        after_stats.data - prev_stats.data
    );
    eprintln!(
        "Number of paths per request type: {}",
        manifest
            .per_request_type
            .iter()
            .map(|(k, v)| { format!("{}: {}", k, v.path_count()) })
            .join("\n")
    );
    eprintln!(
        "Number of unique tracepoints observed in search space: {}",
        manifest
            .per_request_type
            .iter()
            .map(|(_, v)| { v.trace_points() })
            .flatten()
            .collect::<HashSet<_>>()
            .len()
    );
    let path_lens = manifest
        .per_request_type
        .iter()
        .map(|(_, v)| v.path_lengths())
        .flatten()
        .map(|x| x as f64)
        .collect::<Vec<f64>>();
    eprintln!(
        "Average path length: {}",
        path_lens.iter().sum::<f64>() / path_lens.len() as f64
    );
}

pub fn show_manifest(request_type: &str) {
    let settings = Settings::read();
    let manifest_file = settings.manifest_file;
    let manifest =
        Manifest::from_file(manifest_file.as_path()).expect("Couldn't read manifest from cache");
    println!(
        "{}",
        manifest
            .per_request_type
            .get(&RequestType::from_str(request_type).unwrap())
            .unwrap()
    );
}

pub fn dump_traces(tracefile: &str) {
    let settings = Settings::read();
    let mut reader = reader_from_settings(&settings);
    for trace in reader.read_trace_file(tracefile) {
        let mut outfile = dirs::home_dir().unwrap();
        outfile.push(trace.base_id.to_hyphenated().to_string());
        outfile.set_extension("json");
        trace.to_file(&outfile);
    }
}

pub fn get_manifest(manfile: &str, overwrite: bool) {
    let settings = Settings::read();
    let mut reader = reader_from_settings(&settings);
    reader.for_searchspace();
    let traces = reader.read_trace_file(manfile);
    let manifest = Manifest::from_trace_list(&traces);
    println!("{}", manifest);
    let manifest_file = settings.manifest_file;
    if manifest_file.exists() {
        if !overwrite {
            println!(
                "The manifest file {:?} exists. Overwrite? [y/N]",
                manifest_file
            );
            let mut s = String::new();
            stdin().read_line(&mut s).unwrap();
            if s.chars().nth(0).unwrap() != 'y' {
                return;
            }
        }
        println!("Overwriting");
    }
    manifest.to_file(manifest_file.as_path());
}

pub fn measure_search_space_feasibility(trace_file: &str) {
    let settings = Settings::read();
    let mut reader = reader_from_settings(&settings);
    reader.for_searchspace();
    let trace = reader.read_file(trace_file);
    println!("{}", Manifest::try_constructing(&trace));
}

pub fn group_folder(trace_folder: &str) {
    let settings = Settings::read();
    let mut reader = reader_from_settings(&settings);
    let traces = reader.read_dir(trace_folder);
    println!("Read {} traces", traces.len());
    group_traces(traces);
}

pub fn group_from_ids(id_file: &str) {
    let settings = Settings::read();
    let mut reader = reader_from_settings(&settings);
    let file = File::open(id_file).unwrap();
    let traces = io::BufReader::new(file)
        .lines()
        .map(|x| reader.get_trace_from_base_id(&x.unwrap()).unwrap())
        .collect::<Vec<_>>();
    println!("Read {} traces", traces.len());
    group_traces(traces);
}

fn group_traces(traces: Vec<Trace>) {
    let critical_paths = traces
        .iter()
        .filter_map(|t| CriticalPath::from_trace(t).ok())
        .collect::<Vec<CriticalPath>>();
    println!("Got {} paths", critical_paths.len());
    let mut groups = Group::from_critical_paths(critical_paths);
    println!("Got {} groups", groups.len());
    groups.sort_by(|a, b| b.traces.len().partial_cmp(&a.traces.len()).unwrap()); // descending order
    println!(
        "Trace count and variance of each group: {:?}",
        groups
            .iter()
            .map(|x| (x.traces.len(), x.variance))
            .collect::<Vec<_>>()
    );
    println!("Top 5 variance groups");
    groups.sort_by(|a, b| b.variance.partial_cmp(&a.variance).unwrap()); // descending order
    for (idx, i) in groups.iter().enumerate() {
        if idx > 5 {
            break;
        }
        println!(
            "Group length {}, variance {}, each trace duration {:?}\nsample trace: {}",
            i.traces.len(),
            i.variance,
            i.traces.iter().map(|x| x.duration).collect::<Vec<_>>(),
            i
        );
    }
    println!("Top 5 groups with longest traces");
    groups.sort_by(|a, b| b.g.node_count().partial_cmp(&a.g.node_count()).unwrap()); // descending order
    for (idx, i) in groups.iter().enumerate() {
        if idx > 5 {
            break;
        }
        println!(
            "Group length {}, variance {}, each trace duration {:?}\nsample trace: {}",
            i.traces.len(),
            i.variance,
            i.traces.iter().map(|x| x.duration).collect::<Vec<_>>(),
            i
        );
    }
    println!(
        "Group stats:\npath_len,trace_count,variance,trace_ids\n{}",
        groups
            .iter()
            .map(|x| format!(
                "{},{},{},\"{:?}\"",
                x.g.node_count(),
                x.traces.len(),
                x.variance,
                x.traces.iter().map(|x| x.g.base_id).collect::<Vec<_>>()
            ))
            .join("\n")
    );
    groups.sort_by(|a, b| b.variance.partial_cmp(&a.variance).unwrap()); // descending order
    println!("\n\nEdges sorted by variance:\n");
    let problem_edges = groups[0].problem_edges();
    for edge in &problem_edges {
        let endpoints = groups[0].g.edge_endpoints(*edge).unwrap();
        println!(
            "({} -> {}): {}",
            groups[0].g[endpoints.0], groups[0].g[endpoints.1], groups[0].g[*edge]
        );
    }
}

pub fn read_trace_file(trace_file: &str) {
    let settings = Settings::read();
    let mut reader = reader_from_settings(&settings);
    let trace = reader.read_file(trace_file);
    println!("{}", trace);
}

pub fn get_trace(trace_id: &str, to_file: bool) {
    let settings = Settings::read();
    let mut reader = reader_from_settings(&settings);
    let trace = reader.get_trace_from_base_id(trace_id).unwrap();
    println!("{}", trace);
    if to_file {
        let mut tracefile = dirs::home_dir().unwrap();
        tracefile.push(trace_id);
        tracefile.set_extension("json");
        trace.to_file(tracefile.as_path());
        eprintln!("Wrote trace to {}", tracefile.to_str().unwrap());
    }
}

pub fn show_key_value_pairs(trace_id: &str) {
    // let settings = Settings::read();
    // let mut reader = OSProfilerreader_from_settings(&settings);
    // let pairs = reader.get_key_value_pairs(trace_id);
    // println!("{:?}", pairs);
    println!("{:?}", trace_id);
}

pub fn get_crit(trace_id: &str) {
    let settings = Settings::read();
    let mut reader = reader_from_settings(&settings);
    let trace = reader.get_trace_from_base_id(trace_id).unwrap();
    let crit = CriticalPath::from_trace(&trace).unwrap();
    println!("{}", crit.g);
}

pub fn show_config() {
    let settings = Settings::read();
    println!("{:?}", settings);
}

#[derive(Debug)]
pub struct PythiaError(String);

impl fmt::Display for PythiaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Uber error: {}", self.0)
    }
}

impl Error for PythiaError {}

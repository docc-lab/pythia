#[macro_use]
extern crate lazy_static;

pub mod cct;
pub mod controller;
pub mod critical;
pub mod flat;
pub mod grouping;
pub mod hdfs;
pub mod historic;
pub mod manifest;
pub mod osprofiler;
pub mod poset;
pub mod rpclib;
pub mod search;
pub mod searchspace;
pub mod trace;

use std::collections::HashMap;
use std::io::stdin;
use std::path::PathBuf;
use std::time::Instant;

use config::{Config, File, FileFormat};
use petgraph::dot::Dot;
use rand::seq::SliceRandom;

use crate::cct::CCT;
use crate::controller::OSProfilerController;
use crate::critical::CriticalPath;
use crate::flat::FlatSpace;
use crate::grouping::Group;
use crate::hdfs::HDFSReader;
use crate::historic::Historic;
use crate::manifest::Manifest;
use crate::osprofiler::OSProfilerReader;
use crate::osprofiler::RequestType;
use crate::poset::Poset;
use crate::search::SearchState;
use crate::search::SearchStrategy;

/// Make a single instrumentation decision.
pub fn make_decision(epoch_file: &str, dry_run: bool, budget: usize) {
    let settings = get_settings();
    let mut budget = budget;
    let mut rng = &mut rand::thread_rng();
    let controller = OSProfilerController::from_settings(&settings);
    let manifest_file = PathBuf::from(settings.get("manifest_file").unwrap());
    let manifest =
        Manifest::from_file(manifest_file.as_path()).expect("Couldn't read manifest from cache");
    let manifest_method: &str = settings.get("manifest_method").unwrap();
    let strategy: Box<dyn SearchStrategy> = match manifest_method {
        "CCT" => Box::new(CCT::new(manifest)),
        "Poset" => Box::new(Poset::new(manifest)),
        "Flat" => Box::new(FlatSpace::new(manifest)),
        "Historic" => Box::new(Historic::new(manifest)),
        _ => panic!("Unsupported manifest method"),
    };
    let mut reader = OSProfilerReader::from_settings(&settings);
    let now = Instant::now();
    let traces = reader.read_trace_file(epoch_file);
    eprintln!("Reading traces took {}", now.elapsed().as_micros());
    let critical_paths = traces.iter().map(|t| CriticalPath::from_trace(t)).collect();
    let now = Instant::now();
    let mut groups = Group::from_critical_paths(critical_paths);
    eprintln!(
        "Extracting critical paths took {}",
        now.elapsed().as_micros()
    );
    groups.sort_by(|a, b| b.variance.partial_cmp(&a.variance).unwrap()); // descending order
    println!("\n\nGroups sorted by variance:\n");
    for group in &groups {
        println!("Group is: {}", group);
    }
    let mut group_index = 0;
    let mut converged = false;
    let mut old_tracepoints = None;
    let now = Instant::now();
    while !converged {
        if group_index >= groups.len() {
            panic!("Could not find any tracepoints to enable");
        }
        println!("\n\nEdges sorted by variance:\n");
        let problem_edges = groups[group_index].problem_edges();
        for edge in &problem_edges {
            let endpoints = groups[group_index].g.edge_endpoints(*edge).unwrap();
            println!(
                "({} -> {}): {}",
                groups[group_index].g[endpoints.0],
                groups[group_index].g[endpoints.1],
                groups[group_index].g[*edge]
            );
        }
        let mut index = 0;
        while !converged {
            if index >= problem_edges.len() {
                break;
            }
            let problem_edge = problem_edges[index];
            println!("\n\nTry {}: Next tracepoints to enable:\n", index);
            let (tracepoints, state) = strategy.search(&groups[group_index], problem_edge, budget);
            match old_tracepoints {
                Some(list) => {
                    if list == tracepoints {
                        println!("It seems like we entered an infinite loop");
                    }
                }
                None => {}
            }
            println!("{:?}", tracepoints);
            let mut to_enable = controller.get_disabled(
                &tracepoints
                    .iter()
                    .map(|&x| (x, Some(groups[group_index].request_type)))
                    .collect(),
            );
            if to_enable.len() != 0 {
                if budget == 0 {
                    converged = true;
                } else if to_enable.len() > budget {
                    to_enable = to_enable
                        .choose_multiple(&mut rng, budget)
                        .cloned()
                        .collect();
                    budget = 0;
                    converged = true;
                } else {
                    budget -= to_enable.len();
                    if budget == 0 {
                        converged = true;
                    }
                }
                if !dry_run {
                    controller.enable(&to_enable);
                    println!("Enabled tracepoints.");
                }
            }
            match state {
                SearchState::NextEdge => {
                    index += 1;
                }
                SearchState::DepletedBudget => {}
            }
            old_tracepoints = Some(tracepoints);
        }
        group_index += 1;
    }
    eprintln!("Searching plus enabling took {}", now.elapsed().as_micros());
}

pub fn disable_all() {
    let settings = get_settings();
    let controller = OSProfilerController::from_settings(&settings);
    controller.diable_all();
}

pub fn enable_all() {
    let settings = get_settings();
    let controller = OSProfilerController::from_settings(&settings);
    controller.enable_all();
}

pub fn disable_tracepoint(t: &str) {
    let settings = get_settings();
    let controller = OSProfilerController::from_settings(&settings);
    controller.disable(&vec![(&t.to_string(), None)]);
}

pub fn enable_skeleton() {
    let settings = get_settings();
    let manifest_file = PathBuf::from(settings.get("manifest_file").unwrap());
    let manifest =
        Manifest::from_file(manifest_file.as_path()).expect("Couldn't read manifest from cache");
    let controller = OSProfilerController::from_settings(&settings);
    controller.diable_all();
    let to_enable = manifest.entry_points();
    controller.enable(&to_enable.iter().map(|&a| (a, None)).collect());
    println!("Enabled following tracepoints: {:?}", to_enable);
}

pub fn show_manifest(request_type: &str) {
    let settings = get_settings();
    let manifest_file = PathBuf::from(settings.get("manifest_file").unwrap());
    let manifest =
        Manifest::from_file(manifest_file.as_path()).expect("Couldn't read manifest from cache");
    match request_type {
        "ServerCreate" => {
            println!(
                "{}",
                manifest
                    .per_request_type
                    .get(&RequestType::ServerCreate)
                    .unwrap()
            );
        }
        "ServerList" => {
            println!(
                "{}",
                manifest
                    .per_request_type
                    .get(&RequestType::ServerList)
                    .unwrap()
            );
        }
        "ServerDelete" => {
            println!(
                "{}",
                manifest
                    .per_request_type
                    .get(&RequestType::ServerDelete)
                    .unwrap()
            );
        }
        _ => panic!("Invalid request type"),
    }
}

pub fn dump_traces(tracefile: &str) {
    let settings = get_settings();
    let mut reader = OSProfilerReader::from_settings(&settings);
    for trace in reader.read_trace_file(tracefile) {
        let mut outfile = dirs::home_dir().unwrap();
        outfile.push(trace.base_id.to_hyphenated().to_string());
        outfile.set_extension("json");
        trace.to_file(&outfile);
    }
}

pub fn get_manifest(manfile: &str, overwrite: bool) {
    let settings = get_settings();
    let mut reader = OSProfilerReader::from_settings(&settings);
    let traces = reader.read_trace_file(manfile);
    let manifest = Manifest::from_trace_list(&traces);
    println!("{}", manifest);
    let manifest_file = PathBuf::from(settings.get("manifest_file").unwrap());
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

pub fn read_hdfs_trace(trace_file: &str) {
    let settings = get_settings();
    let mut reader = HDFSReader::from_settings(&settings);
    let trace = reader.read_file(trace_file);
    println!("{}", Dot::new(&trace.g));
}

pub fn get_trace(trace_id: &str, to_file: bool) {
    let settings = get_settings();
    let mut reader = OSProfilerReader::from_settings(&settings);
    let trace = reader.get_trace_from_base_id(trace_id).unwrap();
    println!("{}", Dot::new(&trace.g));
    if to_file {
        let mut tracefile = dirs::home_dir().unwrap();
        tracefile.push(trace_id);
        tracefile.set_extension("json");
        trace.to_file(tracefile.as_path());
        eprintln!("Wrote trace to {}", tracefile.to_str().unwrap());
    }
}

pub fn show_key_value_pairs(trace_id: &str) {
    // let settings = get_settings();
    // let mut reader = OSProfilerReader::from_settings(&settings);
    // let pairs = reader.get_key_value_pairs(trace_id);
    // println!("{:?}", pairs);
    println!("{:?}", trace_id);
}

pub fn get_crit(trace_id: &str) {
    let settings = get_settings();
    let mut reader = OSProfilerReader::from_settings(&settings);
    let trace = reader.get_trace_from_base_id(trace_id).unwrap();
    let crit = CriticalPath::from_trace(&trace);
    println!("{}", Dot::new(&crit.g.g));
}

pub fn show_config() {
    let settings = get_settings();
    println!("{:?}", settings);
}

pub fn get_settings() -> HashMap<String, String> {
    let mut settings = Config::default();
    settings
        .merge(File::new(
            "/opt/stack/reconstruction/Settings.toml",
            FileFormat::Toml,
        ))
        .unwrap();
    let mut results = settings.try_into::<HashMap<String, String>>().unwrap();
    let mut manifest_file = PathBuf::from(results.get("pythia_cache").unwrap());
    match results.get("manifest_method").unwrap().as_str() {
        "CCT" => manifest_file.push("cct_manifest"),
        "Poset" => manifest_file.push("poset_manifest"),
        "Historic" => manifest_file.push("historic_manifest"),
        "Flat" => manifest_file.push("flat_manifest"),
        _ => panic!("Unsupported manifest method"),
    }
    results.insert(
        "manifest_file".to_string(),
        manifest_file.to_string_lossy().to_string(),
    );
    let mut trace_cache = PathBuf::from(results.get("pythia_cache").unwrap());
    trace_cache.push("traces");
    results.insert(
        "trace_cache".to_string(),
        trace_cache.to_string_lossy().to_string(),
    );
    results
}

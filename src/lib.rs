#[macro_use]
extern crate lazy_static;

pub mod cct;
pub mod controller;
pub mod critical;
pub mod flat;
pub mod grouping;
pub mod historic;
pub mod manifest;
pub mod osprofiler;
pub mod poset;
pub mod searchspace;
pub mod trace;

use std::collections::HashMap;
use std::io::stdin;
use std::path::PathBuf;

use config::{Config, File, FileFormat};
use petgraph::dot::Dot;

use self::controller::OSProfilerController;
use self::critical::CriticalPath;
use self::grouping::Group;
use self::manifest::Manifest;
use self::osprofiler::OSProfilerReader;
use self::osprofiler::RequestType;
use self::osprofiler::REQUEST_TYPE_MAP;
use self::searchspace::SearchState;

/// Make a single instrumentation decision.
pub fn make_decision(epoch_file: &str, dry_run: bool, budget: usize) {
    let settings = get_settings();
    let mut budget = budget;
    let controller = OSProfilerController::from_settings(&settings);
    let manifest_file = PathBuf::from(settings.get("manifest_file").unwrap());
    let manifest =
        Manifest::from_file(manifest_file.as_path()).expect("Couldn't read manifest from cache");
    let reader = OSProfilerReader::from_settings(&settings);
    let traces = reader.read_trace_file(epoch_file);
    let critical_paths = traces.iter().map(|t| CriticalPath::from_trace(t)).collect();
    let mut groups = Group::from_critical_paths(critical_paths);
    groups.sort_by(|a, b| b.variance.partial_cmp(&a.variance).unwrap()); // descending order
    println!("\n\nGroups sorted by variance:\n");
    for group in &groups {
        println!("Group is: {}", group);
    }
    let problem_group = &groups[0];
    println!("\n\nEdges sorted by variance:\n");
    let problem_edges = problem_group.problem_edges();
    for edge in &problem_edges {
        let endpoints = problem_group.g.edge_endpoints(*edge).unwrap();
        println!(
            "({} -> {}): {}",
            problem_group.g[endpoints.0], problem_group.g[endpoints.1], problem_group.g[*edge]
        );
    }
    let mut converged = false;
    let mut index = 0;
    let mut old_tracepoints = None;
    while !converged {
        let problem_edge = problem_edges[index];
        println!("\n\nTry {}: Next tracepoints to enable:\n", index);
        let (tracepoints, state) = manifest.search(problem_group, problem_edge, budget);
        match old_tracepoints {
            Some(list) => {
                if list == tracepoints {
                    println!("It seems like we entered an infinite loop");
                }
            }
            None => {}
        }
        println!("{:?}", tracepoints);
        let to_enable = controller.get_disabled(&tracepoints);
        if to_enable.len() != 0 {
            if budget == 0 {
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

pub fn enable_skeleton() {
    let settings = get_settings();
    let manifest_file = PathBuf::from(settings.get("manifest_file").unwrap());
    let manifest =
        Manifest::from_file(manifest_file.as_path()).expect("Couldn't read manifest from cache");
    let controller = OSProfilerController::from_settings(&settings);
    controller.diable_all();
    let mut to_enable = manifest.entry_points();
    to_enable.extend(REQUEST_TYPE_MAP.keys().into_iter());
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

pub fn get_manifest(manfile: &str, overwrite: bool) {
    let settings = get_settings();
    let reader = OSProfilerReader::from_settings(&settings);
    let traces = reader.read_trace_file(manfile);
    let manifest_method = settings.get("manifest_method").unwrap();
    let manifest = Manifest::from_trace_list(manifest_method, traces);
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

pub fn get_trace(trace_id: &str) {
    let settings = get_settings();
    let reader = OSProfilerReader::from_settings(&settings);
    let trace = reader.get_trace_from_base_id(trace_id);
    println!("{}", Dot::new(&trace.g));
}

pub fn show_key_value_pairs(trace_id: &str) {
    let settings = get_settings();
    let reader = OSProfilerReader::from_settings(&settings);
    let pairs = reader.get_key_value_pairs(trace_id);
    println!("{:?}", pairs);
}

pub fn get_crit(trace_id: &str) {
    let settings = get_settings();
    let reader = OSProfilerReader::from_settings(&settings);
    let trace = reader.get_trace_from_base_id(trace_id);
    let crit = CriticalPath::from_trace(&trace);
    println!("{}", Dot::new(&crit.g.g));
}

pub fn show_config() {
    let settings = get_settings();
    println!("{:?}", settings);
}

fn get_settings() -> HashMap<String, String> {
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

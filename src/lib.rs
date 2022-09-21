/*
BSD 2-Clause License

Copyright (c) 2022, Diagnosis and Control of Clouds Laboratory
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

//! This is the source code for Pythia.
//!
//! The main Pythia loop is implemented inside the
//! [`pythia_controller`](../pythia_controller/index.html) module; however that
//! module has only the main loop. This module contains the rest of the code used
//! by the main Pythia loop. The remaining 2 modules are
//! `pythia_server` which are the agents that
//! should run on each application node, and
//! [`pythia_common`](../pythia_common/index.html) which contains common code
//! between these two modules.
//!
//! # Installing Pythia
//! ## Without Cloudlab
//! 1. Create symlinks from `/etc/` to the folders under `etc` in this repo. The main Pythia loop
//!    needs the `controller.toml` configuration file, the Pythia agents need `server.toml` file,
//!    and the systemd service can be used to easily launch the pythia agents.
//! 2. Run `cargo install --path .` and `cargo install --path ./pythia_server`, which will install
//!    both the agent and the pythia binaries into somewhere (I think `~/.cargo/`).
//! 3. Update the configuration files to your liking. The application name is the most important
//!    one.
//! ## With Cloudlab
//! 1. Most of the previous steps are automated. Only make sure that the configuration file in etc
//!    selects the correct application.
//!
//! # Creating a search space
//! 1. Run some workload with all the instrumentation enabled. For OpenStack, this workload is in
//!    the script `/local/tracing-pythia/workloads/offline_profiling.sh`. You probably need to
//!    manually pull the latest version of the code to get the script.
//! 2. This script generates a list of trace_ids in the file `~/offline_profiling.txt`.
//! 3. Use `cargo run manifest <path/to/trace/ids>` to generate the manifest. It is stored in
//!    `/opt/stack/manifest.json`.
//!
//! # Using Pythia utils
//! There are a bunch of functions defined in this file, they are used from `cargo run`. Try
//! `cargo run -- --help` to see a list of functions. Typically they are used in the debugging
//! stage. Another way to run it is `cargo install --path .` and then use `pythia`. Some important ones:
//! * `pythia get-trace <trace_id>` read a single trace and print the dot file
//! * `pythia [enable|disable]-all` to enable/disable all tracepoints
//! * `pythia manifest-stats` construct a manifest and print all the stats used for the paper.
//!
//! # Running Pythia loop
//! 1. Make sure everything is configured correctly, read the comments in the toml files
//! 2. Make sure the agents are running on all nodes and configuration has the correct agent
//!    addresses
//! 3. Create a search space according to what's written above
//! 4. Decide the stopping condition. This is done by changing the target points in the file
//!    `src/bin/pythia_controller.rs`.
//! 5. Simply `cargo run --bin pythia_controller /path/to/log/output`. I typically keep the
//!    stdout/stderr and enable backtrace to have a more detailed view of things. So, this command
//!    could also be used: `RUST_BACKTRACE=1 cargo run --bin pythia_controller /path/to/log/output
//!    2>&1 | tee /path/to/verbose/logs`
//! 6. Execute some requests, and wait for Pythia to do its thing. For OpenStack, there is a
//!    `/local/tracing-pythia/workloads/continuous_workload.sh` that will run many requests in a
//!    loop.

#[macro_use]
extern crate lazy_static;

pub mod budget;
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
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;
use std::time::Instant;

use itertools::Itertools;
#[cfg(target_os = "linux")]
use procinfo::pid::statm_self;
use pythia_common::RequestType;

use crate::controller::controller_from_settings;
use crate::critical::CriticalPath;
use crate::grouping::Group;
use crate::manifest::Manifest;
use crate::reader::reader_from_settings;
use crate::settings::ApplicationType;
use crate::settings::Settings;
use crate::trace::Trace;

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
    let controller = controller_from_settings(&settings);
    controller.disable_all();
}

pub fn enable_all() {
    let settings = Settings::read();
    let controller = controller_from_settings(&settings);
    controller.enable_all();
}

pub fn disable_tracepoint(t: &str) {
    let settings = Settings::read();
    assert_eq!(settings.application, ApplicationType::OpenStack);
    let controller = controller_from_settings(&settings);
    controller.disable_by_name(t);
}

pub fn recent_traces() {
    let settings = Settings::read();
    let mut reader = reader_from_settings(&settings);
    for trace in reader.get_recent_traces() {
        println!("Got trace {}: {}", trace.base_id, trace);
    }
}

pub fn enable_skeleton() {
    let settings = Settings::read();
    let manifest_file = &settings.manifest_file;
    let manifest =
        Manifest::from_file(manifest_file.as_path()).expect("Couldn't read manifest from cache");
    let controller = controller_from_settings(&settings);
    controller.disable_all();
    let to_enable = manifest.skeleton();
    controller.enable(&to_enable.iter().map(|&a| (a.clone(), None)).collect());
    println!("Enabled following tracepoints: {:?}", to_enable);
}

pub fn manifest_stats(manfile: &str) {
    println!("mertiko");
    // #[cfg(target_os = "linux")]
    // {
        println!("mertiko2");
        let settings = Settings::read();
        let mut reader = reader_from_settings(&settings);
        reader.for_searchspace();
        let traces = reader.read_trace_file(manfile);
        let now = Instant::now();
        let manifest = Manifest::from_trace_list(&traces);
        let elapsed = now.elapsed();
        println!("Overwriting manifest file");
        let manifest_file = settings.manifest_file;
        manifest.to_file(manifest_file.as_path());
        // let prev_stats = statm_self().unwrap();
        let manifest = Manifest::from_file(manifest_file.as_path())
            .expect("Couldn't read manifest from cache");
        // let after_stats = statm_self().unwrap();
        let critical_paths = traces
            .iter()
            .filter_map(|t| CriticalPath::from_trace(t).ok())
            .collect::<Vec<CriticalPath>>();
        let groups = Group::from_critical_paths(critical_paths);

        // Start outputting stats
        eprintln!(
            "Trace count: {}, event count: {}",
            traces.len(),
            traces.iter().map(|t| t.g.node_count()).sum::<usize>()
        );
        eprintln!("Manifest construction took {:?}", elapsed);
        let output = Command::new("du")
            .arg("-sh")
            .arg(manifest_file)
            .output()
            .unwrap();
        eprint!(
            "Manifest size on disk:\n{}",
            String::from_utf8(output.stdout).unwrap()
        );
        eprintln!(
            "Manifest size in # of tracepoints: {}",
            manifest
                .per_request_type
                .iter()
                .map(|(_, p)| p.path_lengths().iter().sum::<usize>())
                .sum::<usize>()
        );
        // eprintln!(
        //     "Memory footprint (in pages):\nsize: {}, resident: {}, share: {}, text: {}, data: {}",
        //     after_stats.size - prev_stats.size,
        //     after_stats.resident - prev_stats.resident,
        //     after_stats.share - prev_stats.share,
        //     after_stats.text - prev_stats.text,
        //     after_stats.data - prev_stats.data
        // );
        let output = Command::new("getconf").arg("PAGESIZE").output().unwrap();
        eprint!(
            "Page size in bytes: {}",
            String::from_utf8(output.stdout).unwrap()
        );
        eprintln!(
            "Number of paths per request type:\n{}",
            manifest
                .per_request_type
                .iter()
                .map(|(k, v)| { format!("{}: {}", k, v.path_count()) })
                .join("\n")
        );
        eprintln!(
            "Total number of paths: {:?}, added paths: {:?}",
            manifest
                .per_request_type
                .iter()
                .map(|(_, v)| v.path_count())
                .sum::<usize>(),
            manifest
                .per_request_type
                .iter()
                .map(|(_, v)| v.added_paths)
                .sum::<usize>()
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
            .collect::<Vec<usize>>();
        eprintln!(
            "Min/Average/Max path length: {}, {}, {}",
            path_lens.iter().min().unwrap(),
            path_lens.iter().map(|&x| x as f64).sum::<f64>() / path_lens.len() as f64,
            path_lens.iter().max().unwrap(),
        );
        // Warm-up
        let _ = groups
            .iter()
            .take(10)
            .map(|t| manifest.match_performance(t))
            .collect::<Vec<_>>();
        let performances = groups
            .iter()
            .map(|t| manifest.match_performance(t))
            .collect::<Vec<_>>();
        eprintln!(
            "Time to match: min {:?}, max {:?}, mean {:?}",
            performances.iter().min().unwrap(),
            performances.iter().max().unwrap(),
            performances.iter().sum::<Duration>() / (performances.len() as u32)
        );
    // }
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
    let mut traces = reader.read_trace_file(manfile);
    if settings.application == ApplicationType::HDFS {
        for trace in &mut traces {
            trace.prune();
        }
    }
    manifest_from_traces(&traces, overwrite, &settings.manifest_file);
}

pub fn manifest_from_folder(trace_folder: &str) {
    let settings = Settings::read();
    let mut reader = reader_from_settings(&settings);
    reader.for_searchspace();
    let mut traces = reader.read_dir(trace_folder);
    println!("Read {} traces", traces.len());
    if settings.application == ApplicationType::HDFS {
        for trace in &mut traces {
            trace.prune();
        }
    }
    manifest_from_traces(&traces, false, &settings.manifest_file);
}

fn manifest_from_traces(traces: &Vec<Trace>, overwrite: bool, manifest_file: &PathBuf) {
    let now = Instant::now();
    let manifest = Manifest::from_trace_list(&traces);
    let elapsed = now.elapsed();
    println!("{}", manifest);
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
    eprintln!("Manifest construction took {:?}", elapsed);
}

pub fn measure_search_space_feasibility(trace_file: &str) {
    let settings = Settings::read();
    let mut reader = reader_from_settings(&settings);
    reader.for_searchspace();
    let mut trace = reader.read_file(trace_file);
    if settings.application == ApplicationType::HDFS {
        trace.prune();
    }
    println!("{}", Manifest::from_trace_list(&vec![trace]));
}

pub fn group_folder(trace_folder: &str) {
    let settings = Settings::read();
    let mut reader = reader_from_settings(&settings);
    // println!(trace_folder);
    let traces = reader.read_dir(trace_folder);
    println!("Read {} traces", traces.len());
    group_traces(traces);
}

pub fn group_from_ids(id_file: &str) {
    let settings = Settings::read();
    let mut reader = reader_from_settings(&settings);
    println!("{}",id_file);
    let file = File::open(id_file).unwrap();
    // println!("mertiko");
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
                "{}, {},{},\"{:?}\"",
                x.g.node_count(),
                //  {for node in x.g.node_indices() {
                //                                    for (key,value) in &x.g[node].key_value_pair.clone()
                //                                 {
                //                                   println!("{}: {:?}", key, value);
                //                                 break;
                //                           }
                //     break;
                //                                    }},
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
    let mut trace = reader.read_file(trace_file);
    if settings.application == ApplicationType::HDFS {
        trace.prune();
    }
    println!("{}", trace);
}

pub fn get_trace(trace_id: &str, to_file: bool, prune: bool) {
    
    let settings = Settings::read();
    let mut reader = reader_from_settings(&settings);
    
    let mut trace = reader.get_trace_from_base_id(trace_id).unwrap();
    
    if prune {
        trace.prune();
    }
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
    let settings = Settings::read();
    let mut reader = reader_from_settings(&settings);
    let trace = reader.get_trace_from_base_id(trace_id).unwrap();
    trace.get_keys();

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
        write!(f, "Pythia error: {}", self.0)
    }
}

impl Error for PythiaError {}

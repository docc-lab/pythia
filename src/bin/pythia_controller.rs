#[macro_use]
extern crate lazy_static;

use std::collections::HashSet;
use std::fs::File;
use std::io::prelude::*;
use std::thread::sleep;
use std::time::Duration;
use std::time::Instant;

use pythia::budget::BudgetManager;
use pythia::controller::OSProfilerController;
use pythia::critical::CriticalPath;
use pythia::critical::Path;
use pythia::grouping::GroupManager;
use pythia::manifest::Manifest;
use pythia::reader::reader_from_settings;
use pythia::search::get_strategy;
use pythia::settings::Settings;

lazy_static! {
    static ref SETTINGS: Settings = Settings::read();
    static ref CONTROLLER: OSProfilerController = OSProfilerController::from_settings(&SETTINGS);
    static ref MANIFEST: Manifest = Manifest::from_file(&SETTINGS.manifest_file.as_path())
        .expect("Couldn't read manifest from cache");
}

/// Main Pythia function that runs in a loop and makes decisions
fn main() {
    let now = Instant::now();
    let mut reader = reader_from_settings(&SETTINGS);
    let strategy = get_strategy(&SETTINGS, &MANIFEST, &CONTROLLER);
    let mut budget_manager = BudgetManager::from_settings(&SETTINGS);
    let mut groups = GroupManager::new();
    let mut last_decision = Instant::now();
    let mut last_gc = Instant::now();

    let filename = std::env::args().nth(1).unwrap();
    eprintln!("Printing results to {}", filename);
    let mut output_file = File::create(filename).unwrap();
    write!(output_file, "{:?}", *SETTINGS).ok();

    // Enable skeleton
    CONTROLLER.disable_all();
    let to_enable = MANIFEST
        .skeleton()
        .iter()
        .map(|&a| (a.clone(), None))
        .collect();
    CONTROLLER.enable(&to_enable);
    write!(output_file, "Enabled {}", to_enable.len()).ok();
    reader.reset_state();

    println!("Enabled following tracepoints: {:?}", to_enable);

    // Main pythia loop
    let mut jiffy_no = 0;
    loop {
        write!(output_file, "Jiffy {}, {:?}", jiffy_no, Instant::now()).ok();
        budget_manager.read_stats();
        budget_manager.print_stats();
        budget_manager.write_stats(&mut output_file);
        let over_budget = budget_manager.overrun();

        // Collect traces, increment groups
        let traces = reader.get_recent_traces();
        let critical_paths = traces
            .iter()
            .map(|t| CriticalPath::from_trace(t).unwrap())
            .collect();
        groups.update(&critical_paths);
        budget_manager.update_new_paths(&critical_paths);
        println!(
            "Got {} paths of duration {:?} at time {}us",
            traces.len(),
            critical_paths
                .iter()
                .map(|p| p.duration)
                .collect::<Vec<Duration>>(),
            now.elapsed().as_micros()
        );
        println!("Groups: {}", groups);
        write!(output_file, "New traces: {}", critical_paths.len()).ok();
        write!(
            output_file,
            "New tracepoints: {}",
            critical_paths
                .iter()
                .map(|p| p.g.g.node_count())
                .sum::<usize>()
        )
        .ok();

        if over_budget || last_gc.elapsed() > SETTINGS.gc_epoch {
            // Run garbage collection
            if over_budget {
                eprintln!("Over budget, disabling");
                let enabled_tracepoints: HashSet<_> =
                    CONTROLLER.enabled_tracepoints().drain(..).collect();
                let keep_count = enabled_tracepoints.len() * 9 / 10;
                let mut to_keep = HashSet::new();
                while to_keep.len() < keep_count {
                    for g in groups.problem_groups() {
                        let mut nidx = g.start_node;

                        while nidx != g.end_node {
                            if enabled_tracepoints
                                .get(&(g.at(nidx), Some(g.request_type)))
                                .is_none()
                            {
                                eprintln!(
                                    "{} is not enabled for {} but we got it",
                                    g.at(nidx),
                                    g.request_type
                                );
                            } else {
                                to_keep.insert((g.at(nidx), Some(g.request_type)));
                                if to_keep.len() > keep_count {
                                    break;
                                }
                            }
                            nidx = g.next_node(nidx).unwrap();
                        }
                    }
                }
                let mut to_disable = Vec::new();
                for tp in enabled_tracepoints {
                    if to_keep.get(&tp).is_none() {
                        to_disable.push(tp);
                    }
                }
                CONTROLLER.disable(&to_disable);
                write!(output_file, "Disabled {}", to_disable.len()).ok();
            }
            // Disable tracepoints not observed in critical paths
            let to_disable = budget_manager.old_tracepoints();
            CONTROLLER.disable(&to_disable);
            write!(output_file, "Enabled {}", to_disable.len()).ok();

            last_gc = Instant::now();
        }

        if !over_budget && last_decision.elapsed() > SETTINGS.decision_epoch {
            // Make decision
            let mut budget = SETTINGS.tracepoints_per_epoch;
            let problem_groups = groups.problem_groups();
            let mut used_groups = Vec::new();
            println!("Making decision. Top 10 problem groups:");
            for g in problem_groups.iter().take(10) {
                println!("{}", g);
            }
            for g in problem_groups {
                let problem_edges = g.problem_edges();

                println!("Top 10 edges of group {}:", g);
                for edge in problem_edges.iter().take(10) {
                    let endpoints = g.g.edge_endpoints(*edge).unwrap();
                    println!(
                        "({} -> {}): {}",
                        g.g[endpoints.0], g.g[endpoints.1], g.g[*edge]
                    );
                }
                for &edge in problem_edges.iter() {
                    if budget <= 0 {
                        break;
                    }
                    let decisions = strategy
                        .search(g, edge, budget)
                        .iter()
                        .take(budget)
                        .map(|&t| (t, Some(g.request_type)))
                        .collect::<Vec<_>>();
                    budget -= decisions.len();
                    CONTROLLER.enable(&decisions);
                    write!(output_file, "Enabled {}", decisions.len()).ok();
                    if decisions.len() > 0 {
                        used_groups.push(g.hash().to_string());
                    }
                }
                if budget <= 0 {
                    break;
                }
            }
            for g in used_groups {
                groups.used(&g);
            }

            last_decision = Instant::now();
        }

        jiffy_no += 1;
        sleep(SETTINGS.jiffy);
    }
}

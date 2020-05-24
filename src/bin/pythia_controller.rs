#[macro_use]
extern crate lazy_static;

// use std::collections::HashSet;
use std::fs::File;
use std::io::prelude::*;
use std::sync::mpsc::channel;
use std::thread::sleep;
use std::time::Duration;
use std::time::Instant;

use threadpool::ThreadPool;

use pythia::budget::BudgetManager;
use pythia::controller::controller_from_settings;
use pythia::controller::Controller;
use pythia::critical::CriticalPath;
use pythia::critical::Path;
use pythia::grouping::GroupManager;
use pythia::manifest::Manifest;
use pythia::reader::reader_from_settings;
use pythia::search::get_strategy;
use pythia::settings::Settings;
use pythia::trace::TracepointID;

lazy_static! {
    static ref SETTINGS: Settings = Settings::read();
    static ref CONTROLLER: Box<dyn Controller> = controller_from_settings(&SETTINGS);
    static ref MANIFEST: Manifest = Manifest::from_file(&SETTINGS.manifest_file.as_path())
        .expect("Couldn't read manifest from cache");
}

fn reset_reader() {
    let mut reader = reader_from_settings(&SETTINGS);
    reader.reset_state();
}

/// Main Pythia function that runs in a loop and makes decisions
fn main() {
    let now = Instant::now();
    let strategy = get_strategy(&SETTINGS, &MANIFEST, &CONTROLLER);
    let mut budget_manager = BudgetManager::from_settings(&SETTINGS);
    let mut groups = GroupManager::new();
    let mut last_decision = Instant::now();
    let mut last_gc = Instant::now();

    let mut quit_in = -1;
    let target = TracepointID::from_str("nova/usr/local/lib/python3.6/dist-packages/nova/compute/api.py:522:nova.compute.api.API._check_config_drive");
    eprintln!("Target is {}", target);

    let filename = std::env::args().nth(1).unwrap();
    eprintln!("Printing results to {}", filename);
    let mut output_file = File::create(filename).unwrap();
    writeln!(output_file, "{:?}", *SETTINGS).ok();
    writeln!(output_file, "Target: {}", target).ok();

    // Enable skeleton
    CONTROLLER.disable_all();
    let to_enable = MANIFEST
        .skeleton()
        .iter()
        .map(|a| {
            if *a == target {
                panic!("Target is in the skeleton");
            } else {
                a
            }
        })
        .map(|&a| (a.clone(), None))
        .collect();
    CONTROLLER.enable(&to_enable);
    writeln!(output_file, "Enabled {}", to_enable.len()).ok();
    writeln!(output_file, "Enabled {:?}", to_enable).ok();
    reset_reader();

    println!("Enabled following tracepoints: {:?}", to_enable);

    let pool = ThreadPool::new(SETTINGS.n_workers);
    let (tx, rx) = channel();
    for _ in 0..SETTINGS.n_workers {
        let tx = tx.clone();
        pool.execute(move || {
            let mut reader = reader_from_settings(&SETTINGS);
            loop {
                for trace in reader.get_recent_traces() {
                    tx.send(CriticalPath::from_trace(&trace).unwrap())
                        .expect("channel will be there waiting for the pool");
                }
                sleep(SETTINGS.jiffy);
            }
        });
    }

    // Main pythia loop
    let mut jiffy_no = 0;
    loop {
        writeln!(output_file, "Jiffy {}, {:?}", jiffy_no, Instant::now()).ok();
        budget_manager.read_stats();
        budget_manager.print_stats();
        budget_manager.write_stats(&mut output_file);
        let over_budget = budget_manager.overrun();

        // Collect traces, increment groups
        let critical_paths = rx.try_iter().collect::<Vec<_>>();
        groups.update(&critical_paths);
        budget_manager.update_new_paths(&critical_paths);
        println!(
            "Got {} paths of duration {:?} at time {}us",
            critical_paths.len(),
            critical_paths
                .iter()
                .map(|p| p.duration)
                .collect::<Vec<Duration>>(),
            now.elapsed().as_micros()
        );
        println!("Groups: {}", groups);
        writeln!(output_file, "New traces: {}", critical_paths.len()).ok();
        writeln!(
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
                eprintln!("Over budget, would disable but it's not implemented");
                // let enabled_tracepoints: HashSet<_> =
                //     CONTROLLER.enabled_tracepoints().drain(..).collect();
                // let keep_count =
                //     (enabled_tracepoints.len() as f32 * (1.0 - SETTINGS.disable_ratio)) as usize;
                // let mut to_keep = HashSet::new();
                // for g in groups.problem_groups() {
                //     let mut nidx = g.start_node;
                //     while nidx != g.end_node {
                //         if enabled_tracepoints
                //             .get(&(g.at(nidx), Some(g.request_type)))
                //             .is_none()
                //         {
                //             eprintln!(
                //                 "{} is not enabled for {} but we got it",
                //                 g.at(nidx),
                //                 g.request_type
                //             );
                //         } else {
                //             to_keep.insert((g.at(nidx), Some(g.request_type)));
                //             if to_keep.len() > keep_count {
                //                 break;
                //             }
                //         }
                //         nidx = g.next_node(nidx).unwrap();
                //     }

                //     if to_keep.len() > keep_count {
                //         break;
                //     }
                // }
                // let mut to_disable = Vec::new();
                // for tp in enabled_tracepoints {
                //     if to_keep.get(&tp).is_none() {
                //         to_disable.push(tp);
                //     }
                // }
                // CONTROLLER.disable(&to_disable);
                // writeln!(output_file, "Disabled {}", to_disable.len()).ok();
                // writeln!(output_file, "Disabled {:?}", to_disable).ok();
            }
            // Disable tracepoints not observed in critical paths
            let to_disable = budget_manager.old_tracepoints();
            CONTROLLER.disable(&to_disable);
            writeln!(output_file, "Disabled {}", to_disable.len()).ok();
            writeln!(output_file, "Disabled {:?}", to_disable).ok();

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
                    let endpoints = g.g.edge_endpoints(edge).unwrap();
                    println!(
                        "Searching ({} -> {}): {}",
                        g.g[endpoints.0], g.g[endpoints.1], g.g[edge]
                    );
                    let decisions = strategy
                        .search(g, edge, budget)
                        .iter()
                        .take(budget)
                        .map(|&t| (t, Some(g.request_type)))
                        .collect::<Vec<_>>();
                    budget -= decisions.len();
                    for d in &decisions {
                        if d.0 == target {
                            eprintln!("Found the target");
                            quit_in = 20;
                        }
                    }
                    CONTROLLER.enable(&decisions);
                    writeln!(output_file, "Enabled {}", decisions.len()).ok();
                    writeln!(output_file, "Enabled {:?}", decisions).ok();
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
        quit_in -= 1;
        if quit_in == 0 {
            eprintln!("Quitting");
            return;
        }

        jiffy_no += 1;
        sleep(SETTINGS.jiffy);
    }
}

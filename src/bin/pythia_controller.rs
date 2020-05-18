#[macro_use]
extern crate lazy_static;

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

    // Enable skeleton
    CONTROLLER.disable_all();
    let to_enable = MANIFEST
        .skeleton()
        .iter()
        .map(|&a| (a.clone(), None))
        .collect();
    CONTROLLER.enable(&to_enable);
    reader.reset_state();

    println!("Enabled following tracepoints: {:?}", to_enable);

    // Main pythia loop
    loop {
        budget_manager.read_stats();
        budget_manager.print_stats();
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

        if over_budget || last_gc.elapsed() > SETTINGS.gc_epoch {
            // Run garbage collection
            if over_budget {
                eprintln!("Over budget, disabling");
            }
            // Disable tracepoints not observed in critical paths
            CONTROLLER.disable(&budget_manager.old_tracepoints());

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
            for g in problem_groups.iter() {
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

        sleep(SETTINGS.jiffy);
    }
}

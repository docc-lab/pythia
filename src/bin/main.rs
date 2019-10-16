extern crate clap;
extern crate pythia;

use clap::{App, Arg, SubCommand};

use pythia::{disable_all, enable_all, enable_skeleton, get_manifest, get_trace, make_decision};

fn main() {
    let matches = App::new("Pythia")
        .version("1.0")
        .author("Emre Ates <ates@bu.edu>")
        .subcommand(
            SubCommand::with_name("manifest")
                .arg(Arg::with_name("manifest-file").required(true).index(1)),
        )
        .subcommand(
            SubCommand::with_name("get-trace")
                .arg(Arg::with_name("trace-id").required(true).index(1)),
        )
        .subcommand(
            SubCommand::with_name("diagnose")
                .arg(Arg::with_name("epoch-file").required(true).index(1))
                .arg(Arg::with_name("dry-run").long("dry-run")),
        )
        .subcommand(SubCommand::with_name("disable-all"))
        .subcommand(SubCommand::with_name("enable-all"))
        .subcommand(SubCommand::with_name("enable-skeleton"))
        .get_matches();
    match matches.subcommand() {
        ("manifest", Some(matches)) => {
            get_manifest(matches.value_of("manifest-file").unwrap());
        }
        ("get-trace", Some(matches)) => {
            get_trace(matches.value_of("trace-id").unwrap());
        }
        ("diagnose", Some(matches)) => {
            make_decision(
                matches.value_of("epoch-file").unwrap(),
                matches.occurrences_of("dry-run") > 0,
            );
        }
        ("disable-all", Some(_)) => {
            disable_all();
        }
        ("enable-all", Some(_)) => {
            enable_all();
        }
        ("enable-skeleton", Some(_)) => {
            enable_skeleton();
        }
        _ => panic!("Must provide a subcommand, see --help for commands"),
    };
}

extern crate pythia;
extern crate clap;

use clap::{Arg, App, SubCommand};

use pythia::{
    get_manifest,
    get_trace,
    make_decision,
    disable_all,
    enable_all,
    enable_skeleton,
};

fn main() {
    let matches = App::new("Pythia")
        .version("1.0")
        .author("Emre Ates <ates@bu.edu>")
        .subcommand(SubCommand::with_name("manifest")
            .arg(Arg::with_name("manifest_file")
                .required(true)
                .index(1)))
        .subcommand(SubCommand::with_name("get_trace")
            .arg(Arg::with_name("trace_id")
                .required(true)
                .index(1)))
        .subcommand(SubCommand::with_name("diagnose")
            .arg(Arg::with_name("epoch_file")
                .required(true)
                .index(1)))
        .subcommand(SubCommand::with_name("disable_all"))
        .subcommand(SubCommand::with_name("enable_all"))
        .subcommand(SubCommand::with_name("enable_skeleton"))
        .get_matches();
    match matches.subcommand() {
        ("manifest", Some(matches)) => {
            get_manifest(matches.value_of("manifest_file").unwrap());
        },
        ("get_trace", Some(matches)) => {
            get_trace(matches.value_of("trace_id").unwrap());
        },
        ("diagnose", Some(matches)) => {
            make_decision(matches.value_of("epoch_file").unwrap());
        },
        ("disable_all", Some(_)) => { disable_all(); },
        ("enable_all", Some(_)) => { enable_all(); },
        ("enable_skeleton", Some(_)) => { enable_skeleton(); },
        _ => panic!("Must provide a subcommand, see --help for commands")
    };
}

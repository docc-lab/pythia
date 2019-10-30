extern crate clap;
extern crate pythia;

use clap::{App, Arg, SubCommand};

use pythia::{
    disable_all, enable_all, enable_skeleton, get_crit, get_manifest, get_trace, make_decision,
    show_config, show_key_value_pairs, show_manifest,
};

fn main() {
    let matches = App::new("Pythia")
        .version("1.0")
        .author("Emre Ates <ates@bu.edu>")
        .subcommand(
            SubCommand::with_name("manifest")
                .arg(Arg::with_name("manifest-file").required(true).index(1))
                .arg(Arg::with_name("overwrite").long("overwrite")),
        )
        .subcommand(
            SubCommand::with_name("get-trace")
                .arg(Arg::with_name("trace-id").required(true).index(1)),
        )
        .subcommand(
            SubCommand::with_name("get-crit")
                .arg(Arg::with_name("trace-id").required(true).index(1)),
        )
        .subcommand(
            SubCommand::with_name("key-value")
                .arg(Arg::with_name("trace-id").required(true).index(1)),
        )
        .subcommand(
            SubCommand::with_name("diagnose")
                .arg(Arg::with_name("epoch-file").required(true).index(1))
                .arg(
                    Arg::with_name("budget")
                        .long("budget")
                        .short("b")
                        .takes_value(true),
                )
                .arg(Arg::with_name("dry-run").long("dry-run")),
        )
        .subcommand(SubCommand::with_name("disable-all"))
        .subcommand(
            SubCommand::with_name("show-manifest")
                .arg(Arg::with_name("request-type").required(true).index(1)),
        )
        .subcommand(SubCommand::with_name("enable-all"))
        .subcommand(SubCommand::with_name("enable-skeleton"))
        .subcommand(SubCommand::with_name("show-config"))
        .get_matches();
    match matches.subcommand() {
        ("manifest", Some(matches)) => {
            get_manifest(
                matches.value_of("manifest-file").unwrap(),
                matches.occurrences_of("overwrite") > 0,
            );
        }
        ("show-manifest", Some(matches)) => {
            show_manifest(matches.value_of("request-type").unwrap());
        }
        ("get-trace", Some(matches)) => {
            get_trace(matches.value_of("trace-id").unwrap());
        }
        ("get-crit", Some(matches)) => {
            get_crit(matches.value_of("trace-id").unwrap());
        }
        ("key-value", Some(matches)) => {
            show_key_value_pairs(matches.value_of("trace-id").unwrap());
        }
        ("diagnose", Some(matches)) => {
            make_decision(
                matches.value_of("epoch-file").unwrap(),
                matches.occurrences_of("dry-run") > 0,
                match matches.value_of("budget") {
                    Some(x) => x.parse::<usize>().ok().unwrap(),
                    None => 0,
                },
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
        ("show-config", Some(_)) => {
            show_config();
        }
        _ => panic!("Must provide a subcommand, see --help for commands"),
    };
}

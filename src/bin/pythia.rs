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

use clap::{App, Arg, SubCommand};
use std::time::Instant;

use pythia::{
    disable_all, disable_tracepoint, dump_traces, enable_all, enable_skeleton, get_crit,
    get_manifest, get_trace, group_folder, group_from_ids, manifest_from_folder, manifest_stats,
    measure_search_space_feasibility, read_trace_file, recent_traces, show_config,
    show_key_value_pairs, show_manifest,
};

fn main() {
    let now = Instant::now();
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
                .arg(Arg::with_name("trace-id").required(true).index(1))
                .arg(Arg::with_name("to-file").long("to-file"))
                .arg(Arg::with_name("prune").long("prune")),
        )
        .subcommand(
            SubCommand::with_name("manifest-folder")
                .arg(Arg::with_name("trace-folder").required(true).index(1)),
        )
        .subcommand(
            SubCommand::with_name("group-folder")
                .arg(Arg::with_name("trace-folder").required(true).index(1)),
        )
        .subcommand(
            SubCommand::with_name("group-ids")
                .arg(Arg::with_name("traceid-file").required(true).index(1)),
        )
        .subcommand(
            SubCommand::with_name("read-file")
                .arg(Arg::with_name("trace-file").required(true).index(1)),
        )
        .subcommand(
            SubCommand::with_name("dump-traces")
                .arg(Arg::with_name("trace-file").required(true).index(1)),
        )
        .subcommand(
            SubCommand::with_name("get-crit")
                .arg(Arg::with_name("trace-id").required(true).index(1)),
        )
        .subcommand(
            SubCommand::with_name("key-value")
                .arg(Arg::with_name("trace-id").required(true).index(1)),
        )
        .subcommand(SubCommand::with_name("disable-all"))
        .subcommand(SubCommand::with_name("recent-traces"))
        .subcommand(
            SubCommand::with_name("disable-tracepoint")
                .arg(Arg::with_name("tracepoint-id").required(true).index(1)),
        )
        .subcommand(
            SubCommand::with_name("try-manifest")
                .arg(Arg::with_name("trace-file").required(true).index(1)),
        )
        .subcommand(
            SubCommand::with_name("show-manifest")
                .arg(Arg::with_name("request-type").required(true).index(1)),
        )
        .subcommand(SubCommand::with_name("enable-all"))
        .subcommand(SubCommand::with_name("enable-skeleton"))
        .subcommand(SubCommand::with_name("show-config"))
        .subcommand(
            SubCommand::with_name("manifest-stats")
                .arg(Arg::with_name("manifest-file").required(true).index(1)),
        )
        .get_matches();
    match matches.subcommand() {
        ("manifest", Some(matches)) => {
            get_manifest(
                matches.value_of("manifest-file").unwrap(),
                matches.occurrences_of("overwrite") > 0,
            );
        }
        ("manifest-folder", Some(matches)) => {
            manifest_from_folder(matches.value_of("trace-folder").unwrap());
        }
        ("group-folder", Some(matches)) => {
            group_folder(matches.value_of("trace-folder").unwrap());
        }
        ("group-ids", Some(matches)) => {
            group_from_ids(matches.value_of("traceid-file").unwrap());
        }
        ("read-file", Some(matches)) => {
            read_trace_file(matches.value_of("trace-file").unwrap());
        }
        ("try-manifest", Some(matches)) => {
            measure_search_space_feasibility(matches.value_of("trace-file").unwrap());
        }
        ("show-manifest", Some(matches)) => {
            show_manifest(matches.value_of("request-type").unwrap());
        }
        ("dump-traces", Some(matches)) => {
            dump_traces(matches.value_of("trace-file").unwrap());
        }
        ("get-trace", Some(matches)) => {
            get_trace(
                matches.value_of("trace-id").unwrap(),
                matches.occurrences_of("to-file") > 0,
                matches.occurrences_of("prune") > 0,
            );
        }
        ("get-crit", Some(matches)) => {
            get_crit(matches.value_of("trace-id").unwrap());
        }
        ("disable-tracepoint", Some(matches)) => {
            disable_tracepoint(matches.value_of("tracepoint-id").unwrap());
        }
        ("key-value", Some(matches)) => {
            show_key_value_pairs(matches.value_of("trace-id").unwrap());
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
        ("recent-traces", Some(_)) => {
            recent_traces();
        }
        ("show-config", Some(_)) => {
            show_config();
        }
        ("manifest-stats", Some(matches)) => {
            manifest_stats(matches.value_of("manifest-file").unwrap());
        }
        _ => panic!("Must provide a subcommand, see --help for commands"),
    };
    eprintln!("Overall Pythia took {}us", now.elapsed().as_micros());
}

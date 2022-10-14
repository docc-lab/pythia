#!/bin/bash

pids=()

/local/tracing-pythia/workloads/create_delete_ip.sh ~/junk_traces.txt 1000 &
pids+=($!)

/local/tracing-pythia/workloads/create_delete_vm.sh ~/junk_traces.txt 1000 &
pids+=($!)

/local/tracing-pythia/workloads/create_delete_vm.sh ~/junk_traces.txt 1000 &
pids+=($!)

/local/tracing-pythia/workloads/create_delete_vm.sh ~/junk_traces.txt 1000 &
pids+=($!)

/local/tracing-pythia/workloads/create_delete_vm.sh ~/junk_traces.txt 1000 &
pids+=($!)

/local/tracing-pythia/workloads/usage_list.sh ~/junk_traces.txt 1000 &
pids+=($!)

/local/tracing-pythia/workloads/usage_list.sh ~/junk_traces.txt 1000 &
pids+=($!)

for pid in ${pids[@]}
do
    wait $pid
done

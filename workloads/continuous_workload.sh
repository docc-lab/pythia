#!/bin/bash

pids=()

~/pythia/workloads/create_delete_ip.sh ~/junk_traces.txt 1000 &
pids+=($!)

~/pythia/workloads/create_delete_vm.sh ~/junk_traces.txt 1000 &
pids+=($!)

~/pythia/workloads/create_delete_vm.sh ~/junk_traces.txt 1000 &
pids+=($!)

~/pythia/workloads/create_delete_vm.sh ~/junk_traces.txt 1000 &
pids+=($!)

~/pythia/workloads/create_delete_vm.sh ~/junk_traces.txt 1000 &
pids+=($!)

~/pythia/workloads/usage_list.sh ~/junk_traces.txt 1000 &
pids+=($!)

~/pythia/workloads/usage_list.sh ~/junk_traces.txt 1000 &
pids+=($!)

for pid in ${pids[@]}
do
    wait $pid
done

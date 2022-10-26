#!/bin/bash

# this script performs offline profiling

for i in $(find /opt/stack/manifest)
do
    echo 1 > $i
done

$HOME/tracing-pythia/workloads/create_delete_vm.sh ~/offline_traces.txt 40

pids=()
for i in `seq 40`
do
	$HOME/tracing-pythia/workloads/create_delete_vm.sh ~/offline_traces.txt 1 &
        pids+=($!)
done

for pid in ${pids[@]}
do
    wait $pid
done

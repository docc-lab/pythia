#!/bin/bash

# this script performs offline profiling
NUM_ITERS=8

redis-cli flushall
pythia enable-all
rm ~/offline_traces.txt

for script in "create_delete_ip" "create_delete_vm" "create_delete_vm" "create_delete_vm" "usage_list"
do
    ~/pythia/workloads/${script}.sh ~/offline_traces.txt $NUM_ITERS

    pids=()
    for i in `seq $NUM_ITERS`
    do
            ~/pythia/workloads/${script}.sh ~/offline_traces.txt 1 &
            pids+=($!)
    done

    for pid in ${pids[@]}
    do
        wait $pid
    done
done

sleep 300

while read -r line
do
    pythia get-trace $line > ~/$line.dot
done < ~/offline_traces.txt

#!/bin/bash

# this script launches 4 create/delete workloads concurrently

if [[ $# -ne 1 ]]
then
    echo "Usage: $0 <trace_file>"
    exit
fi

pids=()
for i in `seq 10`
do
	~/pythia/workloads/create_delete_vm.sh $1 1 &
        pids+=($!)
done

for pid in ${pids[@]}
do
    wait $pid
done

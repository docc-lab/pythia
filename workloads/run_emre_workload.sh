#!/bin/bash

set -x

# this script launches n create/delete workloads concurrently
# increase quota with `openstack quota set --instances n admin`

if [ "$#" -ne 1 ]
then
    echo "Please input number of concurrent workloads"
    exit
fi

$HOME/tracing-pythia/workloads/create_delete_no_OSP_workload.sh 1 -v
rm -r $HOME/trace-results

pids=()
for i in `seq $1`
do
	$HOME/tracing-pythia/workloads/create_delete_vm.sh 1 -v &
        pids+=($!)
	$HOME/tracing-pythia/workloads/create_delete_ip.sh 1 -v &
        pids+=($!)
	$HOME/tracing-pythia/workloads/create_delete_volume.sh 1 -v &
        pids+=($!)
done

for pid in ${pids[@]}
do
    wait $pid
done

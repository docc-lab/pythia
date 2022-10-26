#!/bin/bash
SECONDS=0
HMAC_KEY=Devstack1

# set verbosity of output
if [[ $2 = "-v" ]]
then
    set -x
fi

# set number of iterations
if [[ $1 =~ ^-?[0-9]+$ ]]
then
    iter=$1
else
    echo "Usage: ./create_delete_indiv_workload <iterations> (<-v> option)"
    exit 1
fi

timestamp() {
  date +"%T"
}

count=1


poll_creation () {
    # Waits for VM creation
    server_id=$1
    openstack server list &> $tmpfile
    server_status=$(grep $server_id $tmpfile | awk '{print $6}')
    if [[ $server_status == 'ERROR' ]]
    then
        delete_vm $server_id
        echo "Server entered ERROR state: exiting"
        exit
    fi

    if [[ $server_status != 'ACTIVE' ]]
    then
        sleep 2
        poll_creation $server_id
    fi
}

create_vm () {
    # Returns the server id
    openstack --os-profile $HMAC_KEY server create test_server --flavor m1.tiny --image cirros-0.4.0-x86_64-disk &> $tmpfile
    server_id=$(grep '| id' $tmpfile | awk '{print $4}')
    trace_id=$(grep 'Trace ID:' $tmpfile | awk '{print $3}')
    echo $trace_id $server_id
    poll_creation $server_id
}

delete_vm () {
    # Requires server id
    server_id=$1
    openstack --os-profile $HMAC_KEY server delete $server_id &> $tmpfile
    trace_id=$(grep 'Trace ID:' $tmpfile | awk '{print $3}')
    echo $trace_id
}

tmpfile=$(mktemp /tmp/workload.XXXXXX)

for i in `seq $iter`
do
    # create VM
    echo "Creating VM $count... "
    create_out=$(create_vm)
    create_traces[$i]=$(echo $create_out | awk '{print $1}')
    server=$(echo $create_out | awk '{print $2}')
    echo "$(timestamp): Created "${server}" (Trace_id: "${create_traces[$i]}")"
    echo "$(timestamp): Deleting "${server}...""

    # dump create trace
    sleep 30
    echo "Dumping 'create' for trace $count: "${create_traces[$i]}""
    $HOME/tracing-pythia/workloads/dump-trace ${create_traces[$i]} server_create

    # delete VM
    echo "Deleting "${server}...""
    delete_traces[$i]=$(delete_vm $server)

    # dump delete trace
    sleep 30
    echo "Dumping 'delete' for trace $count "${create_traces[$i]}""
    $HOME/tracing-pythia/workloads/dump-trace ${delete_traces[$i]} server_delete

    count=$((count+1))
done

rm $tmpfile

duration=$SECONDS
echo "END: $(timestamp) DURATION: $duration seconds"

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
    echo "Usage: ./create_delete_no_OSP_workload.sh <iterations> (<-v> option)"
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
    openstack server create test_server --flavor m1.tiny --image cirros-0.4.0-x86_64-disk &> $tmpfile
    server_id=$(grep '| id' $tmpfile | awk '{print $4}')
    echo $trace_id $server_id
    poll_creation $server_id
}

delete_vm () {
    # Requires server id
    server_id=$1
    openstack server delete $server_id &> $tmpfile
}

echo "START: $(timestamp)"

tmpfile=$(mktemp /tmp/workload.XXXXXX)

for i in `seq $iter`
do
    echo "$(timestamp): Creating VM $count..."
    create_out=$(create_vm)
    server=$(echo $create_out | awk '{print $2}')
    echo "$(timestamp): Created "${server}""
    echo "$(timestamp): Deleting "${server}...""
    delete_vm $server
    count=$((count+1))
done

rm $tmpfile

duration=$SECONDS
echo "END: $(timestamp) DURATION: $duration seconds"

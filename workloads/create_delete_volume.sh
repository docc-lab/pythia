#!/bin/bash
SECONDS=0
HMAC_KEY=Devstack1

# set verbosity of output
if [[ $3 = "-v" ]]
then
    set -x
fi

log() {
    echo "[$$] $(date +'%T'): $@"
}

# set number of iterations
if [[ $2 =~ ^-?[0-9]+$ ]]
then
    TRACE_FILE=$1
    iter=$2
else
    echo "Usage: $0 <trace_file> <iterations> (<-v> option)"
    exit 1
fi

poll_creation () {
    # Waits for VM creation
    local server_id
    local server_status
    server_id=$1
    openstack volume list &> $tmpfile
    server_status=$(grep $server_id $tmpfile | awk '{print $6}')
    if [[ $server_status == 'ERROR' ]]
    then
        delete_vm $server_id
        echo "Server entered ERROR state: exiting"
        exit
    fi
    if [[ $server_status != 'available' ]]
    then
        sleep 2
        poll_creation $server_id
    fi
}

create_volume () {
    # Returns the server id
    local server_id
    local trace_id
    openstack --os-profile $HMAC_KEY volume create --size 1 test_volume &> $tmpfile
    server_id=$(grep '| id' $tmpfile | awk '{print $4}')
    trace_id=$(grep 'Trace ID:' $tmpfile | awk '{print $3}')
    echo $trace_id >> $TRACE_FILE
    echo $server_id
    poll_creation $server_id
}

delete_volume () {
    # Requires server id
    local server_id
    local trace_id
    server_id=$1
    openstack --os-profile $HMAC_KEY volume delete $server_id &> $tmpfile
    trace_id=$(grep 'Trace ID:' $tmpfile | awk '{print $3}')
    echo $trace_id >> $TRACE_FILE
}

list_volume () {
    local trace_id
    openstack --os-profile $HMAC_KEY volume list &> $tmpfile
    trace_id=$(grep 'Trace ID:' $tmpfile | awk '{print $3}')
    echo $trace_id >> $TRACE_FILE
}

tmpfile=$(mktemp /tmp/workload.XXXXXX)
log "START: tmpfile is $tmpfile"

for i in `seq $iter`
do
    log "Creating volume $i ..."
    server=$(create_volume)
    log "Created "${server}
    list_volume
    log "Listed volumes"
    log "Deleting "${server}...""
    delete_volume $server
    log "Deleted "${server}
done

duration=$SECONDS
log "END: DURATION: $duration seconds"

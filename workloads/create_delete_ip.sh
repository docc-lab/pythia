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
    log "Usage: $0 <trace_file> <iterations> (<-v> option)"
    exit 1
fi

tmpfile=$(mktemp /tmp/workload.XXXXXX)
log "START: tmpfile is $tmpfile"

# Get network ID
openstack --os-profile $HMAC_KEY network list &> $tmpfile
NETWORK_ID=$(grep ext $tmpfile | awk '{print $2}')
echo $(grep 'Trace ID:' $tmpfile | awk '{print $3}') >> $TRACE_FILE

create_ip () {
    # Returns the server id
    local server_id
    local trace_id
    openstack --os-profile $HMAC_KEY floating ip create $NETWORK_ID &> $tmpfile
    server_id=$(grep '| id' $tmpfile | awk '{print $4}')
    trace_id=$(grep 'Trace ID:' $tmpfile | awk '{print $3}')
    echo $trace_id >> $TRACE_FILE
    echo $server_id
}

delete_ip () {
    # Requires server id
    local server_id
    local trace_id
    server_id=$1
    openstack --os-profile $HMAC_KEY floating ip delete $server_id &> $tmpfile
    trace_id=$(grep 'Trace ID:' $tmpfile | awk '{print $3}')
    echo $trace_id >> $TRACE_FILE
}

list_ip () {
    local trace_id
    openstack --os-profile $HMAC_KEY floating ip list &> $tmpfile
    trace_id=$(grep 'Trace ID:' $tmpfile | awk '{print $3}')
    echo $trace_id >> $TRACE_FILE
}


for i in `seq $iter`
do
    log "Creating IP $i ..."
    ip=$(create_ip)
    log "Created "${ip}
    sleep 2
    list_ip
    log "Listed IPs"
    log "Deleting "${ip}...""
    delete_ip $ip
    log "Deleted "${ip}
    sleep 2
done

duration=$SECONDS
log "END: DURATION: $duration seconds"

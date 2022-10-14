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


list_usage () {
    local trace_id
    nova --profile $HMAC_KEY usage-list &> $tmpfile
    trace_id=$(grep 'trace show' $tmpfile | awk '{print $5}')
    echo $trace_id >> $TRACE_FILE
}


tmpfile=$(mktemp /tmp/workload.XXXXXX)
log "START: tmpfile is $tmpfile"

for i in `seq $iter`
do
    log "Listing Usage..."
    list_usage
    sleep 2
done

duration=$SECONDS
log "END: DURATION: $duration seconds"

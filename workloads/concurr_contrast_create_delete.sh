#!/bin/bash

# this script first creates and deletes 5 servers, one by one.
# then, it launches into creating and deleting 10 servers at once.
# we should see that the timing and grouping look different for
# the first 5 versus the last 10.

# we do 5 here for a control group - not more because this takes so long
for i in `seq 5`
do
	~/pythia/workloads/create_delete_workload.sh 1
done

# we do 10 concurrent because any less does not seem to produce slow-down
for i in `seq 10`
do
	~/pythia/workloads/create_delete_workload.sh 1 &
done

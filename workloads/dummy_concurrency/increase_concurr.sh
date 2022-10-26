#!/bin/bash

# reference: https://stackoverflow.com/questions/3004811/how-do-you-run-multiple-programs-in-parallel-from-a-bash-script

./concur_wl1.sh &
P1=$!
sleep 10
./concur_wl2.sh &
P2=$!
sleep 10
./concur_wl3.sh &
P3=$!
wait $P1 $P2 $P3

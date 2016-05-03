#!/bin/bash

json_file=$1

if [ "$(grep warm $json_file | wc -l)" == "1" ]
then
    echo warm: $json_file
    cat $json_file
    python ./run_experiment.py $json_file nolog
    for trials in $(seq 1 1); do
        python ./run_experiment.py $json_file
    done
else
    echo cold: $json_file
    cat $json_file
    for trials in $(seq 1 1); do
        free && sync && echo 3 > /proc/sys/vm/drop_caches && free
        sleep 1
        python ./run_experiment.py $json_file
    done
fi

#!/bin/bash

json_file="${1:-}"
num_trials="${2:-1}"

if [ "$json_file" == "" ];
then
    echo "usage: run_experiment.sh json_filename [num_trials:1]"
    exit 1
fi

echo "Starting ${num_trials} trials on ${json_file}"

if [ "$(grep warm $json_file | wc -l)" == "1" ]
then
    echo warm: $json_file
    cat $json_file
    # First do a cold run and throw away the log.
    sudo bash -c "sync || sync || sync || sync"
    sudo bash -c "echo 3 > /proc/sys/vm/drop_caches"
    run_experiment.py "$json_file" log="/dev/null"

    # Now do x trials
    for trials in $(seq 1 $num_trials); do
        run_experiment.py "$json_file"
    done
else
    echo cold: $json_file
    cat $json_file
    for trials in $(seq 1 $num_trials); do
        free
        sudo bash -c "sync || sync || sync || sync"
        sudo bash -c "echo 3 > /proc/sys/vm/drop_caches"
        free
        sleep 1
        run_experiment.py "$json_file"
    done
fi

#!/bin/bash

json_file="${1:-}"
num_trials="${2:-1}"
did="${3:-normal}"
log_path="${4:-normal}"

if [ "$json_file" == "" ];
then
    echo "usage: run_experiment.sh json_filename [num_trials:1]"
    exit 1
fi

echo "Starting ${num_trials} trials on ${json_file}"

SPARK_OPTIONS="--driver-memory 300G --executor-memory 300G --num-executors 32 --conf spark.driver.maxResultSize=10g --packages com.databricks:spark-csv_2.11:1.4.0"

if [ "$(grep warm $json_file | wc -l)" == "1" ]
then
    echo warm: $json_file
    cat $json_file
    # First do a cold run and throw away the log.
    sudo bash -c "sync || sync || sync || sync"
    sudo bash -c "echo 3 > /proc/sys/vm/drop_caches"
    spark-submit $SPARK_OPTIONS $(which run_experiment.py) "$json_file" log="/dev/null" did="$did"

    # Now do x trials
    for trials in $(seq 1 $num_trials); do
        spark-submit $SPARK_OPTIONS $(which run_experiment.py) "$json_file" did="$did" log_path="$log_path"
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
        spark-submit $SPARK_OPTIONS $(which run_experiment.py) "$json_file" did="$did" log_path="$log_path"
    done
fi


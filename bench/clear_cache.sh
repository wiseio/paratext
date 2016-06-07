#!/usr/bin/env bash
#
# Ensure all writes are synced to disk.
sudo bash -c "sync || sync || sync || sync"
# Clear the caches
sudo bash -c "echo 3 > /proc/sys/vm/drop_caches"

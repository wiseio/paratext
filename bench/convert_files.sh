#!/bin/bash

# Convert original CSV files to different formats.

python convert.py mnist.csv mnist.feather mnist.hdf5 mnist.pkl mnist.npy
python convert.py messy.csv messy.feather messy.pkl messy.npy
python convert.py messy2.csv messy2.feather messy2.pkl messy2.npy
python convert.py mnist8m.csv mnist.feather mnist.hdf5 mnist.pkl mnist.npy


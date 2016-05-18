#!/bin/bash

# Convert original CSV files to different formats.

echo mnist
python convert_to_binary.py mnist.csv 1 mnist.feather mnist.hdf5 mnist.pkl mnist.npy
echo messy
python convert_to_binary.py messy.csv 1 messy.feather messy.pkl messy.npy
echo messy2
python convert_to_binary.py messy2.csv 1 messy2.feather messy2.pkl messy2.npy
echo mnist8m
python convert_to_binary.py mnist8m.csv 1 mnist8m.feather mnist8m.hdf5 mnist8m.pkl mnist8m.npy
echo car
python convert_to_binary.py car.csv 1 car.feather car.pkl car.npy
echo floats
python convert_to_binary.py floats.csv 0 floats.feather floats.pkl floats.npy floats.hdf5
python convert_to_binary.py floats2.csv 0 floats2.feather floats2.pkl floats2.npy floats2.hdf5
python convert_to_binary.py floats3.csv 0 floats3.feather floats3.pkl floats3.npy floats3.hdf5
python convert_to_binary.py floats4.csv 0 floats4.feather floats4.pkl floats4.npy floats4.hdf5


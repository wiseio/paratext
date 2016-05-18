#!/bin/bash

# Convert original CSV files to different formats.

echo mnist
python convert_to_binary.py 1 mnist.csv mnist.feather mnist.hdf5 mnist.pkl mnist.npy
echo messy
python convert_to_binary.py 1 messy.csv messy.feather messy.pkl messy.npy
echo messy2
python convert_to_binary.py 1 messy2.csv messy2.feather messy2.pkl messy2.npy
echo mnist8m
python convert_to_binary.py 1 mnist8m.csv mnist8m.feather mnist8m.hdf5 mnist8m.pkl mnist8m.npy
echo car
python convert_to_binary.py 1 car.csv car.feather car.pkl car.npy
echo floats
python convert_to_binary.py 0 floats.csv floats.feather floats.pkl floats.npy floats.hdf5
python convert_to_binary.py 0 floats2.csv floats2.feather floats2.pkl floats2.npy floats2.hdf5
python convert_to_binary.py 0 floats3.csv floats3.feather floats3.pkl floats3.npy floats3.hdf5
python convert_to_binary.py 0 floats4.csv floats4.feather floats4.pkl floats4.npy floats4.hdf5


#!/bin/bash

# Convert original CSV files to different formats.

echo mnist
python convert.py mnist.csv mnist.feather mnist.hdf5 mnist.pkl mnist.npy
echo messy
python convert.py messy.csv messy.feather messy.pkl messy.npy
echo messy2
python convert.py messy2.csv messy2.feather messy2.pkl messy2.npy
echo mnist8m
python convert.py mnist8m.csv mnist8m.feather mnist8m.hdf5 mnist8m.pkl mnist8m.npy
echo car
python convert.py car.csv car.feather car.pkl car.npy
echo floats
python convert.py floats.csv floats.feather floats.pkl floats.npy


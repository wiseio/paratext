#!/usr/bin/env python

#   Licensed to the Apache Software Foundation (ASF) under one
#   or more contributor license agreements.  See the NOTICE file
#   distributed with this work for additional information
#   regarding copyright ownership.  The ASF licenses this file
#   to you under the Apache License, Version 2.0 (the
#   "License"); you may not use this file except in compliance
#   with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0 
#
#   Unless required by applicable law or agreed to in writing,
#   software distributed under the License is distributed on an
#   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#   KIND, either express or implied.  See the License for the
#   specific language governing permissions and limitations
#   under the License.
#
#   Copyright (C) Wise.io, Inc. 2016.

import pandas
import pickle
import feather
import h5py
import numpy as np
import scipy.io as sio
import os
import sys

def convert_feather(df, output_filename):
    feather.write_dataframe(df, output_filename)

def convert_hdf5(df, output_filename):
    X = df.values
    f = h5py.File(output_filename, "w")
    ds=f.create_dataset("mydataset", X.shape, dtype=X.dtype)
    ds[...] = X

def convert_npy(df, output_filename):
    X = df.values
    np.save(output_filename, X)

def convert_pkl(df, output_filename):
    fid = open(output_filename, "wb")
    pickle.dump(df, fid)
    fid.close()

def convert_mat(df, output_filename):
    dd = {key: df[key].values.flatten() for key in df.keys()}
    sio.savemat(output_filename, dd)

input_filename = sys.argv[1]
has_header = int(sys.argv[2])
output_filenames = sys.argv[3:]

if not input_filename.endswith(".csv"):
    print "input must be a CSV file (by extension)"
    sys.exit(1)

if has_header:
    df = pandas.read_csv(input_filename)
else:
    df = pandas.read_csv(input_filename, header=None)

for output_filename in output_filenames:
    _, extension = os.path.splitext(output_filename)
    if extension == ".hdf5":
        convert_hdf5(df, output_filename)
    elif extension == ".feather":
        convert_feather(df, output_filename)
    elif extension == ".pkl":
        convert_pkl(df, output_filename)
    elif extension == ".npy":
        convert_npy(df, output_filename)
    elif extension == ".mat":
        convert_mat(df, output_filename)
    else:
        print "skipping '%s'; invalid output format '%s'" % (output_filename, extension)

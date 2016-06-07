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

# Usage: compile_log_files.py dir1 dir2 dir3 dir4 dir5

import numpy as np
import seaborn
import pandas
import sys
import os
import json

# Compiles the log files into a single log.

def get_dataset_key(fn):
    base_fn = os.path.basename(fn)
    base_fn = base_fn[:base_fn.find(".")]
    if base_fn == "car-pyspark":
        return "car"
    else:
        return base_fn

# The log directory X where the result files are stored.
#
# 1. It searches directories: X/cmdname/*.log for log files.
#
# 2. It outputs CSV files: log-X.csv
#

if len(sys.argv) > 2:
    for bench_name in ["avgcols", "countnl", "cPickle", "disk-to-mem", "feather", "hdf5", "noop", "npy", "numpy", "pandas", "paratext", "pickle", "pyspark", "R-readcsv", "R-fread", "R-readr", "sframe"]:
        df = pandas.DataFrame()
        for bench_dir in sys.argv[1:]:
            bench_subdir = os.path.join(bench_dir, bench_name)
            print bench_subdir
            if not os.path.exists(bench_subdir):
                continue
            bench_files = os.listdir(bench_subdir)
            for filename in bench_files:
                fn = os.path.join(bench_subdir, filename)
                print "opening ", fn
                bench_json = json.load(open(fn))
                log = bench_json["log"]
                mini_df = pandas.DataFrame()
                for i in xrange(0, len(log)):
                    for key in log[i].keys():
                        if log[i][key] == '?':
                            log[i][key] = None
                    mini_df = mini_df.append(log[i], ignore_index = True)
                    mini_df["log_key"] = filename.replace(".log","")
                df = df.append(mini_df)
        if bench_name in ["R-readcsv", "R-fread", "R-readr"]:
            df["mem"] = df["mem"] / 1000000
        if "filename" in df.keys():
            df["ds"] = df["filename"].apply(get_dataset_key)
        else:
            df["ds"] = '?'
        df.to_csv("log-" + bench_name + ".csv", index=False)
else:
    print "usage: gen_plot_files.py [log_dir1] [log_dir2]"

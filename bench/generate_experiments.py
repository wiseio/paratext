#!/usr/bin/env python

import sha
import json
import os

all_params = []

datasets = {"mnist":
            {"csv": "mnist.csv",
             "hdf5": "mnist.hdf5",
             "npy": "mnist.npy",
             "feather": "mnist.feather",
             "pickle": "mnist.pkl",
             "cPickle": "mnist.pkl",
             "no_header": True,
             "number_only": True,
             "to_df": True},
            "mnist8m":
            {"csv": "mnist8m.csv",
             "hdf5": "mnist8m.hdf5",
             "npy": "mnist8m.npy",
             "feather": "mnist8m.feather",
             "pickle": "mnist8m.pkl",
             "cPickle": "mnist8m.pkl",
             "no_header": True,
             "number_only": True,
             "to_df": False},
            "messy":
            {"csv": "messy.csv",
             "feather": "messy.feather",
             "pickle": "messy.pkl",
             "qnl": True,
             "no_header": False,
             "run_pyspark": False,
             "max_level_name_length": 0,
             "contains_text": True,
             "to_df": True},
            "messy2":
            {"csv": "messy2.csv",
             "feather": "messy2.feather",
             "pickle": "messy2.pkl",
             "qnl": True,
             "no_header": False,
             "run_pyspark": False,
             "max_level_name_length": 0,
             "contains_text": True,
             "to_df": True},
            "car":
            {"csv": "car.csv",
             "feather": "car.feather",
             "pickle": "car.pkl",
             "qnl": False,
             "no_header": False,
             "contains_text": True,
             "to_df": True},
            "floats":
            {"csv": "floats.csv",
             "feather": "floats.feather",
             "hdf5": "floats.hdf5",
             "npy": "floats.npy",
             "no_header": False,
             "pickle": "floats.pkl",
             "to_df": True},
            "floats2":
            {"csv": "floats2.csv",
             "feather": "floats2.feather",
             "hdf5": "floats2.hdf5",
             "npy": "floats2.npy",
             "no_header": False,
             "pickle": "floats2.pkl",
             "to_df": True},
            "floats3":
            {"csv": "floats3.csv",
             "feather": "floats3.feather",
             "hdf5": "floats3.hdf5",
             "npy": "floats3.npy",
             "no_header": False,
             "pickle": "floats3.pkl",
             "to_df": True},
            "floats4":
            {"csv": "floats4.csv",
             "feather": "floats4.feather",
             "hdf5": "floats4.hdf5",
             "npy": "floats4.npy",
             "no_header": False,
             "pickle": "floats4.pkl",
             "to_df": True}
             }

scaling_experiments = bool(raw_input("enter 'yes' to do scaling experiments, 'no' to do main benchmarks: ").lower() == 'yes')

print "available datasets: ", datasets.keys()
restrict_keys = raw_input("enter comma-delimited list of datasets to generate experiment json [enter for all]: ")

if restrict_keys != "":
    restrict_keys = set(restrict_keys.split(","))
    for key in datasets.keys():
        if key not in restrict_keys:
            datasets.pop(key)

for name, attr in datasets.iteritems():
    if "csv" in attr:
        csv_filename = attr["csv"]
        for disk_state in ["cold", "warm"]:
            if scaling_experiments:
                num_threads_list = [1,4,8,12,16,20,24,28,32]
            else:
                num_threads_list = [0]
            for num_threads in num_threads_list:
                for block_size in [32768]:
                    if not attr.get("contains_text", False):
                        for type_check in [True, False]:
                            params = {"cmd": "avgcols",
                                      "filename": attr["csv"],
                                      "no_header": attr.get("no_header", True),
                                      "allow_quoted_newlines": attr.get("qnl", False),
                                      "num_threads": num_threads,
                                      "disk_state": disk_state,
                                      "block_size": block_size,
                                      "to_df": True,
                                      "sum_after": True,
                                      "type_check": type_check,
                                      "log": str(len(all_params)) + ".log"}
                            all_params.append(params)
                    for cmd in ["disk-to-mem", "countnl", "paratext"]:
                        params = {"cmd": cmd,
                                  "filename": attr["csv"],
                                  "no_header": attr.get("no_header", True),
                                  "allow_quoted_newlines": attr.get("qnl", False),
                                  "num_threads": num_threads,
                                  "disk_state": disk_state,
                                  "block_size": block_size,
                                  "to_df": True,
                                  "sum_after": True,
                                  "log": str(len(all_params)) + ".log"}
                        if attr.get("number_only", False):
                            params["number_only"] = True
                        mlnl = attr.get("max_level_name_length", None)
                        if mlnl:
                            params["max_level_name_length"] = mlnl
                        all_params.append(params)
        for disk_state in ["cold", "warm"]:
            if attr.get("run_pyspark", True):
                params = {"cmd": "pyspark",
                          "filename": attr["csv"],
                          "no_header": attr.get("no_header", True),
                          "to_df": attr.get("to_df", False),
                          "sum_after": True,
                          "disk_state": disk_state}
                all_params.append(params)

            if params.get("number_only", True):
                params = {"cmd": "numpy",
                          "filename": attr["csv"],
                          "no_header": attr.get("no_header", True),
                          "sum_after": True,
                          "disk_state": disk_state}
                all_params.append(params)                

            for cmd in ["sframe", "pandas", "R-readcsv", "R-readr", "R-fread"]:
                params = {"cmd": cmd,
                          "filename": attr["csv"],
                          "no_header": attr.get("no_header", True),
                          "to_df": attr.get("to_df", False),
                          "sum_after": True,
                          "disk_state": disk_state}
                all_params.append(params)

            for cmd in ["feather", "hdf5", "pickle", "cPickle", "npy"]:
                if cmd in attr:
                    params = {"cmd": cmd,
                              "filename": attr[cmd],
                              "sum_after": True,
                              "disk_state": disk_state}
                if cmd == "hdf5":
                    params["dataset"] = "mydataset"
                all_params.append(params)

if "mnist8m" in datasets.keys():
    for cmd in ["sframe", "paratext", "pyspark"]:
        params = {"cmd": cmd,
                  "filename": "mnist8m.csv",
                  "no_header": True,
                  "to_df": True,
                  "sum_after": True,
                  "disk_state": disk_state}
        all_params.append(params)

params = {"cmd": "noop"}
all_params.append(params)

for i, params in enumerate(all_params):
    hparams = sha.sha(json.dumps(params)).hexdigest()
    prefix = hparams[0:8]
    params["log"] = os.path.join(params["cmd"], "run-" + prefix + ".log")
    if not os.path.exists(params["cmd"]):
        os.makedirs(params["cmd"])
    json.dump(params, open(os.path.join(params["cmd"], "run-" + hparams[0:8] + ".json"), "w"), indent=1)

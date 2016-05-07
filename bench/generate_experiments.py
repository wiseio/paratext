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
             "to_df": True},
            "messy2":
            {"csv": "messy2.csv",
             "feather": "messy2.feather",
             "pickle": "messy2.pkl",
             "qnl": True,
             "no_header": False,
             "run_pyspark": False,
             "max_level_name_length": 0,
             "to_df": True},
            "car":
            {"csv": "car.csv",
             "feather": "car.feather",
             "pickle": "car.pkl",
             "qnl": False,
             "no_header": False,
             "to_df": True},
            "floats":
            {"csv": "floats.csv",
             "feather": "floats.feather",
             "hdf5": "floats.hdf5",
             "npy": "floats.npy",
             "no_header": False,
             "pickle": "floats.pkl",
             "to_df": True}}

for name, attr in datasets.iteritems():
    if "csv" in attr:
        csv_filename = attr["csv"]
        for disk_state in ["cold", "warm"]:
            for num_threads in [1,4,8,12,16,20]:
                for block_size in [1048576]:
                    cmds = ["disk-to-mem", "countnl", "paratext"]
                    if attr.get("number_only", False):
                        cmds.append("avgcols")
                    for cmd in cmds:
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

            # pandas and sframes without type hints
            for cmd in ["sframe", "pandas"]:
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

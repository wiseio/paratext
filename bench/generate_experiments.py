import sha
import json

all_params = []

datasets = {"mnist":
            {"csv": "mnist.csv",
             "hdf5": "mnist.hdf5",
             "npy": "mnist.npy",
             "feather": "mnist.feather",
             "pickle": "mnist.pkl",
             "cPickle": "mnist.pkl"},
            "mnist8m":
            {"csv": "mnist8m.csv",
             "hdf5": "mnist8m.hdf5",
             "npy": "mnist8m.npy",
             "feather": "mnist8m.feather",
             "pickle": "mnist8m.pkl",
             "cPickle": "mnist8m.pkl"},
            "messy":
            {"csv": "messy.csv",
             "feather": "messy.feather",
             "pickle": "messy.pkl",
             "qnl": True},
            "messy2":
            {"csv": "messy2.csv",
             "feather": "messy2.feather",
             "pickle": "messy2.pkl",
             "qnl": True},
            "floats":
            {"csv": "floats.csv",
             "feather": "floats.feather",
             "hdf5": "floats.hdf5",
             "npy": "floats.npy",
             "pickle": "floats.pkl"}}

for name, files in datasets.iteritems():
    if "csv" in files:
        csv_filename = files["csv"]
        for disk_state in ["cold", "warm"]:
            for num_threads in [1,4,8,12,16,20]:
                for block_size in [1048576]:
                    for cmd in ["avgcols", "memcopy", "countnl", "paratext"]:
                        params = {"cmd": cmd,
                                  "filename": files["csv"],
                                  "no_header": True,
                                  "allow_quoted_newlines": files.get("qnl", False),
                                  "num_threads": num_threads,
                                  "disk_state": disk_state,
                                  "block_size": block_size,
                                  "log": str(len(all_params)) + ".log"}
                        all_params.append(params)
        for cmd in ["sframe", "pandas", "numpy"]:
            params = {"cmd": cmd,
                      "filename": "mnist8m.csv",
                      "no_header": True,
                      "type_hints_json": "mnist-hints.json",
                      "disk_state": disk_state}
            all_params.append(params)

    for cmd in ["feather", "hdf5", "pickle", "cPickle", "npy"]:
        if cmd in files:
            params = {"cmd": cmd,
                      "filename": files[cmd],
                      "dataset": "mydataset",
                      "no_header": True,

                      "disk_state": disk_state}
    all_params.append(params)

params = {"cmd": "noop"}
all_params.append(params)

for i, params in enumerate(all_params):
    hparams = sha.sha(json.dumps(params)).hexdigest()
    prefix = hparams[0:8]
    params["log"] = "run-" + prefix + ".log"
    json.dump(params, open("run-" + hparams[0:8] + ".json", "w"), indent=1)

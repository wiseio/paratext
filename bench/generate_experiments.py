import sha
import json

all_params = []

csv_filename = "mnist8m.csv"
for disk_state in ["cold", "warm"]:
    for num_threads in [1,4,8,12,16,20]:
        for block_size in [1048576]:
            for cmd in ["avgcols", "memcopy", "countnl", "paratext"]:
                params = {"cmd": cmd,
                          "filename": "mnist8m.csv",
                          "no_header": True,
                          "allow_quoted_newlines": False,
                          "num_threads": num_threads,
                          "disk_state": disk_state,
                          "block_size": block_size,
                          "log": str(len(all_params)) + ".log"}
                all_params.append(params)
    params = {"cmd": cmd,
              "filename": "mnist8m.csv",
              "no_header": True,
              "allow_quoted_newlines": False,
              "type_hints_json": "mnist-hints.json",
              "num_threads": num_threads,
              "disk_state": disk_state,
              "block_size": block_size,
              "log": str(len(all_params)) + ".log"}
    all_params.append(params)

for i, params in enumerate(all_params):
    json.dump(params, open(str(i) + "-experiment.json", "w"), indent=1)

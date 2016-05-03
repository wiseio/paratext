import sys
import os
import paratext
import pandas
import json
import time
import numpy as np

types = {"num_threads": int,
         "block_size": int,
         "allow_quoted_newlines": bool,
         "type_hints_json": str,
         "no_header": bool}

def memory_usage_psutil():
    # return the memory usage in MB
    import psutil
    process = psutil.Process(os.getpid())
    mem = process.get_memory_info()[0] / float(2 ** 20)
    return mem

def run_memcopy_baseline(params):
    d = paratext.baseline_memcopy(params["filename"], block_size=params.get("block_size", 1048576), num_threads=params.get("num_threads", 1))
    return d

def run_average_columns_baseline(params):
    avg = paratext.baseline_average_columns(params["filename"], block_size=params.get("block_size", 1048576), num_threads=params.get("num_threads", 1), no_header=params.get("no_header", False), allow_quoted_newlines=params.get("allow_quoted_newlines", False))
    return avg

def run_paratext(params):
    avg = paratext.load_csv_to_dict(params["filename"], block_size=params.get("block_size", 1048576), num_threads=params.get("num_threads", 1), no_header=params.get("no_header", False), allow_quoted_newlines=params.get("allow_quoted_newlines", False))
    return avg

def run_count_newlines_baseline(params):
    count = paratext.baseline_newline_count(params["filename"], block_size=params.get("block_size", 1048576), num_threads=params.get("num_threads", 1), no_header=params.get("no_header", False))
    return count

def run_pandas(filename, type_hints_json=None, no_header=False):
    if no_header:
        header = None
    else:
        header = 1
    if type_hints_json is not None:
        type_hints = json.load(open(type_hints_json))
        dtypes = {}
        for key in type_hints.keys():
            dtypes[int(key)] = eval("np." + type_hints[key])
        df = pandas.read_csv(filename, dtype=dtypes, header=header)
    else:
        df = pandas.read_csv(filename, header=header)
    return df

def run_feather(filename):
    df = feather.read_dataframe(filename)
    pass

def generate_params(json_filename, types):
    """
    params = {}
    noop = lambda x: x
    if len(argv) == 0:
        return "", {}
    start = 0
    if "=" not in argv[0]:
        cmd = argv[0]
        start = 1
    for arg in args[start:]:
        x = arg.split("=")
        if len(x) == 2:
            key, value = x[0], x[1]
            params[key] = types.get(key, noop)(value)
    """
    params = json.load(open(json_filename))
    for key in params.keys():
        if type(params[key]) == unicode:
            params[key] = params[key].encode("utf-8")
    return params

def main():
    types = {"num_threads": int,
             "block_size": int,
             "allow_quoted_newlines": bool,
             "type_hints_json": str,
             "filename": str,
             "log": str}
    params = generate_params(sys.argv[1], types)
    cmd = params.get("cmd", "")
    tic = time.time()
    logfn = None
    if "log" in params:
        logfn = params["log"]
        params.pop("log")
    if cmd == "feather":
        retval = run_feather(params)
    elif cmd == "paratext":
        retval = run_paratext(params)
    elif cmd == "pandas":
        retval = run_pandas(params)
    elif cmd == "memcopy":
        retval = run_memcopy_baseline(params)
    elif cmd == "avgcols":
        retval = run_average_columns_baseline(params)
    elif cmd == "pandas":
        retval = run_pandas(params)
    elif cmd == "countnl":
        retval = run_count_newlines_baseline(params)
    elif cmd == "noop":
        retval = 0
    else:
        print "Command not found: '%s'" % cmd
    toc = time.time()
    runtime=toc-tic
    mem=memory_usage_psutil()
    log_entry = params.copy()
    log_entry["runtime"] = runtime
    log_entry["mem"] = mem
    if "filename" in params:
        s = os.stat(params["filename"])
        sz = s.st_size
        log_entry["filesize"] = sz
        log_entry["tput_bytes"] = sz / runtime
        log_entry["tput_MB"] = (sz/1000000) / runtime
        log_entry["tput_MiB"] = (sz/1048576) / runtime
    if logfn is None:
        print log_entry
    else:
        log_fid = open(logfn, "a")
        log_fid.write(json.dumps(log_entry))
    retval = None

if __name__ == "__main__":
    main();




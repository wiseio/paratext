# Import standard library modules needed for the benchmarks.
import sys
import os
import json
import time
import resource

# Import the libraries that we will benchmark.
import paratext
import pandas
import h5py
import feather
import numpy as np
import pickle
import cPickle
import sframe

def sum_sframe(df):
    s = {}
    for key in df.column_names():
        if df[key].dtype() in (str, unicode):
            s[key] = df[key].apply(lambda x: len(x)).sum()
        else:
            s[key] = df[key].sum()
    return s

def sum_dataframe(df):
    s = {}
    for key in df.keys():
        if df[key].dtype in (str, unicode, np.object):
            s[key] = df[key].apply(lambda x: len(x)).sum()
        else:
            s[key] = df[key].sum()
    return s

def sum_dictframe(d, levels):
    s = {}
    for key in d.keys():
        if key in levels.keys():
            level_sums = np.array([len(el) for el in levels[key]])
            s[key] = level_sums[d[key]].sum()
        elif d[key].dtype == np.object_:
            s[key] = level
        else:
            s[key] = d[key].sum()
    return s

def memory_usage_resource():
    "This function is from Fabian Pedregosa's blog post on measuring python memory usage... http://bit.ly/26RguwI"
    import resource
    rusage_denom = 1024.
    if sys.platform == 'darwin':
        rusage_denom = rusage_denom * rusage_denom
    mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / rusage_denom
    return mem

def run_disk_to_mem_baseline(params):
    d = paratext.baseline_disk_to_mem(params["filename"], block_size=params.get("block_size", 1048576), num_threads=params.get("num_threads", 1))
    return {}

def run_average_columns_baseline(params):
    avg = paratext.baseline_average_columns(params["filename"], block_size=params.get("block_size", 1048576), num_threads=params.get("num_threads", 1), no_header=params.get("no_header", False), allow_quoted_newlines=params.get("allow_quoted_newlines", False))
    return {}

def run_paratext(params):
    d, levels = paratext.load_csv_to_dict(params["filename"], block_size=params.get("block_size", 1048576), num_threads=params.get("num_threads", 1), no_header=params.get("no_header", False), allow_quoted_newlines=params.get("allow_quoted_newlines", False))
    sum_time = 0
    if params.get("sum_after", False):
        sum_tic = time.time()
        s = sum_dictframe(d, levels)
        sum_toc = time.time()
        sum_time = sum_toc - sum_tic
    return {"sum_time": sum_time}

def run_count_newlines_baseline(params):
    count = paratext.baseline_newline_count(params["filename"], block_size=params.get("block_size", 1048576), num_threads=params.get("num_threads", 1), no_header=params.get("no_header", False))
    return {}

def run_pandas(params): #filename, type_hints_json=None, no_header=False):
    if params["no_header"]:
        header = None
    else:
        header = 1
    type_hints_json_fn = params.get("type_hints_json", None)
    if type_hints_json_fn is not None:
        type_hints = json.load(open(type_hints_json_fn))
        dtypes = {}
        for key in type_hints.keys():
            dtypes[int(key)] = eval("np." + type_hints[key])
        df = pandas.read_csv(params["filename"], dtype=dtypes, header=header)
    else:
        df = pandas.read_csv(params["filename"], header=header)
    sum_time = 0
    if params.get("sum_after", False):
        sum_tic = time.time()
        s = df.sum(numeric_only=True)
        sum_toc = time.time()
        sum_time = sum_toc - sum_tic
    return {"sum_time": sum_time}

def run_numpy(params):
    if params.get("no_header", False):
        skiprows = 0
    else:
        skiprows = 1
    if "dtype" in params:
        dtype = eval("np." + params["dtype"])
    else:
        dtype = float
    X = np.loadtxt(params["filename"], dtype=dtype, skiprows=skiprows, delimiter=',')
    sum_time = 0
    if params.get("sum_after", False):
        sum_tic = time.time()
        s = X.sum()
        sum_toc = time.time()
        sum_time = sum_toc - sum_tic
    return {"sum_time": sum_time}

def run_pickle(params):
    fid = open(params["filename"])
    df = pickle.load(fid)
    fid.close()
    sum_time = 0
    sum_after = params.get("sum_after", False)
    if sum_after:
        print ">>>", sum_after, type(sum_after)
        sum_tic = time.time()
        s = df.sum(numeric_only=True)
        sum_toc = time.time()
        sum_time = sum_toc - sum_tic
    return {"sum_time": sum_time}

def run_cPickle(params):
    fid = open(params["filename"])
    df = cPickle.load(fid)
    fid.close()
    sum_time = 0
    if params.get("sum_after", False):
        sum_tic = time.time()
        s = df.sum(numeric_only=True)
        sum_toc = time.time()
        sum_time = sum_toc - sum_tic
    return {"sum_time": sum_time}

def run_npy(params):
    X = numpy.load(params["filename"])
    sum_time = 0
    if params.get("sum_after", False):
        sum_tic = time.time()
        s = X.sum()
        sum_toc = time.time()
        sum_time = sum_toc - sum_tic
    return {"sum_time": sum_time}

def run_sframe(params):
    type_hints_json_fn = params.get("type_hints_json", None)
    dtypes = {}
    if type_hints_json_fn is not None:
        type_hints = json.load(open(type_hints_json_fn))
        for key in type_hints.keys():
            dtypes["X" + str(int(key) + 1)] = eval("np." + type_hints[key])
    header = not params.get("no_header", False)
    df = sframe.SFrame.read_csv(params["filename"], header=header, column_type_hints=dtypes)
    sum_time = 0
    if params.get("sum_after", False):
        sum_tic = time.time()
        s = sum_sframe(df)
        sum_toc = time.time()
        sum_time = sum_toc - sum_tic
    return {"sum_time": sum_time}

def run_hdf5(params):
    f = h5py.File(params["filename"])
    ds = f[params["dataset"]]
    X = ds[:, :]
    sum_time = 0
    if params.get("sum_after", False):
        sum_tic = time.time()
        s = X.sum()
        sum_toc = time.time()
        sum_time = sum_toc - sum_tic
    return {"sum_time": sum_time}

def run_feather(params):
    df = feather.read_dataframe(params["filename"])
    sum_time = 0
    if params.get("sum_after", False):
        sum_tic = time.time()
        s = sum_dataframe(df)
        sum_toc = time.time()
        sum_time = sum_toc - sum_tic
    return {"sum_time": sum_time}

def generate_params(json_filename, args, types):
    """
    Generate a parameters dict from the JSON and potentially command
    line args that were passed as key=value args.

       ./run_experiment.py - cmd=pandas filename=my.csv
    """
    if json_filename == "-":
        params = {}
    else:
        params = json.load(open(json_filename))
    noop = lambda x: x
    for arg in args:
        x = arg.split("=")
        if len(x) == 2:
            key, value = x[0], x[1]
            params[key] = types.get(key, noop)(value)
    for key in params.keys():
        if type(params[key]) == unicode:
            params[key] = params[key].encode("utf-8")
    return params

def main():
    bool_converter = lambda x: x in ["True", "true", True]
    param_types = {"num_threads": int,
                   "block_size": int,
                   "allow_quoted_newlines": bool_converter,
                   "type_hints_json": str,
                   "no_header": bool_converter,
                   "sum_after": bool_converter,
                   "filename": str,
                   "log": str}
    params = generate_params(sys.argv[1], sys.argv[2:], param_types)
    cmd = params.get("cmd", "")
    tic = time.time()
    logfn = None
    if "log" in params:
        logfn = params["log"]
        params.pop("log")
    results = {}
    if cmd == "feather":
        results = run_feather(params)
    elif cmd == "paratext":
        results = run_paratext(params)
    elif cmd == "pandas":
        results = run_pandas(params)
    elif cmd == "numpy":
        results = run_numpy(params)
    elif cmd == "hdf5":
        results = run_hdf5(params)
    elif cmd == "npy":
        results = run_npy(params)
    elif cmd == "pickle":
        results = run_pickle(params)
    elif cmd == "cPickle":
        results = run_cPickle(params)
    elif cmd == "sframe":
        results = run_sframe(params)
    elif cmd == "disk_to_mem":
        results = run_disk_to_mem_baseline(params)
    elif cmd == "avgcols":
        results = run_average_columns_baseline(params)
    elif cmd == "countnl":
        results = run_count_newlines_baseline(params)
    elif cmd == "noop":
        results = {}
    else:
        print "Command not found: '%s'" % cmd
    toc = time.time()
    runtime=toc-tic
    mem=memory_usage_resource()
    log_entry = params.copy()
    log_entry["runtime"] = runtime
    log_entry["mem"] = mem
    # Copy run-specific results into the log-entry
    for key in results.keys():
        log_entry[key] = results[key]
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
        old_log = {"log": []}
        if os.path.isfile(logfn):
            old_log_fid = open(logfn, "r")
            old_log = json.load(old_log_fid)
            old_log_fid.close()
        old_log["log"].append(log_entry)
        log_fid = open(logfn, "w")
        json.dump(old_log, log_fid)
        log_fid.close()
    results = None

if __name__ == "__main__":
    main()




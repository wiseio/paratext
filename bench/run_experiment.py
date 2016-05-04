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

types = {"num_threads": int,
         "block_size": int,
         "allow_quoted_newlines": bool,
         "type_hints_json": str,
         "no_header": bool}

def sum_sframe(df):
    s = {}
    for key in df.column_names():
        if df[key].dtype != np.object_:
            s[key] = df[key].sum()
    return s


def memory_usage_resource():
    "This function is from Fabian Pedregosa's blog... http://bit.ly/26RguwI"
    import resource
    rusage_denom = 1024.
    if sys.platform == 'darwin':
        rusage_denom = rusage_denom * rusage_denom
    mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / rusage_denom
    return mem

def run_memcopy_baseline(params):
    d = paratext.baseline_memcopy(params["filename"], block_size=params.get("block_size", 1048576), num_threads=params.get("num_threads", 1))
    return d

def run_average_columns_baseline(params):
    avg = paratext.baseline_average_columns(params["filename"], block_size=params.get("block_size", 1048576), num_threads=params.get("num_threads", 1), no_header=params.get("no_header", False), allow_quoted_newlines=params.get("allow_quoted_newlines", False))
    return avg

def run_paratext(params):
    d, levels = paratext.load_csv_to_dict(params["filename"], block_size=params.get("block_size", 1048576), num_threads=params.get("num_threads", 1), no_header=params.get("no_header", False), allow_quoted_newlines=params.get("allow_quoted_newlines", False))
    if params.get("sum_after", False):
        s = {}
        levels = None
        for key in df.keys():
            if key not in levels and d[keys].dtype != np.object_:
                np.array([len(el) for el in levels[key]])
                d[key] = levels[key][d[key]]
    return d, levels

def run_count_newlines_baseline(params):
    count = paratext.baseline_newline_count(params["filename"], block_size=params.get("block_size", 1048576), num_threads=params.get("num_threads", 1), no_header=params.get("no_header", False))
    return count

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
    if params.get("sum_after", False):
        s = df.sum(numeric_only=True)
    return df

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
    if params.get("sum_after", False):
        s = X.sum()
    return X

def run_pickle(params):
    fid = open(params["filename"])
    df = pickle.load(fid)
    fid.close()
    if params.get("sum_after", False):
        s = df.sum(numeric_only=True)
    return df

def run_cPickle(params):
    fid = open(params["filename"])
    df = cPickle.load(fid)
    fid.close()
    if params.get("sum_after", False):
        s = df.sum(numeric_only=True)
    return df

def run_npy(params):
    X = numpy.load(params["filename"])
    if params.get("sum_after", False):
        s = X.sum()
    return df

def run_sframe(params):
    type_hints_json_fn = params.get("type_hints_json", None)
    if type_hints_json_fn is not None:
        type_hints = json.load(open(type_hints_json_fn))
        dtypes = {}
        for key in type_hints.keys():
            dtypes["X" + str(int(key) + 1)] = eval("np." + type_hints[key])
    header = not params.get("no_header", False)
    df = sframe.SFrame.read_csv(params["filename"], header=header, column_type_hints=dtypes)
    if params.get("sum_after", False):
        s = sum_sframe(df)
    return df

def run_hdf5(params):
    f = h5py.File(params["filename"])
    ds = f[params["dataset"]]
    X = ds[:, :]
    if params.get("sum_after", False):
        s = X.sum()
    return X

def run_feather(params):
    df = feather.read_dataframe(params["filename"])
    if params.get("sum_after", False):
        s = df.sum(numeric_only=True)
    return df

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
    types = {"num_threads": int,
             "block_size": int,
             "allow_quoted_newlines": bool,
             "type_hints_json": str,
             "filename": str,
             "log": str}
    params = generate_params(sys.argv[1], sys.argv[2:], types)
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
    elif cmd == "numpy":
        retval = run_numpy(params)
    elif cmd == "hdf5":
        retval = run_hdf5(params)
    elif cmd == "npy":
        retval = run_npy(params)
    elif cmd == "pickle":
        retval = run_pickle(params)
    elif cmd == "cPickle":
        retval = run_cPickle(params)
    elif cmd == "sframe":
        retval = run_sframe(params)
    elif cmd == "memcopy":
        retval = run_memcopy_baseline(params)
    elif cmd == "avgcols":
        retval = run_average_columns_baseline(params)
    elif cmd == "countnl":
        retval = run_count_newlines_baseline(params)
    elif cmd == "noop":
        retval = 0
    else:
        print "Command not found: '%s'" % cmd
    toc = time.time()
    runtime=toc-tic
    mem=memory_usage_resource()
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
        old_log = []
        if os.path.isfile(logfn):
            old_log = json.load(open(logfn, "r"))
        old_log.append(log_entry)
        log_fid = open(logfn, "a")
        log_fid.write(json.dumps(old_log))
    retval = None

if __name__ == "__main__":
    main()




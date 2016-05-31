#!/usr/bin/env python

# This script runs a benchmarking experiment on a CSV or
# other data reader (HDF5, feather, npy, pkl).
#
# Benchmark parameters are specified with a JSON file or command
# line arguments
#
# For example, to benchmark importing required libraries and then
# doing nothing, do::
#
#  python run_experiment.py - cmd=noop log_dir=/tmp/mylogs
#
# The "-" signifies to not read a json parameters file. The
# command line arguments override json parameters or environment
# variables.
#
#  python run_experiment.py exp.json cmd=noop log_dir=/tmp/mylogs
#

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
import scipy.io as sio
import pickle
import cPickle
import sframe
import tempfile
from subprocess import check_output


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
        if df[key].dtype in (str, unicode, object):
            try:
                s[key] = df[key].apply(lambda x: len(x)).sum()
            except:
                try:
                    s[key] = df[key].values.sum()
                except:
                    s[key] = np.nan
        else:
            s[key] = df[key].values.sum()
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

def sum_mat_dict(dd):
    s = {}
    for key in dd.keys():
        if key.startswith("__"):
            continue
        if dd[key].dtype == np.object_:
            _, c = dd[key].shape
            s[key] = np.sum([len(dd[key][0,i][0]) for i in xrange(0, c)])
        else:
            s[key] = dd[key].sum()
    return s

def sum_ndarray(X):
    s = {}
    rows, cols = X.shape
    for col in xrange(0, cols):
        if type(X[0, col]) in (str, unicode):
            s[col] = pandas.Series(X[:,col]).apply(len).sum()
        else:
            s[col] = X[:,col].sum()
    return s

def sum_spark_dataframe(df):
    from pyspark.sql import functions as F
    str_columns = [field.name for field in df.schema.fields if str(field.dataType)=="StringType"]
    num_columns = [field.name for field in df.schema.fields if str(field.dataType)!="StringType"]
    str_length_sums = [F.sum(F.length(df[k])) for k in str_columns]
    num_sums = [F.sum(df[k]) for k in num_columns]
    sums = str_length_sums + num_sums
    s = df.agg(*sums).collect()
    return s

def read_spark_csv(sc, sqlContext, filename, no_header=False):
    if no_header:
        header = "false"
    else:
        header = "true"
    sdf = sqlContext.read.format('com.databricks.spark.csv').option('header', header).option('inferschema', 'true').load(filename)
    sdf.cache()
    return sdf

def dict_frame_to_data_frame(d, levels):
    column_names = d.keys()
    def dict_frame_to_data_frame_impl():
        for name in column_names:
            column = d.pop(name)
            if name in levels:
                column_levels = levels.pop(name)
                column = column_levels[column]
            yield name, column
    return pandas.DataFrame.from_items(dict_frame_to_data_frame_impl())

def memory_usage_resource():
    "This function is from Fabian Pedregosa's blog post on measuring python memory usage... http://bit.ly/26RguwI"
    import resource
    rusage_denom = 1024.
    if sys.platform == 'darwin':
        rusage_denom = rusage_denom * rusage_denom
    mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / rusage_denom
    return mem

def get_pids(process_name):
    "Return the pids of a given process name."
    return [int(x) for x in check_output(["pidof", process_name]).strip().split(" ")]

def memory_usage_psutil(process_name):
    "This function is adapted from Fabian Pedregosa's blog post on measuring python memory usage... http://bit.ly/26RguwI"
    # return the memory usage in MB
    import psutil
    mem = 0
    pids = get_pids(process_name)
    for pid in pids:
        process = psutil.Process(pid)
        pmem = process.memory_info()[0] / float(2 ** 20)
        mem += pmem
    return mem

def bench_disk_to_mem_baseline(params):
    d = paratext.baseline_disk_to_mem(params["filename"], block_size=params.get("block_size", 32768), num_threads=params.get("num_threads", 1))
    return {}

def bench_average_columns_baseline(params):
    avg = paratext.baseline_average_columns(params["filename"],
                                            type_check=params.get("type_check", True),
                                            block_size=params.get("block_size", 32768),
                                            num_threads=params.get("num_threads", 1),
                                            no_header=params.get("no_header", False),
                                            allow_quoted_newlines=params.get("allow_quoted_newlines", False))
    return {}

def bench_paratext(params):
    load_tic = time.time()
    loader = paratext.internal_create_csv_loader(params["filename"],
                                                 block_size=params.get("block_size", 32768),
                                                 num_threads=params.get("num_threads", 1),
                                                 no_header=params.get("no_header", False),
                                                 allow_quoted_newlines=params.get("allow_quoted_newlines", False),
                                                 max_level_name_length=params.get("max_level_name_length", None),
                                                 number_only=params.get("number_only", False))
    load_toc = time.time()
    load_time = load_toc - load_tic
    transfer_tic = time.time()
    pretransfer_mem = memory_usage_resource()
    transfer = paratext.internal_csv_loader_transfer(loader, forget=True)
    d = {}
    levels = {}
    for name, col, semantics, clevels in transfer:
        if semantics == 'cat':
            levels[name] = clevels
        d[name] = col
    posttransfer_mem = memory_usage_resource()
    transfer = None
    transfer_toc = time.time()
    transfer_time = transfer_toc - transfer_tic
    sum_time = '?'
    if params.get("sum_after", False):
        sum_tic = time.time()
        s = sum_dictframe(d, levels)
        sum_toc = time.time()
        sum_time = sum_toc - sum_tic
    to_df_time = '?'
    if params.get("to_df", False):
        to_df_tic = time.time()
        df = dict_frame_to_data_frame(d, levels)
        to_df_toc = time.time()
        to_df_time = to_df_toc - to_df_tic
    to_df_mem = memory_usage_resource()
    return {"sum_time": sum_time,
            "load_time": load_time,
            "transfer_time": transfer_time,
            "pretransfer_mem": pretransfer_mem,
            "posttransfer_mem": posttransfer_mem,
            "to_df_mem": to_df_mem,
            "to_df_time": to_df_time}

def bench_count_newlines_baseline(params):
    count = paratext.baseline_newline_count(params["filename"], block_size=params.get("block_size", 32768), num_threads=params.get("num_threads", 1), no_header=params.get("no_header", False))
    return {}

def bench_pandas(params): #filename, type_hints_json=None, no_header=False):
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
    sum_time = '?'
    if params.get("sum_after", False):
        sum_tic = time.time()
        s = sum_dataframe(df)
        sum_toc = time.time()
        sum_time = sum_toc - sum_tic
    return {"sum_time": sum_time}

def bench_numpy(params):
    if params.get("no_header", False):
        skiprows = 0
    else:
        skiprows = 1
    if "dtype" in params:
        dtype = eval("np." + params["dtype"])
    else:
        dtype = float
    X = np.loadtxt(params["filename"], dtype=dtype, skiprows=skiprows, delimiter=',')
    sum_time = '?'
    if params.get("sum_after", False):
        sum_tic = time.time()
        s = sum_ndarray(X)
        sum_toc = time.time()
        sum_time = sum_toc - sum_tic
    return {"sum_time": sum_time}

def bench_pickle(params):
    fid = open(params["filename"])
    df = pickle.load(fid)
    fid.close()
    sum_time = '?'
    sum_after = params.get("sum_after", False)
    if sum_after:
        print ">>>", sum_after, type(sum_after)
        sum_tic = time.time()
        s = sum_dataframe(df)
        sum_toc = time.time()
        sum_time = sum_toc - sum_tic
    return {"sum_time": sum_time}

def bench_cPickle(params):
    fid = open(params["filename"])
    df = cPickle.load(fid)
    fid.close()
    sum_time = '?'
    if params.get("sum_after", False):
        sum_tic = time.time()
        s = sum_dataframe(df)
        sum_toc = time.time()
        sum_time = sum_toc - sum_tic
    return {"sum_time": sum_time}

def bench_npy(params):
    X = np.load(params["filename"])
    sum_time = '?'
    if params.get("sum_after", False):
        sum_tic = time.time()
        s = sum_ndarray(X)
        sum_toc = time.time()
        sum_time = sum_toc - sum_tic
    return {"sum_time": sum_time}

def bench_sframe(params):
    type_hints_json_fn = params.get("type_hints_json", None)
    dtypes = {}
    if type_hints_json_fn is not None:
        type_hints = json.load(open(type_hints_json_fn))
        for key in type_hints.keys():
            dtypes["X" + str(int(key) + 1)] = eval("np." + type_hints[key])
    load_tic = time.time()
    header = not params.get("no_header", False)
    sf = sframe.SFrame.read_csv(params["filename"], header=header, column_type_hints=dtypes)
    load_toc = time.time()
    load_time = load_toc - load_tic
    sum_time = '?'
    if params.get("sum_after", False):
        sum_tic = time.time()
        s = sum_sframe(sf)
        sum_toc = time.time()
        sum_time = sum_toc - sum_tic
    pretransfer_mem = memory_usage_resource()
    posttransfer_mem = '?'
    to_df_mem = '?'
    transfer_time = '?'
    if params.get("to_df", False):
        transfer_tic = time.time();
        df = sf.to_dataframe()
        transfer_toc = time.time();
        transfer_time = transfer_toc - transfer_tic
        to_df_mem = memory_usage_resource()
        posttransfer_mem = memory_usage_resource()
    return {"sum_time": sum_time,
            "load_time": load_time,
            "pretransfer_mem": pretransfer_mem,
            "posttransfer_mem": posttransfer_mem,
            "transfer_time": transfer_time,
            "to_df_mem": to_df_mem}

def bench_hdf5(params):
    f = h5py.File(params["filename"])
    ds = f[params["dataset"]]
    X = ds[:, :]
    sum_time = '?'
    if params.get("sum_after", False):
        sum_tic = time.time()
        s = sum_ndarray(X)
        sum_toc = time.time()
        sum_time = sum_toc - sum_tic
    return {"sum_time": sum_time}

def bench_feather(params):
    df = feather.read_dataframe(params["filename"])
    sum_time = '?'
    if params.get("sum_after", False):
        sum_tic = time.time()
        s = sum_dataframe(df)
        sum_toc = time.time()
        sum_time = sum_toc - sum_tic
    return {"sum_time": sum_time}

def bench_R(params):
    f = tempfile.NamedTemporaryFile(delete=False)
    tmp_result_fn = f.name
    runtime_tic = time.time()
    os.system("bench.R %s %s" % (params["filename"], tmp_result_fn))
    runtime_toc = time.time()
    runtime = runtime_toc - runtime_tic
    fid = open(tmp_result_fn)
    result_json = json.load(fid)
    fid.close()
    f.close()
    result_json["runtime"] = runtime
    os.unlink(tmp_result_fn)
    return result_json

def bench_mat(params):
    load_tic = time.time()
    dd = sio.loadmat(params["filename"])
    load_toc = time.time()
    load_time = load_toc - load_tic
    sum_time = '?'
    if params.get("sum_after", False):
        sum_tic = time.time()
        s = sum_mat_dict(dd)
        sum_toc = time.time()
        sum_time = sum_toc - sum_tic
    return {"sum_time": sum_time, "load_time": load_time}

def bench_pyspark(params):
    from pyspark import SparkContext
    from pyspark.sql import SQLContext
    sc = SparkContext("local[*]", "SparkCSV Benchmark", pyFiles=[])
    sqlContext = SQLContext(sc)
    preload_java_mem = memory_usage_psutil("java")
    preload_mem = memory_usage_resource() + memory_usage_psutil("java")
    load_tic = time.time()
    sdf = read_spark_csv(sc, sqlContext, params["filename"], params.get("no_header", False))
    load_toc = time.time()
    load_time = load_toc - load_tic
    sum_time = '?'
    if params.get("sum_after", False):
        sum_tic = time.time()
        s = sum_spark_dataframe(sdf)
        sum_toc = time.time()
        sum_time = sum_toc - sum_tic
    transfer_time = '?'
    pretransfer_mem = '?'
    posttransfer_mem = '?'
    to_df_mem = '?'
    spark_params = {k:v for k,v in sc._conf.getAll()}
    if params.get("to_df", False):
        pretransfer_mem = memory_usage_resource() + memory_usage_psutil("java")
        transfer_tic = time.time();
        to_df_tic = time.time()
        df = sdf.toPandas()
        to_df_toc = time.time()
        transfer_toc = time.time();
        transfer_time = transfer_toc - transfer_tic
        to_df_time = to_df_toc - to_df_tic
        to_df_mem = memory_usage_resource() + memory_usage_psutil("java")
        posttransfer_mem = to_df_mem
    result = {"sum_time": sum_time, 
              "load_time": load_time,
              "preload_java_mem": preload_java_mem,
              "pretransfer_mem": pretransfer_mem,
              "posttransfer_mem": posttransfer_mem,
              "transfer_time": transfer_time,
              "spark_defaultParallelism": sc.defaultParallelism,
              "spark_getNumPartitions": sdf.rdd.getNumPartitions(),
              "to_df_mem": to_df_mem}
    result.update(spark_params)
    return result

def generate_params(json_filename, args, types):
    """
    Generate a parameters dict from the JSON. Command
    line args that were passed in key=value format override
    the fields of the JSON params passed.
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
                   "to_df": bool_converter,
                   "filename": str,
                   "log": str}
    params = generate_params(sys.argv[1], sys.argv[2:], param_types)
    cmd = params.get("cmd", "")
    tic = time.time()
    log_fn = None
    log_path = os.environ.get("PARATEXT_BENCH_LOG_PATH", ".")
    if "log_path" in params:
        log_path = params.pop("log_path")
    if "log" in params:
        log_fn = params.pop("log")
        log_fn = os.path.join(log_path, log_fn)
    results = {}
    if cmd == "feather":
        results = bench_feather(params)
    elif cmd == "paratext":
        results = bench_paratext(params)
    elif cmd == "pandas":
        results = bench_pandas(params)
    elif cmd == "numpy":
        results = bench_numpy(params)
    elif cmd == "hdf5":
        results = bench_hdf5(params)
    elif cmd == "npy":
        results = bench_npy(params)
    elif cmd == "pickle":
        results = bench_pickle(params)
    elif cmd == "pyspark":
        results = bench_pyspark(params)
    elif cmd == "cPickle":
        results = bench_cPickle(params)
    elif cmd == "mat":
        results = bench_mat(params)
    elif cmd == "R-readcsv":
        results = bench_R(params)
    elif cmd == "sframe":
        results = bench_sframe(params)
    elif cmd == "disk-to-mem":
        results = bench_disk_to_mem_baseline(params)
    elif cmd == "avgcols":
        results = bench_average_columns_baseline(params)
    elif cmd == "countnl":
        results = bench_count_newlines_baseline(params)
    elif cmd == "noop":
        results = {}
    else:
        print "Command not found: '%s'" % cmd
        sys.exit(1)
    toc = time.time()
    runtime=toc-tic
    if cmd == "R-readcsv":
        runtime = results["runtime"]
    if cmd == "pyspark":
        mem=memory_usage_resource() + memory_usage_psutil("java")
    elif cmd == "R-readcsv":
        mem=results["mem"]
    else:
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
        log_entry["filesize"] = float(sz)
        log_entry["tput_bytes"] = float(sz) / runtime
        log_entry["tput_MB"] = (float(sz)/1000000) / runtime
        log_entry["tput_MiB"] = (float(sz)/1048576) / runtime
    if log_fn is None:
        print log_entry
    else:
        old_log = {"log": []}
        # See if the directory where the logs should live exist.
        log_dir = os.path.dirname(log_fn)
        if log_dir == "":
            log_dir = "."
        # If not, create the directory.
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        # If the log is already there, let's read it and append to it
        # with the new result.
        if os.path.exists(log_fn) and os.path.isfile(log_fn):
            old_log_fid = open(log_fn, "r")
            old_log = json.load(old_log_fid)
            old_log_fid.close()
        old_log["log"].append(log_entry)
        log_fid = open(log_fn, "w")
        json.dump(old_log, log_fid)
        log_fid.close()
    results = None

if __name__ == "__main__":
    main()

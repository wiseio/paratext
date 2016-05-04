import paratext
import pickle
import feather
import h5py
import numpy as np
import os
import sys

def convert_feather(df, output_filename):
    feather.write_dataframe(df, output_filename)

def convert_hdf5(df, output_filename):
    X = df.values
    f = h5py.File(output_filename, "w")
    ds=f.create_dataset("mnist", X.shape, dtype=X.dtype)
    ds[...] = X

def convert_npy(df, output_filename):
    X = df.values
    np.save(output_filename, X)

def convert_pkl(df, output_filename):
    fid = open(output_filename, "wb")
    pickle.dump(df, fid)
    fid.close()

input_filename = sys.argv[1]
output_filenames = sys.argv[2:]

if not input_filename.endswith(".csv"):
    print "input must be a CSV file (by extension)"
    os.exit(1)

df = paratext.load_csv_to_pandas(input_filename)

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
    else:
        print "skipping '%s'; invalid output format '%s'" % (output_filename, extension)

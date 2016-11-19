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

import numpy as np
import pandas.util.testing
import unittest
import collections
import pandas
import paratext_internal
import os
import random
import sys

from tempfile import NamedTemporaryFile
from contextlib import contextmanager
from six.moves import range
import six

def generate_hell_frame(num_rows, num_columns, include_null=False, fmt='arbitrary'):
    """
    Generate a DataFrame of columns containing randomly generated data.
    """
    if include_null:
        min_byte = 0
    else:
        min_byte = 1
    frame = collections.OrderedDict()
    seed = 0
    keys = []
    colfmts = {}
    for column in range(num_columns):
        key = "col%d" % (column,)
        keys.append(key)
        if fmt == 'mixed':
            colfmts[key] = random.choice(["ascii","arbitrary","printable_ascii","utf-8"])
        else:
            colfmts[key] = fmt
    for key in keys:
        data = []
        colfmt = colfmts[key]
        for row in range(num_rows):
            length = np.random.randint(50,1000)
            if colfmt == 'arbitrary':
                cell = paratext_internal.get_random_string(length, seed, min_byte, 255)
            elif colfmt == 'ascii':
                cell = paratext_internal.get_random_string(length, seed, min_byte, 127)
            elif colfmt == 'printable_ascii':
                cell = paratext_internal.get_random_string(length, seed, 32, 126)
            elif colfmt == 'utf-8' or fmt == 'utf-8':
                cell = paratext_internal.get_random_string_utf8(length, seed, include_null)
            else:
                raise ValueError("unknown format: " + fmt)
            data.append(cell)
        frame[key] = data
    return pandas.DataFrame(frame)

@contextmanager
def generate_tempfile(filedata):
    """
    A context manager that generates a temporary file object that will be deleted
    when the context goes out of scope. The mode of the file is "wb".

    Parameters
    ----------
    filedata : The data of the file to write as a bytes object.
    """
    f = NamedTemporaryFile(delete=False, mode="wb", prefix="paratext-tests")
    f.write(filedata)
    name = f.name
    f.close()
    yield f.name
    os.remove(name)

@contextmanager
def generate_tempfilename():
    """
    A context manager that generates a temporary filename that will be deleted
    when the context goes out of scope.
    """
    f = NamedTemporaryFile(delete=False, prefix="paratext-tests")
    name = f.name
    f.close()
    yield f.name
    os.remove(name)

def assert_seq_almost_equal(left, right):
    left = np.asarray(left)
    right = np.asarray(right)
    left_is_string = np.issubdtype(left.dtype, np.str_) or np.issubdtype(left.dtype, np.unicode_) or left.dtype == np.object_
    right_is_string = np.issubdtype(right.dtype, np.str_) or np.issubdtype(right.dtype, np.unicode_) or right.dtype == np.object_
    if np.issubdtype(left.dtype, np.integer) and np.issubdtype(right.dtype, np.integer):
        if not (left.shape == right.shape):
            raise AssertionError("integer sequences have different sizes: %s vs %s" % (str(left.shape), str(right.shape)))
        if not (left == right).all():
            m = (left != right).mean() * 100.
            raise AssertionError("integer sequences mismatch: %5.5f%% left=%s right=%s" % ((m, str(left[0:20]), str(right[0:20]))))
    elif np.issubdtype(left.dtype, np.floating) and np.issubdtype(right.dtype, np.floating):
        np.testing.assert_almost_equal(left, right)
    elif left_is_string and not right_is_string:
        if len(left) > 0 and len(right) > 0:
            raise AssertionError("sequences differ by dtype: left is string and right is %s" % (str(right.dtype)))
    elif not left_is_string and right_is_string:
        if len(left) > 0 and len(right) > 0:
            raise AssertionError("sequences differ by dtype: left is %s and right is string" % (str(left.dtype)))
    elif left_is_string and right_is_string:
        q = np.zeros((len(left)))
        for i in range(len(q)):
            q[i] = not paratext_internal.are_strings_equal(left[i], right[i])
        m = q.mean() * 100.
        if q.any():
            raise AssertionError("object sequences mismatch: %5.5f%%, rows: %s" % (m, str(np.where(q)[0].tolist())))
    else:
        if np.issubdtype(left.dtype, np.floating):
            left_float = left
        else:
            left_float = np.asarray(left, dtype=np.float_)
        if np.issubdtype(right.dtype, np.floating):
            right_float = right
        else:
            right_float = np.asarray(right, dtype=np.float_)
        pandas.util.testing.assert_almost_equal(left_float, right_float)

def assert_dictframe_almost_equal(left, right, err_msg=""):
    """
    Compares two dictframes for equivalent. A dict-frame is simply
    an object that obeys the Python mapping protocol. Each (key, value)
    represents a column keyed/indexed by `key` where `value` is
    a NumPy array, a Python sequence, or Python iterable.
    """
    left_keys = set(left.keys())
    right_keys = set(right.keys())
    left_missing = right_keys - left_keys
    right_missing = left_keys - right_keys
    together = left_keys.intersection(right_keys)
    msg = err_msg
    for key in left_missing:
        msg += "%s: missing on left\n" % key
    for key in right_missing:
        msg += "%s: missing on right\n" % key
    for key in together:
        try:
            assert_seq_almost_equal(left[key], right[key])
        except AssertionError as e:
            msg += "\n Column %s: %s" % (key, e.args[0])
    if len(msg) > 0:
        raise AssertionError(msg)

_words = None
def sample_words(n):
    global _words
    if _words is None:
        path = "/usr/share/dict/words"
        if os.path.isfile(path):
            fid = open(path)
            _words = [line.strip() for line in fid.readlines()]
        else:
            _words = []

    if _words:
        return random.sample(_words, n)
    elif n > 100000:
        # random.sample also raises if we sample more than _words, and /usr/share/dict/words usually has ~200k entries
        raise ValueError("We're unlikely to be able to generate this many unique words in reasonable time")
    else:
        result = set()
        while len(result) < n:
            word = ''.join([chr(random.randint(ord('a'), ord('z'))) for _ in range(random.randint(4, 10))])
            result.add(word)
        return list(result)

def generate_mixed_frame(num_rows, num_floats, num_cats, num_ints):
    num_cols = num_floats + num_cats + num_ints
    perm = np.random.permutation(num_cols)
    num_catints = num_cats + num_ints
    float_ids = perm[num_catints:]
    int_ids = perm[num_cats:num_catints]
    cat_ids = perm[0:num_cats]
    cat_ids = ["col" + str(id) for id in cat_ids]
    int_ids = ["col" + str(id) for id in int_ids]
    float_ids = ["col" + str(id) for id in float_ids]
    d = collections.OrderedDict()
    dtypes = {}
    for col in cat_ids:
        X = np.zeros((num_rows,), dtype=np.object);
        for row in range(0, num_rows):
            num_newlines = np.random.randint(3,7)
            num_commas = np.random.randint(3,7)
            X[row] = ""
            tricky_delims = np.asarray(["\n"] * num_newlines + [","] * num_commas)
            np.random.shuffle(tricky_delims)
            for delim in tricky_delims:
                X[row] += ' '.join(sample_words(5))
                X[row] += delim
                X[row] += ' '.join(sample_words(5))
        d[col] = X
        dtypes[col] = np.object
    for col in float_ids:
        d[col] = np.asarray(np.random.randn(num_rows), dtype=np.float32)
        dtypes[col] = np.float32
    min_int = [0,   -2**7, 0   , -2**15,     0, -2**31,     0, -2**62]
    max_int = [2**8, 2**7, 2**16,  2**15, 2**32,  2**31, 2**62, 2**62]
    dtypes_int = [np.uint8, np.int8, np.uint16, np.int16, np.uint32, np.int32, np.uint64, np.int64]
    for col in int_ids:
        j = np.random.randint(0, len(min_int))
        d[col] = np.random.randint(min_int[j], max_int[j], num_rows, dtype=dtypes_int[j])
        dtypes[col] = dtypes_int[j]
    return d, dtypes


def internal_compare(filename, *args, **kwargs):
    """
    Loads a Pandas DataFrame with pandas and paratext, and compares their contents.
    """
    import pandas
    dfY = load_csv_to_pandas(filename, *args, **kwargs)
    if kwargs.get("no_header"):
        dfX = pandas.read_csv(filename, header=None, na_values=['?'], names=dfY.keys())
    else:
        dfX = pandas.read_csv(filename, na_values=['?'])
    results = {}
    for key in dfX.columns:
        if dfX[key].dtype in (str, unicode, np.object):
            nonnan_mask = (dfY[key] != 'nan') & (dfY[key] != '?')
            results[key] = (dfX[key][nonnan_mask]!=dfY[key][nonnan_mask]).mean()
        else:
            nonnan_mask = ~np.isnan(dfX[key])
            results[key] = abs(dfX[key][nonnan_mask]-dfY[key][nonnan_mask]).max()
    return results

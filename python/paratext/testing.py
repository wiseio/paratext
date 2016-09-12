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

from tempfile import NamedTemporaryFile
from contextlib import contextmanager
from six.moves import range
import six

def as_quoted_string(s, do_not_escape_newlines=False):
    return paratext_internal.as_quoted_string(s, do_not_escape_newlines)

def generate_hell_frame(num_rows, num_columns, include_null=False, fmt='arbitrary'):
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

def save_frame(filename, frame, allow_quoted_newlines=True, out_format='arbitrary'):
    f = open(filename, 'wb')
    write_frame(f, frame, allow_quoted_newlines, out_format=out_format)
    f.close()

def write_frame(stream, frame, allow_quoted_newlines=True, out_format='arbitrary'):
    # In case .keys() is non-deterministic
    keys = list(frame.keys())
    cols = []

    psafe=paratext_internal.SafeStringOutput()
    psafe.escape_nonascii(True)
    psafe.escape_nonprintables(True)
    safe=paratext_internal.SafeStringOutput()
    safe.escape_special(True)
    if out_format == 'utf-8':
        safe.escape_nonutf8(True)
    elif out_format == 'ascii':
        safe.escape_nonascii(True)
    elif out_format == 'printable_ascii':
        safe.escape_nonascii(True)
        safe.escape_nonprintables(True)
    if not allow_quoted_newlines:
        safe.escape_newlines(True)
        psafe.escape_newlines(True)
    safe.double_quote_output(True)
    psafe.double_quote_output(True)
    for col in range(len(keys)):
        if col > 0:
            stream.write(b",")
            stream.flush()
        key = keys[col]
        if out_format == 'utf-8':
            stream.flush()
            if isinstance(key, bytes):
                skey = psafe.to_raw_string(key)
            else:
                skey = safe.to_raw_string(key)
            stream.write(skey)
        else:
            stream.flush()
            if isinstance(key, bytes):
                skey = psafe.to_raw_string(key)
            else:
                skey = safe.to_raw_string(key)
            stream.write(skey)
        if isinstance(frame[key], pandas.Series):
            cols.append(frame[key].values)
        else:
            cols.append(np.asarray(frame[key]))
    stream.write(b"\n")
    if hasattr(frame, "shape"):
        num_rows = frame.shape[0]
    elif len(keys) == 0:
        num_rows = 0
    else:
        num_rows = len(frame[keys[0]])
    for row in range(num_rows):
        for col in range(len(cols)):
            if col > 0:
                stream.write(b',')
            val = cols[col][row]
            if np.issubdtype(type(val), np.string_) or np.issubdtype(type(val), np.unicode_) or isinstance(val, six.string_types):
                if out_format == 'utf-8':
                    #sval = safe.to_utf8_string(val)
                    if isinstance(val, bytes):
                        sval = psafe.to_raw_string(val)
                    else:
                        sval = safe.to_raw_string(val)
                    stream.write(sval)
                else:
                    sval = safe.to_raw_string(val)
                    stream.write(sval)
            else:
                stream.write(bytes(repr(val), 'utf-8'))
        stream.write(b"\n")

@contextmanager
def generate_tempfile(filedata):
    f = NamedTemporaryFile(delete=False, prefix="paratext-tests")
    f.write(filedata.encode("utf-8"))
    name = f.name
    f.close()
    yield f.name
    os.remove(name)

@contextmanager
def generate_tempfilename():
    f = NamedTemporaryFile(delete=False, prefix="paratext-tests")
    name = f.name
    f.close()
    yield f.name
    os.remove(name)

def assert_seq_almost_equal(left, right):
    left = np.asarray(left)
    right = np.asarray(right)
    if np.issubdtype(left.dtype, np.integer) and np.issubdtype(right.dtype, np.integer):
        if not (left.shape == right.shape):
            raise AssertionError("integer sequences have different sizes: %s vs %s" % (str(left.shape), str(right.shape)))
        if not (left == right).all():
            m = (left != right).mean() * 100.
            raise AssertionError("integer sequences mismatch: %5.5f%%" % (m,))
    elif np.issubdtype(left.dtype, np.floating) and np.issubdtype(right.dtype, np.floating):
        np.testing.assert_almost_equal(left, right)
    elif np.issubdtype(left.dtype, np.str_) or np.issubdtype(left.dtype, np.unicode_) or np.issubdtype(right.dtype, np.str_) or np.issubdtype(right.dtype, np.unicode_) or np.issubdtype(left.dtype, np.object_) or np.issubdtype(right.dtype, np.object_):
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

def generate_mixed_frame(num_rows, num_floats, num_cats, num_ints):
    fid = open("/usr/share/dict/words")
    words=[line.strip() for line in fid.readlines()]
    num_cols = num_floats + num_cats + num_ints
    perm = np.random.permutation(num_cols)
    num_catints = num_cats + num_ints
    float_ids = perm[num_catints:]
    int_ids = perm[num_cats:num_catints]
    cat_ids = perm[0:num_cats]
    cat_ids = ["col" + str(id) for id in cat_ids]
    int_ids = ["col" + str(id) for id in int_ids]
    float_ids = ["col" + str(id) for id in float_ids]
    d = {}
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
                X[row] += ' '.join(random.sample(words, 5))
                X[row] += delim
                X[row] += ' '.join(random.sample(words, 5))
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
        d[col] = np.asarray(np.random.randint(min_int[j], max_int[j], num_rows), dtype=dtypes_int[j])
        dtypes[col] = dtypes_int[j]
    return d, dtypes

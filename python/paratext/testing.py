import numpy as np
import pandas.util.testing
import unittest
import collections
import pandas
import paratext_internal
import os

from tempfile import NamedTemporaryFile
from contextlib import contextmanager
from six.moves import range

def as_quoted_string(s, do_not_escape_newlines=False):
    return paratext_internal.as_quoted_string(s, do_not_escape_newlines)

def generate_hell_frame(num_rows, num_columns):
    frame = collections.OrderedDict()
    for column in range(num_columns):
        key = "col%d" % (column,)
        data = []
        for row in range(num_rows):
            length = np.random.randint(50,1000)
            cell = paratext_internal.get_random_string(length, 1, 127)
            data.append(cell)
        frame[key] = data
    return pandas.DataFrame(frame)

def save_frame(filename, frame):
    f = open(filename, 'w')
    write_frame(f, frame)
    f.close()

def write_frame(stream, frame):
    # In case .keys() is non-deterministic
    keys = list(frame.keys())
    cols = []
    for col in range(len(keys)):
        if col > 0:
            stream.write(",")
        key = keys[col]
        stream.write(as_quoted_string(key, True))
        cols.append(frame[key].values)
    stream.write("\n")
    for row in range(frame.shape[0]):
        for col in range(len(cols)):
            if col > 0:
                stream.write(',')
            translated = as_quoted_string(cols[col][row], True)
            stream.write(translated)
        stream.write("\n")

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
        print(left, right, left == right)
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
            q[i] = left[i] != right[i]
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

def assert_dictframe_almost_equal(left, right):
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
    msg = ""
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

from contextlib import contextmanager
import os
import pandas.util.testing
import unittest
import paratext
import numpy as np
from tempfile import NamedTemporaryFile
from nose_parameterized import parameterized

@contextmanager
def generate_tempfile(filedata):
    f = NamedTemporaryFile(delete=False, prefix="paratext-tests")
    f.write(filedata.encode("utf-8"))
    name = f.name
    f.close()
    yield f.name
    os.remove(name)

def assert_seq_almost_equal(left, right):
    left = np.asarray(left)
    right = np.asarray(right)
    if np.issubdtype(left.dtype, np.integer) and np.issubdtype(right.dtype, np.integer):
        if not (left == right).all():
            m = (left != right).mean() * 100
            raise AssertionError("integer sequences mismatch: %5.5f%%" % (m,))
    elif np.issubdtype(left.dtype, np.floating) and np.issubdtype(right.dtype, np.floating):
        np.testing.assert_almost_equal(left, right)
    else:
        if np.issubdtype(left.dtype, np.floating):
            left_float = left
        else:
            left_float = np.asarray(left, dtype=np.float_)
        if np.issubdtype(right.dtype, np.floating):
            right_float = right
        else:
            right_float = np.asarray(right, dtype=np.float_)
        np.testing.assert_almost_equal(left_float, right_float)

def assert_dictframe_almost_equal(left, right):
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

class TestBasicFiles(unittest.TestCase):

    @parameterized.expand([
        ["1x0 uint8", 1, 0, np.uint8],
        ["0x1 uint8", 0, 1, np.uint8],
        ["1x1 uint8", 1, 1, np.uint8],
        ["2x1 uint8", 2, 1, np.uint8],
        ["3x1 uint8", 3, 1, np.uint8],
        ["4x1 uint8", 4, 1, np.uint8],
        ["5x1 uint8", 5, 1, np.uint8],
        ["6x1 uint8", 6, 1, np.uint8],
        ["0x2 uint8", 0, 2, np.uint8],
        ["1x2 uint8", 1, 2, np.uint8],
        ["2x2 uint8", 2, 2, np.uint8],
        ["3x2 uint8", 3, 2, np.uint8],
        ["4x2 uint8", 4, 2, np.uint8],
        ["5x2 uint8", 5, 2, np.uint8],
        ["6x2 uint8", 6, 2, np.uint8],
        ["0x3 uint8", 0, 3, np.uint8],
        ["1x3 uint8", 1, 3, np.uint8],
        ["2x3 uint8", 2, 3, np.uint8],
        ["3x3 uint8", 3, 3, np.uint8],
        ["4x3 uint8", 4, 3, np.uint8],
        ["5x3 uint8", 5, 3, np.uint8],
        ["6x3 uint8", 6, 3, np.uint8],
        ["0x4 uint8", 0, 4, np.uint8],
        ["1x4 uint8", 1, 4, np.uint8],
        ["2x4 uint8", 2, 4, np.uint8],
        ["3x4 uint8", 3, 4, np.uint8],
        ["4x4 uint8", 4, 4, np.uint8],
        ["5x4 uint8", 5, 4, np.uint8],
        ["6x4 uint8", 6, 4, np.uint8],
        ["0x5 float",0, 5, np.uint8],
        ["1x5 float",1, 5, np.uint8],
        ["2x5 float",2, 5, np.uint8],
        ["3x5 float",3, 5, np.uint8],
        ["4x5 float",4, 5, np.uint8],
        ["5x5 float",5, 5, np.uint8],
        ["6x5 float",6, 5, np.uint8],
        ["1x0 float",1, 0, np.float_],
        ["0x1 float",0, 1, np.float_],
        ["1x1 float",1, 1, np.float_],
        ["2x1 float",2, 1, np.float_],
        ["3x1 float",3, 1, np.float_],
        ["4x1 float",4, 1, np.float_],
        ["5x1 float",5, 1, np.float_],
        ["6x1 float",6, 1, np.float_],
        ["0x2 float",0, 2, np.float_],
        ["1x2 float",1, 2, np.float_],
        ["2x2 float",2, 2, np.float_],
        ["3x2 float",3, 2, np.float_],
        ["4x2 float",4, 2, np.float_],
        ["5x2 float",5, 2, np.float_],
        ["6x2 float",6, 2, np.float_],
        ["0x3 float",0, 3, np.float_],
        ["1x3 float",1, 3, np.float_],
        ["2x3 float",2, 3, np.float_],
        ["3x3 float",3, 3, np.float_],
        ["4x3 float",4, 3, np.float_],
        ["5x3 float",5, 3, np.float_],
        ["6x3 float",6, 3, np.float_],
        ["0x4 float",0, 4, np.float_],
        ["1x4 float",1, 4, np.float_],
        ["2x4 float",2, 4, np.float_],
        ["3x4 float",3, 4, np.float_],
        ["4x4 float",4, 4, np.float_],
        ["5x4 float",5, 4, np.float_],
        ["6x4 float",6, 4, np.float_],
        ["0x5 float",0, 5, np.float_],
        ["1x5 float",1, 5, np.float_],
        ["2x5 float",2, 5, np.float_],
        ["3x5 float",3, 5, np.float_],
        ["4x5 float",4, 5, np.float_],
        ["5x5 float",5, 5, np.float_],
        ["6x5 float",6, 5, np.float_],
    ])
    def test_basic_ints(self, name, num_rows, num_columns, dtype):
        keys = ["A", "B", "C", "D", "E", "F"]
        keys = keys[0:num_columns]
        filedata = ','.join(keys[0:num_columns]) + "\n"
        expected = {}
        for key in keys:
            expected[key] = []
        for row in range(num_rows):
            if np.issubdtype(dtype, np.integer):
                row_data = [row*i for i in range(num_columns)]
            else:
                row_data = np.random.random((num_columns,))
            filedata += ",".join([str(v) for v in row_data]) + "\n"
            for k in range(len(keys)):
                expected[keys[k]].append(row_data[k])
        with generate_tempfile(filedata) as fn:
            print(fn)
            actual = paratext.load_csv_to_pandas(fn)
            assert_dictframe_almost_equal(actual, expected)


    @parameterized.expand([
        ["foo", "a", "a",],
        ["bar", "a", "b"],
        ["lee", "b", "b"],
    ])
    def test_basic_3x3x(self, A, B, C):
        filedata = u"""A,B,C
1,4,7
2,5,8
3,6,9"""
        with generate_tempfile(filedata) as fn:
            expected = {"A": [1,2,3], "B": [4,5,6], "C": [7,8,9]}
            print(fn)
            actual = paratext.load_csv_to_pandas(fn)
            assert_dictframe_almost_equal(actual, expected)

    def test_basic_3x2x(self):
        filedata = u"""A,B,C
1,4,7
2,5,8
"""
        with generate_tempfile(filedata) as fn:
            expected = {"A": [1,2], "B": [4,5], "C": [7,8]}
            print(fn)
            actual = paratext.load_csv_to_pandas(fn)
            assert_dictframe_almost_equal(actual, expected)

    def test_basic_3x1x(self):
        filedata = u"""A,B,C
1,4,7
"""
        with generate_tempfile(filedata) as fn:
            expected = {"A": [1], "B": [4], "C": [7]}
            print(fn)
            actual = paratext.load_csv_to_pandas(fn)
            assert_dictframe_almost_equal(actual, expected)


    def test_basic_3x1x(self):
        filedata = u"""A,B,C
"""
        with generate_tempfile(filedata) as fn:
            expected = {"A": [], "B": [], "C": []}
            print(fn)
            actual = paratext.load_csv_to_pandas(fn)
            assert_dictframe_almost_equal(actual, expected)



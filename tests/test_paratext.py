import os
import unittest
import paratext.testing
from paratext.testing import assert_dictframe_almost_equal, generate_tempfile, generate_tempfilename
import pandas.util.testing
import numpy as np
import logging

class TestBasicFiles:

    def do_basic_nums(self, dtype, num_rows, num_columns, num_threads):
        keys = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"]
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
            logging.debug("filename: %s" % fn)
            actual = paratext.load_csv_to_pandas(fn, num_threads=num_threads)
            assert_dictframe_almost_equal(actual, expected)

    def do_basic_empty(self, file_body, num_threads):
        with generate_tempfile(file_body) as fn:
            logging.debug("filename: %s" % fn)
            actual = paratext.load_csv_to_pandas(fn, num_threads=num_threads)
            expected = pandas.DataFrame()
            assert_dictframe_almost_equal(actual, expected)

    def test_basic_empty(self):
        file_bodies = ["", "\n", "\n\n", " ", " \n", " \n   \n \n", "\n  \n", "\v\t \n", "\n\n\n", "\n\n\n\n"]
        file_bodies += ["\r\n", "\r\n\r\n", " ", " \r\n", " \r\n   \r\n \r\n", "\r\n  \r\n", "\r\v\t \r\n", "\r\n\r\n\r\n", "\r\n\r\n\r\n\r\n"]
        for file_body in file_bodies:
            for num_threads in [1]:
                yield self.do_basic_empty, file_body, num_threads

    def test_basic_ints(self):
        for dtype in [np.float_, np.uint8]:
            for num_rows in [0, 1, 2, 3, 4, 5, 6, 10, 100, 1000]:
                for num_cols in [1, 2, 3, 4, 5, 6, 10]:
                    for num_threads in [0, 1, 2, 3, 4, 5, 6, 7, 8, 15, 20]:
                        yield self.do_basic_nums, dtype, num_rows, num_cols, num_threads

    def test_basic_strange1(self):
        filedata = """A,B,C
"\\\"","",7
"\\\\","X",8
"\n","\\\\\\"",9"""
        with generate_tempfile(filedata) as fn:
            expected = {"A": ["\"","\\","\n"], "B": ["","X","\\\""], "C": [7,8,9]}
            logging.debug("filename: %s" % fn)
            actual = paratext.load_csv_to_pandas(fn, allow_quoted_newlines=True, out_encoding="utf-8")
            assert_dictframe_almost_equal(actual, expected)

    def test_basic_3x2x(self):
        filedata = u"""A,B,C
1,4,7
2,5,8
"""
        with generate_tempfile(filedata) as fn:
            expected = {"A": [1,2], "B": [4,5], "C": [7,8]}
            logging.debug("filename: %s" % fn)
            actual = paratext.load_csv_to_pandas(fn)
            assert_dictframe_almost_equal(actual, expected)

    def test_basic_3x1x(self):
        filedata = u"""A,B,C
1,4,7
"""
        with generate_tempfile(filedata) as fn:
            expected = {"A": [1], "B": [4], "C": [7]}
            logging.debug("filename: %s" % fn)
            actual = paratext.load_csv_to_pandas(fn)
            assert_dictframe_almost_equal(actual, expected)


    def test_basic_3x1x(self):
        filedata = u"""A,B,C
"""
        with generate_tempfile(filedata) as fn:
            expected = {"A": [], "B": [], "C": []}
            logging.debug("filename: %s" % fn)
            actual = paratext.load_csv_to_pandas(fn)
            assert_dictframe_almost_equal(actual, expected)

class TestMixedFiles:

    def run_case(self, num_rows, num_cats, num_floats, num_ints, num_threads):
        expected, types_df = paratext.testing.generate_mixed_frame(num_rows, num_floats, num_cats, num_ints)
        with generate_tempfilename() as fn:
            logging.debug("filename: %s" % fn)
            paratext.testing.save_frame(fn, expected)
            actual = paratext.load_csv_to_pandas(fn, allow_quoted_newlines=True, out_encoding='utf-8', num_threads=num_threads)
            assert_dictframe_almost_equal(actual, expected)

    def test_mixed_frame(self):
        for num_rows in [1, 2, 3, 5, 10, 100, 1000]:
            for num_cats in [1, 3, 5]:
                for num_floats in [1, 3, 5]:
                    for num_ints in [0, 1, 5, 10, 50]:
                        for num_threads in [1, 2, 3, 5, 10, 20]:
                            yield self.run_case, num_rows, num_cats, num_floats, num_ints, num_threads

class TestHellFiles:

    def do_hell_frame(self, dos, frame_encoding, out_encoding, include_null, allow_quoted_newlines, rows, cols, num_threads):
        expected = paratext.testing.generate_hell_frame(rows, cols, include_null=include_null, fmt=frame_encoding)
        with generate_tempfilename() as fn:
            logging.debug("filename: %s" % fn)
            paratext.testing.save_frame(fn, expected, allow_quoted_newlines, out_format=out_encoding, dos=dos)
            actual = paratext.load_csv_to_pandas(fn, allow_quoted_newlines=allow_quoted_newlines, out_encoding=out_encoding, num_threads=num_threads, convert_null_to_space=not include_null)
            assert_dictframe_almost_equal(actual, expected)

    def test_hell_frame(self):
        formatting = [("utf-8", "utf-8"),
                      ("printable_ascii", "utf-8"),
                      ("utf-8", "unknown"),
                      ("arbitrary", "unknown"),
                      ("arbitrary", "utf-8"),
                      ("mixed", "unknown"),
                      ("mixed", "utf-8")]
        for dos in [False, True]:
            for (frame_encoding, out_encoding) in formatting:
                for include_null in [False, True]:
                    for allow_quoted_newlines in [False, True]:
                        for num_rows in [1,2,3,4,10,100,1000,3000]:
                            for num_cols in [1,2,3,4,5,10,20,30]:
                                for num_threads in [1,2,3,4,5,8,10,20,30]:
                                    yield self.do_hell_frame, dos, frame_encoding, out_encoding, include_null, allow_quoted_newlines, num_rows, num_cols, num_threads

import os
import unittest
import paratext.testing
import paratext.serial
from paratext.testing import assert_dictframe_almost_equal, generate_tempfile, generate_tempfilename
import pandas.util.testing
import numpy as np
import logging

class TestBasicFiles:

    def do_basic_nums(self, dtype, num_rows, num_columns, num_threads, number_only, no_header):
        if no_header:
            filedata = ''
            keys = ["col%d" % k for k in range(num_columns)] 
        else:
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
        with generate_tempfile(filedata.encode("utf-8")) as fn:
            logging.debug("filename: %s" % fn)
            actual = paratext.load_csv_to_pandas(fn, num_threads=num_threads, number_only=number_only, no_header=no_header)
            assert_dictframe_almost_equal(actual, expected)

    def do_basic_empty(self, file_body, num_threads):
        with generate_tempfile(file_body) as fn:
            logging.debug("filename: %s" % fn)
            actual = paratext.load_csv_to_pandas(fn, num_threads=num_threads)
            expected = pandas.DataFrame()
            assert_dictframe_almost_equal(actual, expected)

    def test_basic_empty(self):
        file_bodies = [b"", b"\n", b"\n\n", b" ", b" \n", b" \n   \n \n", b"\n  \n", b"\v\t \n", b"\n\n\n", b"\n\n\n\n"]
        file_bodies += [b"\r\n", b"\r\n\r\n", b" ", b" \r\n", b" \r\n   \r\n \r\n", b"\r\n  \r\n", b"\r\v\t \r\n", b"\r\n\r\n\r\n", b"\r\n\r\n\r\n\r\n"]
        for file_body in file_bodies:
            for num_threads in [1]:
                yield self.do_basic_empty, file_body, num_threads

    def test_basic_ints(self):
        for no_header in [False, True]:
            for number_only in [False, True]:
                for dtype in [np.float_, np.int64]:
                    for num_rows in [0, 1, 2, 3, 4, 5, 6, 10, 100, 1000]:
                        for num_cols in [1, 2, 3, 4, 5, 6, 10]:
                            if num_rows * num_cols < 20:
                                thread_set = range(0,30)
                            else:
                                thread_set = [0, 1, 2, 3, 4, 5, 6, 7, 8, 15, 20]
                                for num_threads in thread_set:
                                    yield self.do_basic_nums, dtype, num_rows, num_cols, num_threads, number_only, no_header

    def test_basic_strange1(self):
        filedata = b"""A,B,C
"\\\"","",7
"\\\\","X",8
"\n","\\\\\\"",9"""
        with generate_tempfile(filedata) as fn:
            expected = {"A": ["\"","\\","\n"], "B": ["","X","\\\""], "C": [7,8,9]}
            logging.debug("filename: %s" % fn)
            actual = paratext.load_csv_to_pandas(fn, allow_quoted_newlines=True, out_encoding="utf-8")
            assert_dictframe_almost_equal(actual, expected)

    def test_basic_3x2x(self):
        filedata = b"""A,B,C
1,4,7
2,5,8
"""
        with generate_tempfile(filedata) as fn:
            expected = {"A": [1,2], "B": [4,5], "C": [7,8]}
            logging.debug("filename: %s" % fn)
            actual = paratext.load_csv_to_pandas(fn)
            assert_dictframe_almost_equal(actual, expected)

    def test_basic_3x1x(self):
        filedata = b"""A,B,C
1,4,7
"""
        with generate_tempfile(filedata) as fn:
            expected = {"A": [1], "B": [4], "C": [7]}
            logging.debug("filename: %s" % fn)
            actual = paratext.load_csv_to_pandas(fn)
            assert_dictframe_almost_equal(actual, expected)

    def test_basic_3x0x(self):
        filedata = b"""A,B,C
"""
        with generate_tempfile(filedata) as fn:
            expected = {"A": [], "B": [], "C": []}
            logging.debug("filename: %s" % fn)
            actual = paratext.load_csv_to_pandas(fn)
            assert_dictframe_almost_equal(actual, expected)

    def test_basic_empty_cells_num(self):
        filedata = b"""A,B,C,D,E,F
#,1,#,#,2,#
3,#,#,4,5,#
6,#,#,#,#,#
#,7,#,#,#,#
#,#,8,#,#,#
#,#,#,9,#,#
#,#,#,#,10,#
#,#,#,#,#,11
#,#,12,#,#,13
14,#,#,15,16,17
"""
        filedata = filedata.replace(b"#", b"")
        with generate_tempfile(filedata) as fn:
            expected = {"A": [0,3,6,0,0,0,0,0,0,14], "B": [1,0,0,7,0,0,0,0,0,0], "C": [0,0,0,0,8,0,0,0,12,0], "D": [0,4,0,0,0,9,0,0,0,15], "E": [2,5,0,0,0,0,10,0,0,16], "F": [0,0,0,0,0,0,0,11,13,17]}
            logging.debug("filename: %s" % fn)
            actual = paratext.load_csv_to_pandas(fn, number_only=True)
            assert_dictframe_almost_equal(actual, expected)

class TestMixedFiles:

    def run_case(self, num_rows, num_cats, num_floats, num_ints, num_threads):
        expected, types_df = paratext.testing.generate_mixed_frame(num_rows, num_floats, num_cats, num_ints)
        with generate_tempfilename() as fn:
            logging.debug("filename: %s" % fn)
            paratext.serial.save_frame(fn, expected, allow_quoted_newlines=True, out_encoding='utf-8')
            actual = paratext.load_csv_to_pandas(fn, allow_quoted_newlines=True, out_encoding='utf-8', num_threads=num_threads)
            assert_dictframe_almost_equal(actual, expected)

    def test_mixed_frame(self):
        for num_rows in [0, 1, 2, 3, 5, 10, 100, 1000]:
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
            paratext.serial.save_frame(fn, expected, allow_quoted_newlines, out_encoding=out_encoding, dos=dos)
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
                        for num_rows in [0, 1,2,3,4,10,100,600]:
                            for num_cols in [1,2,3,4,5,10]:
                                for num_threads in [1,2,4,8,16]:
                                    yield self.do_hell_frame, dos, frame_encoding, out_encoding, include_null, allow_quoted_newlines, num_rows, num_cols, num_threads


class TestTypeDetection:

    def test_edge_case1(self):
        filedata = b"""A,B
A.1,3ABC
"""
        with generate_tempfile(filedata) as fn:
            expected = {"A": ["A.1"], "B": ["3ABC"]}
            logging.debug("filename: %s" % fn)
            actual = paratext.load_csv_to_pandas(fn)
            assert_dictframe_almost_equal(actual, expected)

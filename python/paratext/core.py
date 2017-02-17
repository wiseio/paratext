"""
Loads text files using multiple cores.
"""

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


#
#   Coder: Damian Eads
#

import paratext_internal as pti

import os
import ntpath
import six
from six.moves import range
from six.moves.urllib_parse import urlparse

import random
import numpy as np
import string

def _docstring_parameter(*sub):
     def dec(obj):
         obj.__doc__ = obj.__doc__.format(*sub)
         return obj
     return dec

_csv_load_params_doc = """
    filename : string
        The name of the CSV file to load.

    num_threads : int
        The number of parser threads to spawn. (default=num_cores)

    allow_quoted_newlines : bool
        Whether to allow newlines to be quoted. (default=False)

    block_size : int
        The number of bytes to read at a time in each worker. (default=32768)

    number_only : bool
        Whether it can be safely assumed the columns only contain numbers.
        (default=False)

    no_header : bool
        Do not auto-detect the presence of a header. Assume the first line
        is data. (default=False)

    max_level_name_length : int
        The maximum length of a categorical level name. If a text field has
        a length that exceeds this length, the entire column is treated as
        text. (default=max integer)

    max_levels : int
        The maximum number of levels of a categorical column. If a column
        has more than ``max_levels`` unique strings, it is treated as text.
        (default=max integer)

    cat_names : sequence
        A list of column names that should be treated as categorical
        regardless of its inferred type.

    text_names : sequence
        A list of column names that should be treated as rich text
        regardless of its inferred type.

    num_names : sequence
        A list of column names that should be treated as numeric
        regardless of its inferred type.

    in_encoding : str
        The encoding of the data read from the file. (default=None)

    out_encoding : str
        The encoding of the strings returned. If set to 'utf-8', the
        parsed data will be converted to UTF-8 unicode object (Python 2.7)
        or str object (Python 3+). Otherwise a str object (Python 2.7)
        or bytes object (Python 3+) is returned. (default=None)

    convert_null_to_space : bool
        Whether to convert null terminator characters to spaces. (default=False)
"""

def _get_params(num_threads=0, allow_quoted_newlines=False, block_size=32768, number_only=False, no_header=False, max_level_name_length=None, max_levels=None, convert_null_to_space=True):
    params = pti.ParseParams()
    params.allow_quoted_newlines = allow_quoted_newlines
    if num_threads > 0:
        params.num_threads = num_threads
    else:
        params.num_threads = int(max(pti.get_num_cores(), 4))
    params.number_only = number_only
    params.no_header = no_header
    params.convert_null_to_space = convert_null_to_space
    if max_levels is not None:
        params.max_levels = max_levels;
    if max_level_name_length is not None:
        params.max_level_name_length = max_level_name_length
    return params

def _make_posix_filename(fn_or_uri):
     if ntpath.splitdrive(fn_or_uri)[0] or ntpath.splitunc(fn_or_uri)[0]:
         result = fn_or_uri
     else:
         parse_result = urlparse(fn_or_uri)
         if parse_result.scheme in ('', 'file'):
              result = parse_result.path
         else:
              raise ValueError("unsupported protocol '%s'" % fn_or_uri)
     if six.PY2 and isinstance(result, unicode):
       # SWIG needs UTF-8
       result = result.encode("utf-8")
     return result

@_docstring_parameter(_csv_load_params_doc)
def internal_create_csv_loader(filename, num_threads=0, allow_quoted_newlines=False, block_size=32768, number_only=False, no_header=False, max_level_name_length=None, max_levels=None, cat_names=None, text_names=None, num_names=None, in_encoding=None, out_encoding=None, convert_null_to_space=True):
    """
    Creates a ParaText internal C++ CSV reader object and reads the CSV
    file in parallel. This function ordinarily should not be called directly.

    Parameters
    ----------
    {0}

    Returns
    -------
    loader : a paratext_internal.ColBasedLoader object

         Returns a C++ loader object with the parsed CSV embedded in the
         parallel worker's scratch space.
    """
    loader = pti.ColBasedLoader()
    params = pti.ParseParams()
    params.allow_quoted_newlines = allow_quoted_newlines
    if num_threads > 0:
        params.num_threads = num_threads
    else:
        params.num_threads = int(max(pti.get_num_cores(), 4))
    params.number_only = number_only
    params.no_header = no_header
    params.convert_null_to_space = convert_null_to_space
    if max_levels is not None:
        params.max_levels = max_levels;
    if max_level_name_length is not None:
        params.max_level_name_length = max_level_name_length
    if six.PY2:
        encoder = lambda x: x.encode("utf-8")
    else:
        encoder = lambda x: x
    if cat_names is not None:
        for name in cat_names:
            name = encoder(name)
            loader.force_semantics(name, pti.CATEGORICAL)
    if num_names is not None:
        for name in num_names:
            name = encoder(name)
            loader.force_semantics(name, pti.NUMERIC)
    if text_names is not None:
        for name in text_names:
            name = encoder(name)
            loader.force_semantics(name, pti.TEXT)
    if in_encoding is not None and in_encoding not in ("utf-8", "unknown"):
        raise ValueError("invalid encoding: " % in_encoding)
    if out_encoding is not None and out_encoding not in ("utf-8", "unknown"):
        raise ValueError("invalid encoding: " % out_encoding)
    if in_encoding == "utf-8":
        loader.set_in_encoding(pti.UNICODE_UTF8)
    if out_encoding == "utf-8":
        loader.set_out_encoding(pti.UNICODE_UTF8)
    loader.load(_make_posix_filename(filename), params)
    return loader

def internal_csv_loader_transfer(loader, forget=True, expand=False):
    """
    This function should not be called directly. 

    It transfers data already loaded into C++ worker scratch space
    column-by-column into a numpy array.

    Parameters
    ----------
    loader : paratext.paratext_internal.ColBasedLoader
         An internal ParaText loader object. The ``.load()`` function
         has already been called.

    forget : bool
         Whether to deallocate associated scratch space once a column
         has been transfered.

    expand : bool
         Whether to expand categorical data into string columns.

    Returns
    -------
    gen : a Python generator object

         Case 1 (expand=False) Each value of the generator is a tuple::

             (column_name, data, semantics, levels)

         where ``column_name`` is the name of the column, ``data`` is
         a NumPy array, ``semantics`` is a string indicating the kind
         of data stored (``num`` for numeric, ``cat`` for categorical,
         ``text for rich text), and levels is a NumPy array of levels
         strings (if categorical).

         Categorical levels are encoded as integers. A value data[i]
         can be decoded as a string with levels[data[i]].

         Case 2 (expand=True) Each value of the generator is a tuple::

             (column_name, data)

         where ``column_name`` is the name of the column, ``data`` is
         a NumPy array, ``semantics`` is a string indicating the kind
         of data stored (``num`` for numeric, ``cat`` for categorical,
         ``text for rich text), and levels is a NumPy array of levels
         strings (if categorical). All categorical columns are converted
         to string columns, however identical level values refer to the
         same string object to save space.

    """
    for i in range(loader.get_num_columns()):
        col = loader.get_column(i)
        info = loader.get_column_info(i)
        semantics = 'num'
        levels = None
        if info.semantics == pti.CATEGORICAL:
            levels = loader.get_levels(i)
            semantics = 'cat'
        elif info.semantics == pti.NUMERIC:
            semantics = 'num'
        elif info.semantics == pti.TEXT:
            semantics = 'text'
        else:
            semantics = 'unknown'
        if forget:
            loader.forget_column(i)
        if expand:
            if info.semantics == pti.CATEGORICAL:
                yield ((info.name, col[levels], semantics))
            else:
                yield ((info.name, col, semantics))
        else:
            yield ((info.name, col, semantics, levels))

@_docstring_parameter(_csv_load_params_doc)
def load_raw_csv(filename, *args, **kwargs):
    """
    Loads a CSV file, producing a generator object that can be used to
    generate a pandas DataFrame, Wise DataSet, a dictionary, or a custom
    DataFrame.

    This function is very aggressive about freeing memory. After each
    value is generated, the corresponding scratch space in the parser
    and worker threads is deallocated.

    Parameters
    ----------
    {0}

    Returns
    -------
    gen : a Python generator object

         Each value of the generator is a tuple::

             (column_name, data, semantics, levels)

         where ``column_name`` is the name of the column, ``data`` is
         a NumPy array, ``semantics`` is a string indicating the kind
         of data stored (``num`` for numeric, ``cat`` for categorical,
         ``text for rich text), and levels is a NumPy array of levels
         strings (if categorical).

         Categorical levels are encoded as integers. A value data[i]
         can be decoded as a string with levels[data[i]].

    """
    loader = internal_create_csv_loader(filename, *args, **kwargs)
    return internal_csv_loader_transfer(loader, forget=True)

@_docstring_parameter(_csv_load_params_doc)
def load_csv_to_dict(filename, *args, **kwargs):
    """
    Loads a CSV file into a Python dictionary of NumPy arrays.

    Parameters
    ----------
    {0}

    Returns
    -------
    d : dict
        The values are the column data as arrays, which are keyed by
        name.
    levels : dict
        The levels for categorical columns.
    """
    all_levels = {}
    frame = {}
    for name, col, semantics, levels in load_raw_csv(filename, *args, **kwargs):
        if semantics == 'cat':
            all_levels[name] = levels
        frame[name] = col
    return frame, all_levels

@_docstring_parameter(_csv_load_params_doc)
def load_csv_to_expanded_columns(filename, *args, **kwargs):
    """
    Loads a CSV file into a generator. However, levels are represented
    with strings rather than integers.

    This function is aggressive about freeing memory. After each
    value is generated, the corresponding scratch space in the parser
    and worker threads is deallocated.

    Parameters
    ----------
    {0}

    Returns
    -------
    d : dict
        The values are the column data as arrays, which are keyed by
        name.

         Each value of the generator is a tuple::

             (column_name, data)

    Examples
    --------
    >>> def read_csv_into_pandas(filename, *args, **kwargs):
        return pandas.DataFrame.from_items(filename, *args, **kwargs)
    """
    for name, col, semantics, levels in load_raw_csv(filename, *args, **kwargs):
        if levels is not None and len(levels) > 0:
            yield name, levels[col]
        else:
            yield name, col

def load_csv_as_iterator(filename, expand=True, forget=True, *args, **kwargs):
     """
     Loads a CSV file, producing a generator object that can be used to
     generate a pandas DataFrame, Wise DataSet, a dictionary, or a custom
     DataFrame.

     This function is very aggressive about freeing memory. After each
     value is generated, the corresponding scratch space in the parser
     and worker threads is deallocated.
     """
     if expand:
          return load_csv_to_expanded_columns(filename, *args, **kwargs)
     else:
          return load_raw_csv(filename, *args, **kwargs)

@_docstring_parameter(_csv_load_params_doc)
def load_csv_to_pandas(filename, *args, **kwargs):
    """
    Loads a CSV file into a generator.

    This function is aggressive about freeing memory. After each
    value is generated, the corresponding scratch space in the parser
    and worker threads is deallocated.

    Parameters
    ----------
    {0}

    Returns
    -------
    d : pandas.DataFrame
        The contents of the CSV file as a Pandas DataFrame.
    """
    import pandas
    expanded = load_csv_to_expanded_columns(filename, *args, **kwargs)
    if os.path.getsize(filename) < 2 ** 10:  # cover the case of empty files have 0-element generators
         expanded = list(expanded)
         if len(expanded) > 0:
              return pandas.DataFrame.from_items(expanded)
         else:
              return pandas.DataFrame()
    else:
         return pandas.DataFrame.from_items(expanded)

@_docstring_parameter(_csv_load_params_doc)
def baseline_average_columns(filename, type_check=False, *args, **kwargs):
    """
    Computes the sum of numeric columns in a numeric-only CSV file.

    This is useful for computing a baseline for performance metrics.
    It reads a file without any parsing, but the memory requirements
    are fixed in the number of columns. Memory does not grow the larger
    the file in the number of lines.


    Parameters
    ----------
    type_check : bool

         Whether to perform type checking.

    {0}

    Returns
    -------
    d : dictionary
        A dictionary of column averages.
    """
    params = _get_params(*args, **kwargs)
    summer = pti.ParseAndSum();
    summer.load(_make_posix_filename(filename), params, type_check)
    d = {summer.get_column_name(i): summer.get_avg(i) for i in range(summer.get_num_columns())}
    return d

@_docstring_parameter(_csv_load_params_doc)
def baseline_newline_count(filename, *args, **kwargs):
    """
    Computes the number of newline characters in the file.

    Note: this is not the same as the number of lines in a file.

    Parameters
    ----------
    {0}

    Returns
    -------
    n : int
        The number of newlines in a file.
    """
    params = _get_params(*args, **kwargs)
    nc = pti.NewlineCounter();
    count = nc.load(_make_posix_filename(filename), params)
    return count

@_docstring_parameter(_csv_load_params_doc)
def baseline_disk_to_mem(filename, *args, **kwargs):
    """
    This function copies the contents of a file into a collection of buffers.
    The buffers are then deallocated. This is useful for computing a baseline 
    for performance metrics. It reads a file without any parsing, but the
    memory requirements grow with the size of the file.

    Parameters
    ----------
    {0}
    """
    params = _get_params(*args, **kwargs)
    mc = pti.MemCopyBaseline();
    count = mc.load(_make_posix_filename(filename), params)
    return count

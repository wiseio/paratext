"""
Name:    paracsv
Author:  Damian Eads
Purpose: Loads CSV files using multiple cores.
"""
import paratext_internal as pti

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
         Whether it can be safely assumed the columns only contain numbers. (default=False)
"""

@_docstring_parameter(_csv_load_params_doc)
def load_raw_csv(filename, num_threads=0, allow_quoted_newlines=False, block_size=32768, number_only=False):
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
         of data stored (``num`` for numeric, ``cat`` for categorical),
         and levels is a NumPy array of levels strings (if categorical).

         Categorical levels are encoded as integers. A value data[i]
         can be decoded as a string with levels[data[i]].

    """
    loader = pti.ColBasedLoader()
    params = pti.ParseParams()
    params.allow_quoted_newlines = allow_quoted_newlines
    if num_threads > 0:
        params.num_threads = num_threads
    else:
        params.num_threads = 4
    params.number_only = number_only
    loader.load(filename, params)
    data = []
    #all_levels = {}
    for i in xrange(0, loader.get_num_columns()):
        col = loader.get_column(i)
        info = loader.get_column_info(i)
        semantics = 'num'
        levels = None
        if info.semantics == pti.CATEGORICAL:
            levels = loader.get_levels(i)
            semantics = 'cat'
            #all_levels[info.name] = levels
        loader.forget_column(i)
        yield ((info.name, col, semantics, levels))

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
    return pandas.DataFrame.from_items(load_csv_to_expanded_columns(filename, *args, **kwargs))

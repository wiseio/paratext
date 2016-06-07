ParaText
========

ParaText is a C++ library to read text files in parallel on multi-core
machines. The alpha release includes a CSV reader and Python bindings.

ParaText has the following dependencies for its Python bindings:

   - a C++ compiler that is C++11 compliant (gcc 4.8 or above, clang 3.4 or above)
   - SWIG 2.0.11 or above
   - Python (2.7 or above)
   - setuptools

Though Pandas is optional, it must be installed to use ParaText to
read CSV files into Pandas.

The ParaText library can be built with two commands:

```
   cd python/
   python setup.py build install
```

Use the `--prefix` option if you prefer to install ParaText to a
different location:

```
   cd python/
   python setup.py build install --prefix=/my/prefix/dir
```


Using ParaText in Python
========================

First, import the `paratext` Python package.

```
   import paratext
```

Loading into Pandas
-------------------

A CSV file can be loaded into Pandas in just one line of code using
the `load_csv_to_pandas` function.

```
df = paratext.load_csv_to_pandas("hepatitis.csv")
```

Its output looks something like this:

```
In [1]: print df.head()
   AGE     SEX STEROID ANTIVIRALS FATIGUE MALAISE ANOREXIA LIVER_BIG  \
0   30    male      no         no      no      no       no        no   
1   50  female      no         no     yes      no       no        no   
2   78  female     yes         no     yes      no       no       yes   
3   31  female     nan        yes      no      no       no       yes   
4   34  female     yes         no      no      no       no       yes   

  LIVER_FIRM SPLEEN_PALPABLE SPIDERS ASCITES VARICES  BILIRUBIN  \
0         no              no      no      no      no        1.0   
1         no              no      no      no      no        0.9   
2         no              no      no      no      no        0.7   
3         no              no      no      no      no        0.7   
4         no              no      no      no      no        1.0   

   ALK_PHOSPHATE  SGOT  ALBUMIN  PROTIME HISTOLOGY Class  
0             85    18      4.0      NaN        no  LIVE  
1            135    42      3.5      NaN        no  LIVE  
2             96    32      4.0      NaN        no  LIVE  
3             46    52      4.0       80        no  LIVE  
4            NaN   200      4.0      NaN        no  LIVE
```

Loading into Dictionaries (more memory-efficient)
-------------------------------------------------

A Python dictionary of arrays is preferable over a DataFrame
if the memory budget is very tight. The `load_csv_to_dict`
loads a CSV file, storing the columns as a dictionary of
arrays.

```
  dict_frame, levels = paratext.load_csv_to_dict(filename)
```

It returns a two element tuple. The first `dict_frame` is a Python
dictionary that maps column names to column data. The second `levels`
is also a Python dictionary keyed by column name. It contains a list
of level strings for each categorical column.

The following code visits the columns. For each column, it
prints its name, the first 5 values of its data, and the categorical
levels (`None` if not categorical).

```
  for key in dict_frame.keys():
      print key, repr(dict_frame[key][0:5]), levels.get(key, None)
```

The output looks something like this:

```
PROTIME array([ nan,  nan,  nan,  80.,  nan], dtype=float32) None
LIVER_BIG array([0, 0, 1, 1, 1], dtype=uint8) ['no' 'yes' 'nan']
ALBUMIN array([ 4. ,  3.5,  4. ,  4. ,  4. ], dtype=float32) None
ALK_PHOSPHATE array([  85.,  135.,   96.,   46.,   nan], dtype=float32) None
ANTIVIRALS array([0, 0, 0, 1, 0], dtype=uint8) ['no' 'yes']
HISTOLOGY array([0, 0, 0, 0, 0], dtype=uint8) ['no' 'yes']
BILIRUBIN array([ 1.,  0.89999998,  0.69999999,  0.69999999, 1. ], dtype=float32) None
AGE array([30, 50, 78, 31, 34], dtype=uint8) None
SEX array([0, 1, 1, 1, 1], dtype=uint8) ['male' 'female']
STEROID array([0, 0, 1, 2, 1], dtype=uint8) ['no' 'yes' 'nan']
SGOT array([  18.,   42.,   32.,   52.,  200.], dtype=float32) None
MALAISE array([0, 0, 0, 0, 0], dtype=uint8) ['no' 'yes' 'nan']
FATIGUE array([0, 1, 1, 0, 0], dtype=uint8) ['no' 'yes' 'nan']
SPIDERS array([0, 0, 0, 0, 0], dtype=uint8) ['no' 'yes' 'nan']
VARICES array([0, 0, 0, 0, 0], dtype=uint8) ['no' 'nan' 'yes']
LIVER_FIRM array([0, 0, 0, 0, 0], dtype=uint8) ['no' 'yes' 'nan']
SPLEEN_PALPABLE array([0, 0, 0, 0, 0], dtype=uint8) ['no' 'yes' 'nan']
ASCITES array([0, 0, 0, 0, 0], dtype=uint8) ['no' 'yes' 'nan']
Class array([0, 0, 0, 0, 0], dtype=uint8) ['LIVE' 'DIE']
ANOREXIA array([0, 0, 0, 0, 0], dtype=uint8) ['no' 'yes' 'nan']
```

All categorical columns in this data set have 3 or fewer levels so
they are all `uint8_t`. A string representation uses at least 8 times
as much space, but it can also be less computationally efficient. An
integer representation is ideal for learning on categorical columns.
Integer comparisons over contiguous integer buffers are pretty cheap
compared to exhaustive string comparisons on (potentially)
discontiguous string values. This makes a big difference for
combinatorial learning algorithms.

Handling Multi-Line Fields
--------------------------

ParaText supports reading CSV files with multi-line fields in
parallel. This feature must be explicitly activated as it requires
extra overhead to adjust the boundaries of the chunks processed by
the workers.

```
df = paratext.load_csv_to_pandas("messy.csv", allow_quoted_newlines=True)
```

Header Detection
----------------

ParaText detects the presence of a header. This can be turned off with
`no_header=True`.

Column Types Supported
----------------------

Wise ParaText supports three kinds of columns:

    - numeric: for numeric data.
    - categorical: for categorical data.
    - text: for large strings like e-mails and text documents.

In the library, we distinguish between semantics and data type. The
semantics defines how to interpret a column. The data type (`uint8`,
`int64`, `float`, etc.) defines how its encoded.

Parameters
----------

Most CSV loading functions in ParaText have the following

    - `cat_names`: A list of column names to force as categorical regardless
    of the inferred type.

    - `text_names`: A list of column names that should be treated as rich text
    regardless of its inferred type.

    - `num_names`: A list of column names that should be treated as
    numeric regardless of its inferred type.

    - `num_threads`:  The number of parser threads to spawn. The default
    is the number of cores.

    - `allow_quoted_newlines`:  Allows multi-line text fields. This
    is turned off by default.

    - `no_header`: Do not auto-detect the presence of a header. Assume
    the first line is data. This is turned off by default.

    - `max_level_name_length`: If a field's length exceeds this value,
    the entire column is treated as text rather than
    categorical. The default is unlimited.

    - `max_levels`: The maximum number of levels of a categorical column.
    (default=max integer)

    - `number_only`: Whether it can be safely assumed the columns only
    contain numbers. This is turned off by default.

    - `block_size`: The number of bytes to read at a time in each worker.
    (default=32768)

Other Notes
-----------

ParaText is a work-in-progress. There are a few unimplemented features
that may prevent it from working on all CSV files. We note them below.

1. ParaText does not yet support escape characters or comments.

2. There is no support for type hints (e.g. `uint64` or `float`) of a
column.  Only the interpretation of a column (numeric, categorical, or
text) can be forced.

3. ParaText does not support
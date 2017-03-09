ParaText
========

<a href="https://travis-ci.org/wiseio/paratext.svg?branch=master"><img src="https://travis-ci.org/wiseio/paratext.svg?branch=master" qlt="Paratext Travis Status"></a>
<a href="https://github.com/wiseio/paratext/blob/master/README.md"><img src="https://img.shields.io/github/license/wiseio/paratext.svg" qlt="Paratext License"></a>

ParaText is a C++ library to read text files in parallel on multi-core
machines. The alpha release includes a CSV reader and Python bindings.
The library itself has no dependencies other than the standard library.

Depedencies
-----------
ParaText has the following dependencies:

   - a fully C++11-compliant C++ compiler (gcc 4.8 or above, clang 3.4 or above)
   - SWIG 2.0.7 or above (Python 2 bindings)
   - SWIG 3.0.8 or above (Python 3 bindings)
   - Python 2.7 or 3.5
   - setuptools
   - numpy

Pandas is required only if using ParaText to read CSV files into
Pandas. The SWIG available from Ubuntu 14.04 does not work with Python 3.

Anaconda packages the latest version of SWIG that works properly
with Python 3. You can install it as follows:

```
conda install swig
```

Building Python
---------------

First, go into the `python` directory:

```
   cd python/
```

Then run `setup.py`:

```
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

The data frame looks something like this:

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

This gives the following output:

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
they are all `uint8`. A string representation uses at least 8 times
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

Column Typing
-------------

This library distinguishes between a column's data type and its semantics.
The semantics defines how to interpret a column (e.g. numeric vs. categorical).
and the data type (`uint8`, `int64`, `float`, etc.) is the type for encoding
column values.

Three semantic types are supported:

   * `num`: numeric data.

   * `cat`: categorical data.

   * `text`: large strings like e-mails and text documents.

ParaText supports `(u)int(8|16|32|64)|float|double|string` data types.

Parameters
----------

Most CSV loading functions in ParaText have the following parameters:

   * `cat_names`: A list of column names to force as categorical regardless
   of the inferred type.

   * `text_names`: A list of column names that should be treated as rich text
   regardless of its inferred type.

   * `num_names`: A list of column names that should be treated as
   numeric regardless of its inferred type.

   * `num_threads`:  The number of parser threads to spawn. The default
   is the number of cores.

   * `allow_quoted_newlines`:  Allows multi-line text fields. This
   is turned off by default.

   * `no_header`: Do not auto-detect the presence of a header. Assume
   the first line is data. This is turned off by default.

   * `max_level_name_length`: If a field's length exceeds this value,
   the entire column is treated as text rather than
   categorical. The default is unlimited.

   * `max_levels`: The maximum number of levels of a categorical column.
   The default is unlimited.

   * `number_only`: Whether it can be safely assumed the columns only
   contain numbers. The default is unlimited.

   * `block_size`: The number of bytes to read at a time in each worker
   thread. The default is unlimited.

Escape Characters
-----------------

ParaText supports backslash escape characters:

    * `\t': tab

    * `\n': newline

    * `\r': carriage return

    * `\v': vertical tab

    * `\0': null terminator (0x00)

    * `\b': backspace

    * '\xnn': an 8-bit character represented with a 2 digit hexidecimal number.

    * '\unnnn': a Unicode code point represented as 4-digit hexidecimal number.

    * '\Unnnnnnnn': a Unicode code point represented as 8-digit hexiecimal number.

Writing CSV
-----------

ParaText does not yet support parallel CSV writing. However, it bundles a CSV
writer that can be used to write DataFrames with arbitrary string and byte
buffer data in a lossless fashion.

If a character in a Python `string`, `unicode`, or `bytes`
object could be treated as non-data when parsed (e.g. a doublequote or
escape character), it is escaped. Moreover, any character that is outside
the desired encoding is also escaped. This enables, for example,
the lossless writing of non-UTF-8 to a UTF-8 file.

For example, to restrict the encoding to 7-bit printable ASCII, pass
`out_encoding='printable_ascii'`

```
   import paratext.serial
   df = pandas.DataFrame({"X": [b"\xff\\\n \" oh my!"]})
   paratext.serial.save_frame("lossless.csv", df, allow_quoted_newlines=True, out_encoding='printable_ascii', dos=False)
```

This results in a file:

```
"X"
"\xff\\
 \" oh my!"
```

Instead, pass `out_encoding='utf-8'` to ``save_frame``.

```
   import paratext.serial
   df = pandas.DataFrame({"X": [b"\xff\\\n \" oh my!"],"Y": ["\U0001F600"]})
   paratext.serial.save_frame("lossless2.csv", df, allow_quoted_newlines=True, out_encoding='utf-8', dos=False)
```

Now, the file only escapes cells in the DataFrame with
non-UTF8 data. All other UTF8 characters are preserved.
```
"X","Y"
"\xff\\
 \" oh my!","<U+1F600>"
```

Other Notes
-----------

ParaText is a work-in-progress. There are a few unimplemented features
that may prevent it from working on all CSV files. We note them below.

1. There is no way to supply type hints (e.g. `uint64` or `float`) of a
column. Only the interpretation of a column (numeric, categorical, or
text) can be forced.

2. DateTime will be supported in a future release.

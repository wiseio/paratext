ParaText
========

ParaText is a library for parallel reading of text files. Currently,
only bindings exist for Python, but the design and implementation
of the library is language-agnostic.

Dependencies
------------
ParaText requires the following tools:

   - a C++ compiler that is C++11 compliant (gcc 4.8 and above)
   - SWIG 2.0.11 or above
   - Python (2.7 or above)
   - setuptools
   - Pandas

Compilation (Python)
--------------------

1. go into the Python directory::

   cd python/

2. build the binary::

   python setup.py build

3. install it and include an optional prefix::

   python setup.py build install --prefix=/tmp/qqq

Usage (Python)
--------------

First, import the paratext Package::

   import paratext

To load a CSV file into a Python dictionary, do::

  d = paratext.load_csv_to_dict("my.csv", num_threads=16)

Adding Support for Other Languages
----------------------------------

Wise.io welcomes contributions from other developers. To add a SWIG
typemap, please e-mail damian@wise.io for more information.

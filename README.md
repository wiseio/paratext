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

Compilation (Python)
--------------------

First, in the root directory, generate the SWIG bindings::

   swig -c++ -python -Isrc/ -outdir python/ src/paratext_internal.i
   
If the bindings are generated successsfully, go into the Python directory
and build the Python package.

   python setup.py build install
   
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

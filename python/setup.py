
import sys, os, os.path, string, subprocess
import json

# First, check for the presence of swig, which we will need to build
# the Python bindings.
if sys.platform == "win32":
    cmd = "where" # no 'which' command on windows
else:
    cmd = "which"
p = subprocess.Popen([cmd, "swig"])
p.communicate("")
if p.returncode != 0:
    print("Error: you must install SWIG first.")
    sys.exit(1)

# The multi-threaded reader will core dump unless -pthread is given.
extra_link_args = []
extra_compile_args = ["-std=c++11", "-Wall", "-Wextra", "-pthread"]
extra_libraries = []

if sys.platform == 'darwin':
    extra_compile_args += ["-m64", "-D_REENTRANT"]
    extra_link_args += []
    extra_libraries += ["stdc++"]
elif sys.platform.startswith("linux"):
    extra_compile_args += []
    extra_link_args += []
    extra_libraries += ["stdc++"]
elif sys.platform == "win32":
    extra_link_args = []
    extra_compile_args = ["/Wall"]
    extra_libraries = []
    

if len(set(('develop', 'release', 'bdist_egg', 'bdist_rpm',
            'bdist_wininst', 'install_egg_info', 'build_sphinx',
            'egg_info', 'easy_install', 'upload',
            )).intersection(sys.argv)) > 0:
    import setuptools
    extra_setuptools_args = dict(
        zip_safe=False,  # the package can run out of an .egg file
        )
else:
    extra_setuptools_args = dict()

from numpy.distutils.core import setup, Extension

version = "0.1.1rc1"

init_py = open("paratext/__init__.py", "w")

init_py.write("""
__all__ = ['paratext']

from paratext.core import *
from paratext.helpers import *

import paratext_internal
import warnings

__version__ = "%s"
""" % version)

init_py.close()


print(version)

swig_cmd = ["swig", "-c++", "-python"]

if sys.version_info >= (3,):
    swig_cmd += ["-py3"]

swig_cmd += ["-I../src/", "-outdir", "./", "../src/paratext_internal.i"]

print("running swig: ", swig_cmd)
p = subprocess.Popen(swig_cmd)
p.communicate("")
if p.returncode != 0:
    print("Error generating SWIG wrappers.")
    sys.exit(1)

setup(name='paratext',
      version=version,
      description='Reads text files in parallel. The first release includes a parallel CSV reader.',
      long_description="""
See README
""",
      keywords=['csv', 'reading'],
      ext_modules=[Extension('_paratext_internal',
                             ['../src/paratext_internal_wrap.cxx', '../src/paratext_internal.cpp'],
                             extra_link_args = extra_link_args,
                             extra_compile_args = extra_compile_args,
                             include_dirs=['../src/'],
                             libraries = extra_libraries),
                   ],
      py_modules=["paratext_internal"],
      author="Damian Eads",
      author_email="damian@wise.io",
      license="Apache License",
      packages = ['paratext'],
      url = 'http://wise.io',
      include_package_data = True,
      **extra_setuptools_args
      )

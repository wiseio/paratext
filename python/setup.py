
import sys, os, os.path, string, subprocess
import json

p = subprocess.Popen(["which", "swig"])
p.communicate("")
if p.returncode != 0:
    print("Error: you must install SWIG first.")
    sys.exit(1)

extra_link_args = []
extra_compile_args = ["-std=c++11", "-Wall", "-Wextra", "-pthread"]
extra_libraries = []

if sys.platform == 'darwin':
    extra_compile_args += ["-m64", "-D_REENTRANT"]
    extra_libraries += []
elif sys.platform.startswith("linux"):
    extra_compile_args += []
    extra_link_args += []
    extra_libraries += []

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

init_py.write("""#!/usr/bin/python
__all__ = ['paratext']

import core, helpers
from core import *

import paratext_internal
import warnings

__version__ = "%s"
""" % version)

init_py.close()


print(version)

swig_cmd = ["swig", "-c++", "-python", "-py3", "-I../src/", "-outdir", "./", "../src/paratext_internal.i"]

print("running swig: ", swig_cmd)
p = subprocess.Popen(swig_cmd)
p.communicate("")
if p.returncode != 0:
    print("Error: building")
    sys.exit(1)

setup(name='paratext',
      version=version,
      description='Reads text files in parallel. The first release includes a paralell CSV reader.',
      long_description="""
See README
""",
      keywords=['csv', 'reading'],
      ext_modules=[Extension('_paratext_internal',
                             ['../src/paratext_internal_wrap.cxx', '../src/paratext_internal.cpp'],
                             extra_link_args = extra_link_args,
                             extra_compile_args = extra_compile_args,
                             include_dirs=['../src/'],
                             libraries=["stdc++"] + extra_libraries),
                   ],
      py_modules=["paratext_internal"],
      author="Damian Eads",
      author_email="damian@wise.io",
      license="GNU General Public License",
      packages = ['paratext'],
      url = 'http://wise.io',
      include_package_data = True,
      **extra_setuptools_args
      )

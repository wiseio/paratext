/*
    ParaText: parallel text reading
    Copyright (C) 2016. wise.io, Inc.

   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.
 */

/*
  Coder: Damian Eads.
 */

%init %{
  import_array();
%}

%exception {
    try {
        $action
    } catch (const std::string &e) {
      std::string s = e;
      SWIG_exception(SWIG_RuntimeError, s.c_str());
      SWIG_fail;
    } catch (std::exception &e) {
      SWIG_exception(SWIG_RuntimeError, e.what());
      SWIG_fail;
    } catch (...) {
      SWIG_exception(SWIG_RuntimeError, "unknown exception");
      SWIG_fail;
    }
}

%typemap(out) std::vector<int> {
  $result = (PyObject*)::build_array<std::vector<int>>($1);
}

%typemap(out) std::vector<double> {
  $result = (PyObject*)::build_array<std::vector<double>>($1);
}

%typemap(out) std::vector<size_t> {
  $result = (PyObject*)::build_array<std::vector<size_t>>($1);
}

%typemap(out) const std::vector<std::string> & {
  { auto range = std::make_pair($1->begin(), $1->end());
   $result = (PyObject*)::build_array_from_range(range);
  }
}

%typemap(out) std::vector<std::string> {
  $result = (PyObject*)::build_array<std::vector<std::string>>($1);
}

%typemap(out) ParaText::CSV::ColBasedPopulator {
  $result = (PyObject*)::build_populator<ParaText::CSV::ColBasedPopulator>($1);
}

%{
#include "python/numpy_helper.hpp"
%}


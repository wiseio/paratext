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

#define PARATEXT_TYPEMAP_EXCEPTION_START try {

#define PARATEXT_TYPEMAP_EXCEPTION_END    } catch (const std::string &e) {\
      std::string s = e;\
      SWIG_exception(SWIG_RuntimeError, s.c_str());\
      SWIG_fail;\
    } catch (const std::exception &e) {\
      SWIG_exception(SWIG_RuntimeError, e.what());\
      SWIG_fail;\
    } catch (const char *emsg) {\
      SWIG_exception(SWIG_RuntimeError, emsg);\
      SWIG_fail;\
    } catch (...) {\
      SWIG_exception(SWIG_RuntimeError, "unknown exception");\
      SWIG_fail;\
    }

%exception {
    try {
        $action
    } catch (const std::string &e) {
      std::string s = e;
      SWIG_exception(SWIG_RuntimeError, s.c_str());
      SWIG_fail;
    } catch (const std::exception &e) {
      SWIG_exception(SWIG_RuntimeError, e.what());
      SWIG_fail;
    } catch (const char *emsg) {
      SWIG_exception(SWIG_RuntimeError, emsg);
      SWIG_fail;
    } catch (...) {
      SWIG_exception(SWIG_RuntimeError, "unknown exception");
      SWIG_fail;
    }
}

%typemap(out) std::vector<int> {
  $result = (PyObject*)::build_array<ParaText::Encoding::UNKNOWN_BYTES, ParaText::Encoding::UNKNOWN_BYTES, std::vector<int>>($1);
}

%typemap(out) std::vector<double> {
  $result = (PyObject*)::build_array<ParaText::Encoding::UNKNOWN_BYTES, ParaText::Encoding::UNKNOWN_BYTES, std::vector<double>>($1);
}

%typemap(out) std::vector<size_t> {
  $result = (PyObject*)::build_array<ParaText::Encoding::UNKNOWN_BYTES, ParaText::Encoding::UNKNOWN_BYTES, std::vector<size_t>>($1);
}

%typemap(out) const std::vector<std::string> & {
  { auto range = std::make_pair($1->begin(), $1->end());
   $result = (PyObject*)::build_array_from_range(range);
  }
}

%typemap(out) std::vector<std::string> {
  $result = (PyObject*)::build_array<ParaText::Encoding::UNKNOWN_BYTES, ParaText::Encoding::UNICODE_UTF8, std::vector<std::string>>($1);
}

%typemap(out) const std::vector<std::string> & {
  { auto range = std::make_pair($1->begin(), $1->end());
    $result = (PyObject*)::build_array_from_range<ParaText::Encoding::UNKNOWN_BYTES, ParaText::Encoding::UNKNOWN_BYTES>(range);
  }
}

%typemap(out) const std::pair<std::vector<std::string>, ParaText::TagEncoding<UNICODE_UTF8, UNICODE_UTF8> > & {
  { auto range = std::make_pair($1->begin(), $1->end());
    $result = (PyObject*)::build_array_from_range<ParaText::Encoding::UNICODE_UTF8>>(range);
  }
}

%typemap(out) const std::pair<std::vector<std::string>, ParaText::TagEncoding<UNNOWN_BYTES, UNICODE_UTF8> > & {
  { auto range = std::make_pair($1->begin(), $1->end());
    $result = (PyObject*)::build_array_from_range<ParaText::Encoding::UNICODE_UTF8>>(range);
  }
}

%typemap(out) std::vector<std::string> {
  $result = (PyObject*)::build_array<ParaText::Encoding::UNKNOWN_BYTES, ParaText::Encoding::UNICODE_UTF8, std::vector<std::string>>($1);
}

%typemap(out) ParaText::CSV::ColBasedPopulator {
  $result = (PyObject*)::build_populator<ParaText::CSV::ColBasedPopulator>($1);
}

%typemap(out) ParaText::CSV::StringVectorPopulator {
  $result = (PyObject*)::build_populator<ParaText::CSV::StringVectorPopulator>($1);
}

/*
%typemap(in) const std::string & {
  std::string result(ParaText::get_as_string($input, 0));
  $1 = &result;
}

%typemap(in) std::string & {
  std::string result(ParaText::get_as_string($input, 0));
  $1 = &result;
}
*/

%typemap(in) const std::string & {
  PARATEXT_TYPEMAP_EXCEPTION_START
  std::unique_ptr<std::string> result(new std::string(ParaText::get_as_string($input, 0)));
  $1 = result.release();
  PARATEXT_TYPEMAP_EXCEPTION_END
}

%typemap(in) std::string & {
  PARATEXT_TYPEMAP_EXCEPTION_START
  std::unique_ptr<std::string> result(new std::string(ParaText::get_as_string($input, 0)));
  $1 = result.release();
  PARATEXT_TYPEMAP_EXCEPTION_END
}

%typemap(freearg) const std::string & {
  delete $1;
}

%typemap(freearg) std::string & {
  delete $1;
}


%typemap(out) const std::string & {
  AsPythonString<ParaText::Encoding::UNKNOWN_BYTES, ParaText::Encoding::UNICODE_UTF8> helper;
  $result = helper(*$1);
}

%typemap(out) std::string & {
  AsPythonString<ParaText::Encoding::UNKNOWN_BYTES, ParaText::Encoding::UNICODE_UTF8> helper;
  $result = helper($1);
}

%typemap(out) std::string {
  AsPythonString<ParaText::Encoding::UNKNOWN_BYTES, ParaText::Encoding::UNICODE_UTF8> helper;
  $result = helper($1);
}

%typemap(out) ParaText::as_raw_bytes {
  AsPythonString<ParaText::Encoding::UNKNOWN_BYTES, ParaText::Encoding::UNKNOWN_BYTES> helper;
  $result = helper($1.val);
}

%typemap(out) ParaText::as_utf8 {
  AsPythonString<ParaText::Encoding::UNKNOWN_BYTES, ParaText::Encoding::UNICODE_UTF8> helper;
  $result = helper($1.val);
}



%{
#include "python/numpy_helper.hpp"
#include "python/python_input.hpp"
%}


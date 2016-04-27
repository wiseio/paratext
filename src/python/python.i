/*
    ParaText: parallel text reading
    Copyright (C) 2016. wise.io, Inc.

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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


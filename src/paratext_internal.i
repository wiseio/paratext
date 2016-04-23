%module paratext_internal

%include "std_string.i"
%include "std_vector.i"
%include "std_pair.i"

%ignore ParaText::ColBasedPopulator::get_type_index() const;
%ignore ParaText::ColBasedLoader::get_type_index(size_t) const;
%ignore ParaText::ColBasedIterator::operator++();
%ignore ParaText::ColBasedIterator::operator++(int);

%{
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#include <numpy/arrayobject.h>
#include <numpy/npy_math.h>
%}

%init %{
  _import_array();
%}

%exception {
    try {
        $action
    } catch (std::string &e) {
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

namespace std {
  %template(vectori) std::vector<int>;
}

%include "numpy.i"

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

%include "paratext_internal.hpp"
%{
#include "paratext_internal.hpp"
%}

%include "parse_params.hpp"
%{
#include "parse_params.hpp"
%}

%typemap(out) std::pair<ParaText::ColBasedIterator<int8_t, true>, ParaText::ColBasedIterator<int8_t, true> > {
  $result = (PyObject*)::build_array_from_range<ParaText::ColBasedIterator<int8_t, true> >($1);
}

%typemap(out) ParaText::ColBasedPopulator {
  $result = (PyObject*)::build_populator<ParaText::ColBasedPopulator>($1);
}

%include "colbased_loader.hpp"
%{
#include "colbased_loader.hpp"
%}

%include "rowbased_loader.hpp"
%{
#include "rowbased_loader.hpp"
%}

%template(column_range_num_int8) ParaText::ColBasedLoader::column_range<int8_t, true>;




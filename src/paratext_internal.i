%module paratext_internal

#if defined(SWIGPYTHON)
%include "python/python.i"
#else
#warning "no SWIG typemaps defined for the target language"
#endif

%include "std_string.i"
%include "std_vector.i"
%include "std_pair.i"

%ignore ParaText::CSV::ColBasedPopulator::get_type_index() const;
%ignore ParaText::CSV::ColBasedLoader::get_type_index(size_t) const;
%ignore ParaText::CSV::ColBasedIterator::operator++();
%ignore ParaText::CSV::ColBasedIterator::operator++(int);

namespace std {
  %template(vectori) std::vector<int>;
}

/////////
%include "paratext_internal.hpp"
%{
#include "paratext_internal.hpp"
%}

%include "generic/parse_params.hpp"
%{
#include "generic/parse_params.hpp"
%}

/////////

%include "csv/colbased_loader.hpp"
%{
#include "csv/colbased_loader.hpp"
%}

#if defined(PARATEXT_ROWBASED)
%include "csv/rowbased_loader.hpp"
%{
#include "csv/rowbased_loader.hpp"
%}
#endif





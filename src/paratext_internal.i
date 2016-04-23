%module paratext_internal

%include "std_string.i"
%include "std_vector.i"
%include "std_pair.i"

%ignore ParaText::ColBasedPopulator::get_type_index() const;
%ignore ParaText::ColBasedLoader::get_type_index(size_t) const;
%ignore ParaText::ColBasedIterator::operator++();
%ignore ParaText::ColBasedIterator::operator++(int);

namespace std {
  %template(vectori) std::vector<int>;
}

%include "python/python.i"

%include "paratext_internal.hpp"
%{
#include "paratext_internal.hpp"
%}

%include "parse_params.hpp"
%{
#include "parse_params.hpp"
%}

/////////

%include "colbased_loader.hpp"
%{
#include "colbased_loader.hpp"
%}

#if defined(PARATEXT_ROWBASED)
%include "rowbased_loader.hpp"
%{
#include "rowbased_loader.hpp"
%}
#endif





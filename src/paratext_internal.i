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

///////// Generic Header
%include "paratext_internal.hpp"
%{
#include "paratext_internal.hpp"
%}

///////// Parsing Parameters
%include "generic/parse_params.hpp"
%{
#include "generic/parse_params.hpp"
%}

//////// CSV-loading Stuff

%include "csv/colbased_loader.hpp"
%{
#include "csv/colbased_loader.hpp"
%}

%include "diagnostic/memcopy.hpp"
%{
#include "diagnostic/memcopy.hpp"
%}

%include "diagnostic/newline_counter.hpp"
%{
#include "diagnostic/newline_counter.hpp"
%}


#if defined(PARATEXT_ROWBASED_CSV)
%include "csv/rowbased_loader.hpp"
%{
#include "csv/rowbased_loader.hpp"
%}
#endif

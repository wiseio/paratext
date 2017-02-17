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

%module paratext_internal

#if defined(SWIGPYTHON)
%include "python/python.i"
#else
#warning "no SWIG typemaps defined for the target language"
#endif

 //%include "std_string.i"
%include "std_vector.i"
%include "std_pair.i"

%ignore ParaText::CSV::ColBasedPopulator::get_type_index() const;
%ignore ParaText::CSV::StringVectorPopulator::get_type_index() const;
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

%include "generic/encoding.hpp"
%{
#include "generic/encoding.hpp"
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

%include "diagnostic/parse_and_sum.hpp"
%{
#include "diagnostic/parse_and_sum.hpp"
%}

%include "util/safe_string_output.hpp"
%{
#include "util/safe_string_output.hpp"
%}

#if defined(PARATEXT_ROWBASED_CSV)
%include "csv/rowbased_loader.hpp"
%{
#include "csv/rowbased_loader.hpp"
%}
#endif

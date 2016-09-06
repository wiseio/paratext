/*
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

   Copyright (C) wise.io, Inc. 2016.
*/
#ifndef PARATEXT_PARACSV_HPP
#define PARATEXT_PARACSV_HPP

#include "generic/parse_params.hpp"

size_t get_num_cores();

std::string as_quoted_string(const std::string &s, bool do_not_escape_newlines = false);

std::string get_random_string(size_t length, size_t min_char, size_t max_char);

#endif

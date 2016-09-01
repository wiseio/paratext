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

#ifndef PARATEXT_PARSE_PARAMS_HPP
#define PARATEXT_PARSE_PARAMS_HPP

#include <string>
#include <limits>
#include "generic/encoding.hpp"

namespace ParaText {

  typedef enum {ROW_BASED, COL_BASED} ParserType;
  typedef enum {NONE, SNAPPY, MSGPACK} Compression;

  typedef enum {CATEGORICAL, NUMERIC, TEXT, UNKNOWN} Semantics;

  template <class T, int InEncoding, int OutEncoding>
  struct TagEncoding {};

  struct ColumnInfo {
    std::string name;
    Semantics semantics;
  };

  struct ParseParams {
    ParseParams() : no_header(false), number_only(false), block_size(32768), num_threads(16), allow_quoted_newlines(false),  max_level_name_length(std::numeric_limits<size_t>::max()), max_levels(std::numeric_limits<size_t>::max()), compression(Compression::NONE), parser_type(ParserType::COL_BASED) {}
    bool no_header;
    bool number_only;
    bool compute_sum;
    size_t block_size;
    size_t num_threads;
    bool allow_quoted_newlines;
    size_t max_level_name_length;
    size_t max_levels;
    Compression compression;
    ParserType parser_type;
  };

}
#endif

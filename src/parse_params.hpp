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

#ifndef PARATEXT_PARSE_PARAMS_HPP
#define PARATEXT_PARSE_PARAMS_HPP

#include <string>
#include <limits>

namespace ParaText {

  typedef enum {ROW_BASED, COL_BASED} ParserType;
  typedef enum {NONE, SNAPPY, MSGPACK} Compression;

  typedef enum {CATEGORICAL, NUMERIC, TEXT, UNKNOWN} Semantics;

  struct ColumnInfo {
    std::string name;
    Semantics semantics;
  };

  struct ParseParams {
    ParseParams() : no_header(false), number_only(false), block_size(32768), num_threads(16), allow_quoted_newlines(false),  max_level_name_length(std::numeric_limits<size_t>::max()), max_levels(std::numeric_limits<size_t>::max()), compression(Compression::NONE), parser_type(ParserType::ROW_BASED) {}
    bool no_header;
    bool number_only;
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

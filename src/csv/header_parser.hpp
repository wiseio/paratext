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

#ifndef PARATEXT_HEADER_PARSER_HPP
#define PARATEXT_HEADER_PARSER_HPP

#include <vector>
#include <fstream>
#include <unordered_set>

#include "util/strings.hpp"

namespace ParaText {

namespace CSV {

  /*
    Parses the first line of a CSV file to determine the header.
   */
  class HeaderParser {
  public:
    /*
      Constructs an uninitialized header parser.
     */
    HeaderParser() : length_(0), end_of_header_(0), has_header_(false) {}

    /*
      Destroys this parser.
     */
    virtual ~HeaderParser() {}

    /*
      Opens a file and parses its header.
     */
    void open(const std::string &filename, bool no_header) {
      struct stat fs;
      if (stat(filename.c_str(), &fs) == -1) {
        std::ostringstream ostr;
        ostr << "cannot open file '" << filename << "'";
        throw std::logic_error(ostr.str());
      }
      length_ = fs.st_size;
      in_.open(filename);
      if (!in_) {
        std::ostringstream ostr;
        ostr << "cannot open file '" << filename << "'";
        throw std::logic_error(ostr.str());
      }
      parse_header(no_header);
    }

    /*
      Returns the number of columns detected in the header.
     */
    size_t get_num_columns() const {
      return column_names_.size();
    }
    
    /*
      Adds a column of a specified name.
     */
    void add_column_name(const std::string &name) {
      //std::cerr << "col " << column_names_.size() << ": " << name << std::endl;

      std::string transformed_name;
      parse_unquoted_string(name.begin(), name.end(), std::back_inserter(transformed_name));
      convert_null_to_space(transformed_name.begin(), transformed_name.end());
      column_names_.push_back(transformed_name);
    }
    
    /*
      Returns a specific name of a column.
     */
    const std::string &get_column_name(size_t index) const {
      return column_names_[index];
    }
    
    /*
      Parses a header.
     */
    void parse_header(bool no_header=false) {
      std::string token;
      size_t current = 0;
      size_t block_size = 4096;
      size_t escape_jump = 0;
#ifndef _WIN32
      char buf[block_size];
#else
      char *buf = (char *)_malloca(block_size);
#endif
      char quote_started = 0;
      bool eoh_encountered = false;
      bool soh_encountered = false;
      in_.seekg(0, std::ios_base::beg);
      while (current < length_ && !eoh_encountered) {
        if (current % block_size == 0) { /* The block is aligned. */
          in_.read(buf, std::min(length_ - current, block_size));
        }
        else { /* Our first read should ensure our further reads are block-aligned. */
          in_.read(buf, std::min(length_ - current, std::min(block_size, current % block_size)));
        }
        size_t nread = in_.gcount();
        size_t i = 0;
        /* ignore leading whitespace in the file. */
        while (i < nread && !soh_encountered) {
          if (isspace(buf[i])) {
            i++; /* eat the whitespace. */
          } else {
            soh_encountered = true;
            /* do not do i++. we need to process it like non-whitespace */
          }
        }
        while (i < nread && !eoh_encountered) {
          if (quote_started) {
            for (; i < nread; i++) {
              if (escape_jump > 0) {
                escape_jump--;
              }
              else if (buf[i] == '\\') {
                escape_jump = 1;
              }
              else if (buf[i] == quote_started) {
                i++;
                quote_started = 0;
                break;
              }
              token.push_back(buf[i]);
            }
          }
          else {
            for (; i < nread; i++) {
              if (escape_jump > 0) {
                token.push_back(buf[i]);
                escape_jump--;
              }
              else if (buf[i] == '\\') {
                token.push_back(buf[i]);
                escape_jump = 1;
              }
              else if (buf[i] == '\"' || buf[i] == '\'') {
                quote_started = buf[i];
                i++;
                break;
              }
              else if (buf[i] == ',') {
                add_column_name(token);
                token.clear();
              }              
              else if (buf[i] == '\r') { /* do nothing: dos wastes a byte each line. */ }
              else if (buf[i] == '\n') {
                add_column_name(token);
                token.clear();
                end_of_header_ = current + i;
                eoh_encountered = true;
                i++;
                break;
              }
              else {
                token.push_back(buf[i]);
              }
            }
          }
        }
        current += nread;
      }
      if (!soh_encountered) { /* If this is just a file of whitespace, then the end of header is the last pos in the file. */
        end_of_header_ = current;
      }
      std::unordered_set<std::string> cnset;
      for (auto &cname : column_names_) {
        cnset.insert(cname);
      }
      has_header_ = true;
      if (cnset.size() != column_names_.size() || no_header) {
        has_header_ = false;
#ifdef PARALOAD_DEBUG
        std::cout << "column names not unique: " << cnset.size() << " unique column names found." ;
#endif
        size_t num_columns = column_names_.size();
        column_names_.clear();
        for (size_t i = 0; i < num_columns; i++) {
          std::ostringstream ostr;
          ostr << "col" << i;
          std::string sstr(ostr.str());
          column_names_.push_back(sstr);
        }
        end_of_header_ = 0;
      }
#ifdef PARALOAD_DEBUG
      std::cout << "Total columns in header: " << column_names_.size() << std::endl;
#endif
      return;
    }

    /*
      Returns the end of the header.
     */
    size_t get_end_of_header() const {
      return end_of_header_;
    }

    bool has_header() const {
      return has_header_;
    }

  private:
    std::ifstream in_;
    std::vector<std::string> column_names_;
    size_t length_;
    size_t end_of_header_;
    bool has_header_;
  };
}
}
#endif

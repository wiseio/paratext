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

#ifndef WISEIO_PARSE_WORKER_COL_BASED_HPP
#define WISEIO_PARSE_WORKER_COL_BASED_HPP

#include "util/strings.hpp"
#include "util/widening_vector.hpp"

#include <fstream>
#include <exception>
#include <stdexcept>
#include <locale>

namespace ParaText {

namespace CSV {

template <class ColumnHandler>
class ColBasedParseWorker {
public:
  ColBasedParseWorker(std::vector<std::shared_ptr<ColumnHandler> > &handlers) : handlers_(handlers), lines_parsed_(0), quote_started_('\0'), column_index_(0), escape_started_(false) {}

  virtual ~ColBasedParseWorker() {}

  void parse(const std::string &filename,
             size_t begin,
             size_t end,
             size_t data_begin,
             size_t file_end,
             const ParaText::ParseParams &params) {
    try {
      if (params.number_only) {
        parse_impl<true>(filename, begin, end, data_begin, file_end, params);
      }
      else {
        parse_impl<false>(filename, begin, end, data_begin, file_end, params);
      }
    }
    catch (...) {
      thread_exception_ = std::current_exception();
    }
  }

  std::exception_ptr get_exception() {
    return thread_exception_;
  }

  template <bool NumberOnly>
  void parse_impl(const std::string &filename,
                  size_t begin,
                  size_t end,
                  size_t data_begin,
                  size_t file_end,
                  const ParaText::ParseParams &params) {
    (void)data_begin;
    (void)file_end;
    std::ifstream in;
    in.open(filename.c_str());
    column_index_ = 0;
    quote_started_ = '\0';
    escape_started_ = false;
    size_t current = begin;
    size_t spos_line = begin, epos_line = begin;
    const size_t block_size = params.block_size;
#ifndef _WIN32
    char buf[block_size];
#else
    char *buf = (char *)_malloca(block_size);
#endif
    in.seekg(current, std::ios_base::beg);
    definitely_string_ = false;
#ifdef PARALOAD_DEBUG
    size_t round = 0;
#endif
    while (current <= end) {
      if (current % block_size == 0) { /* The block is aligned. */
        in.read(buf, std::min(end - current + 1, block_size));
      }
      else { /* Our first read should ensure our further reads are block-aligned. */
        in.read(buf, std::min(end - current + 1, std::min(block_size, current % block_size)));
      }
      size_t nread = in.gcount();
#ifdef PARALOAD_DEBUG
      if (round == 0) {
        std::cout << "R{" << std::string((char *)buf, (char *)buf + nread) << std::endl;
      }
      round++;
#endif
      if (nread == 0) {
        break;
      }
      if (NumberOnly) {
        size_t i = 0;
        for (; i < nread; i++) {
          if (buf[i] == ',') {
            process_token_number_only();
          }
          else if (buf[i] == '\n') {
            epos_line = current + i;
            if (epos_line - spos_line > 0) {
              process_token_number_only();
              process_newline();
            }
            spos_line = epos_line + 1;
            epos_line = spos_line;
          } else {
            token_.push_back(buf[i]);
          }
        }
      } else {
        for (size_t i = 0; i < nread;) {
          if (quote_started_ != '\0') {
            for (; i < nread; i++) {
              if (buf[i] == quote_started_) {
                i++;
                quote_started_ = '\0';
                break;
              }
              token_.push_back(buf[i]);
            }
          }
          else {
            for (; i < nread; i++) {
              if (buf[i] == '"') {
                i++;
                quote_started_ = '\"';
                definitely_string_ = true;
                break;
              }
              else if (buf[i] == ',') {
                process_token();
              }
              else if (buf[i] == '\n') {
                epos_line = current + i;
                if (epos_line - spos_line > 0) {
                  process_token();
                  process_newline();
                }
                spos_line = epos_line + 1;
                epos_line = spos_line;
              }
              else {
                token_.push_back(buf[i]);
              }
            }
          }
        }
      }
      current += nread;
    }
    epos_line = end + 1;
    //std::cout << "start line: " << spos_line << " end line: " << epos_line << std::endl;
    /*
      If we're in the last column position, process the token as some files
      do not end with a newline.
     */
    if (token_.size() > 0) {
      if (NumberOnly) {
        process_token_number_only();
      } else {
        process_token();
      }
    }
    /*
      If there was data on the last line, process it.
    */
    if (column_index_ > 0) {
      process_newline();
    }
#ifdef PARALOAD_DEBUG
    std::cout << "lines parsed: " << lines_parsed_ << std::endl;
#endif
    return;
  }

  void process_newline() {
    if (column_index_ != handlers_.size()) {
      std::ostringstream ostr;
      ostr << "improper number of columns on line number (unquoted in chunk): " << (lines_parsed_ + 1) << ". Expected: " << handlers_.size();
      throw std::logic_error(ostr.str());
    }
    column_index_ = 0;
    lines_parsed_++;
  }

  void process_token_number_only() {
    if (column_index_ >= handlers_.size()) {
      std::ostringstream ostr;
      ostr << "too many columns on line number (unquoted in chunk): " << (lines_parsed_ + 1) << ". Expected: " << handlers_.size();
      throw std::logic_error(ostr.str());
    }
    size_t i = 0;
    for (; i < token_.size() && isspace(token_[i]); i++) {}
    if (i < token_.size()) {
      if (token_[i] == '?' && token_.size() - i == 1) {
        handlers_[column_index_]->process_float(std::numeric_limits<float>::quiet_NaN());
      }
      else if (token_.size() - i == 3 &&
               ((token_[i] == 'n' || token_[i] == 'N'))
               && ((token_[i+1] == 'a' || token_[i+1] == 'A'))
               && (token_[i+2] == 'n' || token_[i+2] == 'N')) {
        handlers_[column_index_]->process_float(std::numeric_limits<float>::quiet_NaN());
      }
      else {
        if (token_[i] == '-') { i++; }
        for (; i < token_.size() && isdigit(token_[i]); i++) {}
        if (i < token_.size() && (token_[i] == '.' || token_[i] == 'E' || token_[i] == 'e')) {
          handlers_[column_index_]->process_float(bsd_strtod(token_.begin(), token_.end()));
        }
        else {
          handlers_[column_index_]->process_integer(fast_atoi<long>(token_.begin(), token_.end()));
        }
      }
    }
    column_index_++;
    token_.clear();
  }

  void process_token() {
    if (column_index_ >= handlers_.size()) {
      std::ostringstream ostr;
      ostr << "too many columns on line number (unquoted in chunk): " << (lines_parsed_ + 1) << ". Expected: " << handlers_.size();
      throw std::logic_error(ostr.str());
    }
    if (definitely_string_) {
      handlers_[column_index_]->process_categorical(token_.begin(), token_.end());
      definitely_string_ = false;
    }
    else {
      size_t i = 0;
      bool integer_possible = false, float_possible = false, exp_possible = false, handled = false;
      for (; i < token_.size() && isspace(token_[i]); i++) {}
      if (i < token_.size()) {
        if (token_[i] == '-') {
          i++;
        }
        else if (token_[i] == '?' && token_.size() - i == 1) {
          handlers_[column_index_]->process_float(std::numeric_limits<float>::quiet_NaN());
          handled = true;
        }
        else if ((token_[i] == 'n' || token_[i] == 'N') && token_.size() - i == 3) {
          if ((token_[i+1] == 'a' || token_[i+1] == 'A') && (token_[i+2] == 'n' || token_[i+2] == 'N')) {
            handlers_[column_index_]->process_float(std::numeric_limits<float>::quiet_NaN());
            handled = true;
          }
        }
      }
      if (!handled) {
        if (i < token_.size()) {
          const std::locale loc("");
          integer_possible = std::isdigit(token_[i], loc);
          i++;
          float_possible = integer_possible, exp_possible = integer_possible;
          while (i < token_.size() && integer_possible) {
            integer_possible = isdigit(token_[i]);
            i++;
          }
          if (i < token_.size()) {
            integer_possible = false;
            float_possible = token_[i] == '.';
            i++;
            while (i < token_.size() && float_possible) {
              float_possible = isdigit(token_[i]);
              i++;
            }
            if (float_possible && i < token_.size()) {
              float_possible = false;
              exp_possible = token_[i] == 'E' || token_[i] == 'e';
              i++;
              if (exp_possible && i < token_.size()) {
                //std::cout << "A";
                if (token_[i] == '+' || token_[i] == '-') {
                  //std::cout << "B";
                  i++;
                  if (i < token_.size()) {
                    //std::cout << "C";
                    exp_possible = isdigit(token_[i]);
                    i++;
                    while (i < token_.size() && exp_possible) {
                      exp_possible = isdigit(token_[i]);
                      i++;
                    }
                  }
                  else {
                    exp_possible = false;
                  }
                }
                else if (isdigit(token_[i])) {
                  //std::cout << "D";
                  while (i < token_.size() && exp_possible) {
                    exp_possible = isdigit(token_[i]);
                    i++;
                  }
                  //std::cout << "E" << exp_possible << (token_[i-1]);
                }
                else {
                  exp_possible = false;
                }
              }
              else {
                exp_possible = false;
              }
            }
          }
        }
      if (integer_possible) {
        handlers_[column_index_]->process_integer(fast_atoi<long>(token_.begin(), token_.end()));
      }
      else if (float_possible || exp_possible) {
        handlers_[column_index_]->process_float(bsd_strtod(token_.begin(), token_.end()));
      }
      else {
        handlers_[column_index_]->process_categorical(token_.begin(), token_.end());
      }

      }
    }
    column_index_++;
    token_.clear();
  }

  void convert_to_cat_or_text(size_t column_index) {
    handlers_[column_index]->convert_to_cat_or_text();
  }

  void convert_to_text(size_t column_index) {
    handlers_[column_index]->convert_to_text();
  }

private:
  std::vector<std::shared_ptr<ColumnHandler> > handlers_;
  std::vector<char>                            token_;
  std::vector<std::pair<size_t, long> >        long_cache_;
  std::vector<std::pair<size_t, double> >      double_cache_;
  std::vector<char>                            str_cache_data_;
  std::vector<size_t>                          str_cache_offsets_;
  std::vector<size_t>                          str_cache_column_;
  bool                                         definitely_string_;
  size_t                                       lines_parsed_;
  char                                         quote_started_;
  size_t                                       column_index_;
  bool                                         escape_started_;
  std::exception_ptr                           thread_exception_;
};
}
}

#endif

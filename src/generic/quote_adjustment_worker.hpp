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

#ifndef PARATEXT_QUOTE_NEWLINE_WORKER_HPP
#define PARATEXT_QUOTE_NEWLINE_WORKER_HPP

#include <cassert>

namespace ParaText {

class QuoteNewlineAdjustmentWorker {
public:
  QuoteNewlineAdjustmentWorker(size_t chunk_start, size_t chunk_end)
    : chunk_start_(chunk_start),
      chunk_end_(chunk_end),
      num_quotes_(0),
      first_unquoted_newline_(-1),
      first_quoted_newline_(-1) {}

  virtual ~QuoteNewlineAdjustmentWorker() {}

  void parse(const std::string &filename) {
    try {
      parse_impl(filename);
    }
    catch (...) {
      thread_exception_ = std::current_exception();
    }
  }

  std::exception_ptr get_exception() {
    return thread_exception_;
  }

  void parse_impl(const std::string &filename) {

    std::ifstream in;
    in.open(filename.c_str());
    const size_t block_size = 32768;
    char buf[block_size];
    in.seekg(chunk_start_, std::ios_base::beg);
    size_t current = chunk_start_;
    size_t escape_count = 0;
    bool in_quote = false;
    while (current <= chunk_end_) {
      in.read(buf, std::min(chunk_end_ - current + 1, block_size));
      size_t nread = in.gcount();
      if (nread == 0) {
        break;
      }
      size_t i = 0;
      while (i < nread && first_unquoted_newline_ < 0 && first_quoted_newline_ < 0) {
        if (in_quote) {
          for (; i < nread; i++) {
            if (escape_count > 0) {
              escape_count--;
            }
            else if (buf[i] == '\\') {
              escape_count = 1;
            }
            else if (buf[i] == '\"') {
              num_quotes_++;
              in_quote = false;
              i++;
              break;
            }
            else if (buf[i] == '\n') {
              first_quoted_newline_ = current + i;
              i++;
              break;
            }
          }
        }
        else {
          for (; i < nread; i++) {
            if (escape_count > 0) {
              escape_count--;
            }
            else if (buf[i] == '\\') {
              escape_count = 1;
            }
            else if (buf[i] == '\"') {
              num_quotes_++;
              in_quote = true;
              i++;
              break;
            }
            else if (buf[i] == '\n') {
              first_unquoted_newline_ = current + i;
              i++;
              break;
            }            
          }
        }
      }
      while (i < nread && first_unquoted_newline_ < 0) {
        if (in_quote) {
          for (; i < nread; i++) {
            if (escape_count > 0) {
              escape_count--;
            }
            else if (buf[i] == '\\') {
              escape_count = 1;
            }
            else if (buf[i] == '\"') {
              num_quotes_++;
              in_quote = false;
              i++;
              break;
            }
          }
        }
        else {
          for (; i < nread; i++) {
            if (escape_count > 0) {
              escape_count--;
            }
            else if (buf[i] == '\\') {
              escape_count = 1;
            }
            else if (buf[i] == '\"') {
              num_quotes_++;
              in_quote = true;
              i++;
              break;
            }
            else if (buf[i] == '\n') {
              first_unquoted_newline_ = current + i;
              i++;
              break;
            }
          }
        }
      }
      while (i < nread && first_quoted_newline_ < 0) {
        if (in_quote) {
          for (; i < nread; i++) {
            if (escape_count > 0) {
              escape_count--;
            }
            else if (buf[i] == '\\') {
              escape_count = 1;
            }
            else if (buf[i] == '\"') {
              num_quotes_++;
              in_quote = false;
              i++;
              break;
            }
            else if (buf[i] == '\n') {
              first_quoted_newline_ = current + i;
              i++;
              break;
            }
          }
        }
        else {
          for (; i < nread; i++) {
            if (escape_count > 0) {
              escape_count--;
            }
            else if (buf[i] == '\\') {
              escape_count = 1;
            }
            else if (buf[i] == '\"') {
              num_quotes_++;
              in_quote = true;
              i++;
              break;
            }
          }
        }
      }
      /*
        If we got here, then either we've found both the first quoted newline and 
        unquoted newline, or we've processed all the data in the buffer.
      */
      while (i < nread) {
        if (in_quote) {
          for (; i < nread; i++) {
            if (escape_count > 0) {
              escape_count--;
            }
            else if (buf[i] == '\\') {
              escape_count = 1;
            }
            else if (buf[i] == '\"') {
              num_quotes_++;
              in_quote = false;
              i++;
              break;
            } 
          }
        }
        else {
          for (; i < nread; i++) {
            if (escape_count > 0) {
              escape_count--;
            }
            else if (buf[i] == '\\') {
              escape_count = 1;
            }
            else if (buf[i] == '\"') {
              num_quotes_++;
              in_quote = true;
              i++;
              break;
            }
          }
        }
      }
      current += nread;
    }
  }

  size_t get_start() const {
    return chunk_start_;
  }

  size_t get_end() const {
    return chunk_end_;
  }

  size_t get_num_quotes() const {
    return num_quotes_;
  }
  
  long get_first_quoted_newline() const {
    return first_quoted_newline_;
  }

  long get_first_unquoted_newline() const {
    return first_unquoted_newline_;
  }

  void clear() {
    chunk_start_ = 0;
    chunk_end_ = 0;
    num_quotes_ = 0;
    first_unquoted_newline_ = 0;
    first_quoted_newline_ = 0;
  }

private:
  size_t chunk_start_;
  size_t chunk_end_;
  size_t num_quotes_;
  long first_unquoted_newline_;
  long first_quoted_newline_;
  std::exception_ptr thread_exception_;
};
}
#endif

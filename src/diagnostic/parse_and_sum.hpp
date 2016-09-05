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

#ifndef PARATEXT_DIAGNOSTIC_PARSE_AND_SUM_HPP
#define PARATEXT_DIAGNOSTIC_PARSE_AND_SUM_HPP

#include <iostream>
#include <fstream>

#include <sys/types.h>
#include <sys/stat.h>

#ifndef _WIN32
    #include <unistd.h>
#endif
#include <thread>
#include <sstream>

#include "generic/chunker.hpp"
#include "csv/header_parser.hpp"

namespace ParaText {

  namespace Diagnostic {

template <bool TypeCheck>
class ParseAndSumWorker {
public:
  ParseAndSumWorker(size_t chunk_start, size_t chunk_end, size_t block_size, size_t num_columns)
    : chunk_start_(chunk_start),
      chunk_end_(chunk_end),
      block_size_(block_size),
      num_columns_(num_columns) {}

  virtual ~ParseAndSumWorker() {}

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
    const size_t block_size = block_size_;
#ifndef _WIN32
      char buf[block_size];
#else
      char *buf = (char *)_malloca(block_size);
#endif
    in.seekg(chunk_start_, std::ios_base::beg);
    size_t current = chunk_start_;
    sums_.resize(num_columns_);
    std::fill(sums_.begin(), sums_.end(), 0.0);
    column_index_ = 0;
    num_lines_ = 0;
    char token[64];
    size_t j = 0;
    while (current <= chunk_end_) {
      in.read(buf, std::min(chunk_end_ - current + 1, block_size));
      size_t nread = in.gcount();
      if (nread == 0) {
        break;
      }
      for (size_t i = 0; i < nread; i++) {
        if (buf[i] == '\n') {   
          sums_[column_index_] += parse_token<TypeCheck>(token, token + j);
          column_index_ = 0;
          num_lines_++;
          j = 0;
        }
        else if (buf[i] == ',') {
          sums_[column_index_] += parse_token<TypeCheck>(token, token + j);
          column_index_++;
          j = 0;
        }
        else {
          token[j++] = buf[i];
        }
      }
      current += nread;
    }
    if (j > 0) {
      sums_[column_index_] += parse_token<TypeCheck>(token, token + j);
      j = 0;
    }
    if (column_index_ > 0) {
      num_lines_++;
    }
  }
  
  const std::vector<double> &get_sums() const {
    return sums_;
  }

  size_t get_N() const {
    return num_lines_;
  }

  // No type checking
  template <bool TypeCheck_, class Iterator>
  inline typename std::enable_if<!TypeCheck_, double>::type parse_token(Iterator begin, Iterator end) const {
    return bsd_strtod(begin, end);
  }

  // Type checking only for numbers.
  template <bool TypeCheck_, class Iterator>
  inline typename std::enable_if<TypeCheck_, double>::type parse_token(Iterator begin, Iterator end) const {
    Iterator it = begin;
    for (; it != end && isspace(*it); it++) {}
    if (it != end) {
      if (*it == '?' && std::distance(it, end) == 1) {
        return std::numeric_limits<double>::quiet_NaN();
      }
      else if (std::distance(it, end) == 3 &&
               ((*it == 'n' || *it == 'N'))
               && ((*(it+1) == 'a' || *(it+1) == 'A'))
               && ((*(it+2) == 'n' || *(it+2) == 'N'))) {
        return std::numeric_limits<double>::quiet_NaN();
      }
      else {
        if (*it == '-') { it++; }
        for (; it != end && isdigit(*it); it++) {}
        if (it != end && (*it == '.' || *it == 'E' || *it == 'e')) {
          return bsd_strtod(begin, end);
        }
        else {
          return (double)fast_atoi<long>(begin, end);
        }
      }
    }
    return (double)std::distance(begin, end);
  }

private:
  size_t chunk_start_;
  size_t chunk_end_;
  size_t block_size_;
  size_t num_columns_;
  size_t num_lines_;
  size_t column_index_;
  std::vector<double> sums_;
  std::exception_ptr thread_exception_;
};

  class ParseAndSum {
  public:
    ParseAndSum()          {}

    virtual ~ParseAndSum() {}

    size_t load(const std::string &filename, const ParseParams &params, bool type_check) {
      size_t retval = 0;
      if (type_check) {
        retval = load_impl<true>(filename, params);
      }
      else {
        retval = load_impl<false>(filename, params);
      }
      return retval;
    }

    template <bool TypeCheck>
    size_t load_impl(const std::string &filename, const ParseParams &params) {
      std::vector<std::thread> threads;
      std::vector<std::shared_ptr<ParseAndSumWorker<TypeCheck> > > workers;
      header_parser_.open(filename, params.no_header);
      std::exception_ptr thread_exception;
      if (header_parser_.has_header()) {
        chunker_.process(filename, header_parser_.get_end_of_header()+1, params.num_threads, params.allow_quoted_newlines);
      }
      else {
        chunker_.process(filename, 0, params.num_threads, params.allow_quoted_newlines);
      }
      for (size_t worker_id = 0; worker_id < chunker_.num_chunks(); worker_id++) {
        size_t start_of_chunk = 0, end_of_chunk = 0;
        std::tie(start_of_chunk, end_of_chunk) = chunker_.get_chunk(worker_id);
        
        if (start_of_chunk == end_of_chunk) {
          continue;
        }
        workers.push_back(std::make_shared<ParseAndSumWorker<TypeCheck> >(start_of_chunk, end_of_chunk, params.block_size, header_parser_.get_num_columns()));
        threads.emplace_back(&ParseAndSumWorker<TypeCheck>::parse,
                             workers.back(),
                             filename);
      }

      for (size_t i = 0; i < threads.size(); i++) {
        threads[i].join();
        if (!thread_exception) {
          thread_exception = workers[i]->get_exception();
        }
      }
      // We're now outside the parallel region.
      if (thread_exception) {
        std::rethrow_exception(thread_exception);
      }
      N_ = 0.0;
      avgs_.resize(header_parser_.get_num_columns());
      std::fill(avgs_.begin(), avgs_.end(), 0.0);
      for (size_t i = 0; i < workers.size(); i++) {
        auto worker_sums = workers[i]->get_sums();
        N_ += workers[i]->get_N();
        for (size_t j = 0; j < worker_sums.size(); j++) {
          avgs_[j] += worker_sums[j];
        }
      }
      for (size_t j = 0; j < avgs_.size(); j++) {
        avgs_[j] /= N_;
      } 
      return N_;
    }

    size_t get_num_columns() const {
      return header_parser_.get_num_columns();
    }

    double get_avg(size_t column_index) const {
      return avgs_[column_index];
    }

    const std::string &get_column_name(size_t column_index) const {
      return header_parser_.get_column_name(column_index);
    }

    size_t get_N() const {
      return N_;
    }

  private:
    CSV::HeaderParser header_parser_;
    TextChunker chunker_;
    std::vector<double> avgs_;
    size_t N_;
  };
  }
}
#endif

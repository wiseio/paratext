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

#ifndef PARATEXT_DIAGNOSTIC_NL_COUNTER_HPP
#define PARATEXT_DIAGNOSTIC_NL_COUNTER_HPP

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

class NewlineCountWorker {
public:
  NewlineCountWorker(size_t chunk_start, size_t chunk_end, size_t block_size)
    : chunk_start_(chunk_start),
      chunk_end_(chunk_end),
      block_size_(block_size) {}

  virtual ~NewlineCountWorker() {}

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
    in.open(filename.c_str(), std::ios::binary);
    const size_t block_size = block_size_;
#ifndef _WIN32
    char buf[block_size];
#else
    char *buf = (char *)_malloca(block_size);
#endif
    in.seekg(chunk_start_, std::ios_base::beg);
    size_t current = chunk_start_;
    num_newlines_ = 0;
    while (current <= chunk_end_) {
      in.read(buf, std::min(chunk_end_ - current + 1, block_size));
      size_t nread = in.gcount();
      if (nread == 0) {
        break;
      }
      for (size_t i = 0; i < nread; i++) {
        if (buf[i] == '\n') {
          num_newlines_++;
        }
      }
      current += nread;
    }
  }

  size_t get_num_newlines() const {
    return num_newlines_;
  }

private:
  size_t chunk_start_;
  size_t chunk_end_;
  size_t block_size_;
  size_t num_newlines_;
  std::exception_ptr thread_exception_;
};

  class NewlineCounter {
  public:
    NewlineCounter()          {}

    virtual ~NewlineCounter() {}

    size_t load(const std::string &filename, const ParseParams &params) {
      std::vector<std::thread> threads;
      std::vector<std::shared_ptr<NewlineCountWorker> > workers;
      std::exception_ptr thread_exception;
      chunker_.process(filename, 0, params.num_threads, params.allow_quoted_newlines);
      for (size_t worker_id = 0; worker_id < chunker_.num_chunks(); worker_id++) {
        long long start_of_chunk = 0, end_of_chunk = 0;
        std::tie(start_of_chunk, end_of_chunk) = chunker_.get_chunk(worker_id);
        if (start_of_chunk < 0 || end_of_chunk < 0) {
          continue;
        }
        workers.push_back(std::make_shared<NewlineCountWorker>(start_of_chunk, end_of_chunk, params.block_size));
        threads.emplace_back(&NewlineCountWorker::parse,
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
      size_t newline_count = 0;
      for (size_t i = 0; i < workers.size(); i++) {
        newline_count += workers[i]->get_num_newlines();
      }
      return newline_count;
    }

  private:
    TextChunker chunker_;
  };
  }
}
#endif

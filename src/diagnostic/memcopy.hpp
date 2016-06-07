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

#ifndef PARATEXT_DIAGNOSTIC_MEM_COPY_HPP
#define PARATEXT_DIAGNOSTIC_MEM_COPY_HPP

#include <iostream>
#include <fstream>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <thread>
#include <sstream>

#include "generic/chunker.hpp"
#include "csv/header_parser.hpp"

namespace ParaText {

  namespace Diagnostic {

class MemCopyWorker {
public:
  MemCopyWorker(size_t chunk_start, size_t chunk_end, size_t block_size)
    : chunk_start_(chunk_start),
      chunk_end_(chunk_end),
      block_size_(block_size) {}

  virtual ~MemCopyWorker() {}

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
    char buf[block_size];
    in.seekg(chunk_start_, std::ios_base::beg);
    size_t current = chunk_start_;
    while (current <= chunk_end_) {
      in.read(buf, std::min(chunk_end_ - current + 1, block_size));
      size_t nread = in.gcount();
      if (nread == 0) {
        break;
      }
      data_.insert(data_.begin(), buf + 0, buf + nread);
      current += nread;
    }
  }

private:
  size_t chunk_start_;
  size_t chunk_end_;
  size_t block_size_;
  std::vector<char> data_;
  std::exception_ptr thread_exception_;
};

  class MemCopyBaseline {
  public:
    MemCopyBaseline()          {}

    virtual ~MemCopyBaseline() {}

    void load(const std::string &filename, const ParseParams &params) {
      std::vector<std::thread> threads;
      std::vector<std::shared_ptr<MemCopyWorker> > workers;
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
        workers.push_back(std::make_shared<MemCopyWorker>(start_of_chunk, end_of_chunk, params.block_size));
        threads.emplace_back(&MemCopyWorker::parse,
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
    }

  private:
    CSV::HeaderParser header_parser_;
    TextChunker chunker_;
  };
  }
}
#endif

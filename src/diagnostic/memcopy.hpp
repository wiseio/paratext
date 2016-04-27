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
    const size_t block_size = 32768;
    char buf[block_size];
    in.seekg(chunk_start_, std::ios_base::beg);
    size_t current = chunk_start_;
    while (current <= chunk_end_) {
      in.read(buf, std::min(chunk_end_ - current + 1, block_size));
      size_t nread = in.gcount();
      if (nread == 0) {
        break;
      }
      data_.emplace_back(buf, buf + nread);
      current += nread;
    }
  }

private:
  size_t chunk_start_;
  size_t chunk_end_;
  size_t block_size_;
  std::vector<std::vector<char> > data_;
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

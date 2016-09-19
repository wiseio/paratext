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

#ifndef PARATEXT_LINE_CHUNKER2_HPP
#define PARATEXT_LINE_CHUNKER2_HPP

#include <iostream>
#include <fstream>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <thread>
#include <sstream>
#include <vector>

#include "quote_adjustment_worker.hpp"

namespace ParaText {

  /*
    Finds chunks in a text file that break on an unquoted
    newline. Text files are separated by newline separators.  If
    quoted newlines are supported, they are ignored for the purposes
    of separating lines.
   */
  class TextChunker {
  public:
    /*
      Constructs a new chunker with no chunk boundaries initialized.
     */
    TextChunker()          {}

    /*
      Destroys this text chunker.
     */
    virtual ~TextChunker() {}

    /*
      Computes the boundaries of the text chunks.

      \param filename          The text filename to open to computer offsets.
      \param starting_offset   The starting offset of the first chunk.
      \param maximum_chunks    The maximum number of chunks. The number of chunks
                               will be as close to this number as possible.
     */
    void process(const std::string &filename, size_t starting_offset, size_t maximum_chunks, bool allow_quoted_newlines) {
      filename_ = filename;
      starting_offset_ = starting_offset;
      maximum_chunks_ = maximum_chunks;
      struct stat fs;
      if (stat(filename.c_str(), &fs) == -1) {
        std::ostringstream ostr;
        ostr << "cannot open file '" << filename << "'";
        throw std::logic_error(ostr.str());
      }
      length_ = fs.st_size;
      if (length_ > 0) {
        lastpos_ = length_ - 1;
      }
      else {
        lastpos_ = 0;
      }
      in_.open(filename.c_str());
      if (!in_) {
        std::ostringstream ostr;
        ostr << "cannot open file '" << filename << "'";
        throw std::logic_error(ostr.str());
      }
      compute_offsets(allow_quoted_newlines);
    }

    /*
      Returns the number of chunks determined by this chunker.
     */
    size_t num_chunks() const {
      return start_of_chunk_.size();
    }

    /*
      Returns the (start, end) boundaries of a specific chunk. The ending
      index is always inclusive.
     */
    std::pair<long, long> get_chunk(size_t index) const {
      return std::make_pair(start_of_chunk_[index], end_of_chunk_[index]);
    }

  private:
    std::pair<long, char> get_num_trailing_escapes(long start_of_chunk, long end_of_chunk) {
      long num_trailing_escapes = 0;
      long k = end_of_chunk;
      char successor = 0;
      if (end_of_chunk < lastpos_) {
        in_.clear();
        in_.seekg(end_of_chunk + 1, std::ios_base::beg);
        in_.read(&successor, 1);
      }

      for (; k >= start_of_chunk; k--) {
        in_.clear();
        in_.seekg(k, std::ios_base::beg);
        char buf;
        in_.read(&buf, 1);
        size_t nread = in_.gcount();
        if (nread == 0 || buf != '\\') {
          break;
        }
        num_trailing_escapes++;
      }
      return std::make_pair(num_trailing_escapes, successor);
    }

    void compute_offsets(bool allow_quoted_newlines = true) {
      const size_t chunk_size = std::max(2L, (long)((length_ - starting_offset_) / maximum_chunks_));
      long start_of_chunk = starting_offset_;
#ifdef PARALOAD_DEBUG
      std::cerr << "number of threads: " << maximum_chunks_ << std::endl;
      std::cerr << "length: " << length_ << std::endl;
#endif
      for (size_t worker_id = 0; worker_id < maximum_chunks_; worker_id++) {
        long end_of_chunk = std::min(lastpos_, start_of_chunk + (long)chunk_size);
        if (end_of_chunk < start_of_chunk) {
          start_of_chunk = lastpos_ + 1;
          end_of_chunk = lastpos_ + 1;
          start_of_chunk_.push_back(start_of_chunk);
          end_of_chunk_.push_back(end_of_chunk);
          break;
        }
#ifdef PARALOAD_DEBUG
        std::cerr << "initial>>> start_of_chunk: " << start_of_chunk << " end_of_chunk: " << end_of_chunk << std::endl;
#endif
        if (worker_id == maximum_chunks_ - 1) {
          end_of_chunk = lastpos_;
        }
        long trailing_escapes;
        char trailing_successor;
        std::tie(trailing_escapes, trailing_successor) = get_num_trailing_escapes(start_of_chunk, end_of_chunk);
        if (trailing_escapes % 2 == 1) {
          long extra = 0;
          switch (trailing_successor) {
          case 'x': /* \xYY */
            extra = 3;
            break;
          case 'u': /* \uXXXX */
            extra = 5;
            break;
          case 'U': /* \UXXXXXXXX */
            extra = 9;
            break;
          case 'n':
          case '0':
          case 'r':
          case 'v':
          case 't':
          case 'b':
          case '\\':
          case '\"':
          case '\'':
          case '{':
          case '}':
          case ' ':
          case ',':
          case ')':
          case '(':
            extra = 1;
            break;
          default:
            {
              std::ostringstream ostr;
              ostr << "invalid escape character: \\" << ostr;
            }
          }
          if (end_of_chunk + extra > lastpos_) {
            std::ostringstream ostr;
            ostr << "file ends with a trailing escape sequence \\" << trailing_successor;
            throw std::logic_error(ostr.str());
          }
          else {
            end_of_chunk++;
#ifdef PARALOAD_DEBUG
            std::cerr << "cover escape: " << end_of_chunk << std::endl;
#endif
          }
        }
        start_of_chunk_.push_back(start_of_chunk);
        end_of_chunk_.push_back(end_of_chunk);
        if (end_of_chunk >= lastpos_) {
          break;
        }
        start_of_chunk = end_of_chunk + 1;
      }
      if (allow_quoted_newlines) {
        adjust_offsets_according_to_quoted_newlines();
      }
      else {
        adjust_offsets_according_to_unquoted_newlines();
      }
      for (size_t chunk_id = 0; chunk_id < start_of_chunk_.size(); chunk_id++) {
#ifdef PARALOAD_DEBUG
        std::cerr << "final>>> start_of_chunk: " << start_of_chunk_[chunk_id] << " end_of_chunk: " << end_of_chunk_[chunk_id] << std::endl;
#endif
      }
    }

    void adjust_offsets_according_to_unquoted_newlines() {
      const size_t block_size = 512;
      char buf[block_size];
      for (size_t worker_id = 0; worker_id < start_of_chunk_.size(); worker_id++) {
        if (start_of_chunk_[worker_id] < 0 || end_of_chunk_[worker_id] < 0) {
          continue;
        }
        in_.clear();
        in_.seekg(end_of_chunk_[worker_id], std::ios_base::beg);
        long new_end = end_of_chunk_[worker_id];
        bool new_end_found = false;
        long current = new_end;
        while (in_ && !new_end_found) {
          in_.read(buf, block_size);
          size_t nread = in_.gcount();
          if (nread == 0) {
            break;
          }
          for (size_t i = 0; i < nread; i++) {
            if (buf[i] == '\n') {
              new_end = current + i;
              new_end_found = true;
            break;
            }
          }
          current += nread;
        }
        if (!new_end_found) {
          new_end = lastpos_;
        }
        end_of_chunk_[worker_id] = new_end;
        for (size_t other_worker_id = worker_id + 1; other_worker_id < start_of_chunk_.size(); other_worker_id++) {
          if (end_of_chunk_[other_worker_id] <= new_end || new_end == lastpos_) {
            start_of_chunk_[other_worker_id] = -1;
            end_of_chunk_[other_worker_id] = -1;
          } else if (start_of_chunk_[other_worker_id] <= new_end) {            
            start_of_chunk_[other_worker_id] = new_end + 1;
            end_of_chunk_[other_worker_id] = std::max(end_of_chunk_[other_worker_id], new_end + 1);
          }
        }
      }
    }

    void adjust_offsets_according_to_quoted_newlines() {
      std::vector<std::thread> threads;
      std::vector<std::shared_ptr<QuoteNewlineAdjustmentWorker> > workers;
      std::exception_ptr thread_exception;
      for (size_t worker_id = 0; worker_id < start_of_chunk_.size(); worker_id++) {
        workers.push_back(std::make_shared<QuoteNewlineAdjustmentWorker>(start_of_chunk_[worker_id],
                                                                         end_of_chunk_[worker_id]));
        threads.emplace_back(&QuoteNewlineAdjustmentWorker::parse, workers.back(), filename_);
      }
      for (size_t thread_id = 0; thread_id < threads.size(); thread_id++) {
        threads[thread_id].join();
        if (!thread_exception) {
          thread_exception = workers[thread_id]->get_exception();
        }
      }
      for (size_t chunk_id = 0; chunk_id < workers.size(); chunk_id++) {
#ifdef PARALOAD_DEBUG
        std::cerr << "quotes>>> wid=" << chunk_id << " start_of_chunk: " << start_of_chunk_[chunk_id] << " end_of_chunk: " << end_of_chunk_[chunk_id] << " num_quotes: " << workers[chunk_id]->get_num_quotes() << std::endl;
#endif
      }
      // We're now outside the parallel region.
      if (thread_exception) {
        std::rethrow_exception(thread_exception);
      }
      std::vector<size_t> cumulative_quote_sum(workers.size(), 0);
      if (workers.size() > 0) {
        cumulative_quote_sum[0] = workers[0]->get_num_quotes();
        for (size_t i = 1; i < workers.size(); i++) {
        cumulative_quote_sum[i] = cumulative_quote_sum[i - 1] + workers[i]->get_num_quotes();
        }
      }
#ifdef PARALOAD_DEBUG
      std::cerr << "total unescaped quotes: " << cumulative_quote_sum.back() << std::endl;
#endif
      size_t current = 0;
      size_t next = 1;
      while (current < workers.size()) {
        if (end_of_chunk_[current] < 0 || start_of_chunk_[current] < 0) {
          start_of_chunk_[current] = -1;
          end_of_chunk_[current] = -1;
#ifdef PARALOAD_DEBUG
          std::cerr << "negative chunk current=" << current << std::endl;
#endif
          current++;
          /*          if (next_wid < workers.size()) {
            quotes_so_far += workers[next_wid]->get_num_quotes();
            next_wid++;
          }
          continue;*/
        }
        else if (cumulative_quote_sum[next-1] % 2 == 0) { /* even number of quotes so far. */
          if (next < workers.size()) {
#ifdef PARALOAD_DEBUG
            std::cerr << "[A] current=" << current << " next=" << next << " quotes_so_far=" << cumulative_quote_sum[current] << std::endl;
#endif
            long pos = workers[next]->get_first_unquoted_newline();
            if (pos >= 0) { /* resolved */
              end_of_chunk_[current] = pos;
              if (end_of_chunk_[next] == pos) { /* take all of next chunk. */
                start_of_chunk_[next] = -1;
                end_of_chunk_[next] = -1;
                current = next + 1;
                next += 2;
              }
              else {  /* take part of next chunk. */
                start_of_chunk_[next] = pos + 1;
                current = next;
                next++;
              }
            }
            else { /* no resolution. do not increment current */
              end_of_chunk_[current] = end_of_chunk_[next];
              start_of_chunk_[next] = -1;
              end_of_chunk_[next] = -1;
              next++;
            }
          }
          else { /* EOF resolution. */
            end_of_chunk_[current] = lastpos_;
            break;
          }
        }
        else { /* odd number of quotes so far. */
          if (next < workers.size()) {
#ifdef PARALOAD_DEBUG
            std::cerr << "[B] current=" << current << " next=" << next << " quotes_so_far=" << cumulative_quote_sum[next] << std::endl;
#endif
            long pos = workers[next]->get_first_quoted_newline();
            if (pos >= 0) { /* resolution*/
              end_of_chunk_[current] = pos;
              if (end_of_chunk_[next] == pos) { /*take all of next chunk. */
                start_of_chunk_[next] = -1;
                end_of_chunk_[next] = -1;
                current = next + 1;
                next += 2;
              }
              else { /* take part of next chunk. */
                start_of_chunk_[next] = pos + 1;
                current = next;
                next++;
              }
            }
            else { /*no resolution. take all of chunk. */
              end_of_chunk_[current] = end_of_chunk_[next];
              start_of_chunk_[next] = -1;
              end_of_chunk_[next] = -1;
              next++;
            }
          }
          else { /* no resolution and EOF. */
            std::ostringstream ostr;
            ostr << "The file ends with an open quote; a total of " << cumulative_quote_sum[current] << ")";
            throw std::logic_error(ostr.str());
          }
        }
      }
    }

  private:
    std::ifstream in_;
    std::string filename_;
    size_t maximum_chunks_;
    size_t length_;
    long lastpos_;
    long starting_offset_;
    std::vector<long> start_of_chunk_;
    std::vector<long> end_of_chunk_;
  };
}
#endif

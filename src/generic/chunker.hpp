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

#ifndef PARATEXT_LINE_CHUNKER2_HPP
#define PARATEXT_LINE_CHUNKER2_HPP

#include <iostream>
#include <fstream>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <thread>
#include <sstream>

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
    std::pair<size_t, size_t> get_chunk(size_t index) const {
      return std::make_pair(start_of_chunk_[index], end_of_chunk_[index]);
    }

  private:
    void compute_offsets(bool allow_quoted_newlines = true) {
      const size_t chunk_size = (length_ - starting_offset_) / maximum_chunks_;
      size_t start_of_chunk = starting_offset_;
#ifdef PARALOAD_DEBUG
      std::cerr << "number of threads: " << maximum_chunks_ << std::endl;
      std::cerr << "length: " << length_ << std::endl;
#endif
      for (size_t worker_id = 0; worker_id < maximum_chunks_; worker_id++) {
        size_t end_of_chunk = std::min(lastpos_, start_of_chunk + chunk_size);
        if (worker_id == maximum_chunks_ - 1) {
          end_of_chunk = lastpos_;
        }
#ifdef PARALOAD_DEBUG
        std::cout << "start_of_chunk: " << start_of_chunk << " end_of_chunk: " << end_of_chunk << std::endl;
#endif
        start_of_chunk_.push_back(start_of_chunk);
        end_of_chunk_.push_back(end_of_chunk);
        start_of_chunk = std::min(lastpos_, end_of_chunk + 1);
      }
      if (allow_quoted_newlines) {
        adjust_offsets_according_to_quoted_newlines();
      }
      else {
        adjust_offsets_according_to_unquoted_newlines();
      }
    }

    void adjust_offsets_according_to_unquoted_newlines() {
      const size_t block_size = 512;
      char buf[block_size];
      for (size_t worker_id = 0; worker_id < start_of_chunk_.size(); worker_id++) {
        if (start_of_chunk_[worker_id] == end_of_chunk_[worker_id]) {
          continue;
        }
        in_.seekg(end_of_chunk_[worker_id], std::ios_base::beg);
        size_t new_end = end_of_chunk_[worker_id];
        bool new_end_found = false;
        size_t current = new_end;
        while (in_ && !new_end_found) {
          in_.read(buf, block_size);
          size_t nread = in_.gcount();
          if (nread == 0) {
            break;
          }
          for (size_t i = 0; i < nread; i++) {
            if (buf[i] == '\n') {
              new_end = current + i;
              i++;
              new_end_found = true;
            break;
            }
          }
          current += nread;
        }
        end_of_chunk_[worker_id] = new_end;
        for (size_t other_worker_id = worker_id + 1; other_worker_id < start_of_chunk_.size(); other_worker_id++) {
          if (start_of_chunk_[other_worker_id] < new_end && end_of_chunk_[other_worker_id] < new_end) {
            start_of_chunk_[other_worker_id] = 0;
            end_of_chunk_[other_worker_id] = 0;
          } else if (start_of_chunk_[other_worker_id] < new_end) {
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
      for (size_t worker_id = 0; worker_id < maximum_chunks_; worker_id++) {
        workers.push_back(std::make_shared<QuoteNewlineAdjustmentWorker>(start_of_chunk_[worker_id],
                                                                         end_of_chunk_[worker_id]));
        threads.emplace_back(&QuoteNewlineAdjustmentWorker::parse, workers.back(), filename_);
      }
      for (size_t thread_id = 0; thread_id < maximum_chunks_; thread_id++) {
        threads[thread_id].join();
        if (!thread_exception) {
          thread_exception = workers[thread_id]->get_exception();
        }
      }
      // We're now outside the parallel region.q
      if (thread_exception) {
        std::rethrow_exception(thread_exception);
      }
      size_t quotes_so_far = 0;
      size_t cur_wid = 0;
      size_t next_wid = 1;
      quotes_so_far += workers[cur_wid]->get_num_quotes();
      while (cur_wid < workers.size()) {
        //std::cerr << "," << cur_wid;
        if (end_of_chunk_[cur_wid] == start_of_chunk_[cur_wid]) {
          start_of_chunk_[cur_wid] = 0;
          end_of_chunk_[cur_wid] = 0;
          cur_wid++;
          next_wid = cur_wid + 1;
          continue;
        }
        if (quotes_so_far % 2 == 0) {
          if (next_wid < workers.size()) {
            quotes_so_far += workers[next_wid]->get_num_quotes();
            if (workers[next_wid]->get_first_unquoted_newline() >= 0) {
              end_of_chunk_[cur_wid] = workers[next_wid]->get_first_unquoted_newline();
              start_of_chunk_[next_wid] = std::min(end_of_chunk_[next_wid], end_of_chunk_[cur_wid] + 1);
              cur_wid = next_wid;
            }
            else {
              end_of_chunk_[cur_wid] = end_of_chunk_[next_wid];
              start_of_chunk_[next_wid] = 0;
              end_of_chunk_[next_wid] = 0;
            }
            next_wid++;
          }
          else {
            end_of_chunk_[cur_wid] = lastpos_;
            break;
          }
        }
        else {
          if (next_wid < workers.size()) {
            quotes_so_far += workers[next_wid]->get_num_quotes();
            if (workers[next_wid]->get_first_quoted_newline() >= 0) {
              end_of_chunk_[cur_wid] = workers[next_wid]->get_first_quoted_newline();
              start_of_chunk_[next_wid] = std::min(end_of_chunk_[next_wid], end_of_chunk_[cur_wid] + 1);
              cur_wid = next_wid;
            }
            else {
              end_of_chunk_[cur_wid] = end_of_chunk_[next_wid];
              start_of_chunk_[next_wid] = 0;
              end_of_chunk_[next_wid] = 0;
            }
            next_wid++;
          }
          else {
            std::ostringstream ostr;
            ostr << "The file ends with an open quote (" << quotes_so_far << ")";
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
    size_t lastpos_;
    size_t starting_offset_;
    std::vector<size_t> start_of_chunk_;
    std::vector<size_t> end_of_chunk_;
  };
}
#endif

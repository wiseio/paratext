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

#ifndef PARATEXT_QUOTE_NEWLINE_WORKER_HPP
#define PARATEXT_QUOTE_NEWLINE_WORKER_HPP

#include <cassert>

namespace ParaText {

class QuoteNewlineAdjustmentWorker {
public:
  QuoteNewlineAdjustmentWorker(size_t chunk_start, size_t chunk_end, size_t file_size)
    : chunk_start_(chunk_start),
      chunk_end_(chunk_end),
      num_quotes_(0),
      first_unquoted_newline_(-1),
      first_quoted_newline_(-1),
      file_size_(file_size) {}

  virtual ~QuoteNewlineAdjustmentWorker() {}

  void parse(const std::string &filename) {
    std::ifstream in;
    in.open(filename.c_str());
    const size_t block_size = 32768;
    char buf[block_size];
    in.seekg(chunk_start_, std::ios_base::beg);
    size_t current = chunk_start_;
    bool in_quote = false;
    while (current < chunk_end_) {
      in.read(buf, std::min(chunk_end_ - current + 1, block_size));
      size_t nread = in.gcount();
      if (nread == 0) {
        break;
      }
      size_t i = 0;
      while (i < nread && first_unquoted_newline_ < 0 && first_quoted_newline_ < 0) {
        if (in_quote) {
          for (; i < nread; i++) {
            if (buf[i] == '\"') {
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
            if (buf[i] == '\"') {
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
            if (buf[i] == '\"') {
              num_quotes_++;
              in_quote = false;
              i++;
              break;
            }
          }
        }
        else {
          for (; i < nread; i++) {
            if (buf[i] == '\"') {
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
            if (buf[i] == '\"') {
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
            if (buf[i] == '\"') {
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
            if (buf[i] == '\"') {
              num_quotes_++;
              in_quote = false;
              i++;
              break;
            } 
          }
        }
        else {
          for (; i < nread; i++) {
            if (buf[i] == '\"') {
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
    num_quotes_before_first_unquoted_newline_ = 0;
    num_quotes_before_first_quoted_newline_ = 0;
  }

private:
  size_t chunk_start_;
  size_t chunk_end_;
  size_t num_quotes_;
  long first_unquoted_newline_;
  long first_quoted_newline_;
  size_t num_quotes_before_first_unquoted_newline_;
  size_t num_quotes_before_first_quoted_newline_;
  size_t file_size_;
};
}
#endif

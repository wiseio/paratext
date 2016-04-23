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
      num_quotes_before_first_unquoted_newline_(0),
      num_quotes_before_first_quoted_newline_(0),
      num_unquoted_newlines_(0),
      file_size_(file_size),
      ends_with_newline_(false) {}

  virtual ~QuoteNewlineAdjustmentWorker() {}

  void parse(const std::string &filename) {
    std::ifstream in;
    in.open(filename.c_str());
    const size_t block_size = 32768;
    char buf[block_size];
    in.seekg(chunk_start_, std::ios_base::beg);
    size_t current = chunk_start_;
    bool in_quote = false;
    while (in) {
      in.read(buf, std::min(chunk_end_ - current, block_size));
      size_t nread = in.gcount();
      if (nread == 0) {
        break;
      }
      //std::cout << "[" << current << "," << nread << "]";
      if (current + nread == chunk_end_) {
        if ((nread > 0 && buf[nread - 1] == '\n')
            || current + nread == file_size_) {
          ends_with_newline_ = true;
        }
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
              num_quotes_before_first_quoted_newline_ = num_quotes_;
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
              num_quotes_before_first_unquoted_newline_ = num_quotes_;
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
              num_quotes_before_first_unquoted_newline_ = num_quotes_;
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
              num_quotes_before_first_quoted_newline_ = num_quotes_;
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
            num_unquoted_newlines_++;
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

  size_t get_num_quotes_before_first_unquoted_newline() const {
    return num_quotes_before_first_unquoted_newline_;
  }

  size_t get_num_quotes_before_first_quoted_newline() const {
    return num_quotes_before_first_quoted_newline_;
  }

  size_t get_num_unquoted_newlines() const {
    return num_unquoted_newlines_;
  }

  bool ends_with_newline() const {
    return ends_with_newline_;
  }

  void clear() {
    chunk_start_ = 0;
    chunk_end_ = 0;
    num_quotes_ = 0;
    first_unquoted_newline_ = 0;
    first_quoted_newline_ = 0;
    num_quotes_before_first_unquoted_newline_ = 0;
    num_quotes_before_first_quoted_newline_ = 0;
    num_unquoted_newlines_ = 0;
  }

  void steal_first_unquoted_newline(QuoteNewlineAdjustmentWorker &other) {
    if (other.first_unquoted_newline_ < 0) {
      throw std::logic_error("error in parsing: expected unquoted newline to legally perform this operation.");
    }
    if (chunk_start_ > other.chunk_start_) {
      throw std::logic_error("error in parsing: expected unquoted newline to legally perform this operation");
    }
    assert(other.chunk_start_ <= (size_t)other.first_unquoted_newline_);
    assert(other.num_quotes_before_first_unquoted_newline_ <= other.num_quotes_);
    other.num_quotes_ -= other.num_quotes_before_first_unquoted_newline_;
    num_quotes_ += other.num_quotes_before_first_unquoted_newline_;
    chunk_end_ = std::min(file_size_, (size_t)other.first_unquoted_newline_);
    ends_with_newline_ = true;
    other.chunk_start_ = std::min(file_size_, chunk_end_ + 1);
    if (other.chunk_end_ < other.chunk_start_) {
      other.chunk_start_ = other.chunk_end_;
    }
    if (other.first_quoted_newline_ > other.first_unquoted_newline_) {
      other.num_quotes_before_first_quoted_newline_ -= other.num_quotes_before_first_unquoted_newline_;
      other.num_quotes_before_first_unquoted_newline_ = 0;
    }
    else {
      other.num_quotes_before_first_unquoted_newline_ = 0;
      other.num_quotes_before_first_quoted_newline_ = 0;
      other.first_quoted_newline_ = -1;
    }
    other.first_unquoted_newline_ = -1;
  }

  void steal_first_quoted_newline(QuoteNewlineAdjustmentWorker &other) {
    if (other.first_quoted_newline_ < 0) {
      throw std::logic_error("error in parsing: expected quoted newline to legally perform this operation");
    }
    assert(other.chunk_start_ <= (size_t)other.first_unquoted_newline_);
    assert(other.num_quotes_before_first_unquoted_newline_ <= other.num_quotes_);
    num_quotes_ += num_quotes_before_first_quoted_newline_;
    other.num_quotes_ -= num_quotes_before_first_quoted_newline_;
    chunk_end_ = std::min(file_size_, (size_t)other.first_quoted_newline_);
    other.chunk_start_ = std::min(file_size_, chunk_end_ + 1);
    if (other.chunk_end_ < other.chunk_start_) {
      other.chunk_start_ = other.chunk_end_;
    }
    ends_with_newline_ = true;
    if (other.first_unquoted_newline_ > other.first_quoted_newline_) {
      other.num_quotes_before_first_unquoted_newline_ -= other.num_quotes_before_first_quoted_newline_;
      other.num_quotes_before_first_quoted_newline_ = 0;
    }
    else {
      other.num_quotes_before_first_unquoted_newline_ = 0;
      other.num_quotes_before_first_quoted_newline_ = 0;
      other.first_unquoted_newline_ = -1;
    }
    other.first_quoted_newline_ = -1;
  }

private:
  size_t chunk_start_;
  size_t chunk_end_;
  size_t num_quotes_;
  long first_unquoted_newline_;
  long first_quoted_newline_;
  size_t num_quotes_before_first_unquoted_newline_;
  size_t num_quotes_before_first_quoted_newline_;
  size_t num_unquoted_newlines_;
  size_t file_size_;
  bool ends_with_newline_;
};
}
#endif

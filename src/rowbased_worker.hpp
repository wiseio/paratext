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

#ifndef PARATEXT_ROW_BASED_WORKER_HPP
#define PARATEXT_ROW_BASED_WORKER_HPP

#include <snappy.h>

namespace ParaText {

class RowBasedParseWorker {
public:
  RowBasedParseWorker(size_t chunk_start, size_t chunk_end, size_t file_size, size_t block_size, bool compression)
    : chunk_start_(chunk_start),
      chunk_end_(chunk_end),
      file_size_(file_size),
      block_size_(block_size),
      compression_(compression) {}

  virtual ~RowBasedParseWorker() {}

  void parse(const std::string &filename) {
    std::ifstream in;
    in.open(filename.c_str());
    const size_t block_size = block_size_;
    char buf[block_size];
    in.seekg(chunk_start_, std::ios_base::beg);
    size_t current = chunk_start_;
    uint8_t state = 0;
    //std::array <uint8_t, 256> staters;
    // 0: assumed negative-integer
    // 1: assumed integer, only digits encountered
    // 2: assumed float, '.' encountered
    // 3: assumed float, digits encountered before and after '.'
    // 4: assumed float, 'e' encountered
    // 5: closed-string
    // 6: open-string, '"' encountered
    // 7: unquoted delimiter
    // 8: unquoted newline
    std::vector<unsigned char> token;
    state = 0;
    std::vector<unsigned char> input;
    //msgpack::sbuffer ss;
    std::string output;
    column_index_ = 0;
    while (in && current < chunk_end_) {
      in.read(buf, std::min(chunk_end_ - current, block_size));
      size_t nread = in.gcount();
      if (nread == 0) {
        break;
      }
      size_t i = 0;
      if (state == 6) { /* open quote. */
        for (; i < nread; i++) {
          if (buf[i] == '\"') {
            i++;
            state = 5;
            break;
          }
          else {
            token.push_back(buf[i]);
          }
        }
      }
      if (state < 4) {
        if (buf[i] == 'E' || buf[i] == 'e') {
          token.push_back(buf[i]);
          i++;
          state = 4;
        }
      }
      for (size_t i = 0; i < nread; i++) {
        if (buf[i] >= 0x3A) {
          if (state >= 4) {
            state = 5;
            token.push_back(buf[i]);
          }
          else if (buf[i] == 'E' || buf[i] == 'e') {
            state = 4;
            token.push_back(buf[i]);
          }
        }
        else if (buf[i] >= 0x30) {
          token.push_back(buf[i]);
        }
        else {
          if (buf[i] == ',' || buf[i] == '\n') {
            //std::cout << "[" << (int)state << "," << std::string(token.begin(), token.end()) << "]" << std::endl;
            if (state < 2) {
              input.push_back(0);
              long val = fast_atoi<long>(token.begin(), token.end());
                unsigned char *bb = (unsigned char *)(void*)&val;
              //input.insert(0);
              input.insert(input.end(), bb, bb + sizeof(long));
              #if 0
              msgpack::pack(ss, val);
              input.insert(input.end(), ss.data(), ss.data() + ss.size());
              ss.clear();
              #endif
              #if 0
              if (val >= 0 && val < 128) {
                unsigned char v = (unsigned char)val;
                unsigned char *bb = (unsigned char *)(void*)&v;
                input.insert(input.end(), bb, bb + 1);
              }
              else {
                input.push_back(128);
                unsigned char *bb = (unsigned char *)(void*)&val;
                input.insert(input.end(), bb, bb + sizeof(long));
              }
              #endif
            }
            else if (state < 5) {
              input.push_back(1);
              double val = bsd_strtod(token.begin(), token.end());
              unsigned char *bb = (unsigned char *)(void*)&val;
              input.insert(input.end(), bb, bb + sizeof(double));
            }
            else {
              input.push_back(2);
              long len = token.size();
              unsigned char *bl = (unsigned char *)(void*)&len;
              input.insert(input.end(), bl, bl + sizeof(long));
              input.insert(input.end(), token.begin(), token.end());
            }
            if (rows_.size() == 0) {
              starting_state_.push_back(state);
            }
            column_index_++;
            if (column_index_ < starting_state_.size()) {
              state = starting_state_[column_index_];
            }
            else {
              state = 0;
            }
            token.clear();
          }
          else if (buf[i] == '.') {
            if (state < 2) {
              state = 3;
            }
            else {
              state = 5;
            }
            token.push_back('.');
          }
          else if (buf[i] == '"') {
            if (state == 6) {
              state = 5;
            }
            else {
              state = 6;
            }
          }
          else {
            token.push_back(buf[i]);
          }
          if (buf[i] == '\n') {
            //std::cout << input.size() << std::endl;
            if (compression_) {
              snappy::Compress((const char *)input.data(), input.size(), &output);
              input.clear();
              rows_.emplace_back(output.begin(), output.end());
            }
            else {
              rows_.emplace_back(input.begin(), input.end());
              input.clear();
            }
          }
        }
      }
      current += nread;
    }
    if (input.size() > 0) {
      if (compression_) {
        snappy::Compress((const char*)input.data(), input.size(), &output);
        input.clear();
        rows_.emplace_back(output.begin(), output.end());
      }
      else {
        rows_.emplace_back(input.begin(), input.end());
        input.clear();
      }
    }
  }
  
private:
  size_t chunk_start_;
  size_t chunk_end_;
  size_t file_size_;
  size_t column_index_;
  const size_t block_size_;
  bool compression_;
  std::vector<double> maximum_values_;
  std::vector<std::vector<uint8_t> > rows_;
  std::vector<size_t> starting_state_;
};
}

#endif

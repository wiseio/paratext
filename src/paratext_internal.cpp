/*
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

#include "paratext_internal.hpp"
#include "util/strings.hpp"

#include <string>
#include <type_traits>
#include <iostream>
#include <thread>
#include <chrono>
#include <iterator>
//#include <locale>
//#include <codecvt>

size_t get_num_cores() {
  return std::thread::hardware_concurrency();
}

std::string as_quoted_string(const std::string &s, bool do_not_escape_newlines) {
  return get_quoted_string(s.begin(), s.end(), true, do_not_escape_newlines);
}

ParaText::as_raw_bytes get_random_string(size_t length, long seed, long min, long max) {
  std::string output;
  if (seed == 0) {
    seed = std::chrono::system_clock::now().time_since_epoch().count();
  }
  std::default_random_engine e1(seed);
  std::uniform_int_distribution<int> byte_range(min, max);
  for (size_t i = 0; i < length; i++) {
    output.push_back(byte_range(e1));
  }
  ParaText::as_raw_bytes retval;
  retval.val = output;
  return retval;
}

ParaText::as_utf8 get_random_string_utf8(size_t num_sequences, long seed, bool include_null) {
  std::string output;
  if (seed == 0) {
    seed = std::chrono::system_clock::now().time_since_epoch().count();
  }
  std::default_random_engine e1(seed);
  unsigned long surrogate_range = 2048;
  std::uniform_int_distribution<unsigned long> codepoint_range(include_null ? 0 : 1, 0x10FFFF - surrogate_range);
  std::vector<unsigned long> seq;
  for (size_t i = 0; i < num_sequences; i++) {
    unsigned long val = codepoint_range(e1);
    if (val >= 0xD800) {
      val += surrogate_range;
    }
    seq.push_back(val);
  }
  WiseIO::convert_utf32_to_utf8(seq.begin(), seq.end(), std::back_inserter(output));
  ParaText::as_utf8 retval;
  retval.val = output;
  return retval;
}

bool are_strings_equal(const std::string &x, const std::string &y) {
  return x == y;
}

size_t get_string_length(const std::string &s) {
  return s.size();
}

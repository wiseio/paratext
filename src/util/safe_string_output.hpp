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

#ifndef SAFE_STRING_OUTPUT_HPP
#define SAFE_STRING_OUTPUT_HPP

#include <array>
#include <sstream>
#include <iomanip>

namespace WiseIO {

  typedef enum {NO_ESCAPE, ESCAPE, CONTINUATION, LEAD2, LEAD3, LEAD4, POTENTIAL_SURROGATE} SafeCharState;

  class SafeStringOutput {
  public:
    SafeStringOutput() : double_quote_output_(false) {
      should_escape_.fill(NO_ESCAPE);
      should_escape_['\\'] = SafeCharState::ESCAPE;
    }

    ParaText::as_utf8 to_utf8_string(const std::string &input) {
      ParaText::as_utf8 output;
      output.val = output_string(input.begin(), input.end());
      return output;
    }

    ParaText::as_raw_bytes to_raw_string(const std::string &input) {
      ParaText::as_raw_bytes output;
      output.val = output_string(input.begin(), input.end());
      return output;
    }

    template <class Iterator>
    std::string output_string(Iterator begin, Iterator end) {
      /* FIXME: Support filtering of illegal surrogates in UTF8 sequences. */
      std::ostringstream ostr;
      size_t bytes_in_sequence = 0;
      //bool surrogate = false;
      if (double_quote_output_) {
        ostr << '"';
      }
      std::vector<bool> escaped(std::distance(begin, end), false);
      size_t k = 0;
      for (Iterator it = begin; it != end; it++, k++) {
        unsigned char c = (unsigned char)*it;
        bool escape_it = should_escape_[c] == ESCAPE;
        if (bytes_in_sequence > 0) { /* If a UTF8 sequence was started, only escape the byte if its not a continuation. */
          escape_it = should_escape_[c] != CONTINUATION;
          bytes_in_sequence--;
        }
        else if (!escape_it && bytes_in_sequence == 0) { /* If a UTF8 sequence is not progress, check higher order bits. */
          switch (should_escape_[c]) {
          case LEAD4:
            bytes_in_sequence = 3;
            break;
          case POTENTIAL_SURROGATE:
            /*bytes_in_sequence = 2;
              surrogate = true;*/
            break;
          case LEAD3:
            bytes_in_sequence = 2;
            break;
          case LEAD2:
            bytes_in_sequence = 1;
            break;
          case NO_ESCAPE:
            break;
          case ESCAPE: /* Explicit escape. */
          case CONTINUATION: /* An invalid continuation byte, escape it. */
            escape_it = true;
            break;
          }
        }
        escaped[k] = escape_it;
      }
      k = 0;
      for (Iterator it = begin; it != end; it++, k++) {
        unsigned char c = (unsigned char)*it;
        if (escaped[k]) {
          ostr << '\\';
          switch (c) {
          case '\b':
            ostr << 'b';
            break;
          case '\v':
            ostr << 'v';
            break;
          case '\n':
            ostr << 'n';
            break;
          case '\r':
            ostr << 'r';
            break;
          case '\t':
            ostr << 't';
            break;
          case '\\':
            ostr << '\\';
            break;
          case '\"':
            ostr << '\"';
            break;
          case '\'':
            ostr << '\'';
            break;
          default:
            ostr << 'x';
            ostr << to_hex(c >> 4);
            ostr << to_hex(c & 0x0F);
            break;
          }
        }
        else {
          ostr.put(*it);
        }
      }
      if (double_quote_output_) {
        ostr << '"';
      }
      return ostr.str();
    }

    void escape_newlines(bool b) {
      SafeCharState st = b ? SafeCharState::ESCAPE : SafeCharState::NO_ESCAPE;
      should_escape_['\n'] = st;
    }

    void escape_whitespace(bool b) {
      SafeCharState st = b ? SafeCharState::ESCAPE : SafeCharState::NO_ESCAPE;
      should_escape_['\n'] = st;
      should_escape_['\r'] = st;
      should_escape_['\v'] = st;
      should_escape_['\f'] = st;
      should_escape_['\b'] = st;
    }

    void escape_special(bool b) {
      SafeCharState st = b ? SafeCharState::ESCAPE : SafeCharState::NO_ESCAPE;
      should_escape_['\''] = st;
      should_escape_['\"'] = st;
      should_escape_['\\'] = st;
    }

    void escape_delim(bool b) {
      SafeCharState st = b ? SafeCharState::ESCAPE : SafeCharState::NO_ESCAPE;
      should_escape_[','] = st;
      escape_special(true);
    }

    void escape_comments(bool b) {
      SafeCharState st = b ? SafeCharState::ESCAPE : SafeCharState::NO_ESCAPE;
      should_escape_['%'] = st;
      escape_special(true);
    }

    void escape_nonprintables(bool b) {
      SafeCharState st = b ? SafeCharState::ESCAPE : SafeCharState::NO_ESCAPE;
      for (unsigned char c = 0; c < ' '; c++) {
        should_escape_[c] = st;
      }
    }

    void escape_nonascii(bool b) {
      SafeCharState st = b ? SafeCharState::ESCAPE : SafeCharState::NO_ESCAPE;
      for (size_t c = 0x7F; c <= 0xFF; c++) {
        should_escape_[c] = st;
      }
    }

    void escape_nonutf8(bool b) {
      const SafeCharState outside = b ? SafeCharState::ESCAPE : SafeCharState::NO_ESCAPE;
      const SafeCharState cont = b ? SafeCharState::CONTINUATION : SafeCharState::NO_ESCAPE;
      const SafeCharState lead2 = b ? SafeCharState::LEAD2 : SafeCharState::NO_ESCAPE;
      const SafeCharState lead3 = b ? SafeCharState::LEAD3 : SafeCharState::NO_ESCAPE;
      const SafeCharState lead4 = b ? SafeCharState::LEAD4 : SafeCharState::NO_ESCAPE;
      //const SafeCharState surrogate = b ? SafeCharState::POTENTIAL_SURROGATE : SafeCharState::NO_ESCAPE;
      for (size_t c = 0x80; c <= 0xBF; c++) {
        should_escape_[c] = cont;
      }
      for (size_t c = 0xC0; c <= 0xDF; c++) {
        should_escape_[c] = lead2;
      }
      for (size_t c = 0xE0; c <= 0xEF; c++) {
        should_escape_[c] = lead3;
      }
      for (size_t c = 0xF0; c <= 0xF7; c++) {
        should_escape_[c] = lead4;
      }
      for (size_t c = 0xF8; c <= 0xFF; c++) {
        should_escape_[c] = outside;
      }
      //should_escape_[0xED] = surrogate;
    }

    void double_quote_output(bool b) {
      if (b) {
        should_escape_['\"'] = SafeCharState::ESCAPE;
      }
      double_quote_output_ = b;
    }

  private:
    inline char to_hex(int v) {
      if (v >= 0 && v < 10) {
        return '0' + v;
      }
      else if (v >= 10 && v < 16) {
        return 'a' + (v-10);
      }
      else {
        throw std::logic_error("invalid range for hex character");
      }
    }
    
  private:
    std::array<SafeCharState, 256> should_escape_;
    bool double_quote_output_;
  };
}
#endif

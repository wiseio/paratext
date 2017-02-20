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

#ifndef WISEIO_STRINGS_HPP
#define WISEIO_STRINGS_HPP

#include <string>
#include <memory>
#include <sstream>
#include <limits>
#include <random>
#include <locale>
#include "unicode.hpp"

  template <class T>
  struct content_hash {};

  template <class T>
  struct content_hash<std::shared_ptr<T> > {
    typedef std::shared_ptr<T> argument_type;
    typedef typename std::hash<T>::result_type result_type;

    inline result_type operator() (const std::shared_ptr<T> &val) const {
      std::hash<T> hasher;
      return hasher(*val);
    }
  };


  template <class T>
  struct content_equal_to {};

  template <class T>
  struct content_equal_to<std::shared_ptr<T>> {

    inline bool operator()(const T &X, const std::shared_ptr<T> &Y) const {
      return X == *Y;
    }

    inline bool operator()(const std::shared_ptr<T> &X, const T &Y) const {
      return *X == Y;
    }

    inline bool operator()(const std::shared_ptr<T> &X, const std::shared_ptr<T> &Y) const {
      return *X == *Y;
    }
  };

  inline long get_decimal_from_ascii_hex(char x) {
    //char y = tolower(x);
    return (x >> 6) * 9 + (0x0F & x);
  }

  /*
    Converts a unicode character encoded in UCS-2 to
    UTF-8. If successful, one or more characters are
    added to the output iterator.

    If successful, this function returns 0, otherwise
    1 is returned if the UCS-2 character is invalid.
  */
  template <class OutputIterator>
  int ucs2_to_utf8 (int ucs2, OutputIterator out)
  {
    if (ucs2 < 0x80) {
      *(out++) = ucs2;
      return 0;
    }
    if (ucs2 >= 0x80  && ucs2 < 0x800) {
      *(out++) = (ucs2 >> 6)   | 0xC0;
      *(out++) = (ucs2 & 0x3F) | 0x80;
      return 0;
    }
    if (ucs2 >= 0x800 && ucs2 < 0xFFFF) {
      if (ucs2 >= 0xD800 && ucs2 <= 0xDFFF) {
	/* Ill-formed. */
	return 1;
      }
      *(out++) = ((ucs2 >> 12)       ) | 0xE0;
      *(out++) = ((ucs2 >> 6 ) & 0x3F) | 0x80;
      *(out++) = ((ucs2      ) & 0x3F) | 0x80;
      return 0;
    }
    if (ucs2 >= 0x10000 && ucs2 < 0x10FFFF) {
      /* http://tidy.sourceforge.net/cgi-bin/lxr/source/src/utf8.c#L380 */
      *(out++) = 0xF0 | (ucs2 >> 18);
      *(out++) = 0x80 | ((ucs2 >> 12) & 0x3F);
      *(out++) = 0x80 | ((ucs2 >> 6) & 0x3F);
      *(out++) = 0x80 | ((ucs2 & 0x3F));
      return 0;
    }
    return 1;
  }

  /*
    Takes a UTF-8 sequence and returns a string that's double quoted if it is required to parse
    it as a contiguous entity that is not-altered (e.g. space-delimited strings).

    Non-space whitespace characters will be backslash-escaped with \n, \t, \v, \r, \b, \f.

    If ``mandatory_quoting`` is true, the string will automatically be double quoted.
   */
  template <class Iterator>
  inline std::string get_quoted_string(Iterator begin, Iterator end, bool mandatory_quoting = false, bool do_not_escape_newline = false) {
    std::ostringstream ostr;
    bool contains_single_quote = false;
    bool contains_white_space = false;
    bool contains_double_quote = false;
    bool contains_comment_char = false;
    bool contains_terminator = false;
    bool contains_special = false;
    if (begin == end) {
      return "\"\"";
    }
    if (!mandatory_quoting) {
      for (Iterator it = begin; it != end; it++) {
	char c = *it;
	const std::locale loc("");
	if (std::isspace(c, loc)) {
	  contains_white_space = true;
	}
    else if (c < 32 || c >= 127) {
      contains_special = true;
    }
    else {
	switch (c) {
    case ',':
    case '}':
    case '{':
    case '\\':
      contains_special = true;
      break;
	case '\'':
	  contains_single_quote = true;
	  break;
	case '\"':
	  contains_double_quote = true;
	  break;
	case '%':
	  contains_comment_char = true;
	  break;
	case '\0':
	  contains_terminator = true;
	  break;
	default:
	  break;
	}
    }
      }
    }
    if (contains_special || contains_white_space || contains_single_quote || contains_double_quote || contains_comment_char || contains_terminator || mandatory_quoting) {
      ostr << '"';
      for (Iterator it = begin; it != end; it++) {
	switch (*it) {
	case '"':
	  ostr << '\\' << '"';
	  break;
	case '\\':
	  ostr << '\\' << '\\';
	  break;
	case '\n':
      if (do_not_escape_newline) {
        ostr << '\n';
      } else {
        ostr << '\\' << 'n';
      }
	  break;
	case '\t':
	  ostr << '\\' << 't';
	  break;
	case '\r':
	  ostr << '\\' << 'r';
	  break;
	case '\f':
	  ostr << '\\' << 'f';
	  break;
	case '\b':
	  ostr << '\\' << 'b';
	  break;
	case '\v':
	  ostr << '\\' << 'v';
	  break;
	default:
	  ostr << *it;
	  break;
	}
      }
      ostr << '"';
      return ostr.str();
    }
    else {
      return std::string(begin, end);
    }
  }

  /*
    Takes in a valid UTF-8 sequence defined by two iterators `begin` and `end`
    where *begin is some quote character (e.g. '\"'). It returns an iterator
    pointing to the ending unprocessed character (one after the quote
    character).
   */
  template <class Iterator, class OutputIterator>
  inline Iterator parse_quoted_string(Iterator begin, Iterator end, OutputIterator out, const char quote_char) {
    if (begin == end) {
      return end;
    }
    if (*begin == quote_char) {
      begin++;
      while (begin != end) {
	if (*begin == quote_char) {
	  begin++;
	  break;
	}
	else if (*begin == '\\') { // process escape characters
	  if ((begin + 1) != end) {
	    begin++;
	    switch (*begin) {
	    case 'n':
	      *(out++) = '\n';
	      break;
	    case 't':
	      *(out++) = '\t';
	      break;
	    case 'r':
	      *(out++) = '\r';
	      break;
	    case 'b':
	      *(out++) = '\b';
	      break;
	    case 'f':
	      *(out++) = '\f';
	      break;
	    case 'x':
	      {
		int sumv = 0;
		for (int i = 0; i < 2; i++) {
		  if (begin + 1 != end && isxdigit(*(begin + 1))) {
		    begin++;
		    int digit = *begin;
            sumv = sumv * 16 + get_decimal_from_ascii_hex(digit);
		  }
          else {
            std::ostringstream ostr;
            ostr << "literal \\xYY takes 2 hex digits, " << (i+1) << " given.";
            throw std::logic_error(ostr.str());
          }
		}
		*(out++) = (unsigned char)sumv;
	      }
	      break;
	    case 'u':
	      {
		long long sumv = 0;
		for (int i = 0; i < 4; i++) {
		  if (begin + 1 != end && isxdigit(*(begin + 1))) {
		    begin++;
		    int digit = get_decimal_from_ascii_hex(*begin);
            sumv = sumv * 16 + digit;
		  }
          else {
            std::ostringstream ostr;
            ostr << "unicode literal \\uyyyy takes 4 hex digits for codepoint.";
            throw std::logic_error(ostr.str());
          }
		}
        WiseIO::convert_utf32_to_utf8(&sumv, &sumv + 1, out);
	      }
	      break;
	    case 'U':
	      {
		long long sumv = 0;
		for (int i = 0; i < 8; i++) {
		  if (begin + 1 != end && isxdigit(*(begin + 1))) {
		    begin++;
		    int digit = get_decimal_from_ascii_hex(*begin);
            sumv = sumv * 16 + digit;
		  }
          else {
            std::ostringstream ostr;
            ostr << "unicode literal \\Uyyyyyyyy takes 8 hex digits for codepoint.";
            throw std::logic_error(ostr.str());
          }
		}
        WiseIO::convert_utf32_to_utf8(&sumv, &sumv + 1, out);
	      }
	      break;
	    case '0':
	      *(out++) = '\0';
	      break;
	    default:
	      *(out++) = *begin;
	      break;
	    }
	  }
	}
	else {
	  *(out++) = *begin;
	}
	begin++;
      }
    }
    return begin;
  }

  /*
    Takes in a valid UTF-8 sequence defined by two iterators `begin` and `end`
    where *begin is some quote character (e.g. '\"'). It returns an iterator
    pointing to the ending unprocessed character (one after the quote
    character).

    Null-terminator escape sequences \0 are translated into spaces for
    security reasons.
   */
  template <class Iterator, class OutputIterator>
  inline Iterator parse_unquoted_string(Iterator begin, Iterator end, OutputIterator out) {
    if (begin == end) {
      return end;
    }
      while (begin != end) {

	if (*begin == '\\') { // process escape characters
	  if ((begin + 1) != end) {
	    begin++;
	    switch (*begin) {
	    case 'n':
	      *(out++) = '\n';
	      break;
	    case 't':
	      *(out++) = '\t';
	      break;
	    case 'r':
	      *(out++) = '\r';
	      break;
	    case 'b':
	      *(out++) = '\b';
	      break;
	    case 'f':
	      *(out++) = '\f';
	      break;
	    case 'v':
	      *(out++) = '\v';
	      break;
	    case 'e':
	      *(out++) = '\\';
	      break;
	    case 'x':
	      {
		int sumv = 0;
		for (int i = 0; i < 2; i++) {
		  if (begin + 1 != end && isxdigit(*(begin + 1))) {
		    begin++;
		    int digit = *begin;
            sumv = sumv * 16 + get_decimal_from_ascii_hex(digit);
		  }
          else {
            std::ostringstream ostr;
            ostr << "hex literal \\xYY takes 2 hex digits.";
            throw std::logic_error(ostr.str());
          }
		}
		*(out++) = (unsigned char)sumv;
	      }
	      break;
	    case 'u':
	      {
		long long sumv = 0;
		for (int i = 0; i < 4; i++) {
		  if (begin + 1 != end && isxdigit(*(begin + 1))) {
		    begin++;
		    int digit = get_decimal_from_ascii_hex(*begin);
            sumv = sumv * 16 + digit;
		  }
          else {
            std::ostringstream ostr;
            ostr << "unicode literal \\uyyyy takes 4 hex digits for codepoint.";
            throw std::logic_error(ostr.str());
          }
		}
        WiseIO::convert_utf32_to_utf8(&sumv, &sumv + 1, out);
	      }
	      break;
	    case 'U':
	      {
		long long sumv = 0;
		for (int i = 0; i < 8; i++) {
		  if (begin + 1 != end && isxdigit(*(begin + 1))) {
		    begin++;
		    int digit = get_decimal_from_ascii_hex(*begin);
            sumv = sumv * 16 + digit;
		  }
          else {
            std::ostringstream ostr;
            ostr << "unicode literal \\Uyyyyyyyy takes 8 hex digits for codepoint.";
            throw std::logic_error(ostr.str());
          }
		}
        WiseIO::convert_utf32_to_utf8(&sumv, &sumv + 1, out);
	      }
	      break;
	    case '0':
	      *(out++) = '\0';
	      break;
	    default:
	      *(out++) = *begin;
	      break;
	    }
	  }
	}
	else {
	  *(out++) = *begin;
	}
	begin++;
      }
    return begin;
  }

  template <class Iterator>
  void convert_null_to_space(Iterator begin, Iterator end) {
    for (Iterator it = begin; it != end; it++) {
      if (*it == '\0') {
        *it = ' ';
      }
    }
  }

  /*
    Returns an iterator to the first non-whitespace character, or if
    no whitespace characters are found in the sequence, an iterator
    pointing to ``end``.
   */
  template <class Iterator>
  inline Iterator eat_whitespace(Iterator begin, Iterator end) {
    const std::locale loc("");
    while (begin != end && std::isspace(*begin, loc)) { ++begin; }
    return begin;
  }

  template <class T, class Iterator>
  T fast_atoi(Iterator begin, Iterator end) {
    T val = 0;
    bool sign = false;
    // eat the initial whitespace.
    begin = eat_whitespace(begin, end);

    if (begin != end && *begin == '-') {
      sign = true; ++begin;
    }

    // convert the text, stopping when we hit a non-digit.
    while (begin != end && isdigit(*begin)) {
      val = val * 10 + ((*begin++) - '0');
    }

    // return the final value.
    return sign ? -val : val;
  }

  /*
    An STL iterator-based string to floating point number conversion.
    This function was adapted from the C standard library of RetroBSD,
    which is based on Berkeley UNIX.

    This function and this function only is BSD license.
  
    https://retrobsd.googlecode.com/svn/stable/src/libc/stdlib/strtod.c    
   */
  template <class Iterator>
  double bsd_strtod(Iterator begin, Iterator end)
  {
    if (begin == end) {
      return std::numeric_limits<double>::quiet_NaN();
    }
    if (*begin == 'n' || *begin == '?') {
      return std::numeric_limits<double>::quiet_NaN();
    }
        int sign = 0, expSign = 0, i;
        double fraction, dblExp;
        Iterator p = begin;
        char c;

        /* Exponent read from "EX" field. */
        int exp = 0;

        /* Exponent that derives from the fractional part.  Under normal
         * circumstances, it is the negative of the number of digits in F.
         * However, if I is very long, the last digits of I get dropped
         * (otherwise a long I with a large negative exponent could cause an
         * unnecessary overflow on I alone).  In this case, fracExp is
         * incremented one for each dropped digit. */
        int fracExp = 0;

        /* Number of digits in mantissa. */
        int mantSize;

        /* Number of mantissa digits BEFORE decimal point. */
        int decPt;

        /* Temporarily holds location of exponent in str. */
        Iterator pExp;

        /* Largest possible base 10 exponent.
         * Any exponent larger than this will already
         * produce underflow or overflow, so there's
         * no need to worry about additional digits. */
        static int maxExponent = 307;

        /* Table giving binary powers of 10.
         * Entry is 10^2^i.  Used to convert decimal
         * exponents into floating-point numbers. */
        static double powersOf10[] = {
                1e1, 1e2, 1e4, 1e8, 1e16, 1e32, //1e64, 1e128, 1e256,
        };
#if 0
        static double powersOf2[] = {
                2, 4, 16, 256, 65536, 4.294967296e9, 1.8446744073709551616e19,
                //3.4028236692093846346e38, 1.1579208923731619542e77, 1.3407807929942597099e154,
        };
        static double powersOf8[] = {
                8, 64, 4096, 2.81474976710656e14, 7.9228162514264337593e28,
                //6.2771017353866807638e57, 3.9402006196394479212e115, 1.5525180923007089351e231,
        };
        static double powersOf16[] = {
                16, 256, 65536, 1.8446744073709551616e19,
                //3.4028236692093846346e38, 1.1579208923731619542e77, 1.3407807929942597099e154,
        };
#endif

        /*
         * Strip off leading blanks and check for a sign.
         */
        p = begin;
        while (p != end && (*p==' ' || *p=='\t'))
                ++p;
        if (p != end && *p == '-') {
                sign = 1;
                ++p;
        } else if (p != end && *p == '+')
                ++p;

        /*
         * Count the number of digits in the mantissa (including the decimal
         * point), and also locate the decimal point.
         */
        decPt = -1;
        for (mantSize=0;p != end ; ++mantSize) {
                c = *p;
                if (! isdigit (c)) {
                        if (c != '.' || decPt >= 0)
                                break;
                        decPt = mantSize;
                }
                ++p;
        }

        /*
         * Now suck up the digits in the mantissa.  Use two integers to
         * collect 9 digits each (this is faster than using floating-point).
         * If the mantissa has more than 18 digits, ignore the extras, since
         * they can't affect the value anyway.
         */
        pExp = p;
        p -= mantSize;
        if (decPt < 0)
                decPt = mantSize;
        else
                --mantSize;             /* One of the digits was the point. */

        if (mantSize > 2 * 9)
                mantSize = 2 * 9;
        fracExp = decPt - mantSize;
        if (mantSize == 0) {
                fraction = 0.0;
                p = begin;
                goto done;
        } else {
                int frac1, frac2;

                for (frac1=0; mantSize>9 && p != end; --mantSize) {
                        c = *p++;
                        if (c == '.')
                                c = *p++;
                        frac1 = frac1 * 10 + (c - '0');
                }
                for (frac2=0; mantSize>0 && p != end; --mantSize) {
                        c = *p++;
                        if (c == '.')
                                c = *p++;
                        frac2 = frac2 * 10 + (c - '0');
                }
                fraction = (double) 1000000000 * frac1 + frac2;
        }

        /*
         * Skim off the exponent.
         */
        p = pExp;
        if (p != end && (*p=='E' || *p=='e' || *p=='S' || *p=='s' || *p=='F' || *p=='f' ||
             *p=='D' || *p=='d' || *p=='L' || *p=='l')) {
                ++p;
                if (p != end && *p == '-') {
                        expSign = 1;
                        ++p;
                } else if (p != end && *p == '+')
                        ++p;
                while (p != end && isdigit (*p))
                        exp = exp * 10 + (*p++ - '0');
        }
        if (expSign)
                exp = fracExp - exp;
        else
                exp = fracExp + exp;

        /*
         * Generate a floating-point number that represents the exponent.
         * Do this by processing the exponent one bit at a time to combine
         * many powers of 2 of 10. Then combine the exponent with the
         * fraction.
         */
        if (exp < 0) {
                expSign = 1;
                exp = -exp;
        } else
                expSign = 0;
        if (exp > maxExponent)
                exp = maxExponent;
        dblExp = 1.0;
        for (i=0; exp; exp>>=1, ++i)
                if (exp & 01)
                        dblExp *= powersOf10[i];
        if (expSign)
                fraction /= dblExp;
        else
                fraction *= dblExp;

done:
        return sign ? -fraction : fraction;
}

  inline std::string get_quoted_string(const std::string &s) {
    return get_quoted_string(s.begin(), s.end(), false, false);
  }

  inline std::string get_mandatory_quoted_string(const std::string &s) {
    return get_quoted_string(s.begin(), s.end(), true, false);
  }

#endif

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

#ifndef WISEIO_STRINGS_HPP
#define WISEIO_STRINGS_HPP

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

  /*
    Returns an iterator to the first non-whitespace character, or if
    no whitespace characters are found in the sequence, an iterator
    pointing to ``end``.
   */
  template <class Iterator>
  inline Iterator eat_whitespace(Iterator begin, Iterator end) {
    while (begin != end && std::isspace(*begin)) { ++begin; }
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

#endif

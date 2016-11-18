#ifndef WISEIO_UNICODE_HPP
#define WISEIO_UNICODE_HPP

#define UNI_REPLACEMENT_CHAR (WUTF32)0x0000FFFD
#define UNI_MAX_BMP (WUTF32)0x0000FFFF
#define UNI_MAX_UTF16 (WUTF32)0x0010FFFF
#define UNI_MAX_UTF32 (WUTF32)0x7FFFFFFF
#define UNI_MAX_LEGAL_UTF32 (WUTF32)0x0010FFFF

#define UNI_SUR_HIGH_START  (WUTF32)0xD800
#define UNI_SUR_HIGH_END    (WUTF32)0xDBFF
#define UNI_SUR_LOW_START   (WUTF32)0xDC00
#define UNI_SUR_LOW_END     (WUTF32)0xDFFF

namespace WiseIO {

  template <class InputIterator, class OutputIterator>
  int convert_utf32_to_utf8(InputIterator start,
                            InputIterator end,
                            OutputIterator out,
                            bool strict = false) {

    typedef unsigned long WUTF32;
    typedef unsigned char WUTF8;
    static const WUTF8 firstByteMark[7] = { 0x00, 0x00, 0xC0, 0xE0, 0xF0, 0xF8, 0xFC };
    int result = 0;
    unsigned char buf[4] = {0,0,0,0};
    for (InputIterator it = start; it != end; it++) {
      WUTF32 ch = *it;
      unsigned short bytesToWrite = 0;
      const WUTF32 byteMask = 0xBF;
      const WUTF32 byteMark = 0x80;
      if (strict) {
        /* UTF-16 surrogate values are illegal in UTF-32 */
        if (ch >= UNI_SUR_HIGH_START && ch <= UNI_SUR_LOW_END) {
          result = 1;
          break;
        }
      }
      /*
       * Figure out how many bytes the result will require. Turn any
       * illegally large UTF32 things (> Plane 17) into replacement chars.
       */
      if (ch < (WUTF32)0x80) {	     bytesToWrite = 1;
      } else if (ch < (WUTF32)0x800) {     bytesToWrite = 2;
      } else if (ch < (WUTF32)0x10000) {   bytesToWrite = 3;
      } else if (ch <= UNI_MAX_LEGAL_UTF32) {  bytesToWrite = 4;
      } else {			    bytesToWrite = 3;
        ch = UNI_REPLACEMENT_CHAR;
        result = 1;
        break;
      }
      switch (bytesToWrite) { /* note: everything falls through. */
      case 4: buf[3] = (WUTF8)((ch | byteMark) & byteMask); ch >>= 6;
      case 3: buf[2] = (WUTF8)((ch | byteMark) & byteMask); ch >>= 6;
      case 2: buf[1] = (WUTF8)((ch | byteMark) & byteMask); ch >>= 6;
      case 1: buf[0] = (WUTF8) (ch | firstByteMark[bytesToWrite]);
      }
      for(int i = 0; i < bytesToWrite; i++) {
        *(out++) = buf[i];
      }
    }
    return result;
  }
}
#endif

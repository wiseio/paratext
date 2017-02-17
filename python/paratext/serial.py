"""
Single-threaded utilities
"""

#   Licensed to the Apache Software Foundation (ASF) under one
#   or more contributor license agreements.  See the NOTICE file
#   distributed with this work for additional information
#   regarding copyright ownership.  The ASF licenses this file
#   to you under the Apache License, Version 2.0 (the
#   "License"); you may not use this file except in compliance
#   with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0 
#
#   Unless required by applicable law or agreed to in writing,
#   software distributed under the License is distributed on an
#   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#   KIND, either express or implied.  See the License for the
#   specific language governing permissions and limitations
#   under the License.
#
#   Copyright (C) Wise.io, Inc. 2016.


#
#   Coder: Damian Eads
#

import paratext_internal as pti

import os
import six
from six.moves import range

import random
import numpy as np
import string

import numpy as np
import unittest
import collections
import pandas
import paratext_internal
import os
import random
import sys

if sys.version_info>=(3,0):
    def _repr_bytes(o):
        return bytes(repr(o), 'utf-8')
else:
    def _repr_bytes(o):
        return repr(o)

def as_quoted_string(s, do_not_escape_newlines=False):
    return paratext_internal.as_quoted_string(s, do_not_escape_newlines)


def _docstring_parameter(*sub):
     def dec(obj):
         obj.__doc__ = obj.__doc__.format(*sub)
         return obj
     return dec

_save_frame_params = """
    frame : DataFrame, mapping, dict
         This object must be DataFrame-like (ie implement .keys() and __getattr__).

    allow_quoted_newlines : bool
         Whether to allow newlines to be unescaped in a quoted string. If False, all newline
         are encountered are escaped.

    out_encoding : bool
         The encoding to use. Valid options include:
            - `utf-8`: UTF-8 data
            - `arbitrary`: arbitrary bytes (values 0x00-0xFF)
            - `printable_ascii`: values 0x20-0xFF. 0x0A is included if `allow_quoted_newlines`=True
            - `ascii`: values 0x00-0x7F
         If any values are outside of this range, they are backslash-escaped.

    dos : bool
         Whether to add a carriage return before a newline (Windows and DOS compatability).
"""


@_docstring_parameter(_save_frame_params)
def save_frame(filename, frame, allow_quoted_newlines=True, out_encoding='arbitrary', dos=False):
    """
    Saves a dictframe/DataFrame of sequences of the same size to a CSV file.

    Parameters
    ----------
    filename : str, unicode
         The name of the filename to write.

    {0}
    """
    f = open(filename, 'wb')
    write_frame(f, frame, allow_quoted_newlines=allow_quoted_newlines, out_encoding=out_encoding, dos=dos)
    f.close()

@_docstring_parameter(_save_frame_params)
def write_frame(stream, frame, allow_quoted_newlines=True, out_encoding='arbitrary', dos=False):
    """
    Saves a dictframe/DataFrame of sequences of the same size to a byte stream (binary mode).

    Parameters
    ----------
    filename : str, unicode
         The name of the filename to write.

    {0}
    """

    # In case .keys() is non-deterministic
    keys = list(frame.keys())
    cols = []

    psafe=paratext_internal.SafeStringOutput()
    psafe.escape_nonascii(True)
    psafe.escape_nonprintables(True)
    safe=paratext_internal.SafeStringOutput()
    safe.escape_special(True)
    if out_encoding == 'utf-8':
        safe.escape_nonutf8(True)
    elif out_encoding == 'ascii':
        safe.escape_nonascii(True)
    elif out_encoding == 'printable_ascii':
        safe.escape_nonascii(True)
        safe.escape_nonprintables(True)
    if not allow_quoted_newlines:
        safe.escape_newlines(True)
        psafe.escape_newlines(True)
    safe.double_quote_output(True)
    psafe.double_quote_output(True)
    for col in range(len(keys)):
        if col > 0:
            stream.write(b",")
            stream.flush()
        key = keys[col]
        if out_encoding == 'utf-8':
            stream.flush()
            if isinstance(key, bytes):
                skey = psafe.to_raw_string(key)
            else:
                skey = safe.to_raw_string(key)
            stream.write(skey)
        else:
            stream.flush()
            if isinstance(key, bytes):
                skey = psafe.to_raw_string(key)
            else:
                skey = safe.to_raw_string(key)
            stream.write(skey)
        if isinstance(frame[key], pandas.Series):
            cols.append(frame[key].values)
        else:
            cols.append(np.asarray(frame[key]))
    if dos:
        stream.write(b"\r\n")
    else:
        stream.write(b"\n")
    if hasattr(frame, "shape"):
        num_rows = frame.shape[0]
    elif len(keys) == 0:
        num_rows = 0
    else:
        num_rows = len(frame[keys[0]])
    for row in range(num_rows):
        for col in range(len(cols)):
            if col > 0:
                stream.write(b',')
            val = cols[col][row]
            if np.issubdtype(type(val), np.string_) or np.issubdtype(type(val), np.unicode_) or isinstance(val, six.string_types):
                if out_encoding == 'utf-8':
                    #sval = safe.to_utf8_string(val)
                    if isinstance(val, bytes):
                        sval = psafe.to_raw_string(val)
                    else:
                        sval = safe.to_raw_string(val)
                    stream.write(sval)
                else:
                    sval = safe.to_raw_string(val)
                    stream.write(sval)
            else:
                stream.write(bytes(_repr_bytes(val)))
        if dos:
            stream.write(b"\r\n")
        else:
            stream.write(b"\n")

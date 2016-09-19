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

#ifndef PARATEXT_ENCODING_HPP
#define PARATEXT_ENCODING_HPP

namespace ParaText {
  typedef enum {UNKNOWN_BYTES, UNICODE_UTF8, ASCII} Encoding;

  struct as_raw_bytes {
    std::string val;
  };
  
  struct as_utf8 {
    std::string val;
  };

}

#endif

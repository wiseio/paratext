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

def make_messy_frame(num_rows, num_cols, num_cats, num_ints):
    fid = open("/etc/dictionaries-common/words")
    words=[line.strip() for line in fid.readlines()]
    perm = np.random.permutation(num_cols)
    num_catints = num_cats + num_ints
    float_ids = perm[num_catints:]
    int_ids = perm[num_cats:num_catints]
    cat_ids = perm[0:num_cats]
    d = {}
    dtypes = {}
    for col in cat_ids:
        X = np.zeros((num_rows,), dtype=np.object);
        for row in xrange(0, num_rows):
            num_newlines = np.random.randint(3,7)
            num_commas = np.random.randint(3,7)
            X[row] = ""
            tricky_delims = np.asarray(["\n"] * num_newlines + [","] * num_commas)
            np.random.shuffle(tricky_delims)
            for delim in tricky_delims:
                X[row] += string.join(random.sample(words, 5), ' ')
                X[row] += delim
                X[row] += string.join(random.sample(words, 5), ' ')
        d[col] = X
        dtypes[col] = 'string'
    for col in float_ids:
        d[col] = np.random.randn(num_rows)
        dtypes[col] = 'float'
    min_int = [0,   -2**7, 0   , -2**15,     0, -2**31,     0, -2**62]
    max_int = [2**8, 2**7, 2**16,  2**15, 2**32,  2**31, 2**62, 2**62]
    dtypes_int = ["uint8", "int8", "uint16", "int16", "uint32", "int32", "uint64", "int64"]
    for col in int_ids:
        j = np.random.randint(0, len(min_int))
        d[col] = np.random.randint(min_int[j], max_int[j], num_rows)
        dtypes[col] = dtypes_int[j]
    return d, dtypes

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

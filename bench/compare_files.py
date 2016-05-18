#!/usr/bin/env python

import paratext
import json

for fn in ["mnist8m", "mnist", "messy", "messy2", "car", "float1", "float2", "float3", "float4"]:
    if os.path.exists(fn + ".csv"):
        result = paratext.internal_compare(fn)
        fid = open(fn + "-compare.json", "w")
        json.dumps(fid)
        fid.close()

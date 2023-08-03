import os

# list all include directories
include_directories = [
    os.path.sep.join(x.split('/')) for x in ['extension/tpch/include', 'extension/tpch/dbgen/include']
]
# source files
source_files = [
    os.path.sep.join(x.split('/'))
    for x in [
        'extension/tpch/tpch_extension.cpp',
        'extension/tpch/dbgen/bm_utils.cpp',
        'extension/tpch/dbgen/build.cpp',
        'extension/tpch/dbgen/dbgen.cpp',
        'extension/tpch/dbgen/dbgen_gunk.cpp',
        'extension/tpch/dbgen/permute.cpp',
        'extension/tpch/dbgen/rnd.cpp',
        'extension/tpch/dbgen/rng64.cpp',
        'extension/tpch/dbgen/speed_seed.cpp',
        'extension/tpch/dbgen/text.cpp',
    ]
]

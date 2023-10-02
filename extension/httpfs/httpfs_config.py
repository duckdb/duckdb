import os

# list all include directories
include_directories = [
    os.path.sep.join(x.split('/'))
    for x in ['extension/httpfs/include', 'third_party/httplib', 'extension/parquet/include']
]
# source files
source_files = [
    os.path.sep.join(x.split('/'))
    for x in ['extension/httpfs/' + s for s in ['httpfs_extension.cpp', 'httpfs.cpp', 's3fs.cpp', 'crypto.cpp']]
]

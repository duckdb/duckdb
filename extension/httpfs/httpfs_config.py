import os
# list all include directories
include_directories = [os.path.sep.join(x.split('/')) for x in ['extension/httpfs/include', 'third_party/picohash', 'third_party/httplib']]
# source files
source_files = [os.path.sep.join(x.split('/')) for x in ['extension/httpfs/crypto.cpp', 'extension/httpfs/httpfs.cpp', 'extension/httpfs/httpfs-extension.cpp', 'extension/httpfs/s3fs.cpp']]

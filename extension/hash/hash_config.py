import os
# list all include directories
include_directories = [os.path.sep.join(x.split('/')) for x in ['extension/hash/include', 'extension/hash/libs/xxHash']]
# source files
source_files = [os.path.sep.join(x.split('/')) for x in ['extension/hash/hash-extension.cpp', 'extension/hash/libs/xxHash/xxhash.cpp']]

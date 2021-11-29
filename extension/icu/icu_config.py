import os
# list all include directories
include_directories = [os.path.sep.join(x.split('/')) for x in ['extension/icu/include']]
# source files
source_files = [os.path.sep.join(x.split('/')) for x in ['extension/icu/icu-collate.cpp', 'extension/icu/icu-extension.cpp', 'extension/icu/icu-datepart.cpp', 'extension/icu/icu-datetrunc.cpp']]

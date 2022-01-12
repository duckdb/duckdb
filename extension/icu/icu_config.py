import os
# list all include directories
include_directories = [os.path.sep.join(x.split('/')) for x in ['extension/icu/include']]
# source files
source_files = [os.path.sep.join(x.split('/')) for x in ['extension/icu/icu-collate.cpp', 'extension/icu/icu-extension.cpp', 'extension/icu/icu-dateadd.cpp', 'extension/icu/icu-datefunc.cpp', 'extension/icu/icu-datepart.cpp', 'extension/icu/icu-datesub.cpp', 'extension/icu/icu-datetrunc.cpp', 'extension/icu/icu-makedate.cpp']]

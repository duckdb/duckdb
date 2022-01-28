import os
# list all include directories
include_directories = [os.path.sep.join(x.split('/')) for x in ['extension/excel/include', 'extension/excel/numformat/include']]
# source files
source_files = [os.path.sep.join(x.split('/')) for x in ['extension/excel/excel-extension.cpp']]
source_files += [os.path.sep.join(x.split('/')) for x in ['extension/excel/numformat/calendar.cpp', 'extension/excel/numformat/datetime.cxx', 'extension/excel/numformat/localedata.cpp', 'extension/excel/numformat/tdate.cxx', 'extension/excel/numformat/ttime.cxx', 'extension/excel/numformat/zforfind.cxx', 'extension/excel/numformat/zformat.cxx', 'extension/excel/numformat/zforscan.cxx']]

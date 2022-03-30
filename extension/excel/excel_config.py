import os
# list all include directories
include_directories = [os.path.sep.join(x.split('/')) for x in ['extension/excel/include', 'extension/excel/numformat/include', 'extension/excel/variant/include']]
# source files
source_files = [os.path.sep.join(x.split('/')) for x in ['extension/excel/excel-extension.cpp']]
source_files += [os.path.sep.join(x.split('/')) for x in [
    'extension/excel/numformat/nf_calendar.cpp', 
    'extension/excel/numformat/nf_localedata.cpp', 
    'extension/excel/numformat/nf_zformat.cpp', 
    'extension/excel/variant/variant_value.cpp'
    ]]

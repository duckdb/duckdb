import os

# list all include directories
include_directories = [
    os.path.sep.join(x.split('/')) for x in ['extension/excel/include', 'extension/excel/numformat/include']
]
# source files
source_files = [os.path.sep.join(x.split('/')) for x in ['extension/excel/excel_extension.cpp']]
source_files += [
    os.path.sep.join(x.split('/'))
    for x in [
        'extension/excel/numformat/nf_calendar.cpp',
        'extension/excel/numformat/nf_localedata.cpp',
        'extension/excel/numformat/nf_zformat.cpp',
    ]
]

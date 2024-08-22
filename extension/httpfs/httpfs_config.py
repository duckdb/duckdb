import os

# list all include directories
include_directories = [
    os.path.sep.join(x.split('/'))
    for x in ['extension/httpfs/include', 'third_party/httplib', 'extension/parquet/include']
]
# source files
source_files = [
    os.path.sep.join(x.split('/'))
    for x in [
        'extension/httpfs/' + s
        for s in [
            'create_secret_functions.cpp',
            'crypto.cpp',
            'hffs.cpp',
            'http_state.cpp',
            'httpfs.cpp',
            'httpfs_extension.cpp',
            's3fs.cpp',
        ]
    ]
]

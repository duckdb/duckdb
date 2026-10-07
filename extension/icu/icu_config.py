import os

# list all include directories
include_directories = [
    os.path.sep.join(x.split('/'))
    for x in [
        'extension/icu/include',
        'extension/icu/collation/include',
        'extension/icu/datetime/include',
    ]
]
# source files
source_directories = [
    os.path.sep.join(x.split('/'))
    for x in [
        '.',
        'collation',
        'collation/generated',
        'datetime',
        'datetime/generated',
    ]
]
source_files = []
base_path = os.path.dirname(os.path.abspath(__file__))
for dir in source_directories:
    source_files += [
        os.path.join('extension', 'icu', dir, x) for x in os.listdir(os.path.join(base_path, dir)) if x.endswith('.cpp')
    ]

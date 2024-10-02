import subprocess
import sys
import os

if len(sys.argv) < 2 or not os.path.isfile(sys.argv[1]):
    print("Usage: [libduckdb dynamic library file, release build]")
    exit(1)

res = subprocess.run('nm -g -C -P'.split(' ') + [sys.argv[1]], check=True, capture_output=True)
if res.returncode != 0:
    raise ValueError('Failed to run `nm`')

culprits = []

whitelist = [
    '@@GLIBC',
    '@@CXXABI',
    '__gnu_cxx::',
    'std::',
    'N6duckdb',
    'duckdb::',
    'duckdb_miniz::',
    'duckdb_fmt::',
    'duckdb_hll::',
    'duckdb_moodycamel::',
    'duckdb_yyjson::',
    'duckdb_',
    'RefCounter',
    'registerTMCloneTable',
    'RegisterClasses',
    'Unwind_Resume',
    '__gmon_start',
    '_fini',
    '_init',
    '_version',
    '_end',
    '_edata',
    '__bss_start',
    '__udivti3',
    '__popcount',
    'Adbc',
    'ErrorArrayStream',
    'ErrorFromArrayStream',
]

for symbol in res.stdout.decode('utf-8').split('\n'):
    if len(symbol.strip()) == 0:
        continue
    if symbol.endswith(' U'):  # undefined because dynamic linker
        continue
    if symbol.endswith(' U 0 0'):  # undefined because dynamic linker
        continue

    is_whitelisted = False
    for entry in whitelist:
        if entry in symbol:
            is_whitelisted = True
    if is_whitelisted:
        continue

    culprits.append(symbol)


if len(culprits) > 0:
    print("Found leaked symbols. Either white-list above or change visibility:")
    for symbol in culprits:
        print(symbol)
    sys.exit(1)


sys.exit(0)

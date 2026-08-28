import subprocess
import sys
import os
import re

if len(sys.argv) < 2 or not os.path.isfile(sys.argv[1]):
    print("Usage: [libduckdb dynamic library file, release build]")
    exit(1)

res = subprocess.run('nm -g -C -P'.split(' ') + [sys.argv[1]], check=True, capture_output=True)
if res.returncode != 0:
    raise ValueError('Failed to run `nm`')

culprits = []

whitelist = [
    '@GLIBC',
    '@GCC',
    '@CXXABI',
    '__cxa_call_terminate',
    '__gnu_cxx::',
    '_ZNSt4pairI',
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
    'CreateAPIv1()',
]

for value in res.stdout.decode('utf-8').split('\n'):
    symbol = value.strip()
    if not symbol:
        continue
    if re.search(r' [Uw]$', symbol):  # undefined because dynamic linker
        continue
    if re.search(r' [Uw] 0 0$', symbol) and "random_device" not in symbol:  # undefined because dynamic linker
        continue

    is_whitelisted = False
    for entry in whitelist:
        if entry in symbol and "random_device" not in symbol:
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

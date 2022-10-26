import os
import sys
import duckdb
import numpy
import subprocess
from io import StringIO
import csv
import statistics

old_file = None
new_file = None
# the threshold at which we consider something a regression (percentage)
regression_threshold_percentage = 0.1
# minimal seconds diff for something to be a regression (for very fast benchmarks)
regression_threshold_seconds = 0.01

for arg in sys.argv:
    if arg.startswith("--old="):
        old_file = arg.replace("--old=", "")
    elif arg.startswith("--new="):
        new_file = arg.replace("--new=", "")

if old_file is None or new_file is None:
    print("Usage: python scripts/regression_check.py --old=<old_file> --new-<new_file>")
    exit(1)

con = duckdb.connect()
old_timings_l = con.execute(f"SELECT name, median(time) FROM read_csv_auto('{old_file}') t(name, nrun, time) GROUP BY ALL ORDER BY ALL").fetchall()
new_timings_l = con.execute(f"SELECT name, median(time) FROM read_csv_auto('{new_file}') t(name, nrun, time) GROUP BY ALL ORDER BY ALL").fetchall()

old_timings = {}
new_timings = {}

for entry in old_timings_l:
    name  = entry[0]
    timing = entry[1]
    old_timings[name] = timing

for entry in new_timings_l:
    name = entry[0]
    timing = entry[1]
    new_timings[name] = timing

slow_keys = []
multiply_percentage = 1.0 + regression_threshold_percentage

test_keys = list(new_timings.keys())
test_keys.sort()

for key in test_keys:
    new_timing = new_timings[key]
    old_timing = old_timings[key]
    if (old_timing + regression_threshold_seconds) * multiply_percentage < new_timing:
        slow_keys.append(key)

return_code = 0
if len(slow_keys) > 0:
    print('''====================================================
==============  REGRESSIONS DETECTED   =============
====================================================
''')
    return_code = 1
    for key in slow_keys:
        new_timing = new_timings[key]
        old_timing = old_timings[key]
        print(key)
        print(f"Old timing: {old_timing}")
        print(f"New timing: {new_timing}")
        print("")

    print('''====================================================
==================  New Timings   ==================
====================================================
''')
    with open(new_file, 'r') as f:
        print(f.read())
    print('''====================================================
==================  Old Timings   ==================
====================================================
''')
    with open(old_file, 'r') as f:
        print(f.read())
else:
    print('''====================================================
============== NO REGRESSIONS DETECTED  =============
====================================================
''')

print('''====================================================
=================== ALL TIMINGS  ===================
====================================================
''')
for key in test_keys:
    new_timing = new_timings[key]
    old_timing = old_timings[key]
    print(key)
    print(f"Old timing: {old_timing}")
    print(f"New timing: {new_timing}")
    print("")

exit(return_code)
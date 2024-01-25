import sys
import subprocess
import re
import os

# wheth
no_exit = False
for i in range(len(sys.argv)):
    if sys.argv[i] == '--no-exit':
        no_exit = True
        del sys.argv[i]
        i -= 1

if len(sys.argv) < 2:
    print("Expected usage: python3 scripts/run_test_list.py build/debug/test/unittest [--no-exit]")
    exit(1)
unittest_program = sys.argv[1]
extra_args = []
if len(sys.argv) > 2:
    extra_args = [sys.argv[2]]


test_cases = []
for line in sys.stdin:
    if len(line.strip()) == 0:
        continue
    splits = line.rsplit('\t', 1)
    test_cases.append(splits[0])

test_count = len(test_cases)
return_code = 0
for test_number in range(test_count):
    sys.stdout.write("[" + str(test_number) + "/" + str(test_count) + "]: " + test_cases[test_number])
    sys.stdout.flush()
    res = subprocess.run([unittest_program, test_cases[test_number]], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout = res.stdout.decode('utf8')
    stderr = res.stderr.decode('utf8')
    if res.returncode is not None and res.returncode != 0:
        print("FAILURE IN RUNNING TEST")
        print(
            """--------------------
RETURNCODE
--------------------
"""
        )
        print(res.returncode)
        print(
            """--------------------
STDOUT
--------------------
"""
        )
        print(stdout)
        print(
            """--------------------
STDERR
--------------------
"""
        )
        print(stderr)
        return_code = 1
        if not no_exit:
            break

exit(return_code)

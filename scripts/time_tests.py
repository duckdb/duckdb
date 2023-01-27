import sys
import subprocess
import re
import os
import time
import operator

if len(sys.argv) < 2:
	print("Expected usage: python scripts/time_tests.py build/debug/test/unittest")
	exit(1)
unittest_program = sys.argv[1]


proc = subprocess.Popen([unittest_program, '-l'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

stdout = proc.stdout.read().decode('utf8')
stderr = proc.stderr.read().decode('utf8')
if proc.returncode is not None and proc.returncode != 0:
	print("Failed to run program " + unittest_program)
	print(proc.returncode)
	print(stdout)
	print(stderr)
	exit(1)

test_cases = []
first_line = True
for line in stdout.splitlines():
	if first_line:
		first_line = False
		continue
	if len(line.strip()) == 0:
		continue
	splits = line.rsplit('\t', 1)
	test_cases.append(splits[0])

test_count = len(test_cases)
return_code = 0
test_times = {}
for test_number in range(test_count):
	start_time = time.monotonic()
	subprocess.run([unittest_program, test_cases[test_number]], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	test_times[test_cases[test_number]] = time.monotonic() - start_time
test_times = dict( sorted(test_times.items(), key=operator.itemgetter(1),reverse=True))
print (test_times)

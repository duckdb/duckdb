import sys
import subprocess
import re

if len(sys.argv) < 2:
	print("Expected usage: python3 scripts/run_tests_one_by_one.py build/debug/test/unittest")
	exit(1)

unittest_program = sys.argv[1]
extra_args = []
if len(sys.argv) > 2:
	extra_args = [sys.argv[2]]


proc = subprocess.Popen([unittest_program, '-l'] + extra_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
for test_number in range(test_count):
	print("[" + str(test_number) + "/" + str(test_count) + "]: " + test_cases[test_number])
	res = subprocess.run([unittest_program, test_cases[test_number]], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	stdout = res.stdout.decode('utf8')
	stderr = res.stderr.decode('utf8')
	if res.returncode is not None and res.returncode != 0:
		print("FAILURE IN RUNNING TEST")
		print("""--------------------
RETURNCODE
--------------------
""")
		print(res.returncode)
		print("""--------------------
STDOUT
--------------------
""")
		print(stdout)
		print("""--------------------
STDERR
--------------------
""")
		print(stderr)
		exit(1)


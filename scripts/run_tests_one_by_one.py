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
current_test = None

for line in stdout.splitlines():
	if 'All available test cases:' in line:
		continue
	if 'Matching test cases:' in line:
		continue
	if len(re.findall('\d+ .*test cases', line)) != 0:
		continue
	if len(line.strip()) == 0:
		continue
	if current_test is None:
		current_test = line.strip()
	else:
		test_cases.append(current_test)
		current_test = None

test_count = len(test_cases)
for test_number in range(test_count):
	print("[" + str(test_number) + "/" + str(test_count) + "]: " + test_cases[test_number])
	proc = subprocess.Popen([unittest_program, '--start-offset=' + str(test_number), '--end-offset=' + str(test_number + 1)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	stdout = proc.stdout.read().decode('utf8')
	stderr = proc.stderr.read().decode('utf8')
	proc.wait()
	proc.terminate()
	if proc.returncode is not None and proc.returncode != 0:
		print("FAILURE IN RUNNING TEST")
		print("""--------------------
RETURNCODE
--------------------
""")
		print(proc.returncode)
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


# this script re-generates the binary file used for Test serialize_tpch_into_file
# before running this script, increment the version number in src/planner/logical_operator.cpp and
# recompile (BUILD_TPCH=1 make debug)

import os
import subprocess
from python_helpers import open_utf8

shell_proc = os.path.join('build', 'debug', 'test', 'unittest')
gen_binary_file = os.path.join('test', 'api', 'serialized_tpch_queries.binary')

def try_remove_file(fname):
	try:
		os.remove(fname)
	except:
		pass

try_remove_file(gen_binary_file)

def run_test(test):
	print(test)
	res = subprocess.run([shell_proc, test ], capture_output=True)
	stdout = res.stdout.decode('utf8').strip()
	stderr = res.stderr.decode('utf8').strip()
	if res.returncode != 0:
		print("Failed to create binary file!")
		print("----STDOUT----")
		print(stdout)
		print("----STDERR----")
		print(stderr)

run_test("Generate serialized TPCH file")

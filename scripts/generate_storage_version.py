# this script re-generates the storage used for storage_version.test_slow
# before running this script, increment the version number in src/storage/storage_info.cpp and recompile (`make`)
#
# run with python3 scripts/generate_storage_version.py
# 	to regenerate the storage used for storage_version.test_slow
# run with python3 scripts/generate_storage_version.py --old=path/to/duckdb --new=path/to/new/duckdb
#	to test whether 'new' duckdb is forward compatible with 'old' duckdb (using the queries in test/sql/storage_version/generate_storage_version.sql)


import os
import sys
import subprocess
from python_helpers import open_utf8

shell_proc_old = os.path.join('build', 'release', 'duckdb')
shell_proc_new = os.path.join('build', 'release', 'duckdb')

gen_storage_script = os.path.join('test', 'sql', 'storage_version', 'generate_storage_version.sql')
gen_storage_target = os.path.join('test', 'sql', 'storage_version', 'storage_version.db')

for arg in sys.argv:
	if arg.startswith("--old="):
		shell_proc_old = arg.replace("--old=", "")
	if arg.startswith("--new="):
		shell_proc_new = arg.replace("--new=", "")

print("Generator DuckDB = ", shell_proc_old)
print("Tested    DuckDB = ", shell_proc_new)

def try_remove_file(fname):
	try:
		os.remove(fname)
	except:
		pass

def run_command_in_shell(shell_proc, cmd):
	res = subprocess.run([shell_proc, '--batch', '-init', '/dev/null', gen_storage_target], capture_output=True, input=bytearray(cmd, 'utf8'))
	stdout = res.stdout.decode('utf8').strip()
	stderr = res.stderr.decode('utf8').strip()
	if res.returncode != 0:
		print("Failed to create database file!")
		print("----STDOUT----")
		print(stdout)
		print("----STDERR----")
		print(stderr)

with open_utf8(gen_storage_script, 'r') as f:
	cmd = f.read()

def run_iteration(gen_storage_target, shell_proc_generate, shell_proc_test):
	try_remove_file(gen_storage_target)
	try_remove_file(gen_storage_target + '.wal')
	#
	run_command_in_shell(shell_proc_generate, cmd)
	run_command_in_shell(shell_proc_test, 'select * from integral_values')
	run_command_in_shell(shell_proc_test, 'select * from integral_values')
	#
	try_remove_file(gen_storage_target + '.wal')

run_iteration(gen_storage_target, shell_proc_old, shell_proc_new)
run_iteration(gen_storage_target, shell_proc_old, shell_proc_old)

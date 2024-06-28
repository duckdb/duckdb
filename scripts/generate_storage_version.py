# this script re-generates the storage used for storage_version.test_slow
# before running this script, increment the version number in src/storage/storage_info.cpp and recompile (`make`)

import os
import subprocess
from python_helpers import open_utf8

shell_proc = os.path.join('build', 'release', 'duckdb')

gen_storage_script = os.path.join('test', 'sql', 'storage_version', 'generate_storage_version.sql')
gen_storage_target = os.path.join('test', 'sql', 'storage_version', 'storage_version.db')


def try_remove_file(fname):
    try:
        os.remove(fname)
    except:
        pass


try_remove_file(gen_storage_target)
try_remove_file(gen_storage_target + '.wal')


def run_command_in_shell(cmd):
    print(cmd)
    res = subprocess.run(
        [shell_proc, '--batch', '-init', '/dev/null', gen_storage_target],
        capture_output=True,
        input=bytearray(cmd, 'utf8'),
    )
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

run_command_in_shell(cmd)
run_command_in_shell('select * from integral_values')
run_command_in_shell('select * from integral_values')

try_remove_file(gen_storage_target + '.wal')

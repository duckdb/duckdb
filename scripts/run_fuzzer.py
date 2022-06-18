import json
import requests
import sys
import os
import subprocess
import reduce_sql
import fuzzer_helper
import random

seed = -1

fuzzer = None
db = None
shell = None
for param in sys.argv:
    if param == '--sqlsmith':
        fuzzer = 'sqlsmith'
    elif param == '--alltypes':
        db = 'alltypes'
    elif param == '--tpch':
        db = 'tpch'
    elif param.startswith('--shell='):
        shell = param.replace('--shell=', '')
    elif param.startswith('--seed='):
        seed = int(param.replace('--seed=', ''))

if fuzzer is None:
    print("Unrecognized fuzzer to run, expected e.g. --sqlsmith")
    exit(1)

if db is None:
    print("Unrecognized database to run on, expected either --tpch or --alltypes")
    exit(1)

if shell is None:
    print("Unrecognized path to shell, expected e.g. --shell=build/debug/duckdb")
    exit(1)

if seed < 0:
    seed = random.randint(0, 2**30)

git_hash = fuzzer_helper.get_github_hash()

def create_db_script(db):
    if db == 'alltypes':
        return 'create table all_types as select * exclude(small_enum, medium_enum, large_enum) from test_all_types();'
    elif db == 'tpch':
        return 'call dbgen(sf=0.1);'
    else:
        raise Exception("Unknown database creation script")

def run_fuzzer_script(fuzzer):
    if fuzzer == 'sqlsmith':
        return "call sqlsmith(max_queries=${MAX_QUERIES}, seed=${SEED}, verbose_output=1, log='${LAST_LOG_FILE}', complete_log='${COMPLETE_LOG_FILE}');"
    else:
        raise Exception("Unknown fuzzer type")

def run_shell_command(cmd):
    command = [shell, '--batch', '-init', '/dev/null']

    res = subprocess.run(command, input=bytearray(cmd, 'utf8'), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout = res.stdout.decode('utf8').strip()
    stderr = res.stderr.decode('utf8').strip()
    return (stdout, stderr, res.returncode)


# first get a list of all github issues, and check if we can still reproduce them
current_errors = fuzzer_helper.extract_github_issues(shell)

max_queries = 1000
last_query_log_file = 'sqlsmith.log'
complete_log_file = 'sqlsmith.complete.log'

print(f'''==========================================
        RUNNING {fuzzer} on {db}
==========================================''')

load_script = create_db_script(db)
fuzzer = run_fuzzer_script(fuzzer).replace('${MAX_QUERIES}', str(max_queries)).replace('${LAST_LOG_FILE}', last_query_log_file).replace('${COMPLETE_LOG_FILE}', complete_log_file).replace('${SEED}', str(seed))

print(load_script)
print(fuzzer)

cmd = load_script + "\n" + fuzzer

print("==========================================")

(stdout, stderr, returncode) = run_shell_command(cmd)

print(f'''==========================================
        FINISHED RUNNING
==========================================''')
print("==============  STDOUT  ================")
print(stdout)
print("==============  STDERR  =================")
print(stderr)
print("==========================================")

print(returncode)
if returncode == 0:
    print("==============  SUCCESS  ================")
    exit(0)

print("==============  FAILURE  ================")
print("Attempting to reproduce and file issue...")

# run the last query, and see if the issue persists
with open(last_query_log_file, 'r') as f:
    last_query = f.read()

cmd = load_script + '\n' + last_query

(stdout, stderr, returncode) = run_shell_command(cmd)
if returncode == 0:
    print("Failed to reproduce the issue with a single command...")
    exit(0)

print("==============  STDOUT  ================")
print(stdout)
print("==============  STDERR  =================")
print(stderr)
print("==========================================")
if not fuzzer_helper.is_internal_error(stderr):
    print("Failed to reproduce the internal error with a single command")
    exit(0)

error_msg = reduce_sql.sanitize_error(stderr)


print("=========================================")
print("         Reproduced successfully         ")
print("=========================================")

# check if this is a duplicate issue
if error_msg in current_errors:
    print("Skip filing duplicate issue")
    print("Issue already exists: https://github.com/duckdb/duckdb-fuzzer/issues/" + str(current_errors[error_msg]['number']))
    exit(0)

print(last_query)

print("=========================================")
print("        Attempting to reduce query       ")
print("=========================================")

# try to reduce the query as much as possible
last_query = reduce_sql.reduce(last_query, load_script, shell, error_msg)
cmd = load_script + '\n' + last_query + "\n"

fuzzer_helper.file_issue(cmd, error_msg, "SQLSmith", seed, git_hash)

import os
import random
import subprocess
import sys
import reduce_sql
import fuzzer_helper

persistent = False
sqlancer_dir = 'sqlancer'
seed = None
timeout = 600
threads = 1
num_queries = 1000
shell = None

# python3 scripts/run_sqlancer.py --sqlancer=/Users/myth/Programs/sqlancer --shell=build/debug/duckdb --seed=0
for arg in sys.argv:
    if arg == '--persistent':
        persistent = True
    elif arg.startswith('--sqlancer='):
        sqlancer_dir = arg.replace('--sqlancer=', '')
    elif arg.startswith('--seed='):
        seed = int(arg.replace('--seed=', ''))
    elif arg.startswith('--timeout='):
        timeout = int(arg.replace('--timeout=', ''))
    elif arg.startswith('--threads='):
        threads = int(arg.replace('--threads=', ''))
    elif arg.startswith('--num-queries='):
        num_queries = int(arg.replace('--num-queries=', ''))
    elif arg.startswith('--shell='):
        shell = arg.replace('--shell=', '')

if shell is None:
    print("Unrecognized path to shell, expected e.g. --shell=build/debug/duckdb")
    exit(1)

if not os.path.isfile(shell):
    print(f"Could not find shell \"{shell}\"")
    exit(1)

if seed is None:
    seed = random.randint(0, 2**30)

git_hash = fuzzer_helper.get_github_hash()

targetdir = os.path.join(sqlancer_dir, 'target')
filenames = os.listdir(targetdir)
found_filename = ""
for fname in filenames:
    if 'sqlancer-' in fname.lower():
        found_filename = fname
        break

if not found_filename:
    print("FAILED TO RUN SQLANCER")
    print("Could not find target file sqlancer/target/sqlancer-*.jar")
    exit(1)

command_prefix = ['java']
if persistent:
    command_prefix += ['-Dduckdb.database.file=/tmp/lancer_duckdb_db']
command_prefix += ['-jar', os.path.join(targetdir, found_filename)]

seed_text = ''
if seed is not None:
    seed_text = f'--random-seed {seed}'

base_cmd = f'--num-queries {num_queries} --num-threads {threads} {seed_text} --log-each-select=true --timeout-seconds {timeout} duckdb'
command = [x for x in base_cmd.split(' ') if len(x) > 0]

print('--------------------- RUNNING SQLANCER ----------------------')
print(' '.join(command_prefix + command))

subprocess = subprocess.Popen(command_prefix + command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
out = subprocess.stdout.read()
err = subprocess.stderr.read()
subprocess.wait()

if subprocess.returncode == 0:
    print('--------------------- SQLANCER SUCCESS ----------------------')
    print('SQLANCER EXITED WITH CODE ' + str(subprocess.returncode))
    exit(0)

print('--------------------- SQLANCER FAILURE ----------------------')
print('SQLANCER EXITED WITH CODE ' + str(subprocess.returncode))
print('--------------------- SQLANCER ERROR LOG ----------------------')
print(err.decode('utf8', 'ignore'))
print('--------------------- SQLancer Logs ----------------------')
print(out.decode('utf8', 'ignore'))
try:
    with open('duckdb-queries.log', 'r') as f:
        text = f.read()
        print('--------------------- DuckDB Logs ----------------------')
        print(text)
except:
    pass


with open('duckdb-queries.log', 'r') as f:
    query_log = f.read()

# clean up any irrelevant SELECT statements and failing DDL statements
(queries, expected_error) = reduce_sql.cleanup_irrelevant_queries(query_log)
if queries is None:
    print('----------------------------------------------')
    print("Failed to reproduce SQLancer error!")
    print('----------------------------------------------')
    exit(0)

print('----------------------------------------------')
print("Found query log that produces the following error")
print('----------------------------------------------')
if expected_error == '__CRASH__':
    print('CRASH!')
else:
    print(expected_error)

print('----------------------------------------------')
print("Starting reduction process")
print('----------------------------------------------')

# clean up queries from the query log by trying to remove queries one by one
queries = reduce_sql.reduce_query_log(queries, shell)

reduced_test_case = ';\n'.join(queries)
print('----------------------------------------------')
print("Found reproducible test case")
print('----------------------------------------------')
print(reduced_test_case)

(stdout, stderr, returncode) = reduce_sql.run_shell_command(shell, reduced_test_case)
error_msg = reduce_sql.sanitize_error(stderr)

print('----------------------------------------------')
print("Fetching github issues")
print('----------------------------------------------')

# first get a list of all github issues, and check if we can still reproduce them
current_errors = fuzzer_helper.extract_github_issues(shell)

# check if this is a duplicate issue
if error_msg in current_errors:
    print("Skip filing duplicate issue")
    print(
        "Issue already exists: https://github.com/duckdb/duckdb-fuzzer/issues/"
        + str(current_errors[error_msg]['number'])
    )
    exit(0)

fuzzer_helper.file_issue(reduced_test_case, error_msg, "SQLancer", seed, git_hash)

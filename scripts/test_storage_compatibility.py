import argparse
import os
import subprocess
import re
import csv
from pathlib import Path

parser = argparse.ArgumentParser(description='Run a full benchmark using the CLI and report the results.')
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--old-cli', action='store', help='Path to the CLI of the old DuckDB version to test')
group.add_argument('--versions', type=str, action='store', help='DuckDB versions to test')
parser.add_argument('--new-unittest', action='store', help='Path to the new unittester to run', required=True)
parser.add_argument('--new-cli', action='store', help='Path to the new unittester to run', default=None)
parser.add_argument('--compatibility', action='store', help='Storage compatibility version', default='v1.0.0')
parser.add_argument(
    '--test-config', action='store', help='Test config script to run', default='test/configs/storage_compatibility.json'
)
parser.add_argument('--db-name', action='store', help='Database name to write to', default='bwc_storage_test.db')
parser.add_argument('--abort-on-failure', action='store_true', help='Abort on first failure', default=False)
parser.add_argument('--start-offset', type=int, action='store', help='Test start offset', default=None)
parser.add_argument('--end-offset', type=int, action='store', help='Test end offset', default=None)
parser.add_argument('--no-summarize-failures', action='store_true', help='Skip failure summary', default=False)
parser.add_argument('--list-versions', action='store_true', help='Only list versions to test', default=False)
parser.add_argument(
    '--run-empty-tests',
    action='store_true',
    help='Run tests that don' 't have a CREATE TABLE or CREATE VIEW statement',
    default=False,
)

args, extra_args = parser.parse_known_args()

programs_to_test = []
if args.versions is not None:
    version_splits = args.versions.split('|')
    for version in version_splits:
        cli_path = os.path.join(Path.home(), '.duckdb', 'cli', version, 'duckdb')
        if not os.path.isfile(cli_path):
            os.system(f'curl https://install.duckdb.org | DUCKDB_VERSION={version} sh')
        programs_to_test.append(cli_path)
else:
    programs_to_test.append(args.old_cli)

unittest_program = args.new_unittest
db_name = args.db_name
new_cli = args.new_unittest.replace('test/unittest', 'duckdb') if args.new_cli is None else args.new_cli
summarize_failures = not args.no_summarize_failures

# Use the '-l' parameter to output the list of tests to run
proc = subprocess.run(
    [unittest_program, '--test-config', args.test_config, '-l'] + extra_args,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
)
stdout = proc.stdout.decode('utf8').strip()
stderr = proc.stderr.decode('utf8').strip()
if len(stderr) > 0:
    print("Failed to run program " + unittest_program)
    print("Returncode:", proc.returncode)
    print(stdout)
    print(stderr)
    exit(1)


# The output is in the format of 'PATH\tGROUP', we're only interested in the PATH portion
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

test_cases.sort()
if args.compatibility != 'v1.0.0':
    raise Exception("Only v1.0.0 is supported for now (FIXME)")


def escape_cmd_arg(arg):
    if '"' in arg or '\'' in arg or ' ' in arg or '\\' in arg:
        arg = arg.replace('\\', '\\\\')
        arg = arg.replace('"', '\\"')
        arg = arg.replace("'", "\\'")
        return f'"{arg}"'
    return arg


error_container = []


def handle_failure(test, cmd, msg, stdout, stderr, returncode):
    print(f"==============FAILURE============")
    print(test)
    print(f"==============MESSAGE============")
    print(msg)
    print(f"==============COMMAND============")
    cmd_str = ''
    for entry in cmd:
        cmd_str += escape_cmd_arg(entry) + ' '
    print(cmd_str.strip())
    print(f"==============RETURNCODE=========")
    print(str(returncode))
    print(f"==============STDOUT=============")
    print(stdout)
    print(f"==============STDERR=============")
    print(stderr)
    print(f"=================================")
    if args.abort_on_failure:
        exit(1)
    else:
        error_container.append({'test': test, 'stderr': stderr})


def run_program(cmd, description):
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout = proc.stdout.decode('utf8').strip()
    stderr = proc.stderr.decode('utf8').strip()
    if proc.returncode != 0:
        return {
            'test': test,
            'cmd': cmd,
            'msg': f'Failed to {description}',
            'stdout': stdout,
            'stderr': stderr,
            'returncode': proc.returncode,
        }
    return None


def try_run_program(cmd, description):
    result = run_program(cmd, description)
    if result is None:
        return True
    handle_failure(**result)
    return False


index = 0
start = 0 if args.start_offset is None else args.start_offset
end = len(test_cases) if args.end_offset is None else args.end_offset
for i in range(start, end):
    test = test_cases[i]
    skipped = ''
    if not args.run_empty_tests:
        with open(test, 'r') as f:
            test_contents = f.read().lower()
        if 'create table' not in test_contents and 'create view' not in test_contents:
            skipped = ' (SKIPPED)'

    print(f'[{i}/{len(test_cases)}]: {test}{skipped}')
    if skipped != '':
        continue
    # remove the old db
    try:
        os.remove(db_name)
    except:
        pass
    cmd = [unittest_program, '--test-config', args.test_config, test]
    if not try_run_program(cmd, 'Run Test'):
        continue

    if not os.path.isfile(db_name):
        # db not created
        continue

    cmd = [
        programs_to_test[-1],
        db_name,
        '-c',
        '.headers off',
        '-csv',
        '-c',
        '.output table_list.csv',
        '-c',
        'SHOW ALL TABLES',
    ]
    if not try_run_program(cmd, 'List Tables'):
        continue

    tables = []
    with open('table_list.csv', newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            tables.append((row[1], row[2]))
    # no tables / views
    if len(tables) == 0:
        continue

    # read all tables / views
    failures = []
    for cli in programs_to_test:
        cmd = [cli, db_name]
        for table in tables:
            schema_name = table[0].replace('"', '""')
            table_name = table[1].replace('"', '""')
            cmd += ['-c', f'FROM "{schema_name}"."{table_name}"']
        failure = run_program(cmd, 'Query Tables')
        if failure is not None:
            failures.append(failure)
    if len(failures) > 0:
        # we failed to query the tables
        # this MIGHT be expected - e.g. we might have views that reference stale state (e.g. files that are deleted)
        # try to run it with the new CLI - if this succeeds we have a problem
        new_cmd = [new_cli] + cmd[1:]
        new_failure = run_program(new_cmd, 'Query Tables (New)')
        if new_failure is None:
            # we succeeded with the new CLI - report the failure
            for failure in failures:
                handle_failure(**failure)
        continue

if len(error_container) == 0:
    exit(0)

if summarize_failures:
    print(
        '''\n\n====================================================
================  FAILURES SUMMARY  ================
====================================================\n
'''
    )
    for i, error in enumerate(error_container, start=1):
        print(f"\n{i}:", error["test"], "\n")
        print(error["stderr"])

exit(1)

import argparse
import sys
import subprocess
import time
import threading
import tempfile
import os
import shutil
import re


class ErrorContainer:
    def __init__(self):
        self._lock = threading.Lock()
        self._errors = []

    def append(self, item):
        with self._lock:
            self._errors.append(item)

    def get_errors(self):
        with self._lock:
            return list(self._errors)

    def __len__(self):
        with self._lock:
            return len(self._errors)


error_container = ErrorContainer()


def valid_timeout(value):
    try:
        timeout_float = float(value)
        if timeout_float <= 0:
            raise argparse.ArgumentTypeError("Timeout value must be a positive float")
        return timeout_float
    except ValueError:
        raise argparse.ArgumentTypeError("Timeout value must be a float")


parser = argparse.ArgumentParser(description='Run tests one by one with optional flags.')
parser.add_argument('unittest_program', help='Path to the unittest program')
parser.add_argument('--no-exit', action='store_true', help='Execute all tests, without stopping on first error')
parser.add_argument('--fast-fail', action='store_true', help='Terminate on first error')
parser.add_argument('--profile', action='store_true', help='Enable profiling')
parser.add_argument('--no-assertions', action='store_false', help='Disable assertions')
parser.add_argument('--time_execution', action='store_true', help='Measure and print the execution time of each test')
parser.add_argument('--list', action='store_true', help='Print the list of tests to run')
parser.add_argument('--summarize-failures', action='store_true', help='Summarize failures', default=None)
parser.add_argument(
    '--tests-per-invocation', type=int, help='The amount of tests to run per invocation of the runner', default=1
)
parser.add_argument(
    '--print-interval', action='store', help='Prints "Still running..." every N seconds', default=300.0, type=float
)
parser.add_argument(
    '--timeout',
    action='store',
    help='Add a timeout for each test (in seconds, default: 3600s - i.e. one hour)',
    default=3600,
    type=valid_timeout,
)
parser.add_argument('--valgrind', action='store_true', help='Run the tests with valgrind', default=False)

args, extra_args = parser.parse_known_args()

if not args.unittest_program:
    parser.error('Path to unittest program is required')

# Access the arguments
unittest_program = args.unittest_program
no_exit = args.no_exit
fast_fail = args.fast_fail
tests_per_invocation = args.tests_per_invocation

if no_exit:
    if fast_fail:
        print("--no-exit and --fast-fail can't be combined")
        exit(1)

profile = args.profile
assertions = args.no_assertions
time_execution = args.time_execution
timeout = args.timeout

summarize_failures = args.summarize_failures
if summarize_failures is None:
    # get from env
    summarize_failures = False
    if 'SUMMARIZE_FAILURES' in os.environ:
        summarize_failures = os.environ['SUMMARIZE_FAILURES'] == '1'
    elif 'CI' in os.environ:
        # enable by default in CI if not set explicitly
        summarize_failures = True

# Use the '-l' parameter to output the list of tests to run
proc = subprocess.run([unittest_program, '-l'] + extra_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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


test_count = len(test_cases)
if args.list:
    for test_number, test_case in enumerate(test_cases):
        print(print(f"[{test_number}/{test_count}]: {test_case}"))

all_passed = True


def fail():
    global all_passed
    all_passed = False
    if fast_fail:
        exit(1)


def parse_assertions(stdout):
    for line in stdout.splitlines():
        if 'All tests were skipped' in line:
            return "SKIPPED"
        if line == 'assertions: - none -':
            return "0 assertions"

        # Parse assertions in format
        pos = line.find("assertion")
        if pos != -1:
            space_before_num = line.rfind(' ', 0, pos - 2)
            return line[space_before_num + 2 : pos + 10]

    return "ERROR"


is_active = False


def get_test_name_from(text):
    match = re.findall(r'\((.*?)\)\!', text)
    return match[0] if match else ''


def get_clean_error_message_from(text):
    match = re.split(r'^=+\n', text, maxsplit=1, flags=re.MULTILINE)
    return match[1] if len(match) > 1 else text


def print_interval_background(interval):
    global is_active
    current_ticker = 0.0
    while is_active:
        time.sleep(0.1)
        current_ticker += 0.1
        if current_ticker >= interval:
            print("Still running...")
            current_ticker = 0


def launch_test(test, list_of_tests=False):
    global is_active
    # start the background thread
    is_active = True
    background_print_thread = threading.Thread(target=print_interval_background, args=[args.print_interval])
    background_print_thread.start()

    unittest_stdout = sys.stdout if list_of_tests else subprocess.PIPE
    unittest_stderr = subprocess.PIPE

    start = time.time()
    try:
        test_cmd = [unittest_program] + test
        if args.valgrind:
            test_cmd = ['valgrind'] + test_cmd
        # should unset SUMMARIZE_FAILURES to avoid producing exceeding failure logs
        env = os.environ.copy()
        # pass env variables globally
        if list_of_tests or no_exit or tests_per_invocation:
            env['SUMMARIZE_FAILURES'] = '0'
            env['NO_DUPLICATING_HEADERS'] = '1'
        else:
            env['SUMMARIZE_FAILURES'] = '0'
        res = subprocess.run(test_cmd, stdout=unittest_stdout, stderr=unittest_stderr, timeout=timeout, env=env)
    except subprocess.TimeoutExpired as e:
        if list_of_tests:
            print("[TIMED OUT]", flush=True)
        else:
            print(" (TIMED OUT)", flush=True)
        test_name = test[0] if not list_of_tests else str(test)
        error_msg = f'TIMEOUT - exceeded specified timeout of {timeout} seconds'
        new_data = {"test": test_name, "return_code": 1, "stdout": '', "stderr": error_msg}
        error_container.append(new_data)
        fail()
        return

    stdout = res.stdout.decode('utf8') if not list_of_tests else ''
    stderr = res.stderr.decode('utf8')

    if len(stderr) > 0:
        # when list_of_tests test name gets transformed, but we can get it from stderr
        test_name = test[0] if not list_of_tests else get_test_name_from(stderr)
        error_message = get_clean_error_message_from(stderr)
        new_data = {"test": test_name, "return_code": res.returncode, "stdout": stdout, "stderr": error_message}
        error_container.append(new_data)

    end = time.time()

    # join the background print thread
    is_active = False
    background_print_thread.join()

    additional_data = ""
    if assertions:
        additional_data += " (" + parse_assertions(stdout) + ")"
    if args.time_execution:
        additional_data += f" (Time: {end - start:.4f} seconds)"
    print(additional_data, flush=True)
    if profile:
        print(f'{test_case}	{end - start}')
    if res.returncode is None or res.returncode == 0:
        return

    print("FAILURE IN RUNNING TEST")
    print(
        """--------------------
RETURNCODE
--------------------"""
    )
    print(res.returncode)
    print(
        """--------------------
STDOUT
--------------------"""
    )
    print(stdout)
    print(
        """--------------------
STDERR
--------------------"""
    )
    print(stderr)

    # if a test closes unexpectedly (e.g., SEGV), test cleanup doesn't happen,
    # causing us to run out of space on subsequent tests in GH Actions (not much disk space there)
    duckdb_unittest_tempdir = os.path.join(
        os.path.dirname(unittest_program), '..', '..', '..', 'duckdb_unittest_tempdir'
    )
    if os.path.exists(duckdb_unittest_tempdir) and os.listdir(duckdb_unittest_tempdir):
        shutil.rmtree(duckdb_unittest_tempdir)
    fail()


def run_tests_one_by_one():
    for test_number, test_case in enumerate(test_cases):
        if not profile:
            print(f"[{test_number}/{test_count}]: {test_case}", end="", flush=True)
        launch_test([test_case])


def escape_test_case(test_case):
    return test_case.replace(',', '\\,')


def run_tests_batched(batch_count):
    tmp = tempfile.NamedTemporaryFile()
    # write the test list to a temporary file
    with open(tmp.name, 'w') as f:
        for test_case in test_cases:
            f.write(escape_test_case(test_case) + '\n')
    # use start_offset/end_offset to cycle through the test list
    test_number = 0
    while test_number < len(test_cases):
        # gather test cases
        next_entry = test_number + batch_count
        if next_entry > len(test_cases):
            next_entry = len(test_cases)

        launch_test(['-f', tmp.name, '--start-offset', str(test_number), '--end-offset', str(next_entry)], True)
        test_number = next_entry


if args.tests_per_invocation == 1:
    run_tests_one_by_one()
else:
    assertions = False
    run_tests_batched(args.tests_per_invocation)

if all_passed:
    exit(0)
if summarize_failures and len(error_container):
    print(
        '''\n\n====================================================
================  FAILURES SUMMARY  ================
====================================================\n
'''
    )
    for i, error in enumerate(error_container.get_errors(), start=1):
        print(f"\n{i}:", error["test"], "\n")
        print(error["stderr"])

exit(1)

import argparse
import sys
import subprocess
import time
import threading
import tempfile
import os
import shutil
import re
import json
import atexit
import platform


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
parser.add_argument("--test-config", action='store', help='Path to the test configuration file', default=None)
parser.add_argument('--rss-output', action='store', help='Write peak subprocess RSS measurements (bytes) to this JSON file', default=None)

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

is_windows = os.name == 'nt'
rss_tracking_enabled = args.rss_output is not None and not is_windows
if args.rss_output is not None and is_windows:
    print('RSS tracking disabled on Windows: wait4 is unavailable.')

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
rss_measurements = []


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


def ru_maxrss_to_bytes(ru_maxrss):
    if platform.system() == 'Darwin':
        # Darwin reports bytes.
        return int(ru_maxrss)
    # Linux and most other Unix variants report KiB.
    return int(ru_maxrss) * 1024


def run_subprocess_and_measure_rss(test_cmd, unittest_stdout, unittest_stderr, timeout, env):
    if not rss_tracking_enabled:
        return subprocess.run(test_cmd, stdout=unittest_stdout, stderr=unittest_stderr, timeout=timeout, env=env), None

    capture_stdout = unittest_stdout == subprocess.PIPE
    capture_stderr = unittest_stderr == subprocess.PIPE

    with tempfile.TemporaryFile() as stdout_tmp, tempfile.TemporaryFile() as stderr_tmp:
        popen_stdout = stdout_tmp if capture_stdout else unittest_stdout
        popen_stderr = stderr_tmp if capture_stderr else unittest_stderr
        process = subprocess.Popen(test_cmd, stdout=popen_stdout, stderr=popen_stderr, env=env)

        start_time = time.time()
        wait_status = None
        wait_rusage = None
        timed_out = False
        wait4_poll_interval_s = 0.01

        try:
            while True:
                waited_pid, status, rusage = os.wait4(process.pid, os.WNOHANG)
                if waited_pid == process.pid:
                    wait_status = status
                    wait_rusage = rusage
                    break
                if time.time() - start_time > timeout:
                    timed_out = True
                    break
                time.sleep(wait4_poll_interval_s)

            if timed_out:
                process.kill()
                _, wait_status, wait_rusage = os.wait4(process.pid, 0)
                stdout_data = b''
                stderr_data = b''
                if capture_stdout:
                    stdout_tmp.seek(0)
                    stdout_data = stdout_tmp.read()
                if capture_stderr:
                    stderr_tmp.seek(0)
                    stderr_data = stderr_tmp.read()
                timeout_error = subprocess.TimeoutExpired(test_cmd, timeout, output=stdout_data, stderr=stderr_data)
                timeout_error.peak_rss_bytes = ru_maxrss_to_bytes(wait_rusage.ru_maxrss)
                raise timeout_error
        except Exception:
            if process.poll() is None:
                try:
                    process.kill()
                    os.wait4(process.pid, 0)
                except Exception:
                    pass
            raise

        if os.WIFEXITED(wait_status):
            returncode = os.WEXITSTATUS(wait_status)
        elif os.WIFSIGNALED(wait_status):
            returncode = -os.WTERMSIG(wait_status)
        else:
            returncode = process.returncode if process.returncode is not None else 1

        stdout_data = b''
        stderr_data = b''
        if capture_stdout:
            stdout_tmp.seek(0)
            stdout_data = stdout_tmp.read()
        if capture_stderr:
            stderr_tmp.seek(0)
            stderr_data = stderr_tmp.read()

        return subprocess.CompletedProcess(test_cmd, returncode, stdout_data, stderr_data), ru_maxrss_to_bytes(wait_rusage.ru_maxrss)


def append_rss_measurement(test_name, test_cmd, peak_rss_bytes):
    if not rss_tracking_enabled:
        return
    rss_measurements.append(
        {
            'test': test_name,
            'command': test_cmd,
            'peak_rss_bytes': int(peak_rss_bytes or 0),
        }
    )


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
        if args.test_config:
            test_cmd = test_cmd + ['--test-config', args.test_config]
        res, peak_rss_bytes = run_subprocess_and_measure_rss(
            test_cmd, unittest_stdout, unittest_stderr, timeout=timeout, env=env
        )
    except subprocess.TimeoutExpired as e:
        if list_of_tests:
            print("[TIMED OUT]", flush=True)
        else:
            print(" (TIMED OUT)", flush=True)
        test_name = test[0] if not list_of_tests else str(test)
        peak_rss_bytes = getattr(e, 'peak_rss_bytes', None)
        append_rss_measurement(test_name, ' '.join(test_cmd), peak_rss_bytes)
        error_msg = f'TIMEOUT - exceeded specified timeout of {timeout} seconds'
        new_data = {"test": test_name, "return_code": 1, "stdout": '', "stderr": error_msg}
        error_container.append(new_data)
        fail()
        is_active = False
        background_print_thread.join()
        return

    stdout = res.stdout.decode('utf8') if not list_of_tests else ''
    stderr = res.stderr.decode('utf8')
    test_name = test[0] if not list_of_tests else ' '.join(test)
    append_rss_measurement(test_name, ' '.join(test_cmd), peak_rss_bytes)

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
    if rss_tracking_enabled:
        peak_rss_mib = float(int(peak_rss_bytes or 0)) / (1024.0 * 1024.0)
        additional_data += f" (Peak RSS: {peak_rss_mib:.1f} MiB)"
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


def write_rss_output():
    if args.rss_output is None:
        return
    output_dir = os.path.dirname(args.rss_output)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(args.rss_output, 'w') as f:
        json.dump({'measurements': rss_measurements}, f, indent=2)


if args.rss_output is not None:
    atexit.register(write_rss_output)


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

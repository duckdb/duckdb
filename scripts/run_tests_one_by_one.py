import argparse
import sys
import subprocess
import time
import threading
import tempfile
import os
import shutil
import re
import multiprocessing
from multiprocessing import Pool, Manager
from functools import partial


def valid_timeout(value):
    try:
        timeout_float = float(value)
        if timeout_float <= 0:
            raise argparse.ArgumentTypeError("Timeout value must be a positive float")
        return timeout_float
    except ValueError:
        raise argparse.ArgumentTypeError("Timeout value must be a float")


def get_cpu_count() -> int:
    """Get the number of CPU cores available."""
    try:
        return len(os.sched_getaffinity(0))
    # sched_getaffinity only exists in certain platform, if not exist, fallback
    except AttributeError:
        return os.cpu_count() or 1


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
parser.add_argument(
    '--workers',
    type=int,
    help="Number of parallel workers to run tests. Use 0 to auto-detect CPU cores. (default: 0)",
    default=0,
)
parser.add_argument(
    '--no-parallel',
    dest='parallel',
    action='store_false',
    help='Disable parallel execution (use single worker)',
    default=True,
)

args, extra_args = parser.parse_known_args()

if not args.unittest_program:
    parser.error('Path to unittest program is required')

# Access the arguments
unittest_program = args.unittest_program
no_exit = args.no_exit
fast_fail = args.fast_fail
tests_per_invocation = args.tests_per_invocation

# Resolve number of workers
if not args.parallel:
    num_workers = 1  # --no-parallel forces sequential execution
elif args.workers == 0:
    num_workers = get_cpu_count()
    print(f"Auto-detected {num_workers} CPU cores for parallel execution", flush=True)
else:
    num_workers = args.workers

if no_exit:
    if fast_fail:
        print("--no-exit and --fast-fail can't be combined")
        exit(1)

if num_workers > 1 and fast_fail:
    print("Warning: --fast-fail with --workers > 1 may not stop immediately on first failure", flush=True)

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
        print(f"[{test_number}/{test_count}]: {test_case}")

# Used to record errors in tests
all_passed = True
error_list = []


def fail():
    global all_passed
    all_passed = False
    if fast_fail:
        exit(1)


def add_error(error_data):
    error_list.append(error_data)


def parse_assertions(stdout):
    for line in stdout.splitlines():
        if 'All tests were skipped' in line:
            return "SKIPPED"
        if line == 'assertions: - none -':
            return "0 assertions"

        # Example format: "assertions: 123" or similar
        if 'assertions:' in line.lower():
            match = re.search(r'assertions?:\s*(\d+)', line, re.IGNORECASE)
            if match:
                return f"{match.group(1)} assertions"
        # Example format: "123 assertions" or similar
        elif re.search(r'\d+\s+assertions?', line, re.IGNORECASE):
            match = re.search(r'(\d+)\s+assertions?', line, re.IGNORECASE)
            if match:
                return f"{match.group(1)} assertions"

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


def get_worker_temp_dir(worker_id, base_unittest_program):
    """Get a unique temp directory for each worker to avoid conflicts."""
    base_dir = os.path.join(os.path.dirname(base_unittest_program), '..', '..', '..', 'duckdb_unittest_tempdir')
    if worker_id >= 0:
        return os.path.join(base_dir, f'worker_{worker_id}')
    return base_dir


def run_test_task(task, config):
    """
    Worker function for parallel test execution (runs in separate process).
    Task is a dict with 'test_cmd' (list) and 'worker_id', plus optional batch info.
    Returns a result dict with test outcome.
    """
    unittest_program = config['unittest_program']
    base_timeout = config['timeout']
    valgrind = config['valgrind']
    test_config_path = config['test_config']
    assertions = config['assertions']

    # Extract task info
    test_cmd = task['test_cmd']
    worker_id = task['worker_id']
    test_name = task['test_name']
    is_batch = task.get('is_batch', False)
    batch_size = task.get('batch_size', 1)
    start_offset = task.get('start_offset', 0)
    end_offset = task.get('end_offset', 0)

    # Build full command
    if valgrind:
        test_cmd = ['valgrind'] + test_cmd
    if test_config_path:
        test_cmd = test_cmd + ['--test-config', test_config_path]

    # Set up environment
    env = os.environ.copy()
    env['SUMMARIZE_FAILURES'] = '0'
    env['NO_DUPLICATING_HEADERS'] = '1'
    worker_temp_dir = get_worker_temp_dir(worker_id, unittest_program)
    env['DUCKDB_UNITTEST_TEMP_DIR'] = worker_temp_dir

    # Initialize result
    effective_timeout = base_timeout * batch_size
    result = {
        'test_case': test_name,
        'worker_id': worker_id,
        'passed': False,
        'error': None,
        'duration': 0,
        'assertions': '',
        'stdout': '',
        'stderr': '',
        'is_batch': is_batch,
        'batch_size': batch_size,
        'start_offset': start_offset,
        'end_offset': end_offset,
    }

    start = time.time()
    try:
        res = subprocess.run(
            test_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=effective_timeout, env=env
        )
        result['stdout'] = res.stdout.decode('utf8')
        result['stderr'] = res.stderr.decode('utf8')

        if res.returncode is None or res.returncode == 0:
            result['passed'] = True
        else:
            failing_test = test_name
            if is_batch:
                failing_test = get_test_name_from(result['stderr']) or test_name
            result['error'] = {
                'test': failing_test,
                'return_code': res.returncode,
                'stdout': result['stdout'],
                'stderr': get_clean_error_message_from(result['stderr']),
            }

        if assertions and not is_batch:
            result['assertions'] = parse_assertions(result['stdout'])

    except subprocess.TimeoutExpired:
        result['error'] = {
            'test': test_name,
            'return_code': 1,
            'stdout': '',
            'stderr': f'TIMEOUT - exceeded timeout of {effective_timeout} seconds',
        }
    except Exception as e:
        result['error'] = {
            'test': test_name,
            'return_code': 1,
            'stdout': '',
            'stderr': str(e),
        }

    result['duration'] = time.time() - start

    # Clean up temp directory if test failed
    if not result['passed'] and os.path.exists(worker_temp_dir) and os.listdir(worker_temp_dir):
        try:
            shutil.rmtree(worker_temp_dir)
        except Exception:
            pass

    return result


def run_tests_parallel(num_workers, batch_size=1):
    """Run tests in parallel using a process pool.

    Args:
        num_workers: Number of parallel worker processes
        batch_size: Number of tests per invocation (1 = single test mode, >1 = batch mode)
    """
    if batch_size > 1:
        print(
            f"Running {test_count} tests with {num_workers} parallel workers, {batch_size} tests per batch...",
            flush=True,
        )
    else:
        print(f"Running {test_count} tests with {num_workers} parallel worker processes...", flush=True)

    config = {
        'unittest_program': unittest_program,
        'timeout': timeout,
        'valgrind': args.valgrind,
        'test_config': args.test_config,
        'assertions': assertions,
    }

    all_passed_local = True
    errors_collected = []
    completed_tests = 0
    tests_file = None

    try:
        # Create tasks based on mode
        if batch_size > 1:
            # Batch mode: create temp file and batch tasks
            tmp = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt')
            tests_file = tmp.name
            for test_case in test_cases:
                tmp.write(escape_test_case(test_case) + '\n')
            tmp.close()

            tasks = []
            for i, start_offset in enumerate(range(0, test_count, batch_size)):
                end_offset = min(start_offset + batch_size, test_count)
                tasks.append(
                    {
                        'test_cmd': [
                            unittest_program,
                            '-f',
                            tests_file,
                            '--start-offset',
                            str(start_offset),
                            '--end-offset',
                            str(end_offset),
                        ],
                        'test_name': f'batch[{start_offset}:{end_offset}]',
                        'worker_id': i % num_workers,
                        'is_batch': True,
                        'batch_size': end_offset - start_offset,
                        'start_offset': start_offset,
                        'end_offset': end_offset,
                    }
                )
        else:
            # Single test mode
            tasks = [
                {'test_cmd': [unittest_program, tc], 'test_name': tc, 'worker_id': i % num_workers}
                for i, tc in enumerate(test_cases)
            ]

        # Run tasks in parallel
        with Pool(processes=num_workers) as pool:
            worker_func = partial(run_test_task, config=config)

            for result in pool.imap_unordered(worker_func, tasks):
                completed_tests += result['batch_size']
                passed = result['passed']
                duration = result['duration']
                worker_id = result['worker_id']

                status = "PASS" if passed else "FAIL"
                additional_info = f" (Time: {duration:.4f}s)" if time_execution else ""

                if result['is_batch']:
                    start_off, end_off = result['start_offset'], result['end_offset']
                    print(
                        f"[Tests {start_off}-{end_off}/{test_count}] [Worker {worker_id}]: {status}{additional_info}",
                        flush=True,
                    )
                else:
                    if assertions and result['assertions']:
                        additional_info = f" ({result['assertions']})" + additional_info
                    print(
                        f"[{completed_tests}/{test_count}] [Worker {worker_id}] {result['test_case']}: {status}{additional_info}",
                        flush=True,
                    )

                if not passed:
                    all_passed_local = False
                    if result['error']:
                        errors_collected.append(result['error'])
                    if fast_fail:
                        pool.terminate()
                        break

    finally:
        if tests_file and os.path.exists(tests_file):
            try:
                os.unlink(tests_file)
            except Exception:
                pass

    return all_passed_local, errors_collected


def launch_test_sequential(test, list_of_tests=False):
    """Sequential test execution."""
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
        res = subprocess.run(test_cmd, stdout=unittest_stdout, stderr=unittest_stderr, timeout=timeout, env=env)
    except subprocess.TimeoutExpired:
        if list_of_tests:
            print("[TIMED OUT]", flush=True)
        else:
            print(" (TIMED OUT)", flush=True)
        test_name = test[0] if not list_of_tests else str(test)
        error_msg = f'TIMEOUT - exceeded specified timeout of {timeout} seconds'
        add_error({"test": test_name, "return_code": 1, "stdout": '', "stderr": error_msg})
        fail()
        return

    stdout = res.stdout.decode('utf8') if not list_of_tests else ''
    stderr = res.stderr.decode('utf8')

    if len(stderr) > 0:
        test_name = test[0] if not list_of_tests else get_test_name_from(stderr)
        error_message = get_clean_error_message_from(stderr)
        add_error({"test": test_name, "return_code": res.returncode, "stdout": stdout, "stderr": error_message})

    end = time.time()

    is_active = False
    background_print_thread.join()

    additional_data = ""
    if assertions:
        additional_data += " (" + parse_assertions(stdout) + ")"
    if args.time_execution:
        additional_data += f" (Time: {end - start:.4f} seconds)"
    print(additional_data, flush=True)

    if profile:
        print(f'{test[0]}	{end - start}')

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

    duckdb_unittest_tempdir = get_worker_temp_dir(-1, unittest_program)
    if os.path.exists(duckdb_unittest_tempdir) and os.listdir(duckdb_unittest_tempdir):
        shutil.rmtree(duckdb_unittest_tempdir)
    fail()


def run_tests_one_by_one():
    for test_number, test_case in enumerate(test_cases):
        if not profile:
            print(f"[{test_number}/{test_count}]: {test_case}", end="", flush=True)
        launch_test_sequential([test_case])


def escape_test_case(test_case):
    return test_case.replace(',', '\\,')


def run_tests_batched(batch_count):
    tmp = tempfile.NamedTemporaryFile()
    with open(tmp.name, 'w') as f:
        for test_case in test_cases:
            f.write(escape_test_case(test_case) + '\n')
    test_number = 0
    while test_number < len(test_cases):
        next_entry = test_number + batch_count
        if next_entry > len(test_cases):
            next_entry = len(test_cases)

        launch_test_sequential(
            ['-f', tmp.name, '--start-offset', str(test_number), '--end-offset', str(next_entry)], True
        )
        test_number = next_entry


# Main execution
if __name__ == '__main__':
    if num_workers > 1:
        # Parallel execution mode using multiprocessing, which supports batching
        all_passed, errors_collected = run_tests_parallel(num_workers, tests_per_invocation)
        error_list = errors_collected
    elif tests_per_invocation == 1:
        # Sequential one-by-one mode
        run_tests_one_by_one()
    else:
        # Sequential batched mode
        assertions = False
        run_tests_batched(tests_per_invocation)

    if all_passed:
        exit(0)

    if summarize_failures and len(error_list):
        print(
            '''\n\n====================================================
================  FAILURES SUMMARY  ================
====================================================\n
'''
        )
        for i, error in enumerate(error_list, start=1):
            print(f"\n{i}:", error["test"], "\n")
            print(
                """--------------------
STDOUT
--------------------"""
            )
            print(error.get("stdout", ""))
            print(
                """--------------------
STDERR
--------------------"""
            )
            print(error["stderr"])

    exit(1)

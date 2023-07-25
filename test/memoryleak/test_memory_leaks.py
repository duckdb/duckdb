import subprocess
import time
import argparse
import os

parser = argparse.ArgumentParser(description='Runs the memory leak tests')

parser.add_argument(
    '--unittest',
    dest='unittest',
    action='store',
    help='Path to unittest executable',
    default='build/release/test/unittest',
)
parser.add_argument('--test', dest='test', action='store', help='The name of the tests to run (* is all)', default='*')
parser.add_argument(
    '--timeout',
    dest='timeout',
    action='store',
    help='The maximum time to run the test and measure memory usage (in seconds)',
    default=60,
)
parser.add_argument(
    '--threshold-percentage',
    dest='threshold_percentage',
    action='store',
    help='The percentage threshold before we consider an increase a regression',
    default=0.01,
)
parser.add_argument(
    '--threshold-absolute',
    dest='threshold_absolute',
    action='store',
    help='The absolute threshold before we consider an increase a regression',
    default=1000,
)
parser.add_argument('--verbose', dest='verbose', action='store', help='Verbose output', default=True)

args = parser.parse_args()

unittest_program = args.unittest
test_filter = args.test
test_time = int(args.timeout)
verbose = args.verbose
measurements_per_second = 1.0

# get a list of all unittests
proc = subprocess.Popen([unittest_program, '-l', '[memoryleak]'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
stdout = proc.stdout.read().decode('utf8')
stderr = proc.stderr.read().decode('utf8')
if proc.returncode is not None and proc.returncode != 0:
    print("Failed to run program " + unittest_program)
    print(proc.returncode)
    print(stdout)
    print(stderr)
    exit(1)

test_cases = []
first_line = True
for line in stdout.splitlines():
    if first_line:
        first_line = False
        continue
    if len(line.strip()) == 0:
        continue
    splits = line.rsplit('\t', 1)
    if test_filter == '*' or test_filter in splits[0]:
        test_cases.append(splits[0])

if len(test_cases) == 0:
    print(f"No tests matching filter \"{test_filter}\" found")
    exit(0)


def sizeof_fmt(num, suffix="B"):
    for unit in ["", "K", "M", "G", "T", "P", "E", "Z"]:
        if abs(num) < 1000.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1000.0
    return f"{num:.1f}Yi{suffix}"


def run_test(test_case):
    # launch the unittest program
    proc = subprocess.Popen(
        [unittest_program, test_case, '--memory-leak-tests'], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    pid = proc.pid

    # capture the memory output for the duration of the program running
    leak = True
    rss = []
    for i in range(int(test_time * measurements_per_second)):
        time.sleep(1.0 / measurements_per_second)
        if proc.poll() is not None:
            print("------------------------------------------------")
            print("                    FAILURE                     ")
            print("------------------------------------------------")
            print("                    stdout:                     ")
            print("------------------------------------------------")
            print(proc.stdout.read().decode('utf8'))
            print("------------------------------------------------")
            print("                    stderr:                     ")
            print("------------------------------------------------")
            print(proc.stderr.read().decode('utf8'))
            exit(1)
        ps_proc = subprocess.Popen(f'ps -o rss= -p {pid}'.split(' '), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        res = ps_proc.stdout.read().decode('utf8').strip()
        rss.append(int(res))
        if not has_memory_leak(rss):
            leak = False
            break

    proc.terminate()
    if leak:
        print("------------------------------------------------")
        print("                     ERROR                      ")
        print("------------------------------------------------")
        print(f"Memory leak detected in test case \"{test_case}\"")
        print("------------------------------------------------")
    elif verbose:
        print("------------------------------------------------")
        print("                    Success!                    ")
        print("------------------------------------------------")
        print("------------------------------------------------")
        print(f"No memory leaks detected in test case \"{test_case}\"")
        print("------------------------------------------------")
    if verbose or leak:
        print("Observed memory usages")
        print("------------------------------------------------")
        for i in range(len(rss)):
            memory = rss[i]
            print(f"{i / measurements_per_second}: {sizeof_fmt(memory)}")
        if leak:
            exit(1)


def has_memory_leak(rss):
    measurement_count = int(measurements_per_second * 10)
    if len(rss) <= measurement_count:
        # not enough measurements yet
        return True
    differences = []
    for i in range(1, len(rss)):
        differences.append(rss[i] - rss[i - 1])
    max_memory = max(rss)
    sum_differences = sum(differences[-measurement_count:])
    return sum_differences > (max_memory * args.threshold_percentage + args.threshold_absolute)


try:
    for index, test in enumerate(test_cases):
        print(f"[{index}/{len(test_cases)}] {test}")
        run_test(test)
finally:
    os.system('killall -9 unittest')

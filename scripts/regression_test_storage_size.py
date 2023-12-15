import os
import argparse
import subprocess
import tempfile

# the threshold at which we consider something a regression (percentage)
regression_threshold_percentage = 0.05

parser = argparse.ArgumentParser(description='Generate TPC-DS reference results from Postgres.')
parser.add_argument('--old', dest='old_runner', action='store', help='Path to the old shell executable')
parser.add_argument('--new', dest='new_runner', action='store', help='Path to the new shell executable')

args = parser.parse_args()

old_runner = args.old_runner
new_runner = args.new_runner
exit_code = 0

if not os.path.isfile(old_runner):
    print(f"Failed to find old runner {old_runner}")
    exit(1)

if not os.path.isfile(new_runner):
    print(f"Failed to find new runner {new_runner}")
    exit(1)


def load_data(shell_path, load_script):
    with tempfile.NamedTemporaryFile() as f:
        filename = f.name
    proc = subprocess.Popen([shell_path, '-c', load_script, filename])
    proc.wait()
    if proc.returncode != 0:
        print('----------------------------')
        print('FAILED TO RUN')
        print('----------------------------')
        return None
    return os.path.getsize(filename)


def run_benchmark(load_script, benchmark_name):
    print('----------------------------')
    print(f'Running benchmark {benchmark_name}')
    print('----------------------------')
    old_size = load_data(old_runner, load_script)
    if old_size is None:
        return False
    new_size = load_data(new_runner, load_script)
    if new_size is None:
        return False
    print(f'Database size with old runner: {old_size}')
    print(f'Database size with new runner: {new_size}')
    if new_size - new_size * regression_threshold_percentage > old_size:
        print('----------------------------')
        print('FAILURE: SIZE INCREASE')
        print('----------------------------')
        return False
    else:
        print('----------------------------')
        print('SUCCESS!')
        print('----------------------------')
    return True


tpch_load = 'CALL dbgen(sf=1);'
tpcds_load = 'CALL dsdgen(sf=1);'


benchmarks = [[tpch_load, 'TPC-H SF1'], [tpcds_load, 'TPC-DS SF1']]

for benchmark in benchmarks:
    if not run_benchmark(benchmark[0], benchmark[1]):
        print(f'Database size increased in {benchmark[1]}')
        exit_code = 1

exit(exit_code)


import os, sys, subprocess, re, time, threading

last_benchmarked_commit_file = '.last_benchmarked_commit'

def log(msg):
    print(msg)

FNULL = open(os.devnull, 'w')
benchmark_runner = os.path.join('build', 'release', 'benchmark', 'benchmark_runner')
out_file = 'out.csv'
log_file = 'out.log'
benchmark_results_folder = 'benchmark_results'
benchmark_info_folder = os.path.join(benchmark_results_folder, 'info')
default_start_commit = "9ea358b716b33a4929348dc96acbaa16436d83fa"
# 5 minute timeout per benchmark
total_timeout = 300

def get_current_git_version():
    proc = subprocess.Popen(['git', 'rev-parse', 'HEAD'], stdout=subprocess.PIPE)
    return proc.stdout.readline().rstrip()

def pull_new_changes():
    proc = subprocess.Popen(['git', 'pull'], stdout=FNULL)
    proc.wait()

def build_optimized():
    log("Starting optimized build")
    proc = subprocess.Popen(['make', 'opt'], stdout=FNULL, stderr=subprocess.PIPE)
    proc.wait()
    if proc.returncode != 0:
        print("Failed to compile, moving on to next commit")
        while True:
            line = proc.stderr.readline()
            if line == '':
                break
            print(line);
        return False
    else:
        log("Finished optimized build")
        return True

def get_list_of_commits(until_commit=None):
    proc = subprocess.Popen(['git', 'checkout', 'origin/master'], stdout=subprocess.PIPE)
    proc.wait()
    list = []
    commit_regex = re.compile('commit ([a-z0-9]{40})')
    proc = subprocess.Popen(['git', 'log'], stdout=subprocess.PIPE)
    while True:
        line = proc.stdout.readline()
        if line == '':
            break
        match = commit_regex.search(line)
        if match != None:
            commit_number = match.groups()[0]
            if commit_number == until_commit:
                break
            list.append(commit_number)
    return list

def switch_to_commit(commit_number):
    proc = subprocess.Popen(['git', 'checkout', commit_number])
    proc.wait()
    return proc.returncode == 0

def get_benchmark_list():
    list = []
    proc = subprocess.Popen([benchmark_runner, '--list'], stdout=subprocess.PIPE)
    while True:
        line = proc.stdout.readline()
        if line == '':
            break
        list.append(line.rstrip())
    return list

# get a folder for the new benchmark
# folders have the format ID-commit
def make_benchmark_folder(commit):
    # get all the current folders
    files = os.listdir(benchmark_results_folder)
    biggest_number = 0
    for f in files:
        try:
            number = int(f.split('-')[0])
            if number > biggest_number:
                biggest_number = number
        except:
            pass

    folder_name = os.path.join(benchmark_results_folder, "%04d-%s" % (biggest_number + 1, commit))
    os.mkdir(folder_name)
    return folder_name


class RunBenchmark(object):
    def __init__(self, benchmark):
        self.benchmark = benchmark

    def run(self, timeout):
        def run_benchmark_target():
            self.proc = subprocess.Popen([benchmark_runner, '--out=' + self.out_file, '--log=' + self.log_file, self.benchmark], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.proc.communicate()

        thread = threading.Thread(target=run_benchmark_target)
        thread.start()

        thread.join(timeout)
        if thread.is_alive():
            self.proc.terminate()
            thread.join()
            return 1
        return self.proc.returncode


def run_benchmark(benchmark, folder):
    log("Starting benchmark " + benchmark);
    base_path = os.path.join(folder, benchmark)

    runner = RunBenchmark(benchmark)
    runner.file_name = base_path + ".csv"
    runner.log_name = base_path + ".log"
    runner.stdout_name = base_path + ".stdout.log"
    runner.stderr_name = base_path + ".stderr.log"

    return_code = runner.run(total_timeout)

    if return_code != 0:
        log("Failed to run benchmark " + benchmark);
        # failed to run benchmark
        with open(runner.file_name, 'w+') as f:
            f.write("CRASH")
    else:
        log("Succeeded in running benchmark " + benchmark);
        # succeeded, copy results to output directory
        os.rename(runner.out_file, runner.file_name)
    os.rename(runner.log_file, runner.log_name)
    with open(runner.stdout_name, 'w+') as f:
        f.write(runner.proc.stdout.read())
    with open(runner.stderr_name, 'w+') as f:
        f.write(runner.proc.stderr.read())      

def write_benchmark_info(benchmark, folder):
    file = os.path.join(folder, benchmark + '.log')
    # benchmark, write info
    log("Write benchmark info " + benchmark);
    proc = subprocess.Popen([benchmark_runner, '--info', benchmark], stdout=subprocess.PIPE)
    output = proc.stdout.read()
    print(output)
    with open(file, 'w+') as f:
        f.write(output)


if os.path.exists(last_benchmarked_commit_file):
    with open(last_benchmarked_commit_file, 'r') as f:
        default_start_commit = f.read().rstrip()

pull_new_changes()

# get a list of all commits to benchmark
list = get_list_of_commits(default_start_commit)
list.reverse()

if len(list) == 0:
    exit(1)

# create a folder for the benchmark results, if it doesn't exist yet
try:
    os.mkdir(benchmark_results_folder)
    os.mkdir(benchmark_info_folder)
except:
    pass

for commit in list:
    default_start_commit = commit
    log("Benchmarking commit " + commit)
    # switch to this commit in the source tree
    if not switch_to_commit(commit):
        log("Failed to switch to commit! Moving to next commit")
        continue
    # now try to compile it
    if not build_optimized():
        continue

    # make a benchmark folder for this commit
    benchmark_folder = make_benchmark_folder(commit)
    log("Writing to folder: " + benchmark_folder)

    # now run the benchmarks
    benchmarks_to_run = get_benchmark_list()
    for benchmark in benchmarks_to_run:
        write_benchmark_info(benchmark, benchmark_info_folder)
        run_benchmark(benchmark, benchmark_folder)

    # successfully benchmarked this commit, write to file
    with open(last_benchmarked_commit_file, 'w+') as f:
        f.write(commit)


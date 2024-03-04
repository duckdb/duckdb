import subprocess
import os
import sys
from typing import Literal

# Builds the benchmark runner of the current and the main branch and runs the benchmarks.


DEFAULT_RUNNER_PATH = "build/release/benchmark/benchmark_runner"  # this is whats getting build by default
NEW_RUNNER_PATH = "build/release/benchmark/benchmark_runner_new"  # from local branch
OLD_RUNNER_PATH = "build/release/benchmark/benchmark_runner_old"  # from main branch


def build(stash_changes: bool = False):
    original_branch = get_current_branch()

    # Execute git status with the --porcelain option
    output = subprocess.check_output(['git', 'status', '--porcelain']).decode('utf-8')

    # Filter out lines that start with "??" (untracked files)
    changes = [line for line in output.strip().split('\n') if not line.startswith('??')]
    auto_stashed = False
    if changes and stash_changes:
        print("Stashing changes")
        subprocess.check_output(['git', 'stash'])
        auto_stashed = True
    elif changes:
        print("There are uncommitted changes. Please commit or stash them or use --stash to stash them automatically")
        exit(1)

    # checkout the main branch and build the runner
    subprocess.check_output(['git', 'checkout', 'main'])
    print("Building runner on main branch...")
    build_runner()
    subprocess.check_output(['cp', DEFAULT_RUNNER_PATH, OLD_RUNNER_PATH])

    # checkout the original branch and build the runner
    subprocess.check_output(['git', 'checkout', original_branch])

    if auto_stashed:
        print("Unstashing changes")
        subprocess.check_output(['git', 'stash', 'pop'])

    print(f"Building runner on branch {original_branch}...")
    build_runner()
    subprocess.check_output(['cp', DEFAULT_RUNNER_PATH, NEW_RUNNER_PATH])


def get_current_branch():
    return subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD']).decode('utf-8').strip()


def build_runner():
    # set env variables to
    env = {"BUILD_BENCHMARK": "1", "BUILD_TPCH": "1", "BUILD_HTTPFS": "1"}
    # Add the current environment
    env.update(os.environ)
    subprocess.run(["make"], env=env)


def run_benchmark(old_runner, new_runner, benchmark_file):
    "Expected usage: python3 scripts/regression_test_runner.py --old=/old/benchmark_runner --new=/new/benchmark_runner --benchmarks=/benchmark/list.csv"

    if not os.path.isfile(old_runner):
        print(f"Failed to find old runner {old_runner}")
        exit(1)

    if not os.path.isfile(new_runner):
        print(f"Failed to find new runner {new_runner}")
        exit(1)

    command = [
        'python3',
        'scripts/regression_test_runner.py',
        f'--old={old_runner}',
        f'--new={new_runner}',
        f'--benchmarks={benchmark_file}',
        '--threads=4',
    ]

    print(f"Running command: {' '.join(command)}")

    # start the existing runner, make sure to pipe the output to the console
    subprocess.run(command, check=True)


def main():
    benchmark_file = None
    stash_changes = False
    for arg in sys.argv:
        if arg.startswith("--benchmarks="):
            benchmark_file = arg.replace("--benchmarks=", "")
        elif arg == "--stash":
            stash_changes = True

        elif arg == "--help":
            print("Expected usage: python3 scripts/local_regression_test_runner.py --benchmarks=/benchmark/list.csv")
            print("Optional: --stas: Stash changes before running the benchmarks")
            exit(1)

    # make sure that we are in the root directory of the project
    if not os.path.isfile("scripts/local_regression_test_runner.py"):
        print("Please run this script from the root directory of the project")
        exit(1)

    if benchmark_file is None:
        print(
            "Expected usage: python3 scripts/local_regression_test_runner.py ---benchmarks=.github/regression/imdb.csv"
        )
        exit(1)

    build(stash_changes)
    run_benchmark(OLD_RUNNER_PATH, NEW_RUNNER_PATH, benchmark_file)


if __name__ == "__main__":
    main()

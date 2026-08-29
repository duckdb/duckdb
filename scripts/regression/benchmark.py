import csv
import os
import subprocess
from io import StringIO
from pathlib import Path
from typing import List, Optional, Tuple


DEFAULT_PROCESS_TIMEOUT = 600
DISABLED_RUNNER_TIMEOUT = 3600
EXTENSION_DIRECTORY_ENV = "DUCKDB_BENCHMARK_EXTENSION_DIRECTORY"

STDERR_HEADER = '''====================================================
==============         STDERR          =============
====================================================
'''

STDOUT_HEADER = '''====================================================
==============         STDOUT          =============
====================================================
'''


def find_extension_directory(runner_path: str) -> Optional[str]:
    release_directory = Path(runner_path).resolve().parent.parent
    repository_directory = release_directory / "repository"
    extension_directories = sorted(
        {extension_path.parent for extension_path in repository_directory.glob("*/*/*.duckdb_extension")}
    )
    if not extension_directories:
        return None
    if len(extension_directories) != 1:
        directories = ", ".join(str(path) for path in extension_directories)
        raise ValueError(f"Found multiple extension directories for {runner_path}: {directories}")
    return str(extension_directories[0])


class BenchmarkRunner:
    def __init__(
        self,
        path: str,
        label: str,
        threads: Optional[int] = None,
        memory_limit: Optional[str] = None,
        verbose: bool = False,
        disable_timeout: bool = False,
        benchmark_arguments: Optional[List[Tuple[str, str]]] = None,
    ):
        self.path = path
        self.label = label
        self.threads = threads
        self.memory_limit = memory_limit
        self.verbose = verbose
        self.disable_timeout = disable_timeout
        self.benchmark_arguments = benchmark_arguments or []
        self.extension_directory = find_extension_directory(path)

    def run(self, benchmark: str, timed_runs: int) -> Tuple[Optional[List[float]], Optional[str]]:
        arguments = [self.path, benchmark]
        if self.threads is not None:
            arguments.append(f"--threads={self.threads}")
        if self.memory_limit is not None:
            arguments.append(f"--memory_limit={self.memory_limit}")
        if self.disable_timeout:
            arguments.append("--disable-timeout")
        for name, value in self.benchmark_arguments:
            arguments.extend([f"--{name}", value])
        arguments.extend(["--timed-runs", str(timed_runs)])

        process_timeout = DISABLED_RUNNER_TIMEOUT if self.disable_timeout else DEFAULT_PROCESS_TIMEOUT
        process_environment = os.environ.copy()
        if self.extension_directory:
            process_environment[EXTENSION_DIRECTORY_ENV] = self.extension_directory
        try:
            process = subprocess.run(
                arguments,
                env=process_environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=process_timeout,
                text=True,
                check=False,
            )
        except subprocess.TimeoutExpired:
            message = f"Aborted after exceeding the {process_timeout}-second process limit"
            print(f"Failed to run benchmark {benchmark}: {message}", flush=True)
            return None, message

        if process.returncode != 0:
            print(f"Failed to run benchmark {benchmark}", flush=True)
            print(STDERR_HEADER, flush=True)
            print(process.stderr, flush=True)
            print(STDOUT_HEADER, flush=True)
            print(process.stdout, flush=True)
            return None, process.stderr or process.stdout or f"Benchmark runner exited with code {process.returncode}"

        if self.verbose:
            if os.getenv("CI") == "true":
                print(f"::group::raw output: {self.label} {benchmark}", flush=True)
                print(process.stderr, flush=True)
                print("::endgroup::", flush=True)
            else:
                print(process.stderr, flush=True)

        timings = []
        try:
            rows = csv.reader(StringIO(process.stderr), delimiter='\t')
            next(rows)
            for row in rows:
                if row:
                    timings.append(float(row[2]))
        except (IndexError, StopIteration, ValueError) as exception:
            message = f"Could not parse benchmark timings: {exception}"
            print(f"Failed to run benchmark {benchmark}: {message}", flush=True)
            return None, message

        if len(timings) != timed_runs:
            message = f"Expected {timed_runs} benchmark timings, received {len(timings)}"
            print(f"Failed to run benchmark {benchmark}: {message}", flush=True)
            return None, message
        return timings, None

import csv
import os
import subprocess
from io import StringIO
from typing import List, Optional, Tuple


DEFAULT_PROCESS_TIMEOUT = 600
DISABLED_RUNNER_TIMEOUT = 3600

STDERR_HEADER = '''====================================================
==============         STDERR          =============
====================================================
'''

STDOUT_HEADER = '''====================================================
==============         STDOUT          =============
====================================================
'''


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
        try:
            process = subprocess.run(
                arguments,
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

import subprocess
import statistics
from io import StringIO
import csv
from dataclasses import dataclass
import argparse
from typing import Optional, Union, Tuple, List
import functools

print = functools.partial(print, flush=True)

STDERR_HEADER = '''====================================================
==============         STDERR          =============
====================================================
'''

STDOUT_HEADER = '''====================================================
==============         STDOUT          =============
====================================================
'''

# timeouts in seconds
MAX_TIMEOUT = 3600
DEFAULT_TIMEOUT = 600


@dataclass
class BenchmarkRunnerConfig:
    "Configuration for a BenchmarkRunner"

    benchmark_runner: str
    benchmark_file: str
    verbose: bool = False
    threads: Optional[int] = None
    memory_limit: Optional[str] = None
    disable_timeout: bool = False
    max_timeout: int = MAX_TIMEOUT
    root_dir: str = ""
    no_summary: bool = False

    @classmethod
    def from_params(cls, benchmark_runner, benchmark_file, **kwargs) -> "BenchmarkRunnerConfig":
        verbose = kwargs.get("verbose", False)
        threads = kwargs.get("threads", None)
        memory_limit = kwargs.get("memory_limit", None)
        disable_timeout = kwargs.get("disable_timeout", False)
        max_timeout = kwargs.get("max_timeout", MAX_TIMEOUT)
        root_dir = kwargs.get("root_dir", "")
        no_summary = kwargs.get("no_summary", False)

        config = cls(
            benchmark_runner=benchmark_runner,
            benchmark_file=benchmark_file,
            verbose=verbose,
            threads=threads,
            memory_limit=memory_limit,
            disable_timeout=disable_timeout,
            max_timeout=max_timeout,
            root_dir=root_dir,
            no_summary=no_summary,
        )
        return config

    @classmethod
    def from_args(cls) -> "BenchmarkRunnerConfig":
        parser = argparse.ArgumentParser(description="Benchmark script with old and new runners.")

        # Define the arguments
        parser.add_argument("--path", type=str, help="Path to the benchmark_runner executable", required=True)
        parser.add_argument("--benchmarks", type=str, help="Path to the benchmark file.", required=True)
        parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
        parser.add_argument("--threads", type=int, help="Number of threads to use.")
        parser.add_argument("--memory_limit", type=str, help="Memory limit to use.")
        parser.add_argument("--disable-timeout", action="store_true", help="Disable timeout.")
        parser.add_argument(
            "--max-timeout", type=int, default=3600, help="Set maximum timeout in seconds (default: 3600)."
        )
        parser.add_argument("--root-dir", type=str, default="", help="Root directory.")
        parser.add_argument(
            "--no-summary", type=str, default=False, help="No failures summary is outputed when passing this flag."
        )

        # Parse arguments
        parsed_args = parser.parse_args()

        # Create an instance of BenchmarkRunnerConfig using parsed arguments
        config = cls(
            benchmark_runner=parsed_args.path,
            benchmark_file=parsed_args.benchmarks,
            verbose=parsed_args.verbose,
            threads=parsed_args.threads,
            memory_limit=parsed_args.memory_limit,
            disable_timeout=parsed_args.disable_timeout,
            max_timeout=parsed_args.max_timeout,
            root_dir=parsed_args.root_dir,
            no_summary=parsed_args.no_summary,
        )
        return config


class BenchmarkRunner:
    def __init__(self, config: BenchmarkRunnerConfig):
        self.config = config
        self.complete_timings = []
        self.benchmark_list: List[str] = []
        with open(self.config.benchmark_file, 'r') as f:
            self.benchmark_list = [x.strip() for x in f.read().split('\n') if len(x) > 0]

    def construct_args(self, benchmark_path):
        benchmark_args = []
        benchmark_args.extend([self.config.benchmark_runner, benchmark_path])
        if self.config.root_dir:
            benchmark_args.extend(['--root-dir', self.config.root_dir])
        if self.config.threads:
            benchmark_args.extend([f"--threads={self.config.threads}"])
        if self.config.memory_limit:
            benchmark_args.extend([f"--memory_limit={self.config.memory_limit}"])
        if self.config.disable_timeout:
            benchmark_args.extend(["--disable-timeout"])
        if self.config.no_summary:
            benchmark_args.extend(["--no-summary"])
        return benchmark_args

    def run_benchmark(self, benchmark) -> Tuple[Union[float, str], Optional[str]]:
        benchmark_args = self.construct_args(benchmark)
        timeout_seconds = DEFAULT_TIMEOUT
        if self.config.disable_timeout:
            timeout_seconds = self.config.max_timeout

        try:
            proc = subprocess.run(
                benchmark_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout_seconds
            )
            out = proc.stdout.decode('utf8')
            err = proc.stderr.decode('utf8')
            returncode = proc.returncode
        except subprocess.TimeoutExpired:
            print("Failed to run benchmark " + benchmark)
            print(f"Aborted due to exceeding the limit of {timeout_seconds} seconds")
            return (
                'Failed to run benchmark ' + benchmark,
                f"Aborted due to exceeding the limit of {timeout_seconds} seconds",
            )
        if returncode != 0:
            print("Failed to run benchmark " + benchmark)
            print(STDERR_HEADER)
            print(err)
            print(STDOUT_HEADER)
            print(out)
            if 'HTTP' in err:
                print("Ignoring HTTP error and terminating the running of the regression tests")
                exit(0)
            return 'Failed to run benchmark ' + benchmark, err
        if self.config.verbose:
            print(err)
        # read the input CSV
        f = StringIO(err)
        csv_reader = csv.reader(f, delimiter='\t')
        header = True
        timings = []
        try:
            for row in csv_reader:
                if len(row) == 0:
                    continue
                if header:
                    header = False
                else:
                    timings.append(row[2])
                    self.complete_timings.append(row[2])
            return float(statistics.median(timings)), None
        except:
            print("Failed to run benchmark " + benchmark)
            print(err)
            return 'Failed to run benchmark ' + benchmark, err

    def run_benchmarks(self, benchmark_list: List[str]):
        results = {}
        failures = {}
        for benchmark in benchmark_list:
            result, failure_message = self.run_benchmark(benchmark)
            results[benchmark] = result
            failures[benchmark] = failure_message if failure_message else None
        return results, failures


def main():
    config = BenchmarkRunnerConfig.from_args()
    runner = BenchmarkRunner(config)
    runner.run_benchmarks()


if __name__ == "__main__":
    main()

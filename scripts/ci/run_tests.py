#!/usr/bin/env python3
import argparse
import concurrent.futures
import contextlib
import os
import shlex
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

DEFAULT_BATCH_SIZE = 10
DEFAULT_BATCH_TIMEOUT_SECONDS = 600
DEFAULT_RSS_MEMORY_THRESHOLD_MIB = 1024
DEFAULT_RUNTIME_THRESHOLD_SECONDS = 10
DEFAULT_RSS_POLL_INTERVAL_SECONDS = 0.05
# Leave some CPU headroom so parallel test execution does not fully saturate CI runners.
DEFAULT_WORKERS = max(1, int((os.cpu_count() or 1) * 0.5))
MAX_RETRIES = 3


def enable_line_buffering():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(line_buffering=True)


@dataclass(frozen=True)
class TestRunnerConfig:
    test_list: Path
    unittest_bin: str
    pattern: str
    test_command: str
    workers: int
    retry: int
    batch_size: int
    batch_timeout_seconds: int
    rss_memory_threshold_mib: int | None
    runtime_threshold_seconds: int | None
    max_failures: int | None


class BatchRunState:
    def __init__(self):
        self.failed_count = 0
        self.retry_count = 0
        self.stop_launching = False

    def record_retry(self):
        self.retry_count += 1

    def record_failure(self):
        self.failed_count += 1

    def can_retry(self, batch_info, config: TestRunnerConfig):
        return batch_info["attempt"] < config.retry and self.retry_count < MAX_RETRIES

    def should_stop(self, config: TestRunnerConfig):
        return self.stop_launching or (
            config.max_failures is not None and self.failed_count >= config.max_failures
        )


def chunked(items, n):
    # Keep input order, cap batches at n entries, and isolate .test_slow
    # files so each batch contains at most one slow test.
    batch = []
    slow_count = 0
    for item in items:
        item_is_slow = item.endswith(".test_slow")
        if batch and (len(batch) >= n or (item_is_slow and slow_count >= 1)):
            yield batch
            batch = []
            slow_count = 0
        batch.append(item)
        if item_is_slow:
            slow_count += 1
    if batch:
        yield batch


def compute_batch_size(test_count: int, config: TestRunnerConfig):
    if test_count == 0:
        return 1
    return max(1, min(config.batch_size, (test_count + config.workers - 1) // config.workers))


def load_tests(path: Path):
    tests = []
    with path.open("r", encoding="utf8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            tests.append(line)
    return tests


def build_test_command(config: TestRunnerConfig, test_list: str):
    return config.test_command.format(
        binary=shlex.quote(config.unittest_bin),
        test_list=test_list,
    )


def get_process_rss_bytes(pid: int):
    if sys.platform.startswith("linux"):
        try:
            with open(f"/proc/{pid}/status", "r", encoding="utf8") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        fields = line.split()
                        return int(fields[1]) * 1024
        except (FileNotFoundError, ProcessLookupError, ValueError):
            return None
        return None

    if sys.platform == "darwin":
        try:
            proc = subprocess.run(
                ["ps", "-o", "rss=", "-p", str(pid)],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
            )
        except OSError:
            return None

        if proc.returncode != 0:
            return None

        output = proc.stdout.strip()
        if not output:
            return None

        try:
            return int(output) * 1024
        except ValueError:
            return None

    return None


def format_mib(value_bytes: int):
    return value_bytes / (1024 * 1024)


def generate_test_list(test_file, unittest_bin: str, pattern: str):
    # Catch returns the number of matching tests from --list-test-names-only,
    # so a non-zero exit code here is expected when tests are found.
    command = [unittest_bin, "--list-test-names-only", pattern]
    print(f"generated test list using: {shlex.join(command)}")
    proc = subprocess.run(
        command,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.stderr:
        if proc.stdout:
            print(proc.stdout, end="")
        print(proc.stderr, end="", file=sys.stderr)
        raise RuntimeError(f"failed to generate test list from {unittest_bin}")
    test_file.write(proc.stdout)
    test_file.flush()


@contextlib.contextmanager
def open_test_list(test_list: Path | None, unittest_bin: str, pattern: str):
    if test_list is not None:
        with test_list.open("r", encoding="utf8") as test_file:
            yield Path(test_file.name)
        return

    with tempfile.NamedTemporaryFile(mode="w", encoding="utf8", delete=True) as test_file:
        generate_test_list(test_file, unittest_bin, pattern)
        yield Path(test_file.name)


def format_batch_failure(
    batch_idx: int,
    batch,
    config: TestRunnerConfig,
    stdout: str,
    stderr: str,
    message: str | None = None,
):
    rerun_cmd = (
        "printf '%s\\n' "
        + " ".join(shlex.quote(test) for test in batch)
        + " > /tmp/duckdb_test_batch.txt && "
        + build_test_command(config, "/tmp/duckdb_test_batch.txt")
    )
    parts = [f"### failed test batch {batch_idx} ###", ""]
    if message is not None:
        parts.extend([message, ""])
    parts.extend(
        [
            "=== stdout ===",
            stdout.strip(),
            "",
            "=== stderr ===",
            stderr.strip(),
            "",
            "=== reproduce ===",
            rerun_cmd,
            "",
        ]
    )
    return "\n".join(parts)


def normalize_output(output):
    if isinstance(output, bytes):
        return output.decode("utf8", errors="replace")
    return output or ""


def run_batch(config: TestRunnerConfig, batch):
    failed = False
    stdout = ""
    stderr = ""
    message = None
    peak_rss_bytes = 0

    with tempfile.NamedTemporaryFile(mode="w", encoding="utf8", delete=True) as batch_file:
        batch_file.write("\n".join(batch))
        batch_file.write("\n")
        batch_file.flush()
        command = build_test_command(config, shlex.quote(batch_file.name))
        try:
            proc = subprocess.Popen(
                shlex.split(command),
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            deadline = time.monotonic() + config.batch_timeout_seconds

            while proc.poll() is None:
                rss_bytes = get_process_rss_bytes(proc.pid)
                if rss_bytes is not None:
                    peak_rss_bytes = max(peak_rss_bytes, rss_bytes)
                if time.monotonic() >= deadline:
                    proc.kill()
                    stdout, stderr = proc.communicate()
                    stdout = normalize_output(stdout)
                    stderr = normalize_output(stderr)
                    failed = True
                    message = f"batch timed out after {config.batch_timeout_seconds} seconds"
                    break
                if proc.poll() is None:
                    time.sleep(DEFAULT_RSS_POLL_INTERVAL_SECONDS)

            if message is None:
                stdout, stderr = proc.communicate()
                stdout = normalize_output(stdout)
                stderr = normalize_output(stderr)
                rss_bytes = get_process_rss_bytes(proc.pid)
                if rss_bytes is not None:
                    peak_rss_bytes = max(peak_rss_bytes, rss_bytes)
                failed = proc.returncode != 0
        except OSError as exc:
            failed = True
            stderr = str(exc)
            message = "failed to launch batch command"

    return {
        "failed": failed,
        "stdout": stdout,
        "stderr": stderr,
        "message": message,
        "peak_rss_bytes": peak_rss_bytes,
    }


def submit_batch(executor, config: TestRunnerConfig, batch, future_to_batch, batch_idx: int, attempt: int):
    future = executor.submit(run_batch, config, batch)
    future_to_batch[future] = {
        "batch_idx": batch_idx,
        "batch": batch,
        "start": time.monotonic(),
        "attempt": attempt,
    }


def submit_batches(executor, config: TestRunnerConfig, batches, future_to_batch, next_batch_idx: int):
    while next_batch_idx < len(batches) and len(future_to_batch) < config.workers:
        batch = batches[next_batch_idx]
        submit_batch(executor, config, batch, future_to_batch, next_batch_idx, 0)
        next_batch_idx += 1
    return next_batch_idx


def handle_failed_batch(executor, config: TestRunnerConfig, state: BatchRunState, future_to_batch, batch_info, result):
    print(
        format_batch_failure(
            batch_info["batch_idx"],
            batch_info["batch"],
            config,
            result["stdout"],
            result["stderr"],
            result["message"],
        ),
        end="",
    )
    print("========================")
    if state.can_retry(batch_info, config):
        state.record_retry()
        next_attempt = batch_info["attempt"] + 1
        print(
            f"retrying failed test batch {batch_info['batch_idx']} "
            f"(attempt {next_attempt}/{config.retry}, retry {state.retry_count}/{MAX_RETRIES})"
        )
        submit_batch(
            executor,
            config,
            batch_info["batch"],
            future_to_batch,
            batch_info["batch_idx"],
            next_attempt,
        )
        return True

    if batch_info["attempt"] < config.retry:
        print(f"stopping after reaching {MAX_RETRIES} retries")
        state.stop_launching = True

    state.record_failure()
    if state.should_stop(config):
        state.stop_launching = True
    return False


def report_batch_metrics(config: TestRunnerConfig, batch_info, result, elapsed: float):
    if config.runtime_threshold_seconds is not None and elapsed >= config.runtime_threshold_seconds:
        print(f"{batch_info['batch'][0]} took {elapsed:.2f}s")
    if (
        config.rss_memory_threshold_mib is not None
        and format_mib(result["peak_rss_bytes"]) >= config.rss_memory_threshold_mib
    ):
        print(f"batch with file {batch_info['batch'][0]} peak RSS {format_mib(result['peak_rss_bytes']):.0f} MiB")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test-list", type=Path)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("unittest_bin")
    parser.add_argument("pattern", nargs="?", default="")
    parser.add_argument(
        "--test-command",
        default="{binary} --use-colour yes -f {test_list}",
        help="shell command template used to run a test batch; supports {binary} and {test_list}",
    )
    parser.add_argument(
        "--track-runtime",
        type=int,
        nargs="?",
        const=DEFAULT_RUNTIME_THRESHOLD_SECONDS,
        default=None,
    )
    parser.add_argument(
        "--track-rss-memory",
        type=int,
        nargs="?",
        const=DEFAULT_RSS_MEMORY_THRESHOLD_MIB,
        default=None,
        help="print batches whose peak RSS meets or exceeds the threshold in MiB (default: 1024)",
    )
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument("--max-failures", type=int)
    parser.add_argument("--retry", type=int, default=0)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--batch-timeout", type=int, default=DEFAULT_BATCH_TIMEOUT_SECONDS)
    return parser.parse_args()


def main():
    enable_line_buffering()
    args = parse_args()
    max_failures = args.max_failures
    if args.fail_fast:
        max_failures = 1
    retry = max(0, args.retry)
    if retry == 0 and os.environ.get("CI"):
        retry = 1
        print("CI detected, enabling retry=1")
    batch_size = 1 if args.track_runtime is not None else args.batch_size
    with open_test_list(args.test_list, args.unittest_bin, args.pattern) as test_file:
        config = TestRunnerConfig(
            test_list=test_file,
            unittest_bin=args.unittest_bin,
            pattern=args.pattern,
            test_command=args.test_command,
            workers=max(1, args.workers),
            retry=retry,
            batch_size=batch_size,
            batch_timeout_seconds=args.batch_timeout,
            rss_memory_threshold_mib=args.track_rss_memory,
            runtime_threshold_seconds=args.track_runtime,
            max_failures=max_failures,
        )

        tests = load_tests(config.test_list)
        batch_size = compute_batch_size(len(tests), config)

        print(
            f"found {len(tests)} tests, batch_size={batch_size}, workers={config.workers}, "
            f"retry={config.retry}, "
            f"runtime_threshold={config.runtime_threshold_seconds}s, "
            f"rss_memory_threshold={config.rss_memory_threshold_mib}MiB, "
            f"batch_timeout={config.batch_timeout_seconds}s"
        )

        batches = list(chunked(tests, batch_size))
        return run_tests(config, batches)


def run_tests(config: TestRunnerConfig, batches):
    state = BatchRunState()

    with concurrent.futures.ThreadPoolExecutor(max_workers=config.workers) as executor:
        future_to_batch = {}
        next_batch_idx = 0

        next_batch_idx = submit_batches(executor, config, batches, future_to_batch, next_batch_idx)

        while future_to_batch:
            done, _ = concurrent.futures.wait(
                future_to_batch,
                return_when=concurrent.futures.FIRST_COMPLETED,
            )
            for future in done:
                batch_info = future_to_batch.pop(future)
                result = future.result()
                elapsed = time.monotonic() - batch_info["start"]
                report_batch_metrics(config, batch_info, result, elapsed)
                if result["failed"]:
                    if handle_failed_batch(executor, config, state, future_to_batch, batch_info, result):
                        continue

            if not state.stop_launching:
                next_batch_idx = submit_batches(executor, config, batches, future_to_batch, next_batch_idx)

    if state.failed_count:
        print(f"error: found {state.failed_count} test batch failures")
        return 1

    print("all tests passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

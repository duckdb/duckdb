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
from dataclasses import asdict, dataclass
from pathlib import Path

DEFAULT_BATCH_SIZE = 10
DEFAULT_BATCH_TIMEOUT_SECONDS = 600
DEFAULT_RSS_MEMORY_THRESHOLD_MIB = 1024
DEFAULT_RUNTIME_THRESHOLD_SECONDS = 10
DEFAULT_RSS_POLL_INTERVAL_SECONDS = 0.05
# Leave some CPU headroom so parallel test execution does not fully saturate CI runners.
DEFAULT_WORKERS = "75%"
DEFAULT_MAX_RETRIES = 4


def enable_line_buffering():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True, write_through=True)
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(line_buffering=True, write_through=True)


@dataclass(frozen=True)
class TestRunnerConfig:
    test_list: Path
    unittest_bin: str
    test_flags: str
    patterns: list[str]
    test_command: str
    workers: int
    retry: int
    max_retries: int
    batch_size: int
    batch_timeout_seconds: float
    rss_memory_threshold_mib: int | None
    runtime_threshold_seconds: int | None
    max_failures: int | None


@dataclass(frozen=True)
class TestCase:
    name: str
    is_slow: bool


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
        return batch_info["attempt"] < config.retry and self.retry_count < config.max_retries

    def should_stop(self, config: TestRunnerConfig):
        return self.stop_launching or config.max_failures is not None and self.failed_count >= config.max_failures


class DotProgressBar:
    def __init__(self, total_batches: int):
        self.total_batches = total_batches
        self.printed_dots = 0
        self.row_width = 50
        self._line_open = False

    def _write(self, text: str):
        sys.stdout.write(text)
        sys.stdout.flush()

    def flush_line(self):
        if self._line_open:
            self._write("\n")
            self._line_open = False

    def print_message(self, text: str):
        self.flush_line()
        print(text)

    def advance(self, completed_batches: int):
        if self.total_batches <= 0:
            return
        target_dots = min(100, int((completed_batches * 100) / self.total_batches))
        while self.printed_dots < target_dots:
            self._write(".")
            self.printed_dots += 1
            self._line_open = True
            if self.printed_dots % self.row_width == 0:
                self._write(" [{:3}%]\n".format(self.printed_dots))
                self._line_open = False


@dataclass
class RunContext:
    executor: concurrent.futures.ThreadPoolExecutor
    config: TestRunnerConfig
    state: BatchRunState
    future_to_batch: dict
    progress: DotProgressBar


def chunked(items, n):
    # Keep input order, cap batches at n entries, and isolate slow tests so
    # each batch contains at most one slow test.
    batch = []
    slow_count = 0
    for item in items:
        if batch and (len(batch) >= n or (item.is_slow and slow_count >= 1)):
            yield batch
            batch = []
            slow_count = 0
        batch.append(item.name)
        if item.is_slow:
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
            line = line.rstrip("\n")

            # Skip header row from `--list-tests`.
            if line == "name\tgroup":
                continue

            if "\t" not in line:
                name = line
                is_slow = line.endswith(".test_slow")
            else:
                columns = line.split("\t")
                assert len(columns) == 2, repr(columns)

                name, group = columns
                is_slow = "[.]" in group
                assert not name.endswith(".test_slow") or is_slow, name

            tests.append(TestCase(name=name, is_slow=is_slow))

    return tests


def build_test_command(config: TestRunnerConfig, test_list: str):
    flags = shlex.join(shlex.split(config.test_flags))
    return config.test_command.format(
        binary=shlex.quote(config.unittest_bin),
        flags=flags,
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


def resolve_workers(workers: str):
    cpu_count = os.cpu_count() or 1
    workers = workers.strip()
    if workers.endswith("%"):
        percentage = int(workers[:-1])
        return max(1, int(cpu_count * (percentage / 100.0)))
    return max(1, int(workers))


def generate_test_list(test_file, unittest_bin: str, test_flags: str, patterns: list[str]):
    # Catch can return a non-zero status code for list commands when tests
    # are found, so we accept non-zero if stdout still contains test output.
    command = [unittest_bin, *shlex.split(test_flags), "--list-tests", *patterns]
    print(f"generated test list using: {shlex.join(command)}")
    proc = subprocess.run(
        command,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0 and not proc.stdout:
        print("Stderr:", proc.stderr, end="", file=sys.stderr, flush=True)
        raise RuntimeError(f"failed to generate test list from {unittest_bin} (exit: {proc.returncode})")
    test_file.write(proc.stdout)
    test_file.flush()


@contextlib.contextmanager
def open_test_list(test_list: Path | None, unittest_bin: str, test_flags: str, patterns: list[str]):
    if test_list is not None:
        yield test_list
        return

    with tempfile.NamedTemporaryFile(mode="w", encoding="utf8", delete=False) as test_file:
        generate_test_list(test_file, unittest_bin, test_flags, patterns)
    result = Path(test_file.name)
    yield result
    result.unlink()


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

    with tempfile.NamedTemporaryFile(mode="w", encoding="utf8", delete=False) as batch_file:
        batch_file.write("\n".join(batch))
        batch_file.write("\n")
        batch_file.flush()
        batch_file_path = Path(batch_file.name)

    command = build_test_command(config, shlex.quote(str(batch_file_path)))
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
    finally:
        batch_file_path.unlink(missing_ok=True)

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


def handle_failed_batch(ctx: RunContext, batch_info, result):
    ctx.progress.print_message(
        format_batch_failure(
            batch_info["batch_idx"],
            batch_info["batch"],
            ctx.config,
            result["stdout"],
            result["stderr"],
            result["message"],
        ),
    )
    ctx.progress.print_message("========================")
    if ctx.state.can_retry(batch_info, ctx.config):
        ctx.state.record_retry()
        next_attempt = batch_info["attempt"] + 1
        ctx.progress.print_message(
            f"retrying failed test batch {batch_info['batch_idx']} "
            f"(attempt {next_attempt}/{ctx.config.retry}, retry {ctx.state.retry_count}/{ctx.config.max_retries})"
        )
        submit_batch(
            ctx.executor,
            ctx.config,
            batch_info["batch"],
            ctx.future_to_batch,
            batch_info["batch_idx"],
            next_attempt,
        )
        return True

    if batch_info["attempt"] < ctx.config.retry:
        ctx.progress.print_message(
            f"not retrying failed test batch {batch_info['batch_idx']} after reaching {ctx.config.max_retries} retries"
        )

    ctx.state.record_failure()
    if ctx.state.should_stop(ctx.config):
        ctx.state.stop_launching = True
    return False


def report_batch_metrics(ctx: RunContext, batch_info, result, elapsed: float):
    if ctx.config.runtime_threshold_seconds is not None and elapsed >= ctx.config.runtime_threshold_seconds:
        ctx.progress.print_message(f"{batch_info['batch'][0]} took {elapsed:.2f}s")
    if (
        ctx.config.rss_memory_threshold_mib is not None
        and format_mib(result["peak_rss_bytes"]) >= ctx.config.rss_memory_threshold_mib
    ):
        ctx.progress.print_message(
            f"batch with file {batch_info['batch'][0]} peak RSS {format_mib(result['peak_rss_bytes']):.0f} MiB"
        )


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test-list", type=Path)
    parser.add_argument("--workers", default=DEFAULT_WORKERS)
    parser.add_argument(
        "--test-config",
        action="append",
        default=[],
        help="path to test config; may be passed multiple times and is appended to test flags",
    )
    parser.add_argument(
        "--test-flags",
        default="",
        help="additional flags appended to the unittest binary for listing and execution",
    )
    parser.add_argument("unittest_bin")
    parser.add_argument("patterns", nargs="*")
    parser.add_argument(
        "--test-command",
        default="{binary} {flags} --use-colour yes -f {test_list}",
        help="shell command template used to run a test batch; supports {binary}, {flags}, and {test_list}",
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
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--batch-timeout", type=float, default=DEFAULT_BATCH_TIMEOUT_SECONDS)
    # Accept options interleaved with positional patterns, e.g.:
    #   run_tests.py bin "[tag]" --fail-fast test/sql/foo.test
    return parser.parse_intermixed_args()


def main():
    enable_line_buffering()
    args = parse_args()
    test_flags = args.test_flags
    for config in args.test_config:
        # The unittest binary parses "--test-config" as a separate option + value pair.
        config_flag = f"--test-config {shlex.quote(config)}"
        test_flags = " ".join(flag for flag in [test_flags, config_flag] if flag)
    max_failures = args.max_failures
    if args.fail_fast:
        max_failures = 1
    retry = max(0, args.retry)
    if retry == 0 and os.environ.get("CI"):
        retry = 2
        print("CI detected, enabling retry=2 per batch")
    max_retries = max(0, args.max_retries)
    workers = resolve_workers(args.workers)
    unittest_bin = args.unittest_bin
    if os.name == "nt":
        unittest_bin = unittest_bin.replace("/", "\\")
    if args.track_runtime is not None:
        print("enabling runtime tracking forces batch_size=1")
        batch_size = 1
    else:
        batch_size = args.batch_size
    with open_test_list(args.test_list, unittest_bin, test_flags, args.patterns) as test_file:
        config = TestRunnerConfig(
            test_list=test_file,
            unittest_bin=unittest_bin,
            test_flags=test_flags,
            patterns=args.patterns,
            test_command=args.test_command,
            workers=workers,
            retry=retry,
            max_retries=max_retries,
            batch_size=batch_size,
            batch_timeout_seconds=args.batch_timeout,
            rss_memory_threshold_mib=args.track_rss_memory,
            runtime_threshold_seconds=args.track_runtime,
            max_failures=max_failures,
        )

        tests = load_tests(config.test_list)
        batch_size = compute_batch_size(len(tests), config)

        print(f"found {len(tests)} tests")
        config_values = asdict(config)
        config_values["batch_size"] = batch_size
        config_values.pop("test_list", None)
        config_values.pop("unittest_bin", None)
        config_values.pop("test_command", None)
        config_values = {k: v for k, v in config_values.items() if v is not None and v != "" and v != []}
        config_output = ", ".join(f"{key}={value}" for key, value in config_values.items())
        print(f"config: {config_output}")

        batches = list(chunked(tests, batch_size))
        return run_tests(config, batches)


def run_tests(config: TestRunnerConfig, batches):
    start = time.monotonic()
    state = BatchRunState()
    progress = DotProgressBar(len(batches))

    with concurrent.futures.ThreadPoolExecutor(max_workers=config.workers) as executor:
        future_to_batch = {}
        next_batch_idx = 0
        ctx = RunContext(
            executor=executor,
            config=config,
            state=state,
            future_to_batch=future_to_batch,
            progress=progress,
        )

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
                report_batch_metrics(ctx, batch_info, result, elapsed)
                if result["failed"]:
                    if handle_failed_batch(ctx, batch_info, result):
                        continue
                progress.advance(next_batch_idx - len(future_to_batch))

            if not state.stop_launching:
                next_batch_idx = submit_batches(executor, config, batches, future_to_batch, next_batch_idx)

    progress.flush_line()
    elapsed = time.monotonic() - start
    if state.failed_count:
        print(f"error: found {state.failed_count} test batch failures in {elapsed:.0f}s")
        return 1

    print(f"all tests passed in {elapsed:.0f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

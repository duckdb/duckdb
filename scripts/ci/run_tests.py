#!/usr/bin/env python3
import argparse
import concurrent.futures
import os
import shlex
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

DEFAULT_BATCH_SIZE = 20
DEFAULT_BATCH_TIMEOUT_SECONDS = 300
DEFAULT_RUNTIME_THRESHOLD_SECONDS = 10
DEFAULT_WORKERS = os.cpu_count() or 1


@dataclass(frozen=True)
class TestRunnerConfig:
    test_list: Path
    unittest_bin: str
    test_command: str
    workers: int
    batch_size: int
    batch_timeout_seconds: int
    runtime_threshold_seconds: int | None
    max_failures: int | None


def chunked(items, n):
    for i in range(0, len(items), n):
        yield items[i : i + n]


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


def run_batch(config: TestRunnerConfig, batch):
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf8", delete=True) as batch_file:
        batch_file.write("\n".join(batch))
        batch_file.write("\n")
        batch_file.flush()
        command = build_test_command(config, shlex.quote(batch_file.name))
        try:
            proc = subprocess.run(
                shlex.split(command),
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=config.batch_timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            return {
                "stdout": exc.stdout or "",
                "stderr": exc.stderr or "",
                "message": f"batch timed out after {config.batch_timeout_seconds} seconds",
            }
    if proc.returncode == 0:
        return None

    return {
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "message": None,
    }


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test-list", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("unittest_bin")
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
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument("--max-failures", type=int)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--batch-timeout", type=int, default=DEFAULT_BATCH_TIMEOUT_SECONDS)
    return parser.parse_args()


def main():
    args = parse_args()
    max_failures = args.max_failures
    if args.fail_fast:
        max_failures = 1
    batch_size = 1 if args.track_runtime is not None else args.batch_size
    config = TestRunnerConfig(
        test_list=args.test_list,
        unittest_bin=args.unittest_bin,
        test_command=args.test_command,
        workers=max(1, args.workers),
        batch_size=batch_size,
        batch_timeout_seconds=args.batch_timeout,
        runtime_threshold_seconds=args.track_runtime,
        max_failures=max_failures,
    )

    tests = load_tests(config.test_list)
    batch_size = compute_batch_size(len(tests), config)

    print(
        f"found {len(tests)} tests, batch_size={batch_size}, workers={config.workers}, "
        f"runtime_threshold={config.runtime_threshold_seconds}s, "
        f"batch_timeout={config.batch_timeout_seconds}s"
    )

    batches = list(chunked(tests, batch_size))
    failed_count = 0
    stop_launching = False

    with concurrent.futures.ThreadPoolExecutor(max_workers=config.workers) as executor:
        future_to_batch = {}
        next_batch_idx = 0

        while next_batch_idx < len(batches) and len(future_to_batch) < config.workers:
            batch = batches[next_batch_idx]
            future = executor.submit(run_batch, config, batch)
            future_to_batch[future] = {
                "batch_idx": next_batch_idx,
                "batch": batch,
                "start": time.monotonic(),
            }
            next_batch_idx += 1

        while future_to_batch:
            done, _ = concurrent.futures.wait(
                future_to_batch,
                return_when=concurrent.futures.FIRST_COMPLETED,
            )
            for future in done:
                batch_info = future_to_batch.pop(future)
                result = future.result()
                elapsed = time.monotonic() - batch_info["start"]
                if config.runtime_threshold_seconds is not None and elapsed >= config.runtime_threshold_seconds:
                    print(f"{batch_info['batch'][0]} took {elapsed:.2f}s")
                if result is not None:
                    if config.max_failures is not None and failed_count + 1 >= config.max_failures:
                        stop_launching = True
                    failed_count += 1
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

            while not stop_launching and next_batch_idx < len(batches) and len(future_to_batch) < config.workers:
                batch = batches[next_batch_idx]
                future = executor.submit(
                    run_batch,
                    config,
                    batch,
                )
                future_to_batch[future] = {
                    "batch_idx": next_batch_idx,
                    "batch": batch,
                    "start": time.monotonic(),
                }
                next_batch_idx += 1

    if failed_count:
        print(f"error: found {failed_count} test batch failures")
        return 1

    print("all tests passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
import argparse
import concurrent.futures
import json
import math
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import run_tests

REPO_ROOT = Path(__file__).resolve().parents[2]
RUN_TESTS = REPO_ROOT / "scripts" / "ci" / "run_tests.py"
DEFAULT_BINARY = REPO_ROOT / "build" / "codecov" / "test" / "unittest"
DEFAULT_OUTPUT = REPO_ROOT / "test" / "smoke_tests.list"
DEFAULT_WORKERS = 12
DEFAULT_BUDGET_SECONDS = 30.0
DEFAULT_SIGNIFICANCE_PERCENTAGE_POINTS = 0.1
DEFAULT_TEST_RETRY = 2
RUNTIME_PATTERN = re.compile(r"^(?P<name>.+) took (?P<seconds>\d+(?:\.\d+)?)s$")
TEST_FILE_SUFFIXES = (".test", ".test_slow", ".test_coverage")
LLVM_COV_EXPORT_FLAGS = [
    # Keep llvm-cov output smaller and faster to process. We only need file/region
    # data for product code, not per-function output, expansions, or test sources.
    "--skip-functions",
    "--skip-expansions",
    "--unify-instantiations",
    "--ignore-filename-regex=^(test)/",
]


def parse_args():
    parser = argparse.ArgumentParser(description="Compute test/smoke_tests.list from LLVM source-based coverage")
    parser.add_argument("--make-generator", default="ninja")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--coverage-workers", type=int)
    parser.add_argument("--retry", type=int, default=DEFAULT_TEST_RETRY)
    parser.add_argument("--budget-seconds", type=float, default=DEFAULT_BUDGET_SECONDS)
    parser.add_argument("--significance-pp", type=float, default=DEFAULT_SIGNIFICANCE_PERCENTAGE_POINTS)
    parser.add_argument("--binary", type=Path, default=DEFAULT_BINARY)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--profile-root", type=Path, default=REPO_ROOT / "build" / "codecov" / "smoke_profiles")
    parser.add_argument(
        "--summary", type=Path, default=REPO_ROOT / "build" / "codecov" / "smoke_selection_summary.json"
    )
    return parser.parse_args()


def run_command(command, *, cwd=REPO_ROOT, env=None, capture_output=False):
    display_command = []
    for part in command:
        part_str = str(part)
        try:
            resolved = Path(part_str).resolve()
            part_str = resolved.relative_to(cwd).as_posix()
        except Exception:
            pass
        display_command.append(part_str)
    print(f"+ {subprocess.list2cmdline(display_command)}")
    return subprocess.run(
        [str(part) for part in command],
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE if capture_output else None,
        stderr=subprocess.PIPE if capture_output else None,
        check=False,
    )


def build_codecov_binary(make_generator: str):
    env = os.environ.copy()
    llvm_root = Path("/opt/homebrew/opt/llvm")
    if llvm_root.is_dir():
        env["CMAKE_LLVM_PATH"] = str(llvm_root)
        env["PATH"] = str(llvm_root / "bin") + os.pathsep + env.get("PATH", "")
    result = run_command(["make", "codecov", f"GEN={make_generator}"], env=env, capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(result.stdout + result.stderr)


def ensure_tool(tool_name: str):
    llvm_root = Path("/opt/homebrew/opt/llvm")
    if llvm_root.is_dir():
        candidate = llvm_root / "bin" / tool_name
        if candidate.exists():
            return candidate
    resolved = shutil.which(tool_name)
    if resolved is None:
        raise FileNotFoundError(f"required tool not found: {tool_name}")
    return Path(resolved)


def generate_non_hidden_test_list(binary: Path, output_path: Path):
    with output_path.open("w", encoding="utf8") as output_file:
        run_tests.generate_test_list(output_file, str(binary), "", [])
    tests = run_tests.load_tests(output_path)
    # Keep only sqllogictest-style file entries. The default Catch list also includes
    # compiled C++ tests, but test/smoke_tests.list is a file list.
    test_files = [test.name for test in tests if test.name.endswith(TEST_FILE_SUFFIXES)]
    output_path.write_text("".join(test + "\n" for test in test_files), encoding="utf8")
    return test_files


def parse_runtimes(output: str):
    runtimes = {}
    for line in output.splitlines():
        match = RUNTIME_PATTERN.match(line.strip())
        if not match:
            continue
        runtimes[match.group("name")] = float(match.group("seconds"))
    return runtimes


def normalize_runtimes(runtimes):
    non_zero_runtimes = [runtime for runtime in runtimes.values() if runtime > 0.0]
    if not non_zero_runtimes:
        return runtimes
    startup_overhead = min(non_zero_runtimes)
    normalized = {}
    for test_name, runtime in runtimes.items():
        # Every profiled test is launched in its own subprocess. Subtract the
        # smallest non-zero runtime as a simple estimate of fixed process startup
        # cost so selection is driven more by test work than by launch overhead.
        normalized[test_name] = max(0.0, runtime - startup_overhead)
    return normalized


def run_profiled_tests(binary: Path, test_list: Path, profile_root: Path, retry: int):
    # Start from a clean profile directory on every run so stale .profraw files
    # from an earlier attempt cannot affect the next computation.
    if profile_root.exists():
        shutil.rmtree(profile_root)
    profile_root.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        RUN_TESTS,
        "--batch-size",
        "1",
        "--retry",
        str(retry),
        "--track-runtime",
        "0",
        "--profile-dir",
        profile_root,
        "--test-list",
        test_list,
        binary,
    ]
    result = run_command(command, capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(result.stdout + result.stderr)
    # We only need rough runtime numbers. These come from single-test batches while
    # the runner still executes many tests in parallel.
    runtimes = normalize_runtimes(parse_runtimes(result.stdout))
    return runtimes, result.stdout


def relativize_path(filename: str):
    path = Path(filename)
    if not path.is_absolute():
        return path.as_posix()
    try:
        return path.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def repo_relative(path: Path):
    try:
        return path.resolve().relative_to(REPO_ROOT)
    except ValueError:
        return path


def is_coverable_path(filename: str):
    parts = Path(filename).parts
    return not parts or parts[0] != "test"


def extract_atoms(export_payload, atom_ids):
    covered = set()
    universe = set()
    for data in export_payload.get("data", []):
        for file_data in data.get("files", []):
            filename = relativize_path(file_data["filename"])
            if not is_coverable_path(filename):
                continue
            for segment in file_data.get("segments", []):
                if len(segment) < 5:
                    continue
                line, column, count, has_count, _is_region_entry = segment[:5]
                is_gap_region = bool(segment[5]) if len(segment) > 5 else False
                if not has_count or is_gap_region:
                    continue
                # Use (file, line, column) as a simple coverage atom. This gives us a
                # stable unit for greedy selection without storing full coverage objects.
                atom = (filename, int(line), int(column))
                atom_id = atom_ids.setdefault(atom, len(atom_ids))
                universe.add(atom_id)
                if int(count) > 0:
                    covered.add(atom_id)
    return covered, universe


def load_total_atom_count(binary: Path, llvm_cov: Path):
    atom_ids = {}
    # Export an empty profile first so the significance threshold is based on the
    # full mapped binary, not only on lines reached by the test suite.
    export_result = run_command(
        [
            llvm_cov,
            "export",
            repo_relative(binary),
            "--format=text",
            "--empty-profile",
            *LLVM_COV_EXPORT_FLAGS,
        ],
        capture_output=True,
    )
    if export_result.returncode != 0:
        raise RuntimeError(export_result.stdout + export_result.stderr)
    (REPO_ROOT / "default.profraw").unlink(missing_ok=True)
    _covered, universe = extract_atoms(json.loads(export_result.stdout), atom_ids)
    return len(universe)


def merge_single_test_profile(batch_dir: Path, llvm_profdata: Path, runtime_seconds: float):
    # Merge every test first. This is much cheaper than exporting source coverage
    # for every test, and it gives us a simple signal for shortlisting.
    metadata = json.loads((batch_dir / "meta.json").read_text(encoding="utf8"))
    tests = metadata.get("tests", [])
    if len(tests) != 1:
        return None
    test_name = tests[0]
    profraw_files = sorted(batch_dir.glob("*.profraw"))
    if not profraw_files:
        raise RuntimeError(f"no profraw files found for {test_name} in {batch_dir}")
    profdata_path = batch_dir / "merged.profdata"
    merge_result = run_command(
        [
            llvm_profdata,
            "merge",
            "-sparse",
            "-o",
            repo_relative(profdata_path),
            *[repo_relative(path) for path in profraw_files],
        ],
        capture_output=True,
    )
    if merge_result.returncode != 0:
        raise RuntimeError(merge_result.stdout + merge_result.stderr)
    return {
        "test": test_name,
        "runtime_seconds": runtime_seconds,
        "batch_dir": batch_dir,
        "profdata_path": profdata_path,
        "profdata_bytes": profdata_path.stat().st_size,
    }


def export_single_test_coverage(candidate, binary: Path, llvm_cov: Path):
    atom_ids = {}
    export_result = run_command(
        [
            llvm_cov,
            "export",
            repo_relative(binary),
            f"-instr-profile={repo_relative(candidate['profdata_path'])}",
            "--format=text",
            *LLVM_COV_EXPORT_FLAGS,
        ],
        capture_output=True,
    )
    if export_result.returncode != 0:
        raise RuntimeError(export_result.stdout + export_result.stderr)
    covered, _universe = extract_atoms(json.loads(export_result.stdout), atom_ids)
    # Remove the per-test profile directory once we have extracted the atoms.
    # This keeps disk usage bounded during long runs.
    shutil.rmtree(candidate["batch_dir"])
    return candidate["test"], {"runtime_seconds": candidate["runtime_seconds"], "covered_atoms": covered}


def load_profile_summaries(profile_root: Path, llvm_profdata: Path, runtimes, coverage_workers: int):
    profile_summaries = []
    batch_dirs = sorted(path for path in profile_root.iterdir() if path.is_dir())
    total_batches = len(batch_dirs)
    if total_batches == 0:
        return profile_summaries
    # Post-processing spends a lot of time in Python JSON parsing and set building.
    # Use processes here so that work can run truly in parallel instead of being
    # limited by the GIL.
    with concurrent.futures.ProcessPoolExecutor(max_workers=coverage_workers) as executor:
        future_to_batch = {}
        for batch_dir in batch_dirs:
            metadata = json.loads((batch_dir / "meta.json").read_text(encoding="utf8"))
            tests = metadata.get("tests", [])
            if len(tests) != 1:
                continue
            test_name = tests[0]
            future = executor.submit(
                merge_single_test_profile,
                batch_dir,
                llvm_profdata,
                runtimes[test_name],
            )
            future_to_batch[future] = batch_dir
        completed = 0
        for future in concurrent.futures.as_completed(future_to_batch):
            result = future.result()
            if result is None:
                continue
            profile_summaries.append(result)
            completed += 1
            if completed % 100 == 0 or completed == len(future_to_batch):
                print(f"merged profiles for {completed}/{len(future_to_batch)} tests")
    return profile_summaries


def load_test_coverage(binary: Path, candidates, llvm_cov: Path, coverage_workers: int):
    coverages = {}
    if not candidates:
        return coverages
    with concurrent.futures.ProcessPoolExecutor(max_workers=coverage_workers) as executor:
        future_to_candidate = {}
        for candidate in candidates:
            future = executor.submit(export_single_test_coverage, candidate, binary, llvm_cov)
            future_to_candidate[future] = candidate
        completed = 0
        for future in concurrent.futures.as_completed(future_to_candidate):
            test_name, coverage = future.result()
            coverages[test_name] = coverage
            completed += 1
            if completed % 100 == 0 or completed == len(future_to_candidate):
                print(f"exported coverage for {completed}/{len(future_to_candidate)} tests")
    return coverages


def estimate_makespan(runtimes, workers):
    lanes = [0.0] * workers
    for runtime in sorted(runtimes, reverse=True):
        lightest_idx = min(range(workers), key=lanes.__getitem__)
        lanes[lightest_idx] += runtime
    return max(lanes, default=0.0)


def select_tests(coverages, total_atom_count, budget_seconds, significance_pp, workers):
    significance_threshold = max(1, math.ceil(total_atom_count * (significance_pp / 100.0)))
    selected = []
    selected_set = set()
    covered_atoms = set()
    current_runtimes = []
    while True:
        best_name = None
        best_gain = 0
        best_score = -1.0
        best_runtime = 0.0
        for test_name, info in coverages.items():
            if test_name in selected_set:
                continue
            gain = len(info["covered_atoms"] - covered_atoms)
            # Skip tests that do not move total coverage by at least the requested
            # number of percentage points.
            if gain < significance_threshold:
                continue
            runtime = info["runtime_seconds"]
            projected_makespan = estimate_makespan(current_runtimes + [runtime], workers)
            if projected_makespan > budget_seconds:
                continue
            score = gain / max(runtime, 0.001)
            if score > best_score or (score == best_score and gain > best_gain):
                best_name = test_name
                best_gain = gain
                best_score = score
                best_runtime = runtime
        if best_name is None:
            break
        selected.append(
            {
                "test": best_name,
                "runtime_seconds": best_runtime,
                "marginal_atoms": best_gain,
            }
        )
        selected_set.add(best_name)
        current_runtimes.append(best_runtime)
        covered_atoms.update(coverages[best_name]["covered_atoms"])
    return selected, significance_threshold, covered_atoms


def compute_batch_size(test_count: int, workers: int):
    if test_count == 0:
        return 1
    return max(1, min(10, (test_count + workers - 1) // workers))


def order_selected_tests(selected, workers):
    if not selected:
        return []
    batch_size = compute_batch_size(len(selected), workers)
    batches = []
    # The smoke runner keeps input order when it builds batches, so we write the
    # final file in an order that tries to balance batch runtimes.
    for entry in sorted(selected, key=lambda item: item["runtime_seconds"], reverse=True):
        target_batch = None
        for batch in batches:
            if len(batch["tests"]) >= batch_size:
                continue
            if target_batch is None or batch["runtime_seconds"] < target_batch["runtime_seconds"]:
                target_batch = batch
        if target_batch is None:
            target_batch = {"tests": [], "runtime_seconds": 0.0}
            batches.append(target_batch)
        target_batch["tests"].append(entry["test"])
        target_batch["runtime_seconds"] += entry["runtime_seconds"]
    batches.sort(key=lambda batch: batch["runtime_seconds"], reverse=True)
    ordered = []
    for batch in batches:
        ordered.extend(batch["tests"])
    return ordered


def write_outputs(
    ordered_tests,
    output_path: Path,
    summary_path: Path,
    selected,
    total_atom_count,
    covered_atom_count,
    runtimes,
    workers: int,
):
    output_path.write_text("".join(test + "\n" for test in sorted(ordered_tests)), encoding="utf8")
    summary = {
        "selected_test_count": len(ordered_tests),
        "total_atom_count": total_atom_count,
        "covered_atom_count": covered_atom_count,
        "coverage_fraction": 0.0 if total_atom_count == 0 else covered_atom_count / total_atom_count,
        "estimated_makespan_seconds": estimate_makespan(list(runtimes.values()), workers),
        "selected_tests": selected,
    }
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf8")


def main():
    args = parse_args()
    llvm_profdata = ensure_tool("llvm-profdata")
    llvm_cov = ensure_tool("llvm-cov")
    coverage_workers = args.coverage_workers or min(os.cpu_count() or 1, 8)
    if args.profile_root.exists():
        shutil.rmtree(args.profile_root)
    build_codecov_binary(args.make_generator)
    if not args.binary.exists():
        raise FileNotFoundError(f"codecov binary not found: {args.binary}")

    with tempfile.TemporaryDirectory(prefix="smoke-tests-", dir=args.profile_root.parent) as tmpdir:
        tmpdir_path = Path(tmpdir)
        test_list_path = tmpdir_path / "all_non_hidden_tests.list"
        tests = generate_non_hidden_test_list(args.binary, test_list_path)
        print(f"profiling {len(tests)} non-hidden tests")
        runtimes, runner_output = run_profiled_tests(
            args.binary,
            test_list_path,
            args.profile_root,
            args.retry,
        )
        missing_runtimes = [test for test in tests if test not in runtimes]
        if missing_runtimes:
            raise RuntimeError(f"missing runtime measurements for {len(missing_runtimes)} tests")
        total_atom_count = load_total_atom_count(args.binary, llvm_cov)
        profile_summaries = load_profile_summaries(args.profile_root, llvm_profdata, runtimes, coverage_workers)
        coverages = load_test_coverage(args.binary, profile_summaries, llvm_cov, coverage_workers)
        selected, significance_threshold, covered_atoms = select_tests(
            coverages,
            total_atom_count,
            args.budget_seconds,
            args.significance_pp,
            args.workers,
        )
        ordered_tests = order_selected_tests(selected, args.workers)
        selected_runtimes = {entry["test"]: entry["runtime_seconds"] for entry in selected}
        coverage_percentage = 0.0 if total_atom_count == 0 else (len(covered_atoms) / total_atom_count) * 100.0
        write_outputs(
            ordered_tests,
            args.output,
            args.summary,
            selected,
            total_atom_count,
            len(covered_atoms),
            selected_runtimes,
            args.workers,
        )
        print(runner_output, end="")
        print(f"significance threshold: {significance_threshold} atoms")
        print(f"used llvm-cov export on {len(coverages)} tests")
        print(
            f"selected {len(ordered_tests)} tests covering "
            f"{len(covered_atoms)}/{total_atom_count} atoms ({coverage_percentage:.2f}%)"
        )
        print(f"estimated 12-worker makespan: {estimate_makespan(list(selected_runtimes.values()), args.workers):.2f}s")
        print(f"wrote {args.output}")


if __name__ == "__main__":
    raise SystemExit(main())

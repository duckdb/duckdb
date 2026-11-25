import argparse
import glob
import json
import os
import re
import statistics
import subprocess
import sys

if __package__:
    from .regression.comparison import benchmark_measurement, confirmation_run_count, sampling_batch_sizes
    from .regression.local_extensions import extension_loading_args
else:
    from regression.comparison import benchmark_measurement, confirmation_run_count, sampling_batch_sizes
    from regression.local_extensions import extension_loading_args


DB_NAME = "plan_cost.duckdb"
PROFILE_FILENAME = "duckdb_profile.json"

ENABLE_PROFILING = "PRAGMA enable_profiling=json"
PROFILE_OUTPUT = f"PRAGMA profile_output='{PROFILE_FILENAME}'"

BANNER_SIZE = 52
RETRIES = 2
PROFILE_RUNS = 10
# Overlapping cost samples whose medians differ by more than this are still compared.
MEDIAN_TOLERANCE = 0.05


def run_command(command, **kwargs):
    return subprocess.run(
        command,
        check=False,
        **kwargs,
    )


def run_sql_file(cli, dbname, sql_file, extensions=None, **kwargs):
    command = [cli, *extension_loading_args(cli, extensions or []), dbname]
    with open(sql_file, 'rb') as sql_input:
        return run_command(command, stdin=sql_input, **kwargs)


def init_db(cli, dbname, benchmark_dir, extensions=None):
    print(f"INITIALIZING {dbname} ...")
    attempts = RETRIES + 1
    if os.path.exists(dbname):
        os.remove(dbname)

    for attempt in range(1, attempts + 1):
        completed = run_sql_file(
            cli,
            dbname,
            f"{benchmark_dir}/init/schema.sql",
            stdout=subprocess.DEVNULL,
        )
        if completed.returncode == 0:
            completed = run_sql_file(
                cli,
                dbname,
                f"{benchmark_dir}/init/load.sql",
                extensions,
                stdout=subprocess.DEVNULL,
            )
            if completed.returncode == 0:
                print("INITIALIZATION DONE")
                return

        if attempt < attempts:
            if os.path.exists(dbname):
                os.remove(dbname)
            print(
                f"Retrying failure in init db ({dbname}) "
                f"(attempt {attempt + 1}/{attempts}, return code {completed.returncode})"
            )
            continue

        raise subprocess.CalledProcessError(
            completed.returncode,
            completed.args,
            output=completed.stdout,
            stderr=completed.stderr,
        )


class PlanCost:
    def __init__(self):
        self.total = 0
        self.build_side = 0
        self.probe_side = 0
        self.time = 0

    def __add__(self, other):
        self.total += other.total
        self.build_side += other.build_side
        self.probe_side += other.probe_side
        return self

    def more_output_and_build_work(self, other):
        dominates = (
            self.total > other.total and self.build_side > other.build_side and self.probe_side >= other.probe_side
        )
        # Small work increases still need the hot timing comparison before they count as a regression.
        return dominates and any(new > old * 1.03 for new, old in zip(cost_components(self), cost_components(other)))

    def needs_timing(self, other):
        return (
            self != other and not self.more_output_and_build_work(other) and not other.more_output_and_build_work(self)
        )

    def __gt__(self, other):
        if self == other:
            return False
        total_card_increased = self.total > other.total
        build_card_increased = self.build_side > other.build_side
        probe_card_increased = self.probe_side > other.probe_side
        if not (total_card_increased or build_card_increased or probe_card_increased):
            return False
        if self.more_output_and_build_work(other):
            return True
        # Use timings for small work changes and tradeoffs between output rows and build/probe work.
        return self.time > other.time * 1.03

    def __lt__(self, other):
        return other > self

    def __eq__(self, other):
        return self.total == other.total and self.build_side == other.build_side and self.probe_side == other.probe_side


def get_physical_operator_type(op) -> str:
    # New format uses 'type' for the physical operator type (e.g. "HASH_JOIN")
    if 'type' in op:
        return op['type']
    # Legacy format (legacy_metrics_format=true) uses 'operator_name'
    if 'operator_name' in op:
        return op['operator_name']
    # Old format (pre-metricsrefactor) stored the physical type in 'name'
    return op.get('name', '')


def get_operator_cardinality(op) -> int:
    # New format uses 'intermediate_rows', old format uses 'operator_cardinality'
    if 'intermediate_rows' in op:
        return op['intermediate_rows']
    return op.get('operator_cardinality', 0)


def has_operator_cardinality(op) -> bool:
    return 'intermediate_rows' in op or 'operator_cardinality' in op


def get_root_operator(data) -> dict:
    if 'operator' in data:
        return data['operator'][0]
    # main branch (pre-rename) uses 'operator_info'
    if 'operator_info' in data:
        return data['operator_info'][0]
    return data['children'][0]


def get_query_time(data) -> float:
    if isinstance(data.get('query'), dict):
        return data['query']['total_time']
    if 'latency' in data:
        return data['latency']
    return data['timing']


def is_measured_join(op) -> bool:
    if get_physical_operator_type(op) != 'HASH_JOIN':
        return False
    extra_info = op.get('extra_info', {})
    if 'Join Type' not in extra_info:
        return False
    return True


def op_inspect(op) -> PlanCost:
    cost = PlanCost()
    if is_measured_join(op):
        cost.total = get_operator_cardinality(op)
        if has_operator_cardinality(op['children'][0]):
            cost.probe_side += get_operator_cardinality(op['children'][0])
        if has_operator_cardinality(op['children'][1]):
            cost.build_side += get_operator_cardinality(op['children'][1])

        left_cost = op_inspect(op['children'][0])
        right_cost = op_inspect(op['children'][1])
        cost.probe_side += left_cost.probe_side + right_cost.probe_side
        cost.build_side += left_cost.build_side + right_cost.build_side
        cost.total += left_cost.total + right_cost.total
        return cost

    for child_op in op.get('children', []):
        cost += op_inspect(child_op)

    return cost


def query_plan_cost(cli, dbname, query):
    try:
        command = [cli, '--readonly', dbname, '-c', f"{ENABLE_PROFILING};{PROFILE_OUTPUT};{query}"]
        attempts = RETRIES + 1
        for attempt in range(1, attempts + 1):
            completed = run_command(
                command,
                capture_output=True,
            )
            if completed.returncode == 0:
                break
            if attempt < attempts:
                print(
                    f"Retrying failure in query profiling ({dbname}) "
                    f"(attempt {attempt + 1}/{attempts}, return code {completed.returncode})"
                )
                continue
            raise subprocess.CalledProcessError(
                completed.returncode,
                completed.args,
                output=completed.stdout,
                stderr=completed.stderr,
            )
    except subprocess.CalledProcessError as e:
        print("-------------------------")
        print("--------Failure----------")
        print("-------------------------")
        print(e.stderr.decode('utf8'))
        print("-------------------------")
        print("--------Output----------")
        print("-------------------------")
        print(e.output.decode('utf8'))
        print("-------------------------")
        raise e
    with open(PROFILE_FILENAME, 'r') as file:
        data = json.load(file)
        cost = op_inspect(get_root_operator(data))
        cost.time = get_query_time(data)
        return cost


def cost_components(cost):
    return (cost.total, cost.build_side, cost.probe_side)


def median_cost(costs):
    result = PlanCost()
    result.total, result.build_side, result.probe_side = (
        statistics.median_low(values) for values in zip(*(cost_components(cost) for cost in costs))
    )
    return result


def within_run_variation(old_costs, new_costs):
    for old_values, new_values in zip(
        zip(*(cost_components(cost) for cost in old_costs)), zip(*(cost_components(cost) for cost in new_costs))
    ):
        if max(old_values) < min(new_values) or max(new_values) < min(old_values):
            return False
        old_median, new_median = statistics.median_low(old_values), statistics.median_low(new_values)
        if abs(old_median - new_median) > MEDIAN_TOLERANCE * max(old_median, new_median):
            return False
    return True


def query_plan_costs(old, new, dbname, query, query_name="query"):
    old_costs = [query_plan_cost(old, dbname, query)]
    new_costs = [query_plan_cost(new, dbname, query)]
    if old_costs[0] == new_costs[0]:
        return old_costs[0], new_costs[0]
    # Selectivity-paused optional join filters make scan cardinalities vary between runs of one binary.
    for _ in range(PROFILE_RUNS - 1):
        old_costs.append(query_plan_cost(old, dbname, query))
        new_costs.append(query_plan_cost(new, dbname, query))
    if within_run_variation(old_costs, new_costs):
        # The difference is within the run-to-run variation of both binaries.
        return old_costs[0], old_costs[0]
    print(f"{query_name}: comparing plan cost samples")
    for component, old_values, new_values in zip(
        ("total", "build", "probe"),
        zip(*(cost_components(cost) for cost in old_costs)),
        zip(*(cost_components(cost) for cost in new_costs)),
    ):
        print(
            f"  {component}: old range=[{min(old_values)}, {max(old_values)}], "
            f"median={statistics.median_low(old_values)}; "
            f"new range=[{min(new_values)}, {max(new_values)}], median={statistics.median_low(new_values)}"
        )
    return median_cost(old_costs), median_cost(new_costs)


def query_timings(cli, dbname, query, runs):
    # Warm up in the same process before enabling the timer.
    command = [cli, '--readonly', dbname, '-c', query, '-c', '.timer on 6']
    for _ in range(runs):
        command.extend(['-c', query])
    completed = run_command(command, capture_output=True, text=True, timeout=600)
    if completed.returncode:
        raise RuntimeError(f"Query timing failed for {cli}:\n{completed.stderr}")
    timings = re.findall(r'^Run Time \(s\): real ([0-9.]+)', completed.stdout, re.MULTILINE)
    if len(timings) != runs:
        raise RuntimeError(f"Expected {runs} query timings from {cli}, got {len(timings)}:\n{completed.stderr}")
    return [float(timing) for timing in timings]


def measure_query_timings(old, new, query):
    def measure(runs):
        old_timings = []
        new_timings = []
        for index, batch_size in enumerate(sampling_batch_sizes(runs)):
            variants = [(old, old_timings), (new, new_timings)]
            if index % 2:
                variants.reverse()
            for cli, timings in variants:
                timings.extend(query_timings(cli, DB_NAME, query, batch_size))
            print(
                f"  batch {index + 1} ({'new/old' if index % 2 else 'old/new'}): "
                f"median old={statistics.median(old_timings[-batch_size:]):.6f}s, "
                f"new={statistics.median(new_timings[-batch_size:]):.6f}s"
            )
        return benchmark_measurement(old_timings, new_timings)

    measurement = measure(10)
    if measurement.ratio < 0.97 or measurement.ratio > 1.03:
        measurement = measure(confirmation_run_count(measurement))
    return measurement


def print_banner(text):
    text_len = len(text)
    rest = BANNER_SIZE - text_len - 10
    l_width = int(rest / 2)
    r_width = l_width
    if rest % 2 != 0:
        l_width += 1
    print("")
    print("=" * BANNER_SIZE)
    print("=" * l_width + " " * 5 + text + " " * 5 + "=" * r_width)
    print("=" * BANNER_SIZE)


def print_diffs(diffs):
    for query_name, old_cost, new_cost in diffs:
        print("")
        print("Query:", query_name)
        print("Old total cost:", old_cost.total)
        print("Old build cost:", old_cost.build_side)
        print("Old probe cost:", old_cost.probe_side)
        print("New total cost:", new_cost.total)
        print("New build cost:", new_cost.build_side)
        print("New probe cost:", new_cost.probe_side)


def main():
    parser = argparse.ArgumentParser(description="Plan cost regression test script with old and new versions.")

    parser.add_argument("--old", type=str, help="Path to the old runner.", required=True)
    parser.add_argument("--new", type=str, help="Path to the new runner.", required=True)
    parser.add_argument("--dir", type=str, help="Path to the benchmark directory.", required=True)
    parser.add_argument("--extension", action="append", default=[], help="Extension required to initialize the data.")

    args = parser.parse_args()

    old = args.old
    new = args.new
    benchmark_dir = args.dir

    # Compare both readers on the same persisted data and statistics.
    init_db(old, DB_NAME, benchmark_dir, args.extension)

    improvements = []
    regressions = []

    files = glob.glob(f"{benchmark_dir}/queries/*.sql")
    files.sort()

    print("")
    print("RUNNING BENCHMARK QUERIES")
    for f in files:
        query_name = f.split("/")[-1].replace(".sql", "")

        with open(f, "r") as file:
            query = file.read()

        old_cost, new_cost = query_plan_costs(old, new, DB_NAME, query, query_name)

        if old_cost.needs_timing(new_cost):
            print(f"{query_name}: comparing hot query timings")
            measurement = measure_query_timings(old, new, query)
            old_cost.time = measurement.old_timing
            new_cost.time = measurement.new_timing
            print(
                f"{query_name}: {measurement.runs} hot samples per binary, "
                f"median {old_cost.time:.6f}s -> {new_cost.time:.6f}s"
            )

        if old_cost > new_cost:
            improvements.append((query_name, old_cost, new_cost))
        elif new_cost > old_cost:
            regressions.append((query_name, old_cost, new_cost))

    exit_code = 0
    if improvements:
        print_banner("IMPROVEMENTS DETECTED")
        print_diffs(improvements)
    if regressions:
        exit_code = 1
        print_banner("REGRESSIONS DETECTED")
        print_diffs(regressions)
    if not improvements and not regressions:
        print_banner("NO DIFFERENCES DETECTED")

    os.remove(DB_NAME)
    os.remove(PROFILE_FILENAME)

    exit(exit_code)


if __name__ == "__main__":
    main()

import glob
import json
import os
import subprocess
import sys
from tqdm import tqdm


OLD_DB_NAME = "old.duckdb"
NEW_DB_NAME = "new.duckdb"
PROFILE_FILENAME = "duckdb_profile.json"

ENABLE_PROFILING = "PRAGMA enable_profiling=json"
PROFILE_OUTPUT = f"PRAGMA profile_output='{PROFILE_FILENAME}'"

BANNER_SIZE = 52


def print_usage():
    print(
        f"Expected usage: python3 scripts/{os.path.basename(__file__)} --old=/old/duckdb_cli --new=/new/duckdb_cli --dir=/path/to/benchmark/dir"
    )
    exit(1)


def parse_args():
    old = None
    new = None
    benchmark_dir = None
    for arg in sys.argv[1:]:
        if arg.startswith("--old="):
            old = arg.replace("--old=", "")
        elif arg.startswith("--new="):
            new = arg.replace("--new=", "")
        elif arg.startswith("--dir="):
            benchmark_dir = arg.replace("--dir=", "")
        else:
            print_usage()
    if old == None or new == None or benchmark_dir == None:
        print_usage()
    return old, new, benchmark_dir


def init_db(cli, dbname, benchmark_dir):
    print(f"INITIALIZING {dbname} ...")
    subprocess.run(
        f"{cli} {dbname} < {benchmark_dir}/init/schema.sql", shell=True, check=True, stdout=subprocess.DEVNULL
    )
    subprocess.run(f"{cli} {dbname} < {benchmark_dir}/init/load.sql", shell=True, check=True, stdout=subprocess.DEVNULL)
    print("INITIALIZATION DONE")


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

    def __gt__(self, other):
        if self == other or self.total < other.total:
            return False
        # if the total intermediate cardinalities is greater, also inspect time.
        # it's possible a plan reordering increased cardinalities, but overall execution time
        # was not greatly affected
        total_card_increased = self.total > other.total
        build_card_increased = self.build_side > other.build_side
        if total_card_increased and build_card_increased:
            return True
        # we know the total cardinality is either the same or higher and the build side has not increased
        # in this case fall back to the timing. It's possible that even if the probe side is higher
        # since the tuples are in flight, the plan executes faster
        return self.time > other.time * 1.03

    def __lt__(self, other):
        if self == other:
            return False
        return not (self > other)

    def __eq__(self, other):
        return self.total == other.total and self.build_side == other.build_side and self.probe_side == other.probe_side


def is_measured_join(op) -> bool:
    if 'name' not in op:
        return False
    if op['name'] != 'HASH_JOIN':
        return False
    if 'Join Type' not in op['extra_info']:
        return False
    if op['extra_info']['Join Type'].startswith('MARK'):
        return False
    return True


def op_inspect(op) -> PlanCost:
    cost = PlanCost()
    if 'Query' in op:
        cost.time = op['operator_timing']
    if is_measured_join(op):
        cost.total = op['operator_cardinality']
        if 'operator_cardinality' in op['children'][0]:
            cost.probe_side += op['children'][0]['operator_cardinality']
        if 'operator_cardinality' in op['children'][1]:
            cost.build_side += op['children'][1]['operator_cardinality']

        left_cost = op_inspect(op['children'][0])
        right_cost = op_inspect(op['children'][1])
        cost.probe_side += left_cost.probe_side + right_cost.probe_side
        cost.build_side += left_cost.build_side + right_cost.build_side
        cost.total += left_cost.total + right_cost.total
        return cost

    for child_op in op['children']:
        cost += op_inspect(child_op)

    return cost


def query_plan_cost(cli, dbname, query):
    try:
        subprocess.run(
            f"{cli} --readonly {dbname} -c \"{ENABLE_PROFILING};{PROFILE_OUTPUT};{query}\"",
            shell=True,
            check=True,
            capture_output=True,
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
        return op_inspect(json.load(file))


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
    old, new, benchmark_dir = parse_args()
    init_db(old, OLD_DB_NAME, benchmark_dir)
    init_db(new, NEW_DB_NAME, benchmark_dir)

    improvements = []
    regressions = []

    files = glob.glob(f"{benchmark_dir}/queries/*.sql")
    files.sort()

    print("")
    print("RUNNING BENCHMARK QUERIES")
    for f in tqdm(files):
        query_name = f.split("/")[-1].replace(".sql", "")

        with open(f, "r") as file:
            query = file.read()

        old_cost = query_plan_cost(old, OLD_DB_NAME, query)
        new_cost = query_plan_cost(new, NEW_DB_NAME, query)

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

    os.remove(OLD_DB_NAME)
    os.remove(NEW_DB_NAME)
    os.remove(PROFILE_FILENAME)

    exit(exit_code)


if __name__ == "__main__":
    main()

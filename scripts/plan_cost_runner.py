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
    print(f"Expected usage: python3 scripts/{os.path.basename(__file__)} --old=/old/duckdb_cli --new=/new/duckdb_cli --dir=/path/to/benchmark/dir")
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
    subprocess.run(f"{cli} {dbname} < {benchmark_dir}/init/schema.sql", shell=True, check=True, capture_output=True)
    subprocess.run(f"{cli} {dbname} < {benchmark_dir}/init/load.sql", shell=True, check=True, capture_output=True)
    print("INITIALIZATION DONE")


def op_inspect(op):
    cost = 0
    if op['name'] == 'HASH_JOIN' and not op['extra_info'].startswith('MARK'):
        cost = op['cardinality']
    if 'children' not in op:
        return cost
    for child_op in op['children']:
        cost += op_inspect(child_op)
    return cost


def query_plan_cost(cli, dbname, query):
    subprocess.run(f"{cli} --readonly {dbname} -c \"{ENABLE_PROFILING};{PROFILE_OUTPUT};{query}\"", shell=True, check=True, capture_output=True)
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
        print("Old cost:", old_cost)
        print("New cost:", new_cost)


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

        if new_cost < old_cost:
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

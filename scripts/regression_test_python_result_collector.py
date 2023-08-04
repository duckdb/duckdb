from statistics import mean
import duckdb
import time
import sys

out_file = None

for arg in sys.argv[1:]:
    if arg == "--verbose":
        verbose = True
    elif arg.startswith("--threads="):
        threads = int(arg.replace("--threads=", ""))
    elif arg.startswith("--nruns="):
        nruns = int(arg.replace("--nruns=", ""))
    elif arg.startswith("--out-file="):
        out_file = arg.replace("--out-file=", "")
    else:
        print(f"Unrecognized parameter '{arg}'")
        exit(1)

if out_file is not None:
    f = open(out_file, 'w+')

con = duckdb.connect()

types = {
    'varchar': 10_000_000,
    'time': 1_500_000,
    'bool': 10_000_000,
    'large_enum': 1_500_000,
    'varchar_array': 1_500_000,
    'struct': 1_500_000,
}


def create_tables(con, types):
    for type, table_size in types.items():
        con.execute(
            f"""
            create table {type}_table as select
                {type} from (select * from test_all_types() limit 1 offset 1),
                range({table_size})
        """
        )


result_collectors = {"native": "fetchall", "pandas": "df", "arrow": "arrow", "numpy": "fetchnumpy"}

create_tables(con, types)

for name, result_collector in result_collectors.items():
    # Create the tables needed
    for type in types:
        times = []
        table_name = f'{type}_table'
        for i in range(10):
            rel = con.table(table_name)[type]
            start = time.time()
            res = getattr(rel, result_collector)()
            del res
            end = time.time()
            diff = end - start
            times.append(diff)
            timing = f"{name}_{type}\t{i}\t{diff}"
            if out_file is not None:
                f.write(timing)
                f.write('\n')
            else:
                print(timing)
        average = mean(times)
        print(f"\t{name}\t{type}\taverage:\t{average}")

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

# Every column in 'test_all_types', written out explicitly so future additions don't cause unexpected breakages
columns = [
    'bool',
    'tinyint',
    'smallint',
    'int',
    'bigint',
    'hugeint',
    'utinyint',
    'usmallint',
    'uint',
    'ubigint',
    'date',
    'time',
    'timestamp',
    'timestamp_s',
    'timestamp_ms',
    'timestamp_ns',
    'time_tz',
    'timestamp_tz',
    'float',
    'double',
    'dec_4_1',
    'dec_9_4',
    'dec_18_6',
    'dec38_10',
    'uuid',
    'interval',
    'varchar',
    'blob',
    'bit',
    'small_enum',
    'medium_enum',
    'large_enum',
    'int_array',
    'double_array',
    'date_array',
    'timestamp_array',
    'timestamptz_array',
    'varchar_array',
    'nested_int_array',
    'struct',
    'struct_of_arrays',
    'array_of_structs',
    'map',
]

# These don't convert correctly (yet) so we exclude them from this test
excluded = [
    'date',
    'timestamp_s',
    'timestamp_ns',
    'timestamp_ms',
    'timestamp_tz',
    'timestamp',
    'date_array',
    'timestamp_array',
    'timestamptz_array',
]
columns = [col for col in columns if col not in excluded]

# These types are more complex, and that's why the result collection times are longer for them
complex_types = [col for col in columns if any([x in col for x in ['array', 'map', 'struct', 'uuid']])]

test_tables = {
    'regular_types': ([x for x in columns if x not in complex_types], 10_000_000),
    'complex_types': (complex_types, 1_500_000),
}


for table_name, data in test_tables.items():
    list_of_columns, table_size = data
    projection = ", ".join(list_of_columns)
    con.execute(
        f"""
		create table {table_name} as select
			{projection} from (select * from test_all_types() limit 1 offset 1),
			range({table_size})
	"""
    )


result_collectors = {"native": "fetchall", "pandas": "df", "arrow": "arrow", "numpy": "fetchnumpy"}

for name, result_collector in result_collectors.items():
    for table_name, data in test_tables.items():
        list_of_columns, _ = data
        print(table_name)
        print(list_of_columns)
        for col in list_of_columns:
            times = []
            for i in range(10):
                rel = con.table(table_name)[col]
                start = time.time()
                res = getattr(rel, result_collector)()
                del res
                end = time.time()
                diff = end - start
                times.append(diff)
                timing = f"{name}_{col}\t{i}\t{diff}"
                if out_file is not None:
                    f.write(timing)
                    f.write('\n')
                else:
                    print(timing)
            average = mean(times)
            print(f"\t{name}\t{col}\taverage:\t{average}")

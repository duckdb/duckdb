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
	'map'
]

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

projection = ", ".join(columns)

con.execute(f"""
	create table tbl as select
		{projection} from (select * from test_all_types() limit 1 offset 1),
		range(1500000)
""")

result_collectors = {
	"native": "fetchall",
	"pandas": "df",
	"arrow": "arrow",
	"numpy": "fetchnumpy"
}

for name, result_collector in result_collectors.items():
	for col in columns:
		times = []
		for i in range(10):
			rel = con.table('tbl')[col]
			start = time.time()
			res = getattr(rel, result_collector)()
			del(res)
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

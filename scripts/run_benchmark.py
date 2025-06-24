import argparse
import os
import subprocess
import re

parser = argparse.ArgumentParser(description='Run a full benchmark using the CLI and report the results.')
parser.add_argument('--shell', action='store', help='Path to the CLI', default='build/reldebug/duckdb')
parser.add_argument('--database', action='store', help='Path to the database file to load data from')
parser.add_argument(
    '--queries', action='store', help='Path to the list of queries to run (e.g. benchmark/clickbench/queries)'
)
parser.add_argument('--nrun', action='store', help='The number of runs', default=3)

args = parser.parse_args()

queries = os.listdir(args.queries)
queries.sort()
ran_queries = []
timings = []
for q in queries:
    if 'load.sql' in q:
        continue
    command = [args.shell, args.database]
    command += ['-c', '.timer on']
    for i in range(args.nrun):
        command += ['-c', '.read ' + os.path.join(args.queries, q)]
    res = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    stdout = res.stdout.decode('utf8').strip()
    stderr = res.stderr.decode('utf8').strip()
    results = re.findall(r'Run Time \(s\): real (\d+.\d+)', stdout)
    if res.returncode != 0 or 'Error:\n' in stderr or len(results) != args.nrun:
        print("------- Failed to run query -------")
        print(q)
        print("------- stdout -------")
        print(stdout)
        print("------- stderr -------")
        print(stderr)
        exit(1)
    results = [float(x) for x in results]
    print(f"Timings for {q}: " + str(results))
    ran_queries.append(q)
    timings.append(results[1])

print('')
sql_query = 'SELECT UNNEST(['
sql_query += ','.join(["'" + x + "'" for x in ran_queries]) + ']) as query'
sql_query += ","
sql_query += "UNNEST(["
sql_query += ','.join([str(x) for x in timings])
sql_query += "]) as timing;"
print(sql_query)

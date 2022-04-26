import re
import subprocess
import time
import os


get_reduced_query = '''
SELECT * FROM reduce_sql_statement('${QUERY}');
'''


def sanitize_error(err):
    err = re.sub('Error: near line \d+: ', '', err)
    err = err.replace(os.getcwd() + '/', '')
    err = err.replace(os.getcwd(), '')
    return err

def run_shell_command(shell, cmd):
    command = [shell, '-csv', '--batch', '-init', '/dev/null']

    res = subprocess.run(command, input=bytearray(cmd, 'utf8'), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout = res.stdout.decode('utf8').strip()
    stderr = res.stderr.decode('utf8').strip()
    return (stdout, stderr, res.returncode)

def get_reduced_sql(shell, sql_query):
    reduce_query = get_reduced_query.replace('${QUERY}', sql_query.replace("'", "''"))
    (stdout, stderr, returncode) = run_shell_command(shell, reduce_query)
    if returncode != 0:
        print(stdout)
        print(stderr)
        raise Exception("Failed to reduce query")
    reduce_candidates = []
    for line in stdout.split('\n'):
        reduce_candidates.append(line.strip('"').replace('""', '"'))
    return reduce_candidates

def reduce(sql_query, data_load, shell, error_msg, max_time_seconds=300):
    start = time.time()
    while True:
        found_new_candidate = False
        reduce_candidates = get_reduced_sql(shell, sql_query)
        for reduce_candidate in reduce_candidates:
            if reduce_candidate == sql_query:
                continue
            current_time = time.time()
            if current_time - start > max_time_seconds:
                break

            (stdout, stderr, returncode) = run_shell_command(shell, data_load + reduce_candidate)
            new_error = sanitize_error(stderr)
            if new_error == error_msg:
                sql_query = reduce_candidate
                found_new_candidate = True
                print("Found new reduced query")
                print("=======================")
                print(sql_query)
                print("=======================")
                break
        if not found_new_candidate:
            break
    return sql_query

# Example usage:
# error_msg = 'INTERNAL Error: Assertion triggered in file "/Users/myth/Programs/duckdb-bugfix/src/common/types/data_chunk.cpp" on line 41: !types.empty()'
# shell = 'build/debug/duckdb'
# data_load = 'create table all_types as select * from test_all_types();'
# sql_query = '''
# select
#   subq_0.c0 as c0,
#   contains(
#     cast(cast(nullif(
#         argmax(
#           cast(case when 0 then (select varchar from main.all_types limit 1 offset 5)
#                else (select varchar from main.all_types limit 1 offset 5)
#                end
#              as varchar),
#           cast(decode(
#             cast(cast(null as blob) as blob)) as varchar)) over (partition by subq_0.c1 order by subq_0.c1),
#       current_schema()) as varchar) as varchar),
#     cast(cast(nullif(cast(null as varchar),
#       cast(null as varchar)) as varchar) as varchar)) as c1,
#   (select min(time) from main.all_types)
#      as c2,
#   subq_0.c1 as c3,
#   subq_0.c1 as c4,
#   cast(nullif(subq_0.c1,
#     subq_0.c1) as decimal(4,1)) as c5
# from
#   (select
#         ref_0.timestamp_ns as c0,
#         case when (EXISTS (
#               select
#                   ref_0.timestamp_ns as c0,
#                   ref_0.timestamp_ns as c1,
#                   (select timestamp_tz from main.all_types limit 1 offset 4)
#                      as c2,
#                   ref_1.int_array as c3,
#                   ref_1.dec_4_1 as c4,
#                   ref_0.utinyint as c5,
#                   ref_1.int as c6,
#                   ref_0.double as c7,
#                   ref_0.medium_enum as c8,
#                   ref_1.array_of_structs as c9,
#                   ref_1.varchar as c10
#                 from
#                   main.all_types as ref_1
#                 where ref_1.varchar ~~~ ref_1.varchar
#                 limit 28))
#             or (ref_0.varchar ~~~ ref_0.varchar) then ref_0.dec_4_1 else ref_0.dec_4_1 end
#            as c1
#       from
#         main.all_types as ref_0
#       where (0)
#         and (ref_0.varchar ~~ ref_0.varchar)) as subq_0
# where writefile() !~~* writefile()
# limit 88
# '''
#
# print(reduce(sql_query, data_load, shell, error_msg))
import os
from subprocess import Popen, PIPE
import subprocess
import duckdb
import re
from timeit import default_timer as timer
import time

"""
This script will run all the benchmark files in the 'checkpoints' folder. It retrieves the checkpointing times
and the size of the resulting database file and outputs this to a .csv.

The benchmark files are named *_nosorting.benchmark if the PRAGMA force_compression_sorting=false
and *_sorting.benchmark if the PRAGMA force_compression_sorting=true
"""

checkpoints_dir = 'checkpoints'
duckdb_root_dir = 'duckdb'


def write_to_csv(line, mode, time):
    with open(f"results/benchmark-results-{time}.csv", mode) as csv_file:
        csv_file.write(line)
        csv_file.write('\n')


def run_benchmark_file(file, subset):
    # Run the benchmark runner
    benchmark_path = f"benchmark/publicbi/checkpoints/{subset}/{file}"
    benchmark_run = subprocess.run([f"{duckdb_root_dir}/build/release/benchmark/benchmark_runner",
                                    benchmark_path], capture_output=True)

    # Parse the output
    benchmark_run = benchmark_run.stderr.decode('utf-8')
    result_list = re.split('[\t \n]', benchmark_run)
    run_result_indexes = [5, 8, 11, 14, 17]
    result_list = [float(result_list[i]) for i in run_result_indexes]

    # Compute average checkpointing time
    average_run_time = sum(result_list)/len(result_list)
    return average_run_time


def run_checkpoint(subdir, subset, rle_sorting):
    if os.path.exists(f'{subdir}/{subset}.db'):
        os.remove(f'{subdir}/{subset}.db')

    # Create DB file
    db = f'{subdir}/{subset}.db'
    duckdb_conn = duckdb.connect(database=db)

    # Set sorting PRAGMA and force compression
    # duckdb_conn.execute("pragma force_compression='rle'")
    duckdb_conn.execute(f"pragma force_compression_sorting='{rle_sorting}'")

    # Load in the tables and checkpoint
    duckdb_conn.execute(open(os.path.join(f"{duckdb_root_dir}/benchmark/publicbi/data/{subset}/", "load.sql"), "r").read())
    duckdb_conn.execute("CHECKPOINT;")

    # Get amount of used blocks
    total_blocks = duckdb_conn.execute(f"select used_blocks from pragma_database_size();").fetchone()

    # Close to checkpoint and get size
    duckdb_conn.close()
    file_size_disk = os.path.getsize(db)
    os.remove(db)
    return total_blocks, file_size_disk


def run_checkpoint_cli(subdir, subset, rle_sorting):
    if os.path.exists(f'{subdir}/{subset}.db'):
        os.remove(f'{subdir}/{subset}.db')

    db = f'{subdir}/{subset}.db'
    load_file = open(os.path.join(f"{duckdb_root_dir}/benchmark/publicbi/data/{subset}/", "load.sql"), "r").read()

    commands = f""".open {db}\n
PRAGMA wal_autocheckpoint='1TB';\n
pragma force_compression_sorting='{rle_sorting}';\n
{load_file}\n
CHECKPOINT;\n
.quit\n
    """

    subprocess.run([f'{duckdb_root_dir}/build/release/duckdb'], shell=True, input=bytes(commands, encoding='utf8'))

    file_size_disk = os.path.getsize(db)
    total_blocks = 0
    os.remove(db)
    return total_blocks, file_size_disk


def run_benchmark():
    # Prepare results file
    if os.path.exists(f'benchmark-results.csv'):
        os.remove(f'benchmark-results.csv')
    timestr = time.strftime("%Y%m%d-%H%M%S")
    write_to_csv("subset, time_nosort, block_size_nosort, filesize_nosort, time_sort, block_size_sort, filesize_sort, size_change", mode="a", time=timestr)

    # Loop through directories to look for the benchmark files
    for subdir, dirs, files in os.walk(checkpoints_dir):
        for file in files:
            subset = os.path.split(subdir)[1]
            try:
                if file.endswith('nosorting.benchmark'):
                    print(f"Running {file}")
                    # Checkpoint to a file and check size
                    start_nosort = timer()
                    print(f"Running {subset} checkpoint - no sorting")
                    block_size_nosort, file_size_disk_nosort = run_checkpoint(subdir, subset, rle_sorting='false')
                    end_nosort=timer()

                    start_sort = timer()
                    print(f"Running {subset} checkpoint - sorting")
                    block_size_sort, file_size_disk_sort = run_checkpoint(subdir, subset, rle_sorting='true')
                    end_sort=timer()

                    # Calculate average improvement
                    average_improvement = (file_size_disk_nosort - file_size_disk_sort)/file_size_disk_nosort*100

                    print("Writing to file")
                    write_to_csv(f"{subset}, {end_nosort-start_nosort}, {block_size_nosort},{file_size_disk_nosort}, {end_sort-start_sort}, {block_size_sort},{file_size_disk_sort}, {average_improvement}", mode="a", time=timestr)
            except BaseException as err:
                print("Error occured in", subset, "error:", err)


run_benchmark()

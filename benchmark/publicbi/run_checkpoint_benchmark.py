import os
import subprocess
import duckdb
import re

"""
This script will run all the benchmark files in the 'checkpoints' folder. It retrieves the checkpointing times
and the size of the resulting database file and outputs this to a .csv.

The benchmark files are named *_nosorting.benchmark if the PRAGMA force_compression_sorting=false
and *_sorting.benchmark if the PRAGMA force_compression_sorting=true
"""

checkpoints_dir = 'checkpoints'
duckdb_root_dir = 'duckdb'


def write_to_csv(line, mode):
    with open("benchmark-results.csv", mode) as csv_file:
        csv_file.write(line)
        csv_file.write('\n')


def run_benchmark_file(file, subset):
    # Run the benchmark runner
    benchmark_path = f"benchmark/publicbi/checkpoints/{subset}/{file}"
    benchmark_run = subprocess.run([f"{duckdb_root_dir}/release/build/benchmark/benchmark_runner",
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

    # Set sorting PRAGMA and load data
    duckdb_conn.execute(f"pragma force_compression_sorting='{rle_sorting}'")
    duckdb_conn.execute(open(os.path.join(f"{duckdb_root_dir}/benchmark/publicbi/data/{subset}/", "load.sql"), "r").read())

    # Close to checkpoint and get size
    duckdb_conn.close()
    return os.path.getsize(db)


def run_benchmark():
    # Prepare results file
    if os.path.exists(f'benchmark-results.csv'):
        os.remove(f'benchmark-results.csv')
    write_to_csv("subset, average_checkpointing_time, file_size, RLESort", mode="w")

    # Loop through directories to look for the benchmark files
    for subdir, dirs, files in os.walk(checkpoints_dir):
        for file in files:
            # Check if we apply sorting or not
            if file.endswith('sorting.benchmark'):
                subset = os.path.split(subdir)[1]
                # Run the benchmark file to measure the checkpointing speed
                average_run_time = run_benchmark_file(file, subset)
                # # Load the data again but checkpoint to a file and check size
                file_size = run_checkpoint(subdir, subset, rle_sorting='true')

                write_to_csv(f"{subset}, {average_run_time}, {file_size}, true", mode="a")

            if file.endswith('nosorting.benchmark'):
                subset = os.path.split(subdir)[1]
                # Run the benchmark file to measure the checkpointing speed
                average_run_time = run_benchmark_file(file, subset)
                # # Load the data again but checkpoint to a file and check size
                file_size = run_checkpoint(subdir, subset, rle_sorting='false')

                write_to_csv(f"{subset}, {average_run_time}, {file_size}, false", mode="a")


run_benchmark()
